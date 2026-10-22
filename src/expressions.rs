use std::collections::HashMap;
use std::ops::{Neg, Not};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr_fn::{binary_expr, cast, try_cast, when};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::lit;

use crate::ast::{self, BinaryOp, ExprKind, ScalarType, Span, UnaryOp};
use crate::error::{Error, Result};
use crate::literals::{data_type, literal_value};

pub(crate) struct Expressions<'a> {
    pub state: &'a SessionState,
    pub scalars: &'a HashMap<String, Expr>,
    pub now: i64,
}

impl Expressions<'_> {
    pub fn scalar(&self, expr: &ast::Expr, schema: &DFSchema) -> Result<Expr> {
        let result = self.lower(expr, schema, false)?;
        check_expression(&result)?;
        Ok(result)
    }

    pub fn predicate(&self, expr: &ast::Expr, schema: &DFSchema) -> Result<Expr> {
        let result = self.lower(expr, schema, true)?;
        check_expression(&result)?;
        Ok(result)
    }

    pub fn function(&self, name: &str, args: Vec<Expr>) -> Result<Expr> {
        Ok(self.state.udf(name)?.call(args))
    }

    pub fn constant(&self, expr: Expr, span: Span) -> Result<ScalarValue> {
        check_expression(&expr)?;
        let context = SimplifyContext::builder()
            .with_config_options(Arc::clone(self.state.config_options()))
            .with_query_execution_start_time(
                self.state.execution_props().query_execution_start_time,
            )
            .build();
        let simplified = ExprSimplifier::new(context).simplify(expr)?;
        match simplified {
            Expr::Literal(value, _) => Ok(value),
            _ => Err(Error::unsupported(
                "this argument must be a constant scalar expression",
                span,
            )),
        }
    }

    fn lower(&self, expr: &ast::Expr, schema: &DFSchema, filter: bool) -> Result<Expr> {
        Ok(match &expr.kind {
            ExprKind::Literal(value) => lit(literal_value(value, expr.span)?),
            ExprKind::Identifier(parts) => {
                if parts.len() != 1 {
                    return Err(Error::unsupported(
                        "property access; use ['a.b'] for a column whose name contains a dot",
                        expr.span,
                    ));
                }
                let name = &parts[0];
                if let Some(index) = schema.index_of_column_by_name(None, name) {
                    Expr::Column(Column::from(schema.qualified_field(index)))
                } else if let Some(value) = self.scalars.get(name) {
                    value.clone()
                } else {
                    return Err(Error::Plan(format!(
                        "unknown column or scalar binding '{name}'"
                    )));
                }
            }
            ExprKind::Unary { op, expr } => {
                let value = self.lower(expr, schema, false)?;
                match op {
                    UnaryOp::Not => value.not(),
                    UnaryOp::Negate => {
                        if matches!(value.get_type(schema)?, DataType::Duration(_)) {
                            let ty = DataType::Duration(TimeUnit::Nanosecond);
                            cast(cast(cast(value, ty.clone()), DataType::Int64).neg(), ty)
                        } else {
                            value.neg()
                        }
                    }
                    UnaryOp::Plus => {
                        let ty = value.get_type(schema)?;
                        if !ty.is_numeric() && !matches!(ty, DataType::Duration(_) | DataType::Null)
                        {
                            return Err(Error::syntax(
                                "unary plus requires a number or timespan",
                                expr.span,
                            ));
                        }
                        value
                    }
                }
            }
            ExprKind::Binary { left, op, right } => {
                let propagate = filter && matches!(op, BinaryOp::And | BinaryOp::Or);
                let left = self.lower(left, schema, propagate)?;
                let right_expr = self.lower(right, schema, propagate)?;
                self.binary(left, *op, right_expr, right.span, schema, filter)?
            }
            ExprKind::Call { name, args } => self.call(name, args, expr.span, schema)?,
            ExprKind::InList {
                expr,
                values,
                negated,
                case_insensitive,
            } => {
                let value = self.lower(expr, schema, false)?;
                let mut contains_null = false;
                let mut list = Vec::with_capacity(values.len());
                for item in values {
                    let item_expr = self.lower(item, &DFSchema::empty(), false)?;
                    let scalar = self.constant(item_expr, item.span)?;
                    if scalar.is_null() {
                        contains_null = true;
                    } else {
                        let item = lit(scalar);
                        list.push(if *case_insensitive {
                            self.ascii_lower(item)?
                        } else {
                            item
                        });
                    }
                }
                let value = if *case_insensitive {
                    self.ascii_lower(value)?
                } else {
                    value
                };
                let result = if list.is_empty() {
                    if contains_null {
                        when(value.is_null(), lit(ScalarValue::Boolean(None)))
                            .otherwise(lit(false))?
                    } else {
                        lit(false)
                    }
                } else {
                    let result = value.in_list(list, false);
                    if contains_null || filter && !negated {
                        result
                    } else {
                        self.function("coalesce", vec![result, lit(false)])?
                    }
                };
                if *negated { result.not() } else { result }
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.lower(expr, schema, false)?;
                let low = self.lower(low, schema, false)?;
                let mut high = self.lower(high, schema, false)?;
                if matches!(value.get_type(schema)?, DataType::Timestamp(_, _))
                    && matches!(high.get_type(schema)?, DataType::Duration(_))
                {
                    high = self.arithmetic(low.clone(), BinaryOp::Add, high, schema, expr.span)?;
                }
                if *negated {
                    value.not_between(low, high)
                } else {
                    value.between(low, high)
                }
            }
            ExprKind::Wildcard => {
                return Err(Error::unsupported(
                    "wildcard in a scalar expression",
                    expr.span,
                ));
            }
        })
    }

    fn binary(
        &self,
        left: Expr,
        op: BinaryOp,
        right: Expr,
        right_span: Span,
        schema: &DFSchema,
        filter: bool,
    ) -> Result<Expr> {
        use BinaryOp as B;
        Ok(match op {
            B::And => left.and(right),
            B::Or => left.or(right),
            B::Eq | B::NotEq | B::EqInsensitive | B::NotEqInsensitive => {
                let insensitive = matches!(op, B::EqInsensitive | B::NotEqInsensitive);
                let negated = matches!(op, B::NotEq | B::NotEqInsensitive);
                let (left, right) = if insensitive {
                    (
                        self.function("lower", vec![left])?,
                        self.function("lower", vec![right])?,
                    )
                } else {
                    (left, right)
                };
                self.equality(left, right, negated, schema, filter)?
            }
            B::Lt => left.lt(right),
            B::LtEq => left.lt_eq(right),
            B::Gt => left.gt(right),
            B::GtEq => left.gt_eq(right),
            B::Add | B::Subtract | B::Multiply | B::Divide | B::Modulo => {
                self.arithmetic(left, op, right, schema, right_span)?
            }
            B::MatchesRegex => self.function("regexp_like", vec![left, right])?,
            B::Has | B::NotHas | B::HasCs | B::NotHasCs => {
                let constant = self.constant(right, right_span)?;
                let needle = constant.try_as_str().flatten().ok_or_else(|| {
                    Error::unsupported("has requires a constant string term", right_span)
                })?;
                if !needle.chars().all(char::is_alphanumeric) {
                    return Err(Error::unsupported(
                        "has currently requires a single alphanumeric term",
                        right_span,
                    ));
                }
                let pattern = if needle.is_empty() {
                    // An empty string is not a term.
                    String::from(r"\b\B")
                } else {
                    format!(
                        r"(?:^|[^\p{{L}}\p{{N}}]){}(?:$|[^\p{{L}}\p{{N}}])",
                        regex::escape(needle)
                    )
                };
                let mut args = vec![left, lit(pattern)];
                if matches!(op, B::Has | B::NotHas) {
                    args.push(lit("i"));
                }
                let result = self.function("regexp_like", args)?;
                if matches!(op, B::NotHas | B::NotHasCs) {
                    result.not()
                } else {
                    result
                }
            }
            _ => {
                let insensitive = matches!(
                    op,
                    B::Contains
                        | B::NotContains
                        | B::StartsWith
                        | B::NotStartsWith
                        | B::EndsWith
                        | B::NotEndsWith
                );
                let (left, right) = if insensitive {
                    (
                        self.function("lower", vec![left])?,
                        self.function("lower", vec![right])?,
                    )
                } else {
                    (left, right)
                };
                let function = match op {
                    B::Contains | B::NotContains | B::ContainsCs | B::NotContainsCs => "contains",
                    B::StartsWith | B::NotStartsWith | B::StartsWithCs | B::NotStartsWithCs => {
                        "starts_with"
                    }
                    B::EndsWith | B::NotEndsWith | B::EndsWithCs | B::NotEndsWithCs => "ends_with",
                    _ => unreachable!("all other binary operators are handled above"),
                };
                let result = self.function(function, vec![left, right])?;
                if matches!(
                    op,
                    B::NotContains
                        | B::NotContainsCs
                        | B::NotStartsWith
                        | B::NotStartsWithCs
                        | B::NotEndsWith
                        | B::NotEndsWithCs
                ) {
                    result.not()
                } else {
                    result
                }
            }
        })
    }

    fn equality(
        &self,
        left: Expr,
        right: Expr,
        negated: bool,
        schema: &DFSchema,
        filter: bool,
    ) -> Result<Expr> {
        let left_nullable = left.nullable(schema)?;
        let right_nullable = right.nullable(schema)?;
        let comparison = binary_expr(
            left.clone(),
            if negated {
                Operator::NotEq
            } else {
                Operator::Eq
            },
            right.clone(),
        );
        if !left_nullable && !right_nullable || filter && !negated {
            return Ok(comparison);
        }
        if !left_nullable || !right_nullable {
            let nullable = if left_nullable { left } else { right };
            return Ok(if negated {
                comparison.or(nullable.is_null())
            } else {
                comparison.and(nullable.is_not_null())
            });
        }
        // Kusto differs from SQL: one null compares false/true; two nulls
        // compare to bool(null), not to true/false.
        let both_null = left.clone().is_null().and(right.clone().is_null());
        Ok(
            when(both_null, lit(ScalarValue::Boolean(None))).otherwise(binary_expr(
                left,
                if negated {
                    Operator::IsDistinctFrom
                } else {
                    Operator::IsNotDistinctFrom
                },
                right,
            ))?,
        )
    }

    fn arithmetic(
        &self,
        left: Expr,
        op: BinaryOp,
        mut right: Expr,
        schema: &DFSchema,
        span: Span,
    ) -> Result<Expr> {
        let left_type = left.get_type(schema)?;
        let right_type = right.get_type(schema)?;
        let duration_type = DataType::Duration(TimeUnit::Nanosecond);
        if let DataType::Timestamp(_, zone) = &left_type {
            let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, zone.clone());
            let nanos = cast(cast(left.clone(), timestamp_type.clone()), DataType::Int64);
            if matches!(right_type, DataType::Duration(_))
                && matches!(op, BinaryOp::Add | BinaryOp::Subtract)
            {
                let offset = cast(cast(right, duration_type), DataType::Int64);
                let result = if op == BinaryOp::Add {
                    nanos + offset
                } else {
                    nanos - offset
                };
                return Ok(cast(result, timestamp_type));
            }
            if matches!(right_type, DataType::Timestamp(_, _)) && op == BinaryOp::Subtract {
                let other = cast(cast(right, timestamp_type), DataType::Int64);
                return Ok(cast(nanos - other, duration_type));
            }
        }
        if matches!(left_type, DataType::Duration(_)) {
            let nanos = cast(cast(left.clone(), duration_type.clone()), DataType::Int64);
            if matches!(right_type, DataType::Timestamp(_, _)) && op == BinaryOp::Add {
                return self.arithmetic(right, op, left, schema, span);
            }
            if matches!(right_type, DataType::Duration(_)) {
                let other = cast(cast(right, duration_type.clone()), DataType::Int64);
                return match op {
                    BinaryOp::Add => Ok(cast(nanos + other, duration_type)),
                    BinaryOp::Subtract => Ok(cast(nanos - other, duration_type)),
                    BinaryOp::Divide => {
                        Ok(cast(nanos, DataType::Float64) / cast(other, DataType::Float64))
                    }
                    _ => Err(Error::syntax("invalid timespan arithmetic", span)),
                };
            }
            if (right_type.is_integer() || right_type.is_floating())
                && matches!(op, BinaryOp::Multiply | BinaryOp::Divide)
            {
                if op == BinaryOp::Divide {
                    right = self.function("nullif", vec![right, cast(lit(0_i64), right_type)])?;
                }
                let result = if op == BinaryOp::Multiply {
                    nanos * right
                } else {
                    nanos / right
                };
                return Ok(cast(cast(result, DataType::Int64), duration_type));
            }
        }
        if (left_type.is_integer() || left_type.is_floating())
            && matches!(right_type, DataType::Duration(_))
            && op == BinaryOp::Multiply
        {
            return self.arithmetic(right, op, left, schema, span);
        }
        if right_type.is_integer() && matches!(op, BinaryOp::Divide | BinaryOp::Modulo) {
            right = self.function("nullif", vec![right, cast(lit(0_i64), right_type)])?;
        }
        Ok(match op {
            BinaryOp::Add => left + right,
            BinaryOp::Subtract => left - right,
            BinaryOp::Multiply => left * right,
            BinaryOp::Divide => left / right,
            BinaryOp::Modulo => left % right,
            _ => unreachable!("only arithmetic operators reach this function"),
        })
    }

    fn ascii_lower(&self, expr: Expr) -> Result<Expr> {
        self.function(
            "translate",
            vec![
                expr,
                lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                lit("abcdefghijklmnopqrstuvwxyz"),
            ],
        )
    }

    fn call(&self, name: &str, args: &[ast::Expr], span: Span, schema: &DFSchema) -> Result<Expr> {
        let values = args
            .iter()
            .map(|arg| self.lower(arg, schema, false))
            .collect::<Result<Vec<_>>>()?;
        let arity = |count| {
            if values.len() == count {
                Ok(())
            } else {
                Err(Error::syntax(
                    format!("{name} expects {count} argument(s)"),
                    span,
                ))
            }
        };
        Ok(match name {
            "now" | "ago" => {
                if name == "ago" {
                    arity(1)?;
                } else if values.len() > 1 {
                    return Err(Error::syntax("now expects zero or one argument", span));
                }
                let offset = if let Some(value) = values.first() {
                    let Some(offset) = self.duration_constant(value.clone(), span)? else {
                        return Ok(lit(ScalarValue::TimestampNanosecond(None, None)));
                    };
                    offset
                } else {
                    0
                };
                let instant = if name == "ago" {
                    self.now.checked_sub(offset)
                } else {
                    self.now.checked_add(offset)
                }
                .ok_or_else(|| Error::syntax("datetime arithmetic overflow", span))?;
                lit(ScalarValue::TimestampNanosecond(Some(instant), None))
            }
            "bin" | "floor" if values.len() == 2 => {
                self.bin(values[0].clone(), values[1].clone(), schema, span)?
            }
            "bin" => return Err(Error::syntax("bin expects two arguments", span)),
            "iff" | "iif" => {
                arity(3)?;
                when(values[0].clone(), values[1].clone()).otherwise(values[2].clone())?
            }
            "case" => {
                if values.len() < 3 || values.len().is_multiple_of(2) {
                    return Err(Error::syntax(
                        "case expects condition/value pairs followed by a default value",
                        span,
                    ));
                }
                let mut builder = when(values[0].clone(), values[1].clone());
                for pair in values[2..values.len() - 1].as_chunks::<2>().0 {
                    builder = builder.when(pair[0].clone(), pair[1].clone());
                }
                builder.otherwise(values.last().expect("case has a default").clone())?
            }
            "not" => {
                arity(1)?;
                values[0].clone().not()
            }
            "isnull" | "isnotnull" => {
                arity(1)?;
                if name == "isnull" {
                    values[0].clone().is_null()
                } else {
                    values[0].clone().is_not_null()
                }
            }
            "isempty" | "isnotempty" => {
                arity(1)?;
                let value = values[0].clone();
                let empty = value.clone().is_null().or(value.eq(lit("")));
                if name == "isnotempty" {
                    empty.not()
                } else {
                    empty
                }
            }
            "tostring" => {
                arity(1)?;
                self.function(
                    "coalesce",
                    vec![try_cast(values[0].clone(), DataType::Utf8), lit("")],
                )?
            }
            "tolong" | "toint" | "toreal" | "todouble" | "tobool" | "todatetime" => {
                arity(1)?;
                let ty = match name {
                    "tolong" => ScalarType::Long,
                    "toint" => ScalarType::Int,
                    "toreal" | "todouble" => ScalarType::Real,
                    "tobool" => ScalarType::Bool,
                    _ => ScalarType::Datetime,
                };
                try_cast(values[0].clone(), data_type(ty))
            }
            "bool" | "boolean" | "int" | "long" | "real" | "double" | "string" => {
                arity(1)?;
                let ty = data_type(ScalarType::parse(name).expect("known type constructor"));
                if name == "string" {
                    self.function("coalesce", vec![try_cast(values[0].clone(), ty), lit("")])?
                } else {
                    try_cast(values[0].clone(), ty)
                }
            }
            "coalesce" => {
                if values.is_empty() {
                    return Err(Error::syntax(
                        "coalesce requires at least one argument",
                        span,
                    ));
                }
                let strings = values
                    .iter()
                    .map(|v| v.get_type(schema))
                    .collect::<datafusion::error::Result<Vec<_>>>()?
                    .iter()
                    .any(is_string);
                if strings {
                    let mut nonempty = values
                        .into_iter()
                        .map(|v| self.function("nullif", vec![v, lit("")]))
                        .collect::<Result<Vec<_>>>()?;
                    nonempty.push(lit(""));
                    self.function("coalesce", nonempty)?
                } else {
                    self.function("coalesce", values)?
                }
            }
            "substring" => {
                if !(2..=3).contains(&values.len()) {
                    return Err(Error::syntax(
                        "substring expects two or three arguments",
                        span,
                    ));
                }
                let start =
                    self.function("greatest", vec![values[1].clone(), lit(0_i64)])? + lit(1_i64);
                let mut args = vec![values[0].clone(), start];
                if values.len() == 3 {
                    args.push(values[2].clone());
                }
                self.function("substr", args)?
            }
            "strlen" => {
                arity(1)?;
                cast(self.function("character_length", values)?, DataType::Int64)
            }
            "strcat" => self.function("concat", values)?,
            "tolower" => {
                arity(1)?;
                self.function("lower", values)?
            }
            "toupper" => {
                arity(1)?;
                self.function("upper", values)?
            }
            "replace_string" => {
                arity(3)?;
                self.function("replace", values)?
            }
            _ => {
                if matches!(
                    name,
                    "count"
                        | "countif"
                        | "dcount"
                        | "dcountif"
                        | "count_distinct"
                        | "sumif"
                        | "avgif"
                        | "minif"
                        | "maxif"
                        | "stdev"
                        | "variance"
                ) || self.state.aggregate_functions().contains_key(name)
                {
                    return Err(Error::unsupported(
                        "aggregation functions must appear in summarize",
                        span,
                    ));
                }
                let function =
                    self.state.scalar_functions().get(name).ok_or_else(|| {
                        Error::unsupported(format!("scalar function '{name}'"), span)
                    })?;
                function.call(values)
            }
        })
    }

    fn duration_constant(&self, expr: Expr, span: Span) -> Result<Option<i64>> {
        let value = self.constant(expr, span)?;
        if !matches!(value.data_type(), DataType::Duration(_)) {
            return Err(Error::syntax("expected a timespan argument", span));
        }
        match value.cast_to(&DataType::Duration(TimeUnit::Nanosecond))? {
            ScalarValue::DurationNanosecond(value) => Ok(value),
            _ => Err(Error::syntax("expected a timespan argument", span)),
        }
    }

    fn bin(&self, value: Expr, size: Expr, schema: &DFSchema, span: Span) -> Result<Expr> {
        let ty = value.get_type(schema)?;
        if let DataType::Timestamp(_, zone) = &ty {
            let output = DataType::Timestamp(TimeUnit::Nanosecond, zone.clone());
            let nanos = self.duration_constant(size, span)?;
            let Some(nanos) = nanos.filter(|n| *n > 0) else {
                return Ok(lit(ScalarValue::try_from(&output)?));
            };
            return self.function(
                "date_bin",
                vec![
                    lit(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNano::new(0, 0, nanos),
                    ))),
                    cast(value, output),
                    lit(ScalarValue::TimestampNanosecond(Some(0), None)),
                ],
            );
        }
        let duration = matches!(ty, DataType::Duration(_));
        let numeric = if duration {
            DataType::Int64
        } else {
            ty.clone()
        };
        if !matches!(
            numeric,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        ) {
            return Err(Error::unsupported("bin input type", span));
        }
        let value = if duration {
            cast(
                cast(value, DataType::Duration(TimeUnit::Nanosecond)),
                numeric.clone(),
            )
        } else {
            value
        };
        let size = if duration {
            let Some(size) = self.duration_constant(size, span)? else {
                return Ok(lit(ScalarValue::DurationNanosecond(None)));
            };
            lit(size)
        } else {
            try_cast(size, numeric.clone())
        };
        let zero = cast(lit(0_i64), numeric.clone());
        let size = when(size.clone().gt(zero.clone()), size)
            .otherwise(lit(ScalarValue::try_from(&numeric)?))?;
        let result = if numeric.is_integer() {
            let remainder = value.clone() % size.clone();
            let remainder =
                when(remainder.clone().lt(zero), remainder.clone() + size).otherwise(remainder)?;
            value - remainder
        } else {
            self.function("floor", vec![value / size.clone()])? * size
        };
        Ok(if duration {
            cast(result, DataType::Duration(TimeUnit::Nanosecond))
        } else {
            result
        })
    }
}

pub(crate) fn is_string(ty: &DataType) -> bool {
    match ty {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => is_string(value),
        _ => false,
    }
}

pub(crate) fn check_expression(expr: &Expr) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    fn visit(expr: &Expr, depth: usize, nodes: &mut usize) -> Result<()> {
        *nodes += 1;
        if depth > 128 || *nodes > 16_384 {
            return Err(Error::Plan(
                "expanded scalar expression exceeds planning limits".into(),
            ));
        }
        expr.apply_children(|child| {
            visit(child, depth + 1, nodes)
                .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    visit(expr, 0, &mut 0)
}
