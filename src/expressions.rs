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
use crate::dynamic::{self, Kernel};
use crate::error::{Error, Result};
use crate::literals::{data_type, literal_value};

pub(crate) struct Expressions<'a> {
    pub state: &'a SessionState,
    pub scalars: &'a HashMap<String, Expr>,
    pub now: i64,
}

pub(crate) fn parameter_cast(
    value: Expr,
    ty: ScalarType,
    schema: &DFSchema,
    span: Span,
) -> Result<Expr> {
    let actual = value.get_type(schema)?;
    let expected = data_type(ty);
    if ty == ScalarType::Dynamic {
        return Ok(dynamic::call(Kernel::IntoDynamic, vec![value]));
    }
    if ty == ScalarType::Decimal
        && (actual.is_numeric() || crate::decimal::is_decimal(&actual) || actual == DataType::Null)
    {
        return Ok(crate::decimal::call(
            crate::decimal::Op::Convert,
            vec![value],
        ));
    }
    let integer_literal = match &value {
        Expr::Literal(ScalarValue::Int64(value), _) => *value,
        Expr::Negative(expr) => match expr.as_ref() {
            Expr::Literal(ScalarValue::Int64(Some(value)), _) => value.checked_neg(),
            _ => None,
        },
        _ => None,
    };
    let compatible = actual == expected
        || actual == DataType::Null
        || expected == DataType::Int32
            && integer_literal.is_some_and(|value| i32::try_from(value).is_ok())
        || matches!(
            (&actual, &expected),
            (DataType::Int32, DataType::Int64 | DataType::Float64)
                | (DataType::Int64 | DataType::Float32, DataType::Float64)
                | (
                    DataType::Int8 | DataType::Int16,
                    DataType::Int32 | DataType::Int64 | DataType::Float64
                )
                | (DataType::LargeUtf8 | DataType::Utf8View, DataType::Utf8)
                | (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                | (DataType::Duration(_), DataType::Duration(_))
        );
    if !compatible {
        return Err(Error::syntax(
            format!("parameter requires {expected}, received {actual}"),
            span,
        ));
    }
    let value = if actual == expected {
        value
    } else {
        cast(value, expected)
    };
    if ty == ScalarType::String {
        Ok(datafusion::functions::core::expr_fn::coalesce(vec![
            value,
            lit(""),
        ]))
    } else {
        Ok(value)
    }
}

impl Expressions<'_> {
    fn decimal_operands(
        &self,
        values: Vec<Expr>,
        schema: &DFSchema,
        span: Span,
    ) -> Result<Vec<Expr>> {
        let types = values
            .iter()
            .map(|value| value.get_type(schema))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        if !types.iter().any(crate::decimal::is_decimal) {
            return Ok(values);
        }
        if types
            .iter()
            .any(|ty| !(ty.is_numeric() || crate::decimal::is_decimal(ty) || *ty == DataType::Null))
        {
            return Err(Error::syntax("decimal operands must be numeric", span));
        }
        Ok(values
            .into_iter()
            .zip(types)
            .map(|(value, ty)| {
                if ty == crate::decimal::data_type() {
                    value
                } else {
                    crate::decimal::call(crate::decimal::Op::Convert, vec![value])
                }
            })
            .collect())
    }

    fn decimal_results(
        &self,
        values: Vec<Expr>,
        schema: &DFSchema,
        span: Span,
    ) -> Result<Vec<Expr>> {
        let types = values
            .iter()
            .map(|value| value.get_type(schema))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        if types.iter().any(crate::decimal::is_decimal)
            && types
                .iter()
                .any(|ty| !crate::decimal::is_decimal(ty) && *ty != DataType::Null)
        {
            return Err(Error::syntax(
                "decimal conditional results must have matching types",
                span,
            ));
        }
        self.decimal_operands(values, schema, span)
    }

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
        let simplifier = ExprSimplifier::new(context);
        let expr = simplifier.coerce(expr, &DFSchema::empty())?;
        let simplified = simplifier.simplify(expr)?;
        match simplified {
            Expr::Literal(value, _) => Ok(value),
            _ => Err(Error::unsupported(
                "this argument must be a constant scalar expression",
                span,
            )),
        }
    }

    pub(crate) fn constant_string(&self, expr: Expr, span: Span) -> Result<String> {
        let value = self.constant(expr, span)?;
        value
            .try_as_str()
            .flatten()
            .map(str::to_owned)
            .ok_or_else(|| Error::syntax("expected a constant string", span))
    }

    fn lower(&self, expr: &ast::Expr, schema: &DFSchema, filter: bool) -> Result<Expr> {
        Ok(match &expr.kind {
            ExprKind::Literal(value) => lit(literal_value(value, expr.span)?),
            ExprKind::SearchTerm { .. } => {
                return Err(Error::unsupported(
                    "column:term syntax outside search/find",
                    expr.span,
                ));
            }
            ExprKind::ToScalar(_) | ExprKind::DeferredCall { .. } | ExprKind::InTabular { .. } => {
                return Err(Error::Plan(
                    "scalar subquery was not prepared by the planner".into(),
                ));
            }
            ExprKind::BindingReference(name) => self
                .scalars
                .get(name)
                .cloned()
                .ok_or_else(|| Error::Plan("function capture is no longer in scope".into()))?,
            ExprKind::Cast { expr, ty } => {
                let value = self.lower(expr, schema, false)?;
                parameter_cast(value, *ty, schema, expr.span)?
            }
            ExprKind::NamedArgument { .. } => {
                return Err(Error::syntax(
                    "named arguments are only supported for KQL-defined functions",
                    expr.span,
                ));
            }
            ExprKind::ColumnNames { .. } => {
                return Err(Error::unsupported(
                    "output-name lists require a top-level multi-output function in project, extend, serialize or print",
                    expr.span,
                ));
            }
            ExprKind::Identifier(parts) => {
                let name = &parts[0];
                let mut value = if let Some(index) = schema.index_of_column_by_name(None, name) {
                    Expr::Column(Column::from(schema.qualified_field(index)))
                } else if let Some(value) = self.scalars.get(name) {
                    value.clone()
                } else {
                    return Err(Error::Plan(format!(
                        "unknown column or scalar binding '{name}'"
                    )));
                };
                for property in &parts[1..] {
                    value = self.access(value, lit(property.clone()), schema)?;
                }

                value
            }
            ExprKind::Dynamic(value) => {
                let value = self.lower(value, schema, false)?;
                let value = dynamic::call(Kernel::IntoDynamic, vec![value]);
                self.constant(value, expr.span).map(lit)?
            }
            ExprKind::Array(items) => dynamic::call(
                Kernel::PackArray,
                items
                    .iter()
                    .map(|item| self.lower(item, schema, false))
                    .collect::<Result<Vec<_>>>()?,
            ),
            ExprKind::Object(items) => {
                let mut args = Vec::with_capacity(items.len() * 2);
                for (key, value) in items {
                    args.push(lit(key.clone()));
                    args.push(self.lower(value, schema, false)?);
                }
                dynamic::call(Kernel::BagPack, args)
            }
            ExprKind::Access { expr, index } => {
                let value = self.lower(expr, schema, false)?;
                let index = self.lower(index, schema, false)?;
                self.access(value, index, schema)?
            }
            ExprKind::Type(ty) => lit(format!("{ty:?}").to_lowercase()),
            ExprKind::Unary { op, expr } => {
                let value = self.lower(expr, schema, false)?;
                match op {
                    UnaryOp::Not => value.not(),
                    UnaryOp::Negate => {
                        if crate::decimal::is_decimal(&value.get_type(schema)?) {
                            crate::decimal::call(crate::decimal::Op::Negate, vec![value])
                        } else if matches!(value.get_type(schema)?, DataType::Duration(_)) {
                            let ty = DataType::Duration(TimeUnit::Nanosecond);
                            cast(cast(cast(value, ty.clone()), DataType::Int64).neg(), ty)
                        } else {
                            value.neg()
                        }
                    }
                    UnaryOp::Plus => {
                        let ty = value.get_type(schema)?;
                        if !ty.is_numeric()
                            && !crate::decimal::is_decimal(&ty)
                            && !matches!(ty, DataType::Duration(_) | DataType::Null)
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
                let mut value = self.lower(expr, schema, false)?;
                let mut lowered = values
                    .iter()
                    .map(|item| {
                        let value = self.lower(item, &DFSchema::empty(), false)?;
                        Ok(lit(self.constant(value, item.span)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if dynamic::is_dynamic(&value.get_type(schema)?)
                    || lowered
                        .iter()
                        .map(|v| v.get_type(&DFSchema::empty()))
                        .collect::<datafusion::error::Result<Vec<_>>>()?
                        .iter()
                        .any(dynamic::is_dynamic)
                {
                    let mut args = vec![value];
                    args.extend(lowered);
                    return Ok(dynamic::call(
                        Kernel::Membership {
                            negated: *negated,
                            insensitive: *case_insensitive,
                        },
                        args,
                    ));
                }
                let value_type = value.get_type(schema)?;
                for item in &lowered {
                    crate::guid::check_comparison(
                        &value_type,
                        &item.get_type(&DFSchema::empty())?,
                        expr.span,
                    )?;
                }
                let mut operands = vec![value];
                operands.extend(lowered);
                let mut operands = self
                    .decimal_operands(operands, schema, expr.span)?
                    .into_iter();
                value = operands.next().expect("membership has a left operand");
                lowered = operands
                    .map(|item| self.constant(item, expr.span).map(lit))
                    .collect::<Result<Vec<_>>>()?;
                if crate::decimal::is_decimal(&value.get_type(schema)?) {
                    if *case_insensitive {
                        return Err(Error::syntax(
                            "case-insensitive membership requires strings",
                            expr.span,
                        ));
                    }
                    let mut arguments = vec![value];
                    arguments.extend(lowered);
                    let result = crate::decimal::call(crate::decimal::Op::Contains, arguments);
                    return Ok(if *negated { result.not() } else { result });
                }
                let mut contains_null = false;
                let mut list = Vec::with_capacity(values.len());
                for item in lowered {
                    let Expr::Literal(scalar, _) = item else {
                        unreachable!("list values are literal");
                    };
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
            ExprKind::TermSet {
                expr: value,
                values,
                tabular,
                all,
            } => {
                if tabular.is_some() {
                    return Err(Error::Plan("tabular term search was not prepared".into()));
                }
                let value = self.lower(value, schema, false)?;
                let values = values
                    .iter()
                    .map(|value| self.lower(value, schema, false))
                    .collect::<Result<Vec<_>>>()?;
                crate::terms::plan(self, value, values, *all, schema, expr.span)?
            }
            ExprKind::InPrepared {
                expr,
                binding,
                negated,
                case_insensitive,
            } => {
                let Some(Expr::ScalarSubquery(subquery)) = self.scalars.get(binding) else {
                    return Err(Error::Plan(
                        "tabular membership query is no longer in scope".into(),
                    ));
                };
                let value = self.lower(expr, schema, false)?;
                if dynamic::is_dynamic(&value.get_type(schema)?)
                    || dynamic::is_dynamic(subquery.subquery.schema().field(0).data_type())
                {
                    return Err(Error::unsupported(
                        "cast dynamic tabular membership operands to a scalar type",
                        expr.span,
                    ));
                }
                let value = if *case_insensitive {
                    self.ascii_lower(value)?
                } else {
                    value
                };
                let raw = datafusion::logical_expr::expr_fn::in_subquery(
                    value.clone(),
                    Arc::clone(&subquery.subquery),
                );
                let fallback =
                    when(value.is_null(), lit(ScalarValue::Boolean(None))).otherwise(lit(false))?;
                let result = self.function("coalesce", vec![raw, fallback])?;
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
                let [value, low, high] = self
                    .decimal_operands(vec![value, low, high], schema, expr.span)?
                    .try_into()
                    .expect("between has three operands");
                if crate::decimal::is_decimal(&value.get_type(schema)?) {
                    let result =
                        crate::decimal::call(crate::decimal::Op::Between, vec![value, low, high]);
                    return Ok(if *negated { result.not() } else { result });
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
        if matches!(op, B::Eq | B::NotEq)
            && (dynamic::is_dynamic(&left.get_type(schema)?)
                || dynamic::is_dynamic(&right.get_type(schema)?))
        {
            return Ok(dynamic::call(
                Kernel::Equal(op == B::NotEq),
                vec![left, right],
            ));
        }
        let (left, right) = if matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq) {
            let [left, right] = self
                .decimal_operands(vec![left, right], schema, right_span)?
                .try_into()
                .expect("comparison has two operands");
            (left, right)
        } else {
            (left, right)
        };
        if matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq)
            && crate::decimal::is_decimal(&left.get_type(schema)?)
        {
            return Ok(crate::decimal::call(
                crate::decimal::Op::Compare(op),
                vec![left, right],
            ));
        }
        if matches!(
            op,
            B::Eq
                | B::NotEq
                | B::EqInsensitive
                | B::NotEqInsensitive
                | B::Lt
                | B::LtEq
                | B::Gt
                | B::GtEq
        ) {
            crate::guid::check_comparison(
                &left.get_type(schema)?,
                &right.get_type(schema)?,
                right_span,
            )?;
        }
        Ok(match op {
            B::And => left.and(right),
            B::Or => left.or(right),
            B::Eq | B::NotEq | B::EqInsensitive | B::NotEqInsensitive => {
                let insensitive = matches!(op, B::EqInsensitive | B::NotEqInsensitive);
                let negated = matches!(op, B::NotEq | B::NotEqInsensitive);
                let (left, right) = if insensitive {
                    (
                        self.function("lower", vec![self.string_value(left, schema)?])?,
                        self.function("lower", vec![self.string_value(right, schema)?])?,
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
            B::Has
            | B::NotHas
            | B::HasCs
            | B::NotHasCs
            | B::HasPrefix
            | B::NotHasPrefix
            | B::HasPrefixCs
            | B::NotHasPrefixCs
            | B::HasSuffix
            | B::NotHasSuffix
            | B::HasSuffixCs
            | B::NotHasSuffixCs => {
                let constant = self.constant(right, right_span)?;
                let needle = constant.try_as_str().flatten().ok_or_else(|| {
                    Error::unsupported("has requires a constant string term", right_span)
                })?;
                if matches!(op, B::Has | B::NotHas | B::HasCs | B::NotHasCs)
                    && !needle.chars().all(char::is_alphanumeric)
                {
                    return Err(Error::unsupported(
                        "has currently requires a single alphanumeric term",
                        right_span,
                    ));
                }
                let prefix = matches!(
                    op,
                    B::HasPrefix | B::NotHasPrefix | B::HasPrefixCs | B::NotHasPrefixCs
                );
                let suffix = matches!(
                    op,
                    B::HasSuffix | B::NotHasSuffix | B::HasSuffixCs | B::NotHasSuffixCs
                );
                let pattern = crate::terms::pattern(needle, !suffix, !prefix);
                let mut args = vec![self.string_value(left, schema)?, lit(pattern)];
                if matches!(
                    op,
                    B::Has
                        | B::NotHas
                        | B::HasPrefix
                        | B::NotHasPrefix
                        | B::HasSuffix
                        | B::NotHasSuffix
                ) {
                    args.push(lit("i"));
                }
                let result = self.function("regexp_like", args)?;
                if matches!(
                    op,
                    B::NotHas
                        | B::NotHasCs
                        | B::NotHasPrefix
                        | B::NotHasPrefixCs
                        | B::NotHasSuffix
                        | B::NotHasSuffixCs
                ) {
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
        if crate::decimal::is_decimal(&left_type) || crate::decimal::is_decimal(&right_type) {
            if !(left_type.is_numeric()
                || crate::decimal::is_decimal(&left_type)
                || left_type == DataType::Null)
                || !(right_type.is_numeric()
                    || crate::decimal::is_decimal(&right_type)
                    || right_type == DataType::Null)
            {
                return Err(Error::syntax(
                    "decimal arithmetic requires numeric operands",
                    span,
                ));
            }
            return Ok(crate::decimal::call(
                crate::decimal::arithmetic(op),
                vec![left, right],
            ));
        }
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
        if left_type.is_integer()
            && right_type.is_integer()
            && matches!(op, BinaryOp::Divide | BinaryOp::Modulo)
        {
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

    pub(crate) fn ascii_lower(&self, expr: Expr) -> Result<Expr> {
        self.function(
            "translate",
            vec![
                expr,
                lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                lit("abcdefghijklmnopqrstuvwxyz"),
            ],
        )
    }

    fn access(&self, value: Expr, index: Expr, schema: &DFSchema) -> Result<Expr> {
        if crate::decimal::is_decimal(&value.get_type(schema)?) {
            return Err(Error::Plan(
                "decimal values do not have properties or indices".into(),
            ));
        }
        match value.get_type(schema)? {
            ty if dynamic::is_dynamic(&ty) => Ok(dynamic::call(Kernel::Get, vec![value, index])),
            DataType::Struct(_) => self.function("get_field", vec![value, index]),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                let index = when(index.clone().lt(lit(0_i64)), index.clone())
                    .otherwise(index + lit(1_i64))?;
                self.function("array_element", vec![value, index])
            }
            ty if crate::arrow_dynamic::is_collection(&ty) => {
                Ok(dynamic::call(Kernel::Get, vec![value, index]))
            }
            ty => Err(Error::Plan(format!(
                "cannot subscript {ty}; parse JSON or use a dynamic value"
            ))),
        }
    }

    pub(crate) fn sketch_value(&self, value: Expr, schema: &DFSchema, span: Span) -> Result<Expr> {
        let ty = value.get_type(schema)?;
        if dynamic::is_dynamic(&ty) {
            Ok(dynamic::call(Kernel::EncodeJson, vec![value]))
        } else if is_string(&ty) || ty == DataType::Null {
            Ok(cast(value, DataType::Utf8))
        } else {
            Err(Error::syntax(
                "sketch functions require a sketch or its serialized JSON string",
                span,
            ))
        }
    }

    pub(crate) fn string_value(&self, value: Expr, schema: &DFSchema) -> Result<Expr> {
        Ok(
            if dynamic::is_dynamic(&value.get_type(schema)?)
                || crate::arrow_dynamic::is_collection(&value.get_type(schema)?)
            {
                dynamic::call(Kernel::Convert(DataType::Utf8), vec![value])
            } else if value.get_type(schema)? == DataType::Boolean {
                datafusion::logical_expr::expr_fn::case(value)
                    .when(lit(true), lit("True"))
                    .when(lit(false), lit("False"))
                    .otherwise(lit(ScalarValue::Utf8(None)))?
            } else if crate::decimal::is_decimal(&value.get_type(schema)?) {
                crate::decimal::call(crate::decimal::Op::Format, vec![value])
            } else if value.get_type(schema)? == crate::guid::data_type() {
                crate::guid::call(value, true)
            } else if matches!(value.get_type(schema)?, DataType::Duration(_)) {
                crate::text_functions::timespan(cast(
                    value,
                    DataType::Duration(TimeUnit::Nanosecond),
                ))
            } else {
                value
            },
        )
    }

    fn call(&self, name: &str, args: &[ast::Expr], span: Span, schema: &DFSchema) -> Result<Expr> {
        if crate::series_multi::Multi::lookup(name).is_some() {
            return Err(Error::unsupported(
                "multi-output series functions must be top-level projection expressions, not scalar arguments",
                span,
            ));
        }
        if matches!(name, "real" | "double") && args.len() == 1 {
            let constant = match &args[0].kind {
                ExprKind::Identifier(parts) if parts.len() == 1 => match parts[0].as_str() {
                    "nan" => Some(f64::NAN),
                    "inf" => Some(f64::INFINITY),
                    _ => None,
                },
                ExprKind::Unary {
                    op: UnaryOp::Negate,
                    expr,
                } if matches!(&expr.kind, ExprKind::Identifier(parts) if parts.as_slice() == ["inf"]) => {
                    Some(f64::NEG_INFINITY)
                }
                _ => None,
            };
            if let Some(value) = constant {
                return Ok(lit(value));
            }
        }
        if name == "column_ifexists" {
            if args.len() != 2 {
                return Err(Error::syntax(
                    "column_ifexists expects name and fallback",
                    span,
                ));
            }
            let name = self.constant_string(
                self.lower(&args[0], &DFSchema::empty(), false)?,
                args[0].span,
            )?;
            return if let Some(index) = schema.index_of_column_by_name(None, &name) {
                Ok(Expr::Column(Column::from(schema.qualified_field(index))))
            } else {
                self.lower(&args[1], schema, false)
            };
        }
        if name == "pack_all" {
            if args.len() > 1 {
                return Err(Error::syntax(
                    "pack_all expects an optional ignore-null/empty flag",
                    span,
                ));
            }
            let ignore_empty = if let Some(arg) = args.first() {
                match self.constant(self.lower(arg, &DFSchema::empty(), false)?, arg.span)? {
                    ScalarValue::Boolean(Some(value)) => value,
                    _ => {
                        return Err(Error::syntax(
                            "pack_all expects a constant boolean",
                            arg.span,
                        ));
                    }
                }
            } else {
                false
            };
            let fields = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| !crate::ordering::hidden(field))
                .collect::<Vec<_>>();
            return Ok(dynamic::call(
                Kernel::PackAll {
                    names: fields
                        .iter()
                        .map(|(_, field)| field.name().clone())
                        .collect(),
                    ignore_empty,
                },
                fields
                    .iter()
                    .map(|(i, _)| Expr::Column(Column::from(schema.qualified_field(*i))))
                    .collect(),
            ));
        }
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
        if matches!(
            name,
            "tdigest_merge"
                | "merge_tdigest"
                | "percentile_tdigest"
                | "percentiles_array_tdigest"
                | "percentile_array_tdigest"
        ) {
            let merge = matches!(name, "tdigest_merge" | "merge_tdigest");
            if merge {
                if !(2..=16).contains(&values.len()) {
                    return Err(Error::syntax(
                        "scalar tdigest_merge expects 2-16 sketches",
                        span,
                    ));
                }
                let values = values
                    .into_iter()
                    .zip(args)
                    .map(|(value, arg)| self.sketch_value(value, schema, arg.span))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(crate::tdigest_functions::scalar(
                    crate::tdigest_functions::Scalar::Merge,
                    values,
                ));
            }
            let array = name != "percentile_tdigest";
            if values.len() < 2 || !array && values.len() > 3 {
                return Err(Error::syntax(
                    "percentile_tdigest expects a sketch, rank and optional typeof",
                    span,
                ));
            }
            let sketch = self.sketch_value(values[0].clone(), schema, args[0].span)?;
            let mut end = values.len();
            let ty = if let Some(ast::Expr {
                kind: ExprKind::Type(ty),
                ..
            }) = args.last()
            {
                end -= 1;
                let ty = data_type(*ty);
                if crate::tdigest::Kind::from_type(&ty).is_none() {
                    return Err(Error::syntax(
                        "typed t-digest output requires int, long, real, datetime or timespan",
                        span,
                    ));
                }
                Some(ty)
            } else {
                None
            };
            if !array {
                if end != 2 {
                    return Err(Error::syntax(
                        "percentile_tdigest third argument must be typeof",
                        span,
                    ));
                }
                let rank = &values[1];
                if !rank.get_type(schema)?.is_numeric() && rank.get_type(schema)? != DataType::Null
                {
                    return Err(Error::syntax(
                        "percentile rank must be numeric",
                        args[1].span,
                    ));
                }
                return Ok(crate::tdigest_functions::scalar(
                    crate::tdigest_functions::Scalar::Percentile(ty),
                    vec![sketch, cast(rank.clone(), DataType::Float64)],
                ));
            }
            if !(2..=1025).contains(&end) {
                return Err(Error::syntax(
                    "t-digest requires 1-1024 percentile ranks",
                    span,
                ));
            }
            let ranks = if end == 2
                && (dynamic::is_dynamic(&values[1].get_type(schema)?)
                    || crate::arrow_dynamic::is_collection(&values[1].get_type(schema)?))
            {
                dynamic::call(Kernel::IntoDynamic, vec![values[1].clone()])
            } else {
                for (value, arg) in values[1..end].iter().zip(&args[1..end]) {
                    if !value.get_type(schema)?.is_numeric() {
                        return Err(Error::syntax("percentile ranks must be numeric", arg.span));
                    }
                }
                dynamic::call(Kernel::PackArray, values[1..end].to_vec())
            };
            return Ok(crate::tdigest_functions::scalar(
                crate::tdigest_functions::Scalar::Array(ty),
                vec![sketch, ranks],
            ));
        }
        if matches!(name, "indexof" | "strrep" | "replace_regex") {
            let text = |index: usize| -> Result<Expr> {
                let value = values[index].clone();
                self.function(
                    "coalesce",
                    vec![
                        cast(self.string_value(value, schema)?, DataType::Utf8),
                        lit(""),
                    ],
                )
            };
            let integer = |index: usize| -> Result<Expr> {
                let ty = values[index].get_type(schema)?;
                if !ty.is_integer() && ty != DataType::Null {
                    return Err(Error::syntax(
                        format!("{name} requires integer position/count arguments"),
                        args[index].span,
                    ));
                }
                Ok(cast(values[index].clone(), DataType::Int64))
            };
            return match name {
                "indexof" => {
                    if !(2..=5).contains(&values.len()) {
                        return Err(Error::syntax(
                            "indexof expects source, match, and optional start, length, occurrence",
                            span,
                        ));
                    }
                    let occurrence = if values.len() == 5 {
                        match self.constant(integer(4)?, args[4].span)? {
                            ScalarValue::Int64(Some(value)) if value > 0 => value,
                            _ => {
                                return Err(Error::syntax(
                                    "indexof occurrence must be a constant positive integer",
                                    args[4].span,
                                ));
                            }
                        }
                    } else {
                        1
                    };
                    Ok(crate::text_functions::indexof(
                        vec![
                            text(0)?,
                            text(1)?,
                            if values.len() > 2 {
                                integer(2)?
                            } else {
                                lit(0_i64)
                            },
                            if values.len() > 3 {
                                integer(3)?
                            } else {
                                lit(-1_i64)
                            },
                        ],
                        occurrence,
                    ))
                }
                "strrep" => {
                    if !(2..=3).contains(&values.len()) {
                        return Err(Error::syntax(
                            "strrep expects value, multiplier, and an optional delimiter",
                            span,
                        ));
                    }
                    Ok(crate::text_functions::strrep(vec![
                        text(0)?,
                        integer(1)?,
                        if values.len() == 3 { text(2)? } else { lit("") },
                    ]))
                }
                _ => {
                    arity(3)?;
                    let pattern = self.constant_string(values[1].clone(), args[1].span)?;
                    let replacement = self.constant_string(values[2].clone(), args[2].span)?;
                    crate::text_functions::replace_regex(text(0)?, pattern, replacement, span)
                }
            };
        }
        if matches!(name, "dcount_hll" | "hll_merge") {
            if name == "dcount_hll" {
                arity(1)?;
            } else if !(2..=64).contains(&values.len()) {
                return Err(Error::syntax(
                    "scalar hll_merge expects 2-64 sketches",
                    span,
                ));
            }
            let values = values
                .into_iter()
                .zip(args)
                .map(|(value, argument)| self.sketch_value(value, schema, argument.span))
                .collect::<Result<Vec<_>>>()?;
            return Ok(crate::hll_functions::scalar(
                if name == "dcount_hll" {
                    crate::hll_functions::Scalar::Count
                } else {
                    crate::hll_functions::Scalar::Merge
                },
                values,
            ));
        }
        if name == "gettype" {
            arity(1)?;
            let (type_name, _) = crate::tabular::type_names(&values[0].get_type(schema)?);
            if type_name != "dynamic" {
                return Ok(lit(type_name));
            }
        }
        if let Some(op) = crate::ip::Op::lookup(name) {
            return crate::ip::plan(self, op, values, schema, span);
        }
        if let Some(op) = crate::telemetry_text::Op::lookup(name) {
            arity(1)?;
            return Ok(crate::telemetry_text::call(
                op,
                cast(
                    self.string_value(values[0].clone(), schema)?,
                    DataType::Utf8,
                ),
            ));
        }
        if let Some(op) = crate::series_functions::Op::lookup(name) {
            return Ok(dynamic::call(Kernel::Series(op), values));
        }
        let kernel = match name {
            "parse_json" | "parsejson" | "todynamic" => Some(Kernel::ParseJson),
            "gettype" => Some(Kernel::GetType),
            "array_length" => Some(Kernel::ArrayLength),
            "pack_array" => Some(Kernel::PackArray),
            "bag_pack" | "pack" | "pack_dictionary" => Some(Kernel::BagPack),
            "bag_keys" => Some(Kernel::BagKeys),
            "bag_has_key" => Some(Kernel::BagHasKey),
            "bag_merge" => Some(Kernel::BagMerge),
            "bag_set_key" => Some(Kernel::BagSetKey),
            "bag_remove_keys" => Some(Kernel::BagRemoveKeys),
            "array_concat" => Some(Kernel::ArrayConcat),
            "array_slice" => Some(Kernel::ArraySlice),
            "array_index_of" => Some(Kernel::ArrayIndexOf),
            "set_has_element" => Some(Kernel::ArrayHas),
            "array_reverse" => Some(Kernel::ArrayReverse),
            "array_sum" => Some(Kernel::ArraySum),
            "set_union" => Some(Kernel::SetUnion),
            "set_intersect" => Some(Kernel::SetIntersect),
            "set_difference" => Some(Kernel::SetDifference),
            "zip" => Some(Kernel::Zip),
            "repeat" => Some(Kernel::Repeat),
            "range" => Some(Kernel::Range),
            "strcat_array" => Some(Kernel::StrcatArray),
            "split" => Some(Kernel::Split),
            _ => None,
        };
        if let Some(kernel) = kernel {
            return Ok(dynamic::call(kernel, values));
        }
        Ok(match name {
            "trim" | "trim_start" | "trim_end" => {
                arity(2)?;
                let pattern = self.constant_string(values[0].clone(), args[0].span)?;
                regex::Regex::new(&pattern).map_err(|error| {
                    Error::syntax(
                        format!("invalid trim regular expression: {error}"),
                        args[0].span,
                    )
                })?;
                let mut value = self.string_value(values[1].clone(), schema)?;
                if name != "trim_end" {
                    value = self.function(
                        "regexp_replace",
                        vec![value, lit(format!("^(?:{pattern})")), lit("")],
                    )?;
                }
                if name != "trim_start" {
                    value = self.function(
                        "regexp_replace",
                        vec![value, lit(format!("(?:{pattern})$")), lit("")],
                    )?;
                }
                value
            }
            "todecimal" => {
                arity(1)?;
                if dynamic::is_dynamic(&values[0].get_type(schema)?) {
                    dynamic::call(Kernel::Convert(crate::decimal::data_type()), values)
                } else {
                    crate::decimal::call(crate::decimal::Op::Convert, values)
                }
            }
            "toguid" => {
                arity(1)?;
                if dynamic::is_dynamic(&values[0].get_type(schema)?) {
                    dynamic::call(Kernel::Convert(crate::guid::data_type()), values)
                } else {
                    crate::guid::call(values[0].clone(), false)
                }
            }
            "new_guid" => {
                arity(0)?;
                crate::guid::call(
                    datafusion::functions::string::uuid().call(Vec::new()),
                    false,
                )
            }
            "format_datetime" | "format_timespan" => {
                arity(2)?;
                let pattern = self.constant_string(values[1].clone(), args[1].span)?;
                let duration = name == "format_timespan";
                let value = if duration {
                    cast(values[0].clone(), DataType::Duration(TimeUnit::Nanosecond))
                } else {
                    self.date_value(values[0].clone(), schema)?
                };
                crate::format_time::call(value, &pattern, duration, args[1].span)?
            }
            "getyear" | "getmonth" | "monthofyear" | "dayofmonth" | "dayofyear" | "hourofday"
            | "week_of_year" | "weekofyear" => {
                arity(1)?;
                let part = match name {
                    "getyear" => "year",
                    "getmonth" | "monthofyear" => "month",
                    "dayofmonth" => "day",
                    "dayofyear" => "doy",
                    "hourofday" => "hour",
                    _ => "week",
                };
                let value = self.date_value(values[0].clone(), schema)?;
                cast(
                    self.function("date_part", vec![lit(part), value])?,
                    DataType::Int32,
                )
            }
            "datetime_part" | "datepart" => {
                arity(2)?;
                let part = self
                    .constant_string(values[0].clone(), args[0].span)?
                    .to_ascii_lowercase();
                let part = match part.as_str() {
                    "week_of_year" | "weekofyear" => "week",
                    "dayofyear" => "doy",
                    part => part,
                }
                .to_owned();
                let value = self.date_value(values[1].clone(), schema)?;
                cast(
                    self.function("date_part", vec![lit(part), value])?,
                    DataType::Int32,
                )
            }
            "dayofweek" => {
                arity(1)?;
                let value = self.date_value(values[0].clone(), schema)?;
                let days = cast(
                    self.function("date_part", vec![lit("dow"), value])?,
                    DataType::Int64,
                );
                cast(
                    days * lit(86_400_000_000_000_i64),
                    DataType::Duration(TimeUnit::Nanosecond),
                )
            }
            "startofday" | "startofweek" | "startofmonth" | "startofyear" | "endofday"
            | "endofweek" | "endofmonth" | "endofyear" => {
                if !(1..=2).contains(&values.len()) {
                    return Err(Error::syntax(
                        "period boundary functions expect date and optional offset",
                        span,
                    ));
                }
                let unit = if name.ends_with("day") {
                    crate::calendar::Unit::Day
                } else if name.ends_with("week") {
                    crate::calendar::Unit::Week
                } else if name.ends_with("month") {
                    crate::calendar::Unit::Month
                } else {
                    crate::calendar::Unit::Year
                };
                let date = self.date_value(values[0].clone(), schema)?;
                let offset = if let Some(value) = values.get(1) {
                    cast(value.clone(), DataType::Int64)
                } else {
                    lit(0_i64)
                };
                let op = if name.starts_with("start") {
                    crate::calendar::Op::Start(unit)
                } else {
                    crate::calendar::Op::End(unit)
                };
                crate::calendar::call(op, vec![date, offset])
            }
            "datetime_add" | "datetime_diff" => {
                arity(3)?;
                let unit = self.constant_string(values[0].clone(), args[0].span)?;
                let unit = crate::calendar::Unit::parse(&unit)
                    .ok_or_else(|| Error::syntax("unknown datetime period", args[0].span))?;
                if name == "datetime_add" {
                    let date = self.date_value(values[2].clone(), schema)?;
                    crate::calendar::call(
                        crate::calendar::Op::Add(unit),
                        vec![date, cast(values[1].clone(), DataType::Int64)],
                    )
                } else {
                    let left = self.date_value(values[1].clone(), schema)?;
                    let right = self.date_value(values[2].clone(), schema)?;
                    crate::calendar::call(crate::calendar::Op::Diff(unit), vec![left, right])
                }
            }
            "datetime_to_unixtime" => {
                arity(1)?;
                crate::calendar::call(
                    crate::calendar::Op::ToUnix,
                    vec![self.date_value(values[0].clone(), schema)?],
                )
            }
            "unixtime_seconds_todatetime"
            | "unixtime_milliseconds_todatetime"
            | "unixtime_microseconds_todatetime"
            | "unixtime_nanoseconds_todatetime" => {
                arity(1)?;
                let unit = match name {
                    "unixtime_seconds_todatetime" => 1_000_000_000,
                    "unixtime_milliseconds_todatetime" => 1_000_000,
                    "unixtime_microseconds_todatetime" => 1_000,
                    _ => 1,
                };
                let ty = values[0].get_type(schema)?;
                if !ty.is_numeric() && ty != DataType::Null {
                    return Err(Error::syntax(
                        "Unix time requires a numeric value",
                        args[0].span,
                    ));
                }
                let value = cast(
                    values[0].clone(),
                    if ty.is_integer() {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    },
                );
                crate::calendar::call(crate::calendar::Op::FromUnix(unit), vec![value])
            }
            "make_datetime" => {
                if values.len() == 1 {
                    self.date_value(values[0].clone(), schema)?
                } else {
                    if !(3..=6).contains(&values.len()) {
                        return Err(Error::syntax(
                            "make_datetime expects 1 or 3-6 arguments",
                            span,
                        ));
                    }
                    let args = values
                        .into_iter()
                        .enumerate()
                        .map(|(i, value)| {
                            cast(
                                value,
                                if i == 5 {
                                    DataType::Float64
                                } else {
                                    DataType::Int64
                                },
                            )
                        })
                        .collect();
                    crate::calendar::call(crate::calendar::Op::MakeDatetime, args)
                }
            }
            "make_timespan" => {
                if values.len() == 1 {
                    let value = cast(
                        self.string_value(values[0].clone(), schema)?,
                        DataType::Utf8,
                    );
                    crate::temporal::parse(crate::temporal::Target::Timespan, value)
                } else {
                    if !(2..=4).contains(&values.len()) {
                        return Err(Error::syntax("make_timespan expects 1-4 arguments", span));
                    }
                    let length = values.len();
                    let args = values
                        .into_iter()
                        .enumerate()
                        .map(|(i, value)| {
                            cast(
                                value,
                                if length >= 3 && i == length - 1 {
                                    DataType::Float64
                                } else {
                                    DataType::Int64
                                },
                            )
                        })
                        .collect();
                    crate::calendar::call(crate::calendar::Op::MakeTimespan, args)
                }
            }
            "extract" => {
                if !(3..=4).contains(&values.len()) {
                    return Err(Error::syntax(
                        "extract expects regex, capture group, source, and optional typeof",
                        span,
                    ));
                }
                let pattern = self.constant_string(values[0].clone(), args[0].span)?;
                let compiled = regex::Regex::new(&pattern)
                    .map_err(|e| Error::syntax(format!("invalid regex: {e}"), args[0].span))?;
                let source = cast(
                    self.string_value(values[2].clone(), schema)?,
                    DataType::Utf8,
                );
                let captures =
                    self.function("regexp_match", vec![source, lit(format!("({pattern})"))])?;
                let index = cast(values[1].clone(), DataType::Int64);
                let valid = index
                    .clone()
                    .gt_eq(lit(0_i64))
                    .and(index.clone().lt(lit(compiled.captures_len() as i64)));
                let index =
                    when(valid, index).otherwise(lit(ScalarValue::Int64(None)))? + lit(1_i64);
                let extracted = self.function("array_element", vec![captures, index])?;
                self.extracted_type(extracted, args.get(3), span)?
            }
            "extract_all" | "extractall" => {
                if !(2..=3).contains(&values.len()) {
                    return Err(Error::syntax(
                        "extract_all expects regex, optional groups, and source",
                        span,
                    ));
                }
                let pattern = self.constant_string(values[0].clone(), args[0].span)?;
                let groups = if values.len() == 3 {
                    let json = self.constant_string(
                        dynamic::call(Kernel::EncodeJson, vec![values[1].clone()]),
                        args[1].span,
                    )?;
                    let groups: Vec<serde_json::Value> =
                        serde_json::from_str(&json).map_err(|_| {
                            Error::syntax("capture groups must be a dynamic array", args[1].span)
                        })?;
                    Some(
                        groups
                            .into_iter()
                            .map(|group| match group {
                                serde_json::Value::String(name) => {
                                    Ok(crate::regex_extract::Capture::Name(name))
                                }
                                value => value
                                    .as_u64()
                                    .and_then(|i| usize::try_from(i).ok())
                                    .map(crate::regex_extract::Capture::Index)
                                    .ok_or_else(|| {
                                        Error::syntax(
                                            "capture groups must be integer indices or names",
                                            args[1].span,
                                        )
                                    }),
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                } else {
                    None
                };
                let source = cast(
                    self.string_value(values[values.len() - 1].clone(), schema)?,
                    DataType::Utf8,
                );
                crate::regex_extract::extract_all(pattern, groups, source, span)?
            }
            "extract_json" | "extractjson" => {
                if !(2..=3).contains(&values.len()) {
                    return Err(Error::syntax(
                        "extract_json expects path, JSON text, and optional typeof",
                        span,
                    ));
                }
                let path = self.constant_string(values[0].clone(), args[0].span)?;
                let path = crate::json_path::parse(&path, args[0].span)?;
                if matches!(
                    args.get(2).map(|value| &value.kind),
                    Some(ExprKind::Type(ScalarType::Decimal))
                ) {
                    let source = cast(
                        self.string_value(values[1].clone(), schema)?,
                        DataType::Utf8,
                    );
                    return Ok(crate::decimal::extract_json(source, path));
                }
                let mut value = dynamic::call(Kernel::ParseJsonStrict, vec![values[1].clone()]);
                for segment in path {
                    let index = match segment {
                        crate::json_path::Segment::Key(key) => lit(key),
                        crate::json_path::Segment::Index(index) => lit(index),
                    };
                    value = dynamic::call(Kernel::Get, vec![value, index]);
                }
                let ty = args
                    .get(2)
                    .map(|arg| {
                        if let ExprKind::Type(ty) = arg.kind {
                            Ok(ty)
                        } else {
                            Err(Error::syntax("expected typeof(type)", arg.span))
                        }
                    })
                    .transpose()?
                    .unwrap_or(ScalarType::String);
                if ty == ScalarType::Dynamic {
                    value
                } else if ty == ScalarType::Datetime || ty == ScalarType::Timespan {
                    let text = dynamic::call(Kernel::Convert(DataType::Utf8), vec![value]);
                    self.extracted_type(text, args.get(2), span)?
                } else {
                    dynamic::call(Kernel::Convert(data_type(ty)), vec![value])
                }
            }
            "totimespan" | "totime" => {
                arity(1)?;
                let ty = values[0].get_type(schema)?;
                if dynamic::is_dynamic(&ty) {
                    dynamic::call(
                        Kernel::Convert(DataType::Duration(TimeUnit::Nanosecond)),
                        values,
                    )
                } else if matches!(ty, DataType::Duration(_)) {
                    cast(values[0].clone(), DataType::Duration(TimeUnit::Nanosecond))
                } else {
                    let text = cast(
                        self.string_value(values[0].clone(), schema)?,
                        DataType::Utf8,
                    );
                    crate::temporal::parse(crate::temporal::Target::Timespan, text)
                }
            }
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
            "bin_at" => {
                arity(3)?;
                self.bin_at(
                    values[0].clone(),
                    values[1].clone(),
                    values[2].clone(),
                    schema,
                    span,
                )?
            }
            "bin" => return Err(Error::syntax("bin expects two arguments", span)),
            "floor" | "ceiling" | "ceil"
                if values.len() == 1
                    && crate::decimal::is_decimal(&values[0].get_type(schema)?) =>
            {
                crate::decimal::call(
                    if name == "floor" {
                        crate::decimal::Op::Floor
                    } else {
                        crate::decimal::Op::Ceiling
                    },
                    values,
                )
            }
            "iff" | "iif" => {
                arity(3)?;
                let [yes, no] = self
                    .decimal_results(vec![values[1].clone(), values[2].clone()], schema, span)?
                    .try_into()
                    .expect("conditional has two result branches");
                when(values[0].clone(), yes).otherwise(no)?
            }
            "case" => {
                if values.len() < 3 || values.len().is_multiple_of(2) {
                    return Err(Error::syntax(
                        "case expects condition/value pairs followed by a default value",
                        span,
                    ));
                }
                let mut values = values;
                let indices = (1..values.len())
                    .filter(|i| i % 2 == 1 || *i == values.len() - 1)
                    .collect::<Vec<_>>();
                let results = self.decimal_results(
                    indices.iter().map(|i| values[*i].clone()).collect(),
                    schema,
                    span,
                )?;
                for (index, result) in indices.into_iter().zip(results) {
                    values[index] = result;
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
                    vec![
                        try_cast(
                            self.string_value(values[0].clone(), schema)?,
                            DataType::Utf8,
                        ),
                        lit(""),
                    ],
                )?
            }
            "todatetime" | "todate" => {
                arity(1)?;
                let ty = values[0].get_type(schema)?;
                if matches!(ty, DataType::Timestamp(_, _)) {
                    cast(values[0].clone(), data_type(ScalarType::Datetime))
                } else {
                    let text = cast(
                        self.string_value(values[0].clone(), schema)?,
                        DataType::Utf8,
                    );
                    crate::temporal::parse(crate::temporal::Target::Datetime, text)
                }
            }
            "tolong" | "toint" | "toreal" | "todouble" | "tobool" => {
                arity(1)?;
                let ty = match name {
                    "tolong" => ScalarType::Long,
                    "toint" => ScalarType::Int,
                    "toreal" | "todouble" => ScalarType::Real,
                    _ => ScalarType::Bool,
                };
                if dynamic::is_dynamic(&values[0].get_type(schema)?) {
                    return Ok(dynamic::call(Kernel::Convert(data_type(ty)), values));
                }
                if values[0].get_type(schema)? == crate::guid::data_type() {
                    return Ok(lit(ScalarValue::try_from(&data_type(ty))?));
                }
                if crate::decimal::is_decimal(&values[0].get_type(schema)?) {
                    return Ok(crate::decimal::cast(values[0].clone(), data_type(ty)));
                }
                try_cast(values[0].clone(), data_type(ty))
            }
            "bool" | "boolean" | "int" | "long" | "real" | "double" | "string" => {
                arity(1)?;
                let ty = data_type(ScalarType::parse(name).expect("known type constructor"));
                if dynamic::is_dynamic(&values[0].get_type(schema)?) {
                    return Ok(dynamic::call(Kernel::Convert(ty), values));
                }
                if name == "string" {
                    self.function(
                        "coalesce",
                        vec![
                            try_cast(self.string_value(values[0].clone(), schema)?, ty),
                            lit(""),
                        ],
                    )?
                } else {
                    if crate::decimal::is_decimal(&values[0].get_type(schema)?) {
                        crate::decimal::cast(values[0].clone(), ty)
                    } else {
                        try_cast(values[0].clone(), ty)
                    }
                }
            }
            "coalesce" => {
                if values.is_empty() {
                    return Err(Error::syntax(
                        "coalesce requires at least one argument",
                        span,
                    ));
                }
                if values
                    .iter()
                    .map(|v| v.get_type(schema))
                    .collect::<datafusion::error::Result<Vec<_>>>()?
                    .iter()
                    .any(dynamic::is_dynamic)
                {
                    return Ok(dynamic::call(Kernel::Coalesce, values));
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
                    self.function("coalesce", self.decimal_results(values, schema, span)?)?
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
            "strcat" => self.function(
                "concat",
                values
                    .into_iter()
                    .map(|value| self.string_value(value, schema))
                    .collect::<Result<Vec<_>>>()?,
            )?,
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
                if crate::aggregation::is_aggregate(self.state, name) {
                    return Err(Error::unsupported(
                        "aggregation functions must appear in summarize",
                        span,
                    ));
                }
                if name == "abs"
                    && values.len() == 1
                    && crate::decimal::is_decimal(&values[0].get_type(schema)?)
                {
                    return Ok(crate::decimal::call(crate::decimal::Op::Abs, values));
                }
                let values = if matches!(
                    name,
                    "sqrt"
                        | "exp"
                        | "ln"
                        | "log"
                        | "log2"
                        | "log10"
                        | "pow"
                        | "power"
                        | "sin"
                        | "cos"
                        | "tan"
                        | "asin"
                        | "acos"
                        | "atan"
                        | "atan2"
                        | "sinh"
                        | "cosh"
                        | "tanh"
                        | "asinh"
                        | "acosh"
                        | "atanh"
                        | "cot"
                        | "degrees"
                        | "radians"
                        | "isnan"
                        | "isinf"
                        | "isfinite"
                        | "round"
                        | "trunc"
                        | "signum"
                        | "sign"
                ) {
                    values
                        .into_iter()
                        .map(|value| {
                            Ok(if crate::decimal::is_decimal(&value.get_type(schema)?) {
                                crate::decimal::call(crate::decimal::Op::ImplicitReal, vec![value])
                            } else {
                                value
                            })
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    values
                };
                let native_name = if name == "sign" { "signum" } else { name };
                let function = self
                    .state
                    .scalar_functions()
                    .get(native_name)
                    .ok_or_else(|| Error::unsupported(format!("scalar function '{name}'"), span))?;
                function.call(values)
            }
        })
    }

    fn date_value(&self, value: Expr, schema: &DFSchema) -> Result<Expr> {
        let ty = value.get_type(schema)?;
        Ok(
            if matches!(
                ty,
                DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
            ) {
                cast(value, DataType::Timestamp(TimeUnit::Nanosecond, None))
            } else {
                let value = cast(self.string_value(value, schema)?, DataType::Utf8);
                crate::temporal::parse(crate::temporal::Target::Datetime, value)
            },
        )
    }

    pub(crate) fn extracted_type(
        &self,
        value: Expr,
        ty: Option<&ast::Expr>,
        span: Span,
    ) -> Result<Expr> {
        let ty = match ty {
            Some(ast::Expr {
                kind: ExprKind::Type(ty),
                ..
            }) => *ty,
            None => ScalarType::String,
            _ => return Err(Error::syntax("expected typeof(type)", span)),
        };
        Ok(match ty {
            ScalarType::String => self.function("coalesce", vec![value, lit("")])?,
            ScalarType::Datetime => {
                crate::temporal::parse(crate::temporal::Target::Datetime, value)
            }
            ScalarType::Timespan => {
                crate::temporal::parse(crate::temporal::Target::Timespan, value)
            }
            ScalarType::Guid => crate::guid::call(value, false),
            ScalarType::Decimal => crate::decimal::call(crate::decimal::Op::Convert, vec![value]),
            ScalarType::Dynamic => dynamic::call(Kernel::ParseJson, vec![value]),
            _ => try_cast(value, data_type(ty)),
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

    pub(crate) fn bin_at(
        &self,
        value: Expr,
        size: Expr,
        origin: Expr,
        schema: &DFSchema,
        span: Span,
    ) -> Result<Expr> {
        let ty = value.get_type(schema)?;
        if crate::decimal::is_decimal(&ty) {
            let size_type = size.get_type(schema)?;
            let origin_type = origin.get_type(schema)?;
            if !(size_type.is_numeric()
                || crate::decimal::is_decimal(&size_type)
                || size_type == DataType::Null)
                || !(origin_type.is_numeric()
                    || crate::decimal::is_decimal(&origin_type)
                    || origin_type == DataType::Null)
            {
                return Err(Error::syntax(
                    "decimal bin_at requires numeric size and origin",
                    span,
                ));
            }
            return Ok(crate::decimal::call(
                crate::decimal::Op::BinAt,
                vec![value, size, origin],
            ));
        }
        let temporal = matches!(ty, DataType::Timestamp(_, _) | DataType::Duration(_));
        let (value, origin, size, output) = if temporal {
            let output = if matches!(ty, DataType::Timestamp(_, _)) {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            } else {
                DataType::Duration(TimeUnit::Nanosecond)
            };
            let width = self.duration_constant(size, span)?;
            let Some(width) = width.filter(|w| *w > 0) else {
                return Ok(lit(ScalarValue::try_from(&output)?));
            };
            if matches!(ty, DataType::Timestamp(_, _)) && matches!(origin, Expr::Literal(_, _)) {
                return self.function(
                    "date_bin",
                    vec![
                        lit(ScalarValue::IntervalMonthDayNano(Some(
                            IntervalMonthDayNano::new(0, 0, width),
                        ))),
                        cast(value, output.clone()),
                        cast(origin, output),
                    ],
                );
            }
            (
                cast(cast(value, output.clone()), DataType::Int64),
                cast(cast(origin, output.clone()), DataType::Int64),
                lit(width),
                output,
            )
        } else {
            let types = [ty.clone(), size.get_type(schema)?, origin.get_type(schema)?];
            if types.iter().any(|ty| !ty.is_integer() && !ty.is_floating()) {
                return Err(Error::unsupported(
                    "bin_at requires numeric or temporal values",
                    span,
                ));
            }
            let output = if types.iter().any(DataType::is_floating) {
                DataType::Float64
            } else if types.contains(&DataType::Int64) {
                DataType::Int64
            } else {
                DataType::Int32
            };
            (
                cast(value, output.clone()),
                cast(origin, output.clone()),
                cast(size, output.clone()),
                output,
            )
        };
        if output.is_floating() {
            let size =
                when(size.clone().gt(lit(0.0)), size).otherwise(lit(ScalarValue::Float64(None)))?;
            return Ok(origin.clone()
                + self.function("floor", vec![(value - origin) / size.clone()])? * size);
        }
        let wide = DataType::Decimal128(38, 0);
        let (value, origin, size) = (
            cast(value, wide.clone()),
            cast(origin, wide.clone()),
            cast(size, wide.clone()),
        );
        let zero = cast(lit(0_i64), wide.clone());
        let size = when(size.clone().gt(zero.clone()), size)
            .otherwise(lit(ScalarValue::try_from(&wide)?))?;
        let difference = value - origin.clone();
        let remainder = difference.clone() % size.clone();
        let remainder =
            when(remainder.clone().lt(zero), remainder.clone() + size).otherwise(remainder)?;
        let result = origin + difference - remainder;
        Ok(if temporal {
            cast(try_cast(result, DataType::Int64), output)
        } else {
            try_cast(result, output)
        })
    }

    fn bin(&self, value: Expr, size: Expr, schema: &DFSchema, span: Span) -> Result<Expr> {
        let ty = value.get_type(schema)?;
        if crate::decimal::is_decimal(&ty) {
            if !size.get_type(schema)?.is_numeric()
                && !crate::decimal::is_decimal(&size.get_type(schema)?)
                && size.get_type(schema)? != DataType::Null
            {
                return Err(Error::syntax("decimal bin requires a numeric size", span));
            }
            return Ok(crate::decimal::call(
                crate::decimal::Op::Bin,
                vec![value, size],
            ));
        }
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
        } else if ty.is_floating() || size.get_type(schema)?.is_floating() {
            DataType::Float64
        } else if ty == DataType::Int64 || size.get_type(schema)? == DataType::Int64 {
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
            cast(value, numeric.clone())
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
