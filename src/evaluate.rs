use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, DataFusionError, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use crate::ast::{self, ExprKind, NamedExpr, OutputSchema, Span};
use crate::dynamic::{self, Kernel};
use crate::expressions::{Expressions, is_string};
use crate::{Error, Result, ordering};

fn field(input: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(Column::from(input.schema().qualified_field(index)))
}
fn named(input: &LogicalPlan, name: &str) -> Result<Expr> {
    Ok(field(
        input,
        input
            .schema()
            .index_of_column_by_name(None, name)
            .ok_or_else(|| Error::Plan(format!("unknown plugin column '{name}'")))?,
    ))
}
fn column(expr: &ast::Expr) -> Result<&str> {
    if let ExprKind::Identifier(parts) = &expr.kind
        && parts.len() == 1
    {
        return Ok(&parts[0]);
    }
    Err(Error::syntax(
        "plugin expects a column name; use extend for expressions",
        expr.span,
    ))
}
fn ast_column(name: &str) -> ast::Expr {
    ast::Expr {
        kind: ExprKind::Identifier(vec![name.into()]),
        span: Span::default(),
        depth: 1,
    }
}
fn string(expressions: &Expressions<'_>, expr: &ast::Expr) -> Result<String> {
    expressions.constant_string(expressions.scalar(expr, &DFSchema::empty())?, expr.span)
}
fn arguments<'a>(args: &'a [ast::Expr], parameters: &[&str]) -> Result<Vec<Option<&'a ast::Expr>>> {
    let mut output = vec![None; parameters.len()];
    let mut named = false;
    for (i, arg) in args.iter().enumerate() {
        let (index, value) = if let ExprKind::NamedArgument { name, value } = &arg.kind {
            named = true;
            (
                parameters.iter().position(|p| p == name).ok_or_else(|| {
                    Error::syntax(format!("unknown plugin argument '{name}'"), arg.span)
                })?,
                value.as_ref(),
            )
        } else {
            if named {
                return Err(Error::syntax(
                    "positional plugin arguments must precede named arguments",
                    arg.span,
                ));
            }
            (i, arg)
        };
        let slot = output
            .get_mut(index)
            .ok_or_else(|| Error::syntax("too many plugin arguments", arg.span))?;
        if slot.replace(value).is_some() {
            return Err(Error::syntax("duplicate plugin argument", arg.span));
        }
    }
    Ok(output)
}

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    name: &str,
    args: &[ast::Expr],
    schema: Option<&OutputSchema>,
    temporary: String,
    span: Span,
) -> Result<LogicalPlan> {
    if !matches!(name, "bag_unpack" | "pivot") {
        return Err(Error::unsupported(
            format!("evaluate plugin '{name}'"),
            span,
        ));
    }
    let schema=schema.ok_or_else(||Error::unsupported(format!("evaluate {name} requires an explicit : (OutputSchema); implicit data-dependent schema discovery is not performed"),span))?;
    match name {
        "bag_unpack" => unpack(expressions, input, args, schema, temporary, span),
        "pivot" => pivot(expressions, input, args, schema, temporary, span),
        _ => Err(Error::unsupported(
            format!("evaluate plugin '{name}'"),
            span,
        )),
    }
}

fn unpack(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    args: &[ast::Expr],
    schema: &OutputSchema,
    temporary: String,
    span: Span,
) -> Result<LogicalPlan> {
    let args = arguments(
        args,
        &[
            "Column",
            "OutputColumnPrefix",
            "columnsConflict",
            "ignoredProperties",
        ],
    )?;
    let source =
        args[0].ok_or_else(|| Error::syntax("bag_unpack requires a dynamic column", span))?;
    let source_name = column(source)?;
    let source_index = input
        .schema()
        .index_of_column_by_name(None, source_name)
        .ok_or_else(|| Error::syntax(format!("unknown bag column '{source_name}'"), source.span))?;
    if !dynamic::is_dynamic(input.schema().field(source_index).data_type()) {
        return Err(Error::syntax(
            "bag_unpack requires a dynamic column",
            source.span,
        ));
    }
    let prefix = args[1]
        .map(|arg| string(expressions, arg))
        .transpose()?
        .unwrap_or_default();
    let conflict = args[2]
        .map(|arg| string(expressions, arg))
        .transpose()?
        .unwrap_or_else(|| "error".into());
    if !matches!(
        conflict.as_str(),
        "error" | "keep_source" | "replace_source"
    ) {
        return Err(Error::syntax(
            "columnsConflict must be error, keep_source, or replace_source",
            span,
        ));
    }
    let ignored = if let Some(arg) = args[3] {
        let value = expressions.scalar(arg, &DFSchema::empty())?;
        let value = dynamic::call(Kernel::EncodeJson, vec![value]);
        let text = expressions.constant_string(value, arg.span)?;
        serde_json::from_str::<Vec<String>>(&text)
            .map_err(|_| {
                Error::syntax(
                    "ignoredProperties must be a constant array of strings",
                    arg.span,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let keys = schema
        .columns
        .iter()
        .map(|(name, _)| {
            name.strip_prefix(&prefix)
                .map(str::to_owned)
                .ok_or_else(|| {
                    Error::syntax(
                        format!("unpacked output '{name}' must start with prefix '{prefix}'"),
                        span,
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut projection = (0..input.schema().fields().len())
        .map(|i| field(&input, i))
        .collect::<Vec<_>>();
    if !keys.is_empty() {
        projection.push(ordering::helper(
            dynamic::call(
                Kernel::Unpack { keys: keys.clone() },
                vec![field(&input, source_index)],
            ),
            temporary.clone(),
            false,
        ));
    }
    let input = LogicalPlanBuilder::from(input)
        .project(projection)?
        .build()?;
    let mut projection = Vec::<Expr>::new();
    let mut outputs = HashMap::new();
    if schema.include_input {
        for (i, field_) in input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, field_)| *i != source_index && !ordering::hidden(field_))
        {
            outputs.insert(field_.name().clone(), projection.len());
            projection.push(field(&input, i));
        }
    }
    for (index, ((name, ty), key)) in schema.columns.iter().zip(keys).enumerate() {
        let existing = input
            .schema()
            .index_of_column_by_name(None, name)
            .filter(|&i| i != source_index && !ordering::hidden(input.schema().field(i)));
        if existing.is_some() && conflict == "error" && !ignored.contains(&key) {
            return Err(Error::syntax(
                format!("bag_unpack output '{name}' conflicts with an input column"),
                span,
            ));
        }
        let ty = crate::literals::data_type(*ty);
        let value = if let Some(i) =
            existing.filter(|_| conflict == "keep_source" || ignored.contains(&key))
        {
            datafusion::logical_expr::expr_fn::try_cast(field(&input, i), ty.clone())
        } else if ignored.contains(&key) {
            lit(ScalarValue::try_from(&ty)?)
        } else {
            let value = expressions.function(
                "get_field",
                vec![
                    Expr::Column(Column::from_name(&temporary)),
                    lit(format!("field{index}")),
                ],
            )?;
            if dynamic::is_dynamic(&ty) {
                value
            } else {
                dynamic::call(Kernel::TypeOnly(ty.clone()), vec![value])
            }
        };
        let value = if is_string(&ty) {
            expressions.function("coalesce", vec![value, lit("")])?
        } else {
            value
        };
        let value = ordering::user_alias(value, name.clone(), input.schema())?;
        if let Some(&index) = outputs.get(name) {
            projection[index] = value;
        } else {
            outputs.insert(name.clone(), projection.len());
            projection.push(value);
        }
    }
    if let Some(order) = ordering::ordinal(input.schema()) {
        projection.push(order);
    }
    Ok(LogicalPlanBuilder::from(input)
        .project(projection)?
        .build()?)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PivotValues {
    values: Vec<ScalarValue>,
    signature: Signature,
}

impl ScalarUDFImpl for PivotValues {
    fn name(&self) -> &str {
        "__kql_pivot_values"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let array = args.args[0].to_array(1)?;
        if let Some(strings) = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
        {
            let allowed = self
                .values
                .iter()
                .filter_map(|value| value.try_as_str().flatten())
                .collect::<HashSet<_>>();
            for value in strings.iter() {
                if value.is_none_or(|value| !allowed.contains(value)) {
                    return Err(DataFusionError::Execution(format!(
                        "pivot value {value:?} is not declared in OutputSchema"
                    )));
                }
            }
            return Ok(args.args[0].clone());
        }
        let allowed = self.values.iter().cloned().collect::<HashSet<_>>();
        for row in 0..array.len() {
            let value = ScalarValue::try_from_array(&array, row)?;
            if !allowed.contains(&value) {
                return Err(DataFusionError::Execution(format!(
                    "pivot value {value} is not declared in OutputSchema"
                )));
            }
        }
        Ok(args.args[0].clone())
    }
}

fn pivot(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    args: &[ast::Expr],
    schema: &OutputSchema,
    temporary: String,
    span: Span,
) -> Result<LogicalPlan> {
    if schema.include_input {
        return Err(Error::syntax(
            "pivot requires a complete output schema, not '*'",
            span,
        ));
    }
    let Some(first) = args.first() else {
        return Err(Error::syntax("pivot requires a pivot column", span));
    };
    let pivot_name = column(first)?;
    let pivot_expr = named(&input, pivot_name)?;
    let pivot_ty = crate::tabular::canonical(&pivot_expr.get_type(input.schema())?);
    if dynamic::is_dynamic(&pivot_ty) || pivot_ty.is_nested() {
        return Err(Error::unsupported(
            "pivot keys must be scalar values",
            first.span,
        ));
    }
    let default = ast::Expr {
        kind: ExprKind::Call {
            name: "count".into(),
            args: Vec::new(),
        },
        span,
        depth: 1,
    };
    let (aggregate, start) = if let Some(arg) = args.get(1)
        && matches!(arg.kind, ExprKind::Call { .. })
    {
        (arg, 2)
    } else {
        (&default, 1)
    };
    let ExprKind::Call {
        name: aggregate_name,
        args: aggregate_args,
    } = &aggregate.kind
    else {
        return Err(Error::syntax(
            "pivot expects an aggregate call",
            aggregate.span,
        ));
    };
    let mut used = HashSet::from([pivot_name.to_owned()]);
    fn referenced(expr: &ast::Expr, used: &mut HashSet<String>) {
        if let ExprKind::Identifier(parts) = &expr.kind {
            used.insert(parts[0].clone());
        }
        expr.map_children(|child| {
            referenced(child, used);
            Ok(child.clone())
        })
        .expect("pivot reference inspection does not fail");
    }
    referenced(aggregate, &mut used);
    let groups = if args.len() > start {
        args[start..]
            .iter()
            .map(column)
            .map(|name| name.map(str::to_owned))
            .collect::<Result<Vec<_>>>()?
    } else {
        input
            .schema()
            .fields()
            .iter()
            .filter(|field| !ordering::hidden(field) && !used.contains(field.name()))
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    let group_set = groups.iter().cloned().collect::<HashSet<_>>();
    if group_set.len() != groups.len() {
        return Err(Error::syntax("duplicate pivot grouping column", span));
    }
    for name in &groups {
        if schema.columns.iter().all(|(column, _)| column != name) {
            return Err(Error::syntax(
                format!("pivot grouping column '{name}' is missing from OutputSchema"),
                span,
            ));
        }
        named(&input, name)?;
    }
    let mut buckets = Vec::new();
    for (name, ty) in &schema.columns {
        if !group_set.contains(name) {
            let value = if pivot_ty == crate::guid::data_type() {
                let (bytes, _) = crate::guid::parse_prefix(name)
                    .filter(|(_, end)| *end == name.len())
                    .ok_or_else(|| Error::Plan(format!("invalid guid pivot output '{name}'")))?;
                ScalarValue::FixedSizeBinary(16, Some(bytes.to_vec()))
            } else {
                ScalarValue::try_from_string(name.clone(), &pivot_ty).map_err(|error| {
                    Error::Plan(format!("invalid pivot output '{name}': {error}"))
                })?
            };
            buckets.push((name.clone(), *ty, value));
        }
    }
    if buckets.is_empty() {
        return Err(Error::syntax(
            "pivot OutputSchema must declare at least one value column",
            span,
        ));
    }
    let validator = ScalarUDF::from(PivotValues {
        values: buckets.iter().map(|(_, _, value)| value.clone()).collect(),
        signature: Signature::exact(vec![pivot_ty.clone()], Volatility::Immutable),
    });
    let pivot_expr = datafusion::logical_expr::expr_fn::cast(pivot_expr, pivot_ty);
    let mut projection = (0..input.schema().fields().len())
        .map(|i| field(&input, i))
        .collect::<Vec<_>>();
    projection.push(ordering::helper(
        validator.call(vec![pivot_expr]),
        temporary.clone(),
        false,
    ));
    let input = LogicalPlanBuilder::from(input)
        .project(projection)?
        .build()?;
    let mut locals = expressions.scalars.clone();
    let mut aggregates = Vec::new();
    for (index, (name, _, value)) in buckets.iter().enumerate() {
        let key = format!("{temporary}_value_{index}");
        locals.insert(key.clone(), lit(value.clone()));
        let condition = ast::Expr {
            kind: ExprKind::Binary {
                left: Box::new(ast_column(&temporary)),
                op: ast::BinaryOp::Eq,
                right: Box::new(ast::Expr {
                    kind: ExprKind::BindingReference(key),
                    span,
                    depth: 1,
                }),
            },
            span,
            depth: 2,
        };
        let (name_, args_) = match aggregate_name.as_str() {
            "count" if aggregate_args.len() <= 1 => ("countif", vec![condition]),
            "dcount" if (1..=2).contains(&aggregate_args.len()) => {
                let mut values = vec![aggregate_args[0].clone(), condition];
                values.extend_from_slice(&aggregate_args[1..]);
                ("dcountif", values)
            }
            name @ ("sum" | "min" | "max" | "avg" | "stdev" | "variance" | "take_any"
            | "make_list" | "make_set" | "make_bag")
                if aggregate_args.len() == 1 =>
            {
                let mapped = match name {
                    "sum" => "sumif",
                    "min" => "minif",
                    "max" => "maxif",
                    "avg" => "avgif",
                    "stdev" => "stdevif",
                    "variance" => "varianceif",
                    "take_any" => "take_anyif",
                    "make_list" => "make_list_if",
                    "make_set" => "make_set_if",
                    _ => "make_bag_if",
                };
                (mapped, vec![aggregate_args[0].clone(), condition])
            }
            _ => {
                return Err(Error::unsupported(
                    format!(
                        "pivot aggregate '{aggregate_name}' with {} arguments",
                        aggregate_args.len()
                    ),
                    aggregate.span,
                ));
            }
        };
        aggregates.push(NamedExpr {
            name: Some(name.clone()),
            expr: ast::Expr {
                kind: ExprKind::Call {
                    name: name_.into(),
                    args: args_,
                },
                span,
                depth: aggregate.depth + 3,
            },
        });
    }
    let groups = groups
        .iter()
        .map(|name| NamedExpr {
            name: Some(name.clone()),
            expr: ast_column(name),
        })
        .collect::<Vec<_>>();
    let result = crate::aggregation::plan(
        expressions.state,
        &locals,
        expressions.now,
        input,
        &aggregates,
        &groups,
    )?;
    let mut projection = Vec::new();
    for (name, ty) in &schema.columns {
        let column = named(&result, name)?;
        let actual = crate::tabular::canonical(&column.get_type(result.schema())?);
        let expected = crate::literals::data_type(*ty);
        if actual != expected {
            return Err(Error::syntax(
                format!("pivot output '{name}' is {actual}, not declared {expected}"),
                span,
            ));
        }
        projection.push(column);
    }
    Ok(LogicalPlanBuilder::from(result)
        .project(projection)?
        .build()?)
}
