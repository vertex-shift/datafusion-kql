use std::collections::HashSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr_fn::{cast, when};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::ast::{self, ParseKind, ParsePart, ScalarType, Span};
use crate::expressions::Expressions;
use crate::literals::data_type;
use crate::{Error, Result};

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    input: LogicalPlan,
    operator: &ast::SpannedOperator,
    temporary: String,
) -> Result<LogicalPlan> {
    let ast::Operator::Parse {
        input: value,
        kind,
        flags,
        pattern,
        filter,
    } = &operator.kind
    else {
        return Err(Error::Plan("invalid parse operator".into()));
    };
    let mut regex = if *kind == ParseKind::Regex {
        String::new()
    } else {
        "(?s)".to_owned()
    };
    if !flags.is_empty() {
        if !flags
            .chars()
            .all(|flag| matches!(flag, 'i' | 'm' | 's' | 'U' | 'x' | 'u' | 'R'))
        {
            return Err(Error::syntax("unsupported regex flag", operator.span));
        }
        regex.push_str(&format!("(?{flags})"));
    }
    regex.push('^');
    let mut fields = Vec::new();
    let mut captures = Vec::new();
    let mut previous = None;
    for part in pattern {
        match part {
            ParsePart::Literal(text) => {
                regex.push_str(&if *kind == ParseKind::Regex {
                    format!("(?:{text})")
                } else {
                    regex::escape(text)
                });
                previous = None;
            }
            ParsePart::Wildcard => {
                if previous == Some(ScalarType::String) {
                    return Err(Error::syntax(
                        "a parse wildcard cannot immediately follow a string capture",
                        operator.span,
                    ));
                }
                regex.push_str(".*?");
                previous = None;
            }
            ParsePart::Capture { name, ty } => {
                if previous.is_some() {
                    return Err(Error::syntax(
                        "parse captures must be separated by delimiters",
                        operator.span,
                    ));
                }
                let capture = format!("__kql_capture_{}", fields.len());
                let inner = if *kind == ParseKind::Relaxed {
                    ".*?"
                } else {
                    match ty {
                        ScalarType::Long | ScalarType::Int => r"[-+]?[0-9]+",
                        ScalarType::Real => {
                            r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?"
                        }
                        ScalarType::Bool => "(?i:true|false)",
                        _ => ".*?",
                    }
                };
                regex.push_str(&format!("(?P<{capture}>{inner})"));
                fields.push((name.clone(), *ty));
                captures.push(capture);
                previous = Some(*ty);
            }
        }
    }
    regex.push('$');
    let source = expressions.scalar(value, input.schema())?;
    let source = cast(
        expressions.string_value(source, input.schema())?,
        DataType::Utf8,
    );
    let parsed =
        crate::regex_extract::parse(regex, captures, source, operator.span)?.alias(&temporary);
    let mut projection = (0..input.schema().fields().len())
        .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projection.push(parsed);
    let parsed = LogicalPlanBuilder::from(input.clone())
        .project(projection)?
        .build()?;
    let field = |name: String| {
        expressions.function(
            "get_field",
            vec![Expr::Column(Column::from_name(&temporary)), lit(name)],
        )
    };
    let mut valid = field("matched".into())?;
    let mut converted = Vec::new();
    for (index, (name, ty)) in fields.iter().enumerate() {
        let raw = field(format!("field_{index}"))?;
        let target = ast::Expr {
            kind: ast::ExprKind::Type(*ty),
            span: Span::default(),
            depth: 1,
        };
        let value = expressions.extracted_type(raw, Some(&target), operator.span)?;
        if *kind != ParseKind::Relaxed && *ty != ScalarType::String {
            valid = valid.and(value.clone().is_not_null());
        }
        converted.push((name, ty, value));
    }
    let names = fields
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<HashSet<_>>();
    let mut projection = input
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !names.contains(field.name().as_str()))
        .map(|(i, _)| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    for (name, ty, value) in converted {
        let value = if *kind == ParseKind::Relaxed {
            value
        } else {
            let null = if *ty == ScalarType::String {
                lit("")
            } else {
                lit(ScalarValue::try_from(&data_type(*ty))?)
            };
            when(valid.clone(), value).otherwise(null)?
        };
        projection.push(value.alias(name));
    }
    let parsed = if *filter {
        LogicalPlanBuilder::from(parsed).filter(valid)?.build()?
    } else {
        parsed
    };
    Ok(LogicalPlanBuilder::from(parsed)
        .project(projection)?
        .build()?)
}
