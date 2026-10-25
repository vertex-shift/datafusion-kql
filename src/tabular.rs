use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::ast::{self, UnionKind};
use crate::{Error, Result, dynamic, ordering};

pub(crate) fn canonical(ty: &DataType) -> DataType {
    if dynamic::is_dynamic(ty) {
        return dynamic::data_type();
    }
    if crate::decimal::is_decimal(ty) {
        return crate::decimal::data_type();
    }
    match ty {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Int8 | DataType::Int16 => DataType::Int32,
        DataType::Float16 | DataType::Float32 => DataType::Float64,
        DataType::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Duration(_) => DataType::Duration(TimeUnit::Nanosecond),
        ty => ty.clone(),
    }
}

pub(crate) fn type_names(ty: &DataType) -> (String, String) {
    if crate::decimal::is_decimal(ty) {
        return ("decimal".into(), "System.Decimal".into());
    }
    let (name, system) = match ty {
        DataType::Dictionary(_, value) => return type_names(value),
        DataType::Boolean => ("bool", "System.Boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ("int", "System.Int32"),
        DataType::Int64 => ("long", "System.Int64"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ("real", "System.Double"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ("string", "System.String"),
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
            ("datetime", "System.DateTime")
        }
        DataType::Duration(_) => ("timespan", "System.TimeSpan"),
        DataType::FixedSizeBinary(16) => ("guid", "System.Guid"),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => ("decimal", "System.Decimal"),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => ("dynamic", "System.Object"),
        ty if dynamic::is_dynamic(ty) => ("dynamic", "System.Object"),
        ty => return (ty.to_string().to_lowercase(), format!("Arrow.{ty}")),
    };
    (name.into(), system.into())
}

pub(crate) fn union(
    plans: Vec<LogicalPlan>,
    kind: UnionKind,
    withsource: Option<&str>,
    labels: &[String],
) -> Result<LogicalPlan> {
    let plans = plans
        .into_iter()
        .map(ordering::visible)
        .collect::<Result<Vec<_>>>()?;
    let mut pairs = Vec::<(String, DataType)>::new();
    let mut seen = HashSet::new();
    for plan in &plans {
        for field in plan.schema().fields() {
            let pair = (field.name().clone(), canonical(field.data_type()));
            if seen.insert(pair.clone()) {
                pairs.push(pair);
            }
        }
    }
    let mut types = HashMap::<String, usize>::new();
    for (name, _) in &pairs {
        *types.entry(name.clone()).or_default() += 1;
    }
    if kind == UnionKind::Inner {
        pairs.retain(|(name, ty)| {
            plans.iter().all(|plan| {
                plan.schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == name && canonical(field.data_type()) == *ty)
            })
        });
    }
    let mut reserved = pairs
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<HashSet<_>>();
    if let Some(source) = withsource {
        if reserved.contains(source) {
            return Err(Error::Plan(format!(
                "union withsource column '{source}' conflicts with an input column; rename it first"
            )));
        }
        reserved.insert(source.to_owned());
        if labels.len() != plans.len() {
            return Err(Error::Plan(
                "union source labels do not match its inputs".into(),
            ));
        }
    }
    let mut used = HashSet::new();
    let fields = pairs
        .into_iter()
        .map(|(original, ty)| {
            let mut name = if types[&original] > 1 {
                format!("{original}_{}", type_names(&ty).0)
            } else {
                original.clone()
            };
            let base = name.clone();
            let mut suffix = 1;
            while used.contains(&name) || (name != original && reserved.contains(&name)) {
                name = format!("{base}{suffix}");
                suffix += 1;
            }
            used.insert(name.clone());
            (original, name, ty)
        })
        .collect::<Vec<_>>();
    let mut aligned = Vec::new();
    for (leg, plan) in plans.into_iter().enumerate() {
        let mut projected = Vec::new();
        if let Some(source) = withsource {
            projected.push(lit(labels[leg].clone()).alias(source));
        }
        for (original, name, ty) in &fields {
            let index = plan
                .schema()
                .index_of_column_by_name(None, original)
                .filter(|index| canonical(plan.schema().field(*index).data_type()) == *ty);
            let expression = if let Some(index) = index {
                let value = Expr::Column(Column::from(plan.schema().qualified_field(index)));
                if dynamic::is_dynamic(ty) && plan.schema().field(index).data_type() != ty {
                    dynamic::call(dynamic::Kernel::IntoDynamic, vec![value])
                } else if crate::decimal::is_decimal(ty) {
                    crate::decimal::call(crate::decimal::Op::Convert, vec![value])
                } else {
                    cast(value, ty.clone())
                }
            } else if crate::expressions::is_string(ty) {
                cast(lit(""), ty.clone())
            } else {
                lit(ScalarValue::try_from(ty)?)
            };
            projected.push(expression.alias(name));
        }
        aligned.push(LogicalPlanBuilder::from(plan).project(projected)?.build()?);
    }
    let mut plans = aligned.into_iter();
    let mut result = LogicalPlanBuilder::from(
        plans
            .next()
            .ok_or_else(|| Error::Plan("union matched no input tables".into()))?,
    );
    for plan in plans {
        result = result.union(plan)?;
    }
    Ok(result.build()?)
}

pub(crate) fn schema(input: &LogicalPlan) -> Result<LogicalPlan> {
    let fields = input
        .schema()
        .fields()
        .iter()
        .filter(|f| !ordering::hidden(f))
        .collect::<Vec<_>>();
    let names = fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>();
    let types = fields
        .iter()
        .map(|f| type_names(f.data_type()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        ("ColumnName", Arc::new(StringArray::from(names)) as _),
        (
            "ColumnOrdinal",
            Arc::new(Int64Array::from_iter_values(0..fields.len() as i64)) as _,
        ),
        (
            "DataType",
            Arc::new(StringArray::from_iter_values(
                types.iter().map(|(_, system)| system.as_str()),
            )) as _,
        ),
        (
            "ColumnType",
            Arc::new(StringArray::from_iter_values(
                types.iter().map(|(name, _)| name.as_str()),
            )) as _,
        ),
    ])
    .map_err(datafusion::error::DataFusionError::from)?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    Ok(
        LogicalPlanBuilder::scan("__kql_getschema", provider_as_source(Arc::new(table)), None)?
            .build()?,
    )
}

pub(crate) fn reorder(input: LogicalPlan, columns: &[ast::ReorderColumn]) -> Result<LogicalPlan> {
    let mut selected = Vec::new();
    let mut used = HashSet::new();
    for column in columns {
        let mut matched = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                !ordering::hidden(field)
                    && crate::planner::pattern_matches(&column.pattern, field.name())
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if matched.is_empty() && !column.pattern.contains('*') {
            return Err(Error::Plan(format!(
                "unknown reorder column '{}'",
                column.pattern
            )));
        }
        if let Some((ascending, natural)) = column.order {
            matched.sort_by(|a, b| {
                let (a, b) = (
                    input.schema().field(*a).name(),
                    input.schema().field(*b).name(),
                );
                let order = if natural {
                    natural_compare(a, b)
                } else {
                    a.cmp(b)
                };
                if ascending { order } else { order.reverse() }
            });
        }
        for index in matched {
            if used.insert(index) {
                selected.push(index);
            }
        }
    }
    for index in 0..input.schema().fields().len() {
        if used.insert(index) {
            selected.push(index);
        }
    }
    let projected = selected
        .into_iter()
        .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(input)
        .project(projected)?
        .build()?)
}

fn natural_compare(left: &str, right: &str) -> Ordering {
    let (mut a, mut b) = (left.as_bytes(), right.as_bytes());
    while !a.is_empty() && !b.is_empty() {
        if a[0].is_ascii_digit() && b[0].is_ascii_digit() {
            let na = a.iter().take_while(|c| c.is_ascii_digit()).count();
            let nb = b.iter().take_while(|c| c.is_ascii_digit()).count();
            let da = &a[..na];
            let db = &b[..nb];
            let va = da
                .iter()
                .position(|c| *c != b'0')
                .map_or(&da[da.len()..], |i| &da[i..]);
            let vb = db
                .iter()
                .position(|c| *c != b'0')
                .map_or(&db[db.len()..], |i| &db[i..]);
            let order = va
                .len()
                .cmp(&vb.len())
                .then_with(|| va.cmp(vb))
                .then_with(|| na.cmp(&nb));
            if order != Ordering::Equal {
                return order;
            }
            a = &a[na..];
            b = &b[nb..];
        } else {
            let order = a[0].cmp(&b[0]);
            if order != Ordering::Equal {
                return order;
            }
            a = &a[1..];
            b = &b[1..];
        }
    }
    a.len().cmp(&b.len())
}

pub(crate) fn source_name(source: &ast::Source) -> String {
    match source {
        ast::Source::Table { name, .. } => name.join("."),
        ast::Source::Call { name, .. } => name.clone(),
        ast::Source::Pattern {
            prefix, pattern, ..
        } => prefix
            .iter()
            .chain(std::iter::once(pattern))
            .cloned()
            .collect::<Vec<_>>()
            .join("."),
        ast::Source::Subquery(query) => source_name(&query.pipeline.source),
        ast::Source::Print(_) => "print".into(),
        ast::Source::Datatable { .. } => "datatable".into(),
        ast::Source::Range { .. } => "range".into(),
        ast::Source::Union { .. } => "union".into(),
        ast::Source::Search { find, .. } => if find.is_some() { "find" } else { "search" }.into(),
    }
}
