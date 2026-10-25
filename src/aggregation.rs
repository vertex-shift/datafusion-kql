use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr_fn::{ExprFunctionExt, cast, when};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::lit;

use crate::aggregate_functions::{self, CollectionKind, QuantileOutput};
use crate::ast::{self, ExprKind, NamedExpr, Span};
use crate::dynamic::{self, Kernel};
use crate::error::{Error, Result};
use crate::expressions::Expressions;

enum Post {
    Direct,
    Zero,
    Nan,
    Signed,
    Duration(bool),
    Field(String),
    OrderedList(usize),
}

struct Reference {
    name: String,
    root: usize,
    post: Post,
}

struct Collector<'a> {
    expressions: Expressions<'a>,
    schema: Arc<DFSchema>,
    roots: Vec<Expr>,
    indices: HashMap<Expr, usize>,
    reserved: HashSet<String>,
    references: Vec<Reference>,
    ordering: Vec<datafusion::logical_expr::expr::Sort>,
}

pub(crate) fn plan(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    now: i64,
    input: LogicalPlan,
    aggregates: &[NamedExpr],
    by: &[NamedExpr],
) -> Result<LogicalPlan> {
    let expressions = Expressions {
        state,
        scalars,
        now,
    };
    let mut used = HashSet::new();
    let mut groups = Vec::new();
    for (index, item) in by.iter().enumerate() {
        groups.push(crate::ordering::user_alias(
            expressions.scalar(&item.expr, input.schema())?,
            crate::planner::output_name(item, index, &mut used)?,
            input.schema(),
        )?);
    }
    let group_names = used.clone();
    let mut reserved = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();
    reserved.extend(scalars.keys().cloned());
    reserved.extend(group_names.iter().cloned());
    let mut collector = Collector {
        expressions,
        schema: Arc::clone(input.schema()),
        roots: Vec::new(),
        indices: HashMap::new(),
        reserved,
        references: Vec::new(),
        ordering: input_ordering(&input),
    };
    let mut outputs = Vec::new();
    for (index, item) in aggregates.iter().enumerate() {
        if let ExprKind::Call { name, args } = &item.expr.kind {
            if matches!(name.as_str(), "arg_max" | "arg_min" | "argmax" | "argmin")
                || matches!(name.as_str(), "take_any" | "any")
                    && (args.len() != 1 || matches!(args[0].kind, ExprKind::Wildcard))
            {
                let columns = collector.row_selection(name, args, item.expr.span)?;
                for (column_index, (name, reference)) in columns.into_iter().enumerate() {
                    if group_names.contains(&name) {
                        continue;
                    }
                    let name = if column_index == 0 {
                        item.name.clone().unwrap_or(name)
                    } else {
                        name
                    };
                    let named = NamedExpr {
                        name: Some(name),
                        expr: reference,
                    };
                    let name = crate::planner::output_name(&named, index, &mut used)?;
                    outputs.push((name, named.expr));
                }
                continue;
            }
            if matches!(name.as_str(), "percentiles" | "percentilesw") {
                if item.name.is_some() {
                    return Err(Error::unsupported(
                        "rename multiple percentile outputs with project-rename after summarize",
                        item.expr.span,
                    ));
                }
                let (root, ranks) =
                    collector.quantile(name, args, QuantileOutput::Columns, item.expr.span)?;
                let base = if let ExprKind::Identifier(parts) = &args[0].kind {
                    parts.last().cloned().unwrap_or_default()
                } else {
                    "Column".into()
                };
                for (i, rank) in ranks.iter().enumerate() {
                    let output =
                        format!("percentile_{base}_{}", rank.to_string().replace('.', "_"));
                    let reference = collector.reference(
                        root.clone(),
                        Post::Field(format!("percentile_{i}")),
                        item.expr.span,
                    )?;
                    let named = NamedExpr {
                        name: Some(output),
                        expr: reference,
                    };
                    let name = crate::planner::output_name(&named, index, &mut used)?;
                    outputs.push((name, named.expr));
                }
                continue;
            }
        }
        let name = crate::planner::output_name(item, index, &mut used)?;
        outputs.push((name, collector.rewrite(&item.expr)?));
    }
    if collector.roots.is_empty() && groups.is_empty() {
        return Err(Error::Plan(
            "summarize needs an aggregate or grouping expression".into(),
        ));
    }
    let result = LogicalPlanBuilder::from(input)
        .aggregate(groups, collector.roots)?
        .build()?;
    let mut bindings = scalars.clone();
    for reference in collector.references {
        let index = by.len() + reference.root;
        let mut value = Expr::Column(Column::from(result.schema().qualified_field(index)));
        match reference.post {
            Post::Direct => {}
            Post::Zero => {
                value = collector.expressions.function(
                    "coalesce",
                    vec![
                        value,
                        cast(lit(0_i64), result.schema().field(index).data_type().clone()),
                    ],
                )?
            }
            Post::Nan => {
                value = collector.expressions.function(
                    "coalesce",
                    vec![
                        value,
                        cast(
                            lit(f64::NAN),
                            result.schema().field(index).data_type().clone(),
                        ),
                    ],
                )?;
            }
            Post::Signed => value = cast(value, DataType::Int64),
            Post::Duration(zero) => {
                if zero {
                    value = collector
                        .expressions
                        .function("coalesce", vec![value, lit(0_i64)])?;
                }
                value = cast(
                    cast(value, DataType::Int64),
                    DataType::Duration(TimeUnit::Nanosecond),
                );
            }
            Post::Field(field) => {
                value = collector
                    .expressions
                    .function("get_field", vec![value, lit(field)])?
            }
            Post::OrderedList(limit) => {
                value = dynamic::call(Kernel::DecodeList { limit }, vec![value]);
            }
        }
        if crate::decimal::is_decimal(&value.get_type(result.schema())?) {
            value = crate::decimal::call(crate::decimal::Op::Convert, vec![value]);
        }
        bindings.insert(reference.name, value);
    }
    let expressions = Expressions {
        state,
        scalars: &bindings,
        now,
    };
    let mut projected = (0..by.len())
        .map(|i| Expr::Column(Column::from(result.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    for (name, expression) in outputs {
        projected.push(crate::ordering::user_alias(
            expressions.scalar(&expression, result.schema())?,
            name,
            result.schema(),
        )?);
    }
    Ok(LogicalPlanBuilder::from(result)
        .project(projected)?
        .build()?)
}

impl Collector<'_> {
    fn reference(&mut self, mut root: Expr, post: Post, span: Span) -> Result<ast::Expr> {
        let index = if let Some(index) = self.indices.get(&root) {
            *index
        } else {
            let index = self.roots.len();
            self.indices.insert(root.clone(), index);
            let native = root.name_for_alias()?;
            if !self.reserved.insert(native) {
                let mut suffix = index;
                let mut internal = format!("__kql_aggregate_{suffix}");
                while !self.reserved.insert(internal.clone()) {
                    suffix += 1;
                    internal = format!("__kql_aggregate_{suffix}");
                }
                root = root.alias(internal);
            }
            self.roots.push(root);
            index
        };
        let mut suffix = self.references.len();
        let mut name = format!("__kql_aggregate_reference_{suffix}");
        while !self.reserved.insert(name.clone()) {
            suffix += 1;
            name = format!("__kql_aggregate_reference_{suffix}");
        }
        self.references.push(Reference {
            name: name.clone(),
            root: index,
            post,
        });
        Ok(ast::Expr {
            kind: ExprKind::Identifier(vec![name]),
            span,
            depth: 1,
        })
    }

    fn rewrite(&mut self, expression: &ast::Expr) -> Result<ast::Expr> {
        if let ExprKind::Call { name, args } = &expression.kind
            && is_aggregate(self.expressions.state, name)
            && !(name == "hll_merge" && args.len() != 1)
        {
            let (root, post) = self.aggregate(name, args, expression.span)?;
            return self.reference(root, post, expression.span);
        }
        expression.map_children(|child| self.rewrite(child))
    }

    fn aggregate(&self, name: &str, args: &[ast::Expr], span: Span) -> Result<(Expr, Post)> {
        if matches!(name, "dcount" | "dcountif" | "hll" | "hll_if" | "hll_merge") {
            return self
                .hll(name, args, span)
                .map(|value| (value, Post::Direct));
        }
        if matches!(
            name,
            "percentile" | "percentilew" | "percentiles_array" | "percentilesw_array"
        ) {
            let output = if name.ends_with("_array") {
                QuantileOutput::Array
            } else {
                QuantileOutput::Single
            };
            return Ok((self.quantile(name, args, output, span)?.0, Post::Direct));
        }
        let collection = match name {
            "make_list" | "makelist" | "make_list_if" => Some(CollectionKind::List),
            "make_list_with_nulls" => Some(CollectionKind::ListWithNulls),
            "make_set" | "makeset" | "make_set_if" => Some(CollectionKind::Set),
            "make_bag" | "make_dictionary" | "make_bag_if" => Some(CollectionKind::Bag),
            _ => None,
        };
        if let Some(kind) = collection {
            let conditional = name.ends_with("_if");
            let minimum = if conditional { 2 } else { 1 };
            if args.len() < minimum || args.len() > minimum + 1 {
                return Err(Error::syntax(
                    format!("{name} expects a value, optional predicate, and optional maxSize"),
                    span,
                ));
            }
            let limit = if let Some(size) = args.get(minimum) {
                let value = self.expressions.scalar(size, &DFSchema::empty())?;
                match self
                    .expressions
                    .constant(cast(value, DataType::Int64), size.span)?
                {
                    ScalarValue::Int64(Some(value))
                        if (1..=dynamic::MAX_COLLECTION_SIZE as i64).contains(&value) =>
                    {
                        value as usize
                    }
                    _ => {
                        return Err(Error::syntax(
                            "collection maxSize must be between 1 and 1048576",
                            size.span,
                        ));
                    }
                }
            } else if matches!(name, "makelist" | "makeset") {
                128
            } else {
                dynamic::MAX_COLLECTION_SIZE
            };
            let value = self.expressions.scalar(&args[0], &self.schema)?;
            let ordered = !self.ordering.is_empty()
                && matches!(kind, CollectionKind::List | CollectionKind::ListWithNulls);
            let mut root = if ordered {
                for key in &self.ordering {
                    key.expr.get_type(self.schema.as_ref())?;
                }
                self.expressions
                    .state
                    .udaf("array_agg")?
                    .call(vec![dynamic::call(Kernel::EncodeWire, vec![value.clone()])])
            } else {
                aggregate_functions::collection(kind, limit, value.clone())
            };
            let mut filter = if ordered && kind == CollectionKind::List {
                Some(value.is_not_null())
            } else {
                None
            };
            if conditional {
                let condition = self.expressions.predicate(&args[1], &self.schema)?;
                filter = Some(filter.map_or(condition.clone(), |filter| filter.and(condition)));
            }
            if ordered {
                let mut builder = root.order_by(self.ordering.clone());
                if let Some(filter) = filter {
                    builder = builder.filter(filter);
                }
                root = builder.build()?;
            } else if let Some(filter) = filter {
                root = root.filter(filter).build()?;
            }
            return Ok((
                root,
                if ordered {
                    Post::OrderedList(limit)
                } else {
                    Post::Direct
                },
            ));
        }
        let (native, filtered, distinct, zero, signed) = match name {
            "count" => ("count", false, false, false, false),
            "countif" => ("count", true, false, false, false),
            "count_distinct" | "count_distinctif" => {
                ("count", name == "count_distinctif", true, false, false)
            }
            "sum" | "sumif" => ("sum", name == "sumif", false, true, false),
            "avg" | "avgif" => ("avg", name == "avgif", false, false, false),
            "min" | "minif" => ("min", name == "minif", false, false, false),
            "max" | "maxif" => ("max", name == "maxif", false, false, false),
            "stdev" | "stdevif" => ("stddev_samp", name == "stdevif", false, true, false),
            "stdevp" | "stdevpif" => ("stddev_pop", name == "stdevpif", false, true, false),
            "variance" | "varianceif" => ("var_samp", name == "varianceif", false, true, false),
            "variancep" | "variancepif" => ("var_pop", name == "variancepif", false, true, false),
            "take_any" | "any" | "take_anyif" => {
                ("first_value", name == "take_anyif", false, false, false)
            }
            custom
                if self
                    .expressions
                    .state
                    .aggregate_functions()
                    .contains_key(custom) =>
            {
                (custom, false, false, false, false)
            }
            _ => return Err(Error::unsupported(format!("aggregate '{name}'"), span)),
        };
        let standard = !self
            .expressions
            .state
            .aggregate_functions()
            .contains_key(name)
            || matches!(
                name,
                "sum" | "avg" | "min" | "max" | "count" | "variance" | "stdev"
            );
        let expected = if filtered { 2 } else { 1 };
        let mut filter = None;
        let mut values = if name == "count" {
            if args.len() > 1 {
                return Err(Error::syntax("count expects zero or one argument", span));
            }
            if let Some(value) = args.first()
                && !matches!(value.kind, ExprKind::Wildcard)
            {
                self.expressions.scalar(value, &self.schema)?;
            }
            vec![lit(1_i64)]
        } else if name == "countif" {
            if args.len() != 1 {
                return Err(Error::syntax("countif expects one predicate", span));
            }
            filter = Some(self.expressions.predicate(&args[0], &self.schema)?);
            vec![lit(1_i64)]
        } else {
            if standard && args.len() != expected {
                return Err(Error::unsupported(
                    format!("{name} expects {expected} argument(s)"),
                    span,
                ));
            }
            let slice = if filtered {
                &args[..args.len() - 1]
            } else {
                args
            };
            let values = slice
                .iter()
                .map(|arg| self.expressions.scalar(arg, &self.schema))
                .collect::<Result<Vec<_>>>()?;
            if filtered {
                filter = Some(
                    self.expressions
                        .predicate(&args[args.len() - 1], &self.schema)?,
                );
            }
            values
        };
        let duration = values.len() == 1
            && matches!(
                values[0].get_type(self.schema.as_ref())?,
                DataType::Duration(_)
            )
            && matches!(native, "sum" | "avg");
        if duration {
            values[0] = cast(
                cast(values[0].clone(), DataType::Duration(TimeUnit::Nanosecond)),
                DataType::Int64,
            );
        }
        if (native == "approx_distinct" || native == "count" && distinct)
            && (dynamic::is_dynamic(&values[0].get_type(self.schema.as_ref())?)
                || crate::arrow_dynamic::is_collection(&values[0].get_type(self.schema.as_ref())?))
        {
            values[0] = when(values[0].clone().is_null(), lit(ScalarValue::Utf8(None)))
                .otherwise(dynamic::call(Kernel::EncodeWire, vec![values[0].clone()]))?;
        }
        if matches!(name, "take_any" | "any" | "take_anyif") {
            let mut valid = values[0].clone().is_not_null();
            if crate::expressions::is_string(&values[0].get_type(self.schema.as_ref())?) {
                valid = valid.and(values[0].clone().not_eq(lit("")));
            }
            filter = Some(filter.map_or(valid.clone(), |filter| filter.and(valid)));
        }
        let decimal = values.len() == 1
            && crate::decimal::is_decimal(&values[0].get_type(self.schema.as_ref())?);
        let decimal_sum = decimal && matches!(native, "sum" | "avg" | "min" | "max");
        if decimal
            && matches!(
                native,
                "stddev_samp" | "stddev_pop" | "var_samp" | "var_pop"
            )
        {
            values[0] = crate::decimal::cast(values[0].clone(), DataType::Float64);
        }
        if decimal && matches!(native, "approx_distinct" | "count") {
            let key = self.expressions.function(
                "get_field",
                vec![
                    crate::decimal::call(crate::decimal::Op::Convert, vec![values[0].clone()]),
                    lit("__kql_decimal_order_v1"),
                ],
            )?;
            values[0] = when(
                values[0].clone().is_null(),
                lit(ScalarValue::FixedSizeBinary(16, None)),
            )
            .otherwise(key)?;
        }
        let mut root = if decimal_sum {
            crate::decimal::aggregate(native, values[0].clone())
        } else {
            self.expressions.state.udaf(native)?.call(values)
        };
        if distinct || filter.is_some() {
            let mut builder = root.order_by(Vec::new());
            if distinct {
                builder = builder.distinct();
            }
            if let Some(filter) = filter {
                builder = builder.filter(filter);
            }
            root = builder.build()?;
        }
        let post = if decimal_sum {
            Post::Direct
        } else if duration {
            Post::Duration(zero)
        } else if native == "avg" && root.get_type(self.schema.as_ref())?.is_floating() {
            Post::Nan
        } else if zero {
            Post::Zero
        } else if signed {
            Post::Signed
        } else {
            Post::Direct
        };
        Ok((root, post))
    }

    fn hll(&self, name: &str, args: &[ast::Expr], span: Span) -> Result<Expr> {
        use crate::hll_functions::{Kind, aggregate};
        if name == "hll_merge" {
            if args.len() != 1 {
                return Err(Error::syntax(
                    "aggregate hll_merge expects one sketch column",
                    span,
                ));
            }
            let value = self.expressions.scalar(&args[0], &self.schema)?;
            let value = self
                .expressions
                .hll_value(value, &self.schema, args[0].span)?;
            return Ok(aggregate(Kind::Merge, value, ""));
        }
        let conditional = matches!(name, "dcountif" | "hll_if");
        let minimum = if conditional { 2 } else { 1 };
        if !(minimum..=minimum + 1).contains(&args.len()) {
            return Err(Error::syntax(
                format!(
                    "{name} expects a value, {}optional accuracy",
                    if conditional {
                        "predicate, and "
                    } else {
                        "and "
                    }
                ),
                span,
            ));
        }
        let accuracy = if let Some(argument) = args.get(minimum) {
            let value = self.expressions.scalar(argument, &DFSchema::empty())?;
            if !value.get_type(&DFSchema::empty())?.is_integer() {
                return Err(Error::syntax(
                    "HLL accuracy must be a constant integer from 0 through 4",
                    argument.span,
                ));
            }
            match self
                .expressions
                .constant(cast(value, DataType::Int64), argument.span)?
            {
                ScalarValue::Int64(Some(value @ 0..=4)) => value as u8,
                _ => {
                    return Err(Error::syntax(
                        "HLL accuracy must be a constant integer from 0 through 4",
                        argument.span,
                    ));
                }
            }
        } else {
            1
        };
        let mut value = self.expressions.scalar(&args[0], &self.schema)?;
        let ty = value.get_type(self.schema.as_ref())?;
        let domain = crate::tabular::type_names(&ty).0;
        if dynamic::is_dynamic(&ty) || crate::arrow_dynamic::is_collection(&ty) {
            value = when(value.clone().is_null(), lit(ScalarValue::Utf8(None)))
                .otherwise(dynamic::call(Kernel::EncodeWire, vec![value]))?;
        } else if crate::decimal::is_decimal(&ty) {
            let key = self.expressions.function(
                "get_field",
                vec![
                    crate::decimal::call(crate::decimal::Op::Convert, vec![value.clone()]),
                    lit("__kql_decimal_order_v1"),
                ],
            )?;
            value = when(value.is_null(), lit(ScalarValue::FixedSizeBinary(16, None)))
                .otherwise(key)?;
        } else {
            let mut logical = &ty;
            while let DataType::Dictionary(_, values) = logical {
                logical = values;
            }
            let canonical = crate::tabular::canonical(logical);
            if canonical != ty {
                value = cast(value, canonical);
            }
        }
        let mut result = aggregate(
            if matches!(name, "dcount" | "dcountif") {
                Kind::Count(accuracy)
            } else {
                Kind::Build(accuracy)
            },
            value,
            &domain,
        );
        if conditional {
            result = result
                .filter(self.expressions.predicate(&args[1], &self.schema)?)
                .build()?;
        }
        Ok(result)
    }

    fn quantile(
        &self,
        name: &str,
        args: &[ast::Expr],
        output: QuantileOutput,
        span: Span,
    ) -> Result<(Expr, Vec<f64>)> {
        let weighted = name.starts_with("percentilew") || name.starts_with("percentilesw");
        let first_rank = if weighted { 2 } else { 1 };
        if args.len() <= first_rank {
            return Err(Error::syntax(
                "percentile requires a value and one or more constant ranks",
                span,
            ));
        }
        let value = self.expressions.scalar(&args[0], &self.schema)?;
        let ty = value.get_type(self.schema.as_ref())?;
        if !ty.is_numeric() && !crate::decimal::is_decimal(&ty) {
            return Err(Error::syntax(
                "percentile input must be numeric",
                args[0].span,
            ));
        }
        let integer = ty.is_integer();
        let value = if crate::decimal::is_decimal(&ty) {
            crate::decimal::cast(value, DataType::Float64)
        } else {
            cast(
                value,
                if integer {
                    DataType::Int64
                } else {
                    DataType::Float64
                },
            )
        };
        let weight = if weighted {
            cast(
                self.expressions.scalar(&args[1], &self.schema)?,
                DataType::UInt64,
            )
        } else {
            lit(1_u64)
        };
        let mut ranks = Vec::new();
        for rank in &args[first_rank..] {
            let value = self.expressions.scalar(rank, &DFSchema::empty())?;
            if dynamic::is_dynamic(&value.get_type(&DFSchema::empty())?) {
                let encoded = self
                    .expressions
                    .constant(dynamic::call(Kernel::EncodeJson, vec![value]), rank.span)?;
                let text = encoded
                    .try_as_str()
                    .flatten()
                    .ok_or_else(|| Error::syntax("invalid percentile array", rank.span))?;
                let values: Vec<f64> = serde_json::from_str(text).map_err(|_| {
                    Error::syntax("percentile arrays must contain numeric ranks", rank.span)
                })?;
                ranks.extend(values);
            } else {
                match self
                    .expressions
                    .constant(cast(value, DataType::Float64), rank.span)?
                {
                    ScalarValue::Float64(Some(value)) => ranks.push(value),
                    _ => {
                        return Err(Error::syntax(
                            "percentile ranks must be constant numbers",
                            rank.span,
                        ));
                    }
                }
            }
        }
        if ranks.is_empty()
            || ranks.len() > 1024
            || output == QuantileOutput::Single && ranks.len() != 1
            || ranks
                .iter()
                .any(|p| !p.is_finite() || !(0.0..=100.0).contains(p))
        {
            return Err(Error::syntax(
                "percentile ranks must be between 0 and 100 (at most 1024 ranks)",
                span,
            ));
        }
        Ok((
            aggregate_functions::quantile(integer, &ranks, output, value, weight),
            ranks,
        ))
    }

    fn row_selection(
        &mut self,
        name: &str,
        args: &[ast::Expr],
        span: Span,
    ) -> Result<Vec<(String, ast::Expr)>> {
        let extremum = matches!(name, "arg_max" | "arg_min" | "argmax" | "argmin");
        let first = usize::from(extremum);
        if args.len() <= first {
            return Err(Error::syntax(
                "row selection aggregate needs a returned expression",
                span,
            ));
        }
        let mut returned = if extremum {
            vec![args[0].clone()]
        } else {
            Vec::new()
        };
        for value in &args[first..] {
            if matches!(value.kind, ExprKind::Wildcard) {
                for field in self.schema.fields() {
                    if crate::ordering::hidden(field) {
                        continue;
                    }
                    if returned.iter().any(|expr| matches!(&expr.kind, ExprKind::Identifier(parts) if parts == &[field.name().clone()])) { continue; }
                    returned.push(ast::Expr {
                        kind: ExprKind::Identifier(vec![field.name().clone()]),
                        span,
                        depth: 1,
                    });
                }
            } else {
                returned.push(value.clone());
            }
        }
        let mut packed = Vec::new();
        let mut names = Vec::new();
        let mut used = HashSet::new();
        for (i, value) in returned.iter().enumerate() {
            names.push(crate::planner::output_name(
                &NamedExpr {
                    name: None,
                    expr: value.clone(),
                },
                i,
                &mut used,
            )?);
            packed.push(lit(format!("field_{i}")));
            packed.push(self.expressions.scalar(value, &self.schema)?);
        }
        let value = self.expressions.function("named_struct", packed)?;
        let mut root = self
            .expressions
            .state
            .udaf("first_value")?
            .call(vec![value]);
        if extremum {
            let key = crate::ordering::sort_value(
                self.expressions.scalar(&args[0], &self.schema)?,
                &self.schema,
            )?;
            root = root
                .order_by(vec![key.sort(matches!(name, "arg_min" | "argmin"), false)])
                .build()?;
        }
        names
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                Ok((
                    name,
                    self.reference(root.clone(), Post::Field(format!("field_{i}")), span)?,
                ))
            })
            .collect()
    }
}

fn input_ordering(input: &LogicalPlan) -> Vec<datafusion::logical_expr::expr::Sort> {
    if let Some(order) = crate::ordering::ordinal(input.schema()) {
        return vec![order.sort(true, false)];
    }
    match input {
        LogicalPlan::Sort(sort) => sort.expr.clone(),
        LogicalPlan::Projection(projection) => input_ordering(&projection.input),
        LogicalPlan::Filter(filter) => input_ordering(&filter.input),
        LogicalPlan::Limit(limit) => input_ordering(&limit.input),
        _ => Vec::new(),
    }
}

pub(crate) fn is_aggregate(state: &SessionState, name: &str) -> bool {
    state.aggregate_functions().contains_key(name)
        || matches!(
            name,
            "countif"
                | "dcount"
                | "dcountif"
                | "hll"
                | "hll_if"
                | "hll_merge"
                | "count_distinct"
                | "count_distinctif"
                | "sumif"
                | "avgif"
                | "minif"
                | "maxif"
                | "stdev"
                | "stdevif"
                | "stdevp"
                | "stdevpif"
                | "variance"
                | "varianceif"
                | "variancep"
                | "variancepif"
                | "take_any"
                | "take_anyif"
                | "any"
                | "arg_max"
                | "arg_min"
                | "argmax"
                | "argmin"
                | "make_list"
                | "make_list_if"
                | "make_list_with_nulls"
                | "makelist"
                | "make_set"
                | "make_set_if"
                | "makeset"
                | "make_bag"
                | "make_bag_if"
                | "make_dictionary"
                | "percentile"
                | "percentiles"
                | "percentiles_array"
                | "percentilew"
                | "percentilesw"
                | "percentilesw_array"
        )
}
