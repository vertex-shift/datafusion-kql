use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::expr_fn::{ExprFunctionExt, binary_expr, cast, when};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use datafusion::prelude::lit;

use crate::ast::{self, ExprKind, Operator};
use crate::dynamic::{self, Kernel};
use crate::expressions::Expressions;
use crate::{Error, Result, ordering};

pub(crate) fn prepare(
    state: &SessionState,
    scalars: &HashMap<String, Expr>,
    now: i64,
    input: LogicalPlan,
    operator: &Operator,
) -> Result<(LogicalPlan, Operator, HashMap<String, Expr>)> {
    let ordinal = ordering::ordinal(input.schema()).ok_or_else(|| {
        Error::Plan("row functions require a serialized input; use sort or serialize first".into())
    })?;
    let mut windows = Windows {
        state,
        scalars: scalars.clone(),
        now,
        plan: input,
        ordinal,
        cache: HashMap::new(),
        next: 0,
    };
    let mut operator = operator.clone();
    match &mut operator {
        Operator::Where(expr) | Operator::Parse { input: expr, .. } => {
            *expr = windows.rewrite(expr)?
        }
        Operator::Project(items) | Operator::Extend(items) | Operator::Serialize(items) => {
            for item in items {
                item.expr = windows.rewrite(&item.expr)?;
            }
        }
        Operator::MvExpand { columns, .. } | Operator::MvApply { columns, .. } => {
            for column in columns {
                column.value.expr = windows.rewrite(&column.value.expr)?;
            }
        }
        Operator::Summarize { aggregates, by } => {
            for item in aggregates.iter_mut().chain(by) {
                item.expr = windows.rewrite(&item.expr)?;
            }
        }
        Operator::MakeSeries(series) => {
            for aggregate in &mut series.aggregates {
                aggregate.value.expr = windows.rewrite(&aggregate.value.expr)?;
            }
            for group in &mut series.by {
                group.expr = windows.rewrite(&group.expr)?;
            }
            series.axis = windows.rewrite(&series.axis)?;
        }
        Operator::Sort(keys) => {
            for key in keys {
                key.expr = windows.rewrite(&key.expr)?;
            }
        }
        Operator::Top { key, .. } => key.expr = windows.rewrite(&key.expr)?,
        Operator::Distinct(items) => {
            for item in items {
                *item = windows.rewrite(item)?;
            }
        }
        _ => {}
    }
    let plan = LogicalPlanBuilder::from(windows.plan)
        .sort(vec![windows.ordinal.sort(true, false)])?
        .build()?;
    Ok((plan, operator, windows.scalars))
}

struct Windows<'a> {
    state: &'a SessionState,
    scalars: HashMap<String, Expr>,
    now: i64,
    plan: LogicalPlan,
    ordinal: Expr,
    cache: HashMap<Expr, Expr>,
    next: usize,
}

impl Windows<'_> {
    fn expressions(&self) -> Expressions<'_> {
        Expressions {
            state: self.state,
            scalars: &self.scalars,
            now: self.now,
        }
    }

    fn temporary(&mut self, prefix: &str) -> String {
        loop {
            let name = format!("__kql_{prefix}_{}", self.next);
            self.next += 1;
            if self
                .plan
                .schema()
                .index_of_column_by_name(None, &name)
                .is_none()
                && !self.scalars.contains_key(&name)
                && self
                    .plan
                    .schema()
                    .index_of_column_by_name(None, &format!("{name}_value"))
                    .is_none()
            {
                return name;
            }
        }
    }

    fn window(&mut self, expr: Expr) -> Result<Expr> {
        if let Some(column) = self.cache.get(&expr) {
            return Ok(column.clone());
        }
        if self.cache.len() >= 128 {
            return Err(Error::Plan("window expression limit exceeded".into()));
        }
        let name = self.temporary("window_column");
        let raw = format!("{name}_value");
        let input = self.plan.clone();
        let window = LogicalPlanBuilder::from(input.clone())
            .window(vec![expr.clone().alias(&raw)])?
            .build()?;
        let mut columns = (0..input.schema().fields().len())
            .map(|i| Expr::Column(Column::from(input.schema().qualified_field(i))))
            .collect::<Vec<_>>();
        columns.push(ordering::helper(
            Expr::Column(Column::from_name(raw)),
            name.clone(),
            false,
        ));
        self.plan = LogicalPlanBuilder::from(window).project(columns)?.build()?;
        let column = Expr::Column(Column::from_name(name));
        self.cache.insert(expr, column.clone());
        Ok(column)
    }

    fn counter(&mut self, partition: Option<Expr>) -> Result<Expr> {
        let mut builder = self
            .state
            .udwf("row_number")?
            .call(Vec::new())
            .order_by(vec![self.ordinal.clone().sort(true, false)]);
        if let Some(partition) = partition {
            builder = builder.partition_by(vec![partition]);
        }
        Ok(cast(self.window(builder.build()?)?, DataType::Int64))
    }

    fn cumulative(&mut self, name: &str, value: Expr, partition: Option<Expr>) -> Result<Expr> {
        let function = WindowFunction::new(self.state.udaf(name)?, vec![value]);
        let mut builder = Expr::from(function)
            .order_by(vec![self.ordinal.clone().sort(true, false)])
            .window_frame(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            ));
        if let Some(partition) = partition {
            builder = builder.partition_by(vec![partition]);
        }
        self.window(builder.build()?)
    }

    fn restart(&mut self, args: &[ast::Expr], at: usize) -> Result<Option<Expr>> {
        if let Some(restart) = args.get(at) {
            let value = self.expressions().scalar(restart, self.plan.schema())?;
            if !matches!(
                value.get_type(self.plan.schema().as_ref())?,
                DataType::Boolean | DataType::Null
            ) {
                return Err(Error::syntax(
                    "row restart expression must be boolean",
                    restart.span,
                ));
            }
            Ok(Some(self.cumulative(
                "sum",
                cast(value.is_true(), DataType::Int64),
                None,
            )?))
        } else {
            Ok(None)
        }
    }

    fn shift(
        &mut self,
        name: &str,
        value: Expr,
        offset: i64,
        default: Option<Expr>,
    ) -> Result<Expr> {
        if offset == 0 {
            return Ok(value);
        }
        let ty = value.get_type(self.plan.schema().as_ref())?;
        let dynamic = dynamic::is_dynamic(&ty);
        let (value, default) = if dynamic {
            let default = default.unwrap_or(lit(ScalarValue::Null));
            (
                dynamic::call(Kernel::EncodeWindow, vec![value]),
                dynamic::call(Kernel::EncodeWindow, vec![default]),
            )
        } else {
            (value, default.unwrap_or(lit(ScalarValue::try_from(&ty)?)))
        };
        let window = self
            .state
            .udwf(if name == "prev" { "lag" } else { "lead" })?
            .call(vec![value, lit(offset), default])
            .order_by(vec![self.ordinal.clone().sort(true, false)])
            .build()?;
        let value = self.window(window)?;
        if dynamic {
            Ok(dynamic::call(Kernel::DecodeWindow, vec![value]))
        } else if crate::expressions::is_string(&ty) {
            self.expressions()
                .function("coalesce", vec![value, lit("")])
        } else {
            Ok(value)
        }
    }

    fn rewrite(&mut self, expression: &ast::Expr) -> Result<ast::Expr> {
        let rewritten = expression.map_children(|child| self.rewrite(child))?;
        let ExprKind::Call { name, args } = &rewritten.kind else {
            return Ok(rewritten);
        };
        if !ordering::is_window(name) {
            return Ok(rewritten);
        }
        let value = match name.as_str() {
            "prev" | "next" => {
                if !(1..=3).contains(&args.len()) {
                    return Err(Error::syntax(
                        "prev/next expect value, optional offset and default",
                        expression.span,
                    ));
                }
                let value = self.expressions().scalar(&args[0], self.plan.schema())?;
                let offset = if let Some(offset) = args.get(1) {
                    let value = self.expressions().scalar(offset, self.plan.schema())?;
                    match self
                        .expressions()
                        .constant(cast(value, DataType::Int64), offset.span)?
                    {
                        ScalarValue::Int64(Some(value)) if value >= 0 => value,
                        _ => {
                            return Err(Error::syntax(
                                "row offset must be a nonnegative constant integer",
                                offset.span,
                            ));
                        }
                    }
                } else {
                    1
                };
                let default = args
                    .get(2)
                    .map(|arg| self.expressions().scalar(arg, self.plan.schema()))
                    .transpose()?;
                self.shift(name, value, offset, default)?
            }
            "row_number" => {
                if args.len() > 2 {
                    return Err(Error::syntax(
                        "row_number expects optional start and restart",
                        expression.span,
                    ));
                }
                let start = if let Some(start) = args.first() {
                    let value = self.expressions().scalar(start, self.plan.schema())?;
                    match self
                        .expressions()
                        .constant(cast(value, DataType::Int64), start.span)?
                    {
                        ScalarValue::Int64(Some(value)) => value,
                        _ => {
                            return Err(Error::syntax(
                                "row_number start must be a constant long",
                                start.span,
                            ));
                        }
                    }
                } else {
                    1
                };
                let partition = self.restart(args, 1)?;
                self.counter(partition)? - lit(1_i64) + lit(start)
            }
            "row_cumsum" => {
                if !(1..=2).contains(&args.len()) {
                    return Err(Error::syntax(
                        "row_cumsum expects a term and optional restart",
                        expression.span,
                    ));
                }
                let value = self.expressions().scalar(&args[0], self.plan.schema())?;
                if !value.get_type(self.plan.schema().as_ref())?.is_numeric() {
                    return Err(Error::syntax(
                        "row_cumsum term must be numeric",
                        args[0].span,
                    ));
                }
                let partition = self.restart(args, 1)?;
                let sum = self.cumulative("sum", value, partition)?;
                let ty = sum.get_type(self.plan.schema().as_ref())?;
                self.expressions()
                    .function("coalesce", vec![sum, cast(lit(0_i64), ty)])?
            }
            "row_rank_dense" | "row_rank_min" => {
                if !(1..=2).contains(&args.len()) {
                    return Err(Error::syntax(
                        "row rank expects a term and optional restart",
                        expression.span,
                    ));
                }
                let value = self.expressions().scalar(&args[0], self.plan.schema())?;
                let previous = self.shift("prev", value.clone(), 1, None)?;
                let partition = self.restart(args, 1)?;
                let row = self.counter(partition.clone())?;
                let different =
                    if dynamic::is_dynamic(&value.get_type(self.plan.schema().as_ref())?) {
                        dynamic::call(Kernel::Equal(true), vec![value, previous]).is_true()
                    } else {
                        binary_expr(
                            value,
                            datafusion::logical_expr::Operator::IsDistinctFrom,
                            previous,
                        )
                    };
                let changed = row.clone().eq(lit(1_i64)).or(different);
                if name == "row_rank_dense" {
                    self.cumulative("sum", cast(changed, DataType::Int64), partition)?
                } else {
                    let position = when(changed, row).otherwise(lit(ScalarValue::Int64(None)))?;
                    self.cumulative("max", position, partition)?
                }
            }
            "row_window_session" => {
                if !(3..=4).contains(&args.len()) {
                    return Err(Error::syntax(
                        "row_window_session expects datetime, two timespans, and optional restart",
                        expression.span,
                    ));
                }
                let source = self.expressions().scalar(&args[0], self.plan.schema())?;
                if !matches!(
                    source.get_type(self.plan.schema().as_ref())?,
                    DataType::Timestamp(_, _)
                ) {
                    return Err(Error::syntax(
                        "session input must be datetime",
                        args[0].span,
                    ));
                }
                let mut values = vec![cast(
                    source,
                    DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
                )];
                for distance in &args[1..3] {
                    let value = self.expressions().scalar(distance, self.plan.schema())?;
                    if !matches!(
                        value.get_type(self.plan.schema().as_ref())?,
                        DataType::Duration(_) | DataType::Null
                    ) {
                        return Err(Error::syntax(
                            "session distances must be timespans",
                            distance.span,
                        ));
                    }
                    values.push(cast(
                        value,
                        DataType::Duration(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
                    ));
                }
                let restart = if let Some(restart) = args.get(3) {
                    let value = self.expressions().scalar(restart, self.plan.schema())?;
                    if !matches!(
                        value.get_type(self.plan.schema().as_ref())?,
                        DataType::Boolean | DataType::Null
                    ) {
                        return Err(Error::syntax(
                            "session restart must be boolean",
                            restart.span,
                        ));
                    }
                    value.is_true()
                } else {
                    lit(false)
                };
                values.push(restart);
                let window = crate::temporal::session(values)
                    .order_by(vec![self.ordinal.clone().sort(true, false)])
                    .build()?;
                self.window(window)?
            }
            _ => unreachable!("window names are checked above"),
        };
        let name = self.temporary("window_value");
        self.scalars.insert(name.clone(), value);
        Ok(ast::Expr {
            kind: ExprKind::Identifier(vec![name]),
            span: expression.span,
            depth: 1,
        })
    }
}
