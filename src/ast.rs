use std::fmt;

/// Half-open UTF-8 byte offsets in the original query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub(crate) fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub bindings: Vec<Binding>,
    pub pipeline: Pipeline,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub name: String,
    pub value: BindingValue,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindingValue {
    Scalar(Expr),
    Tabular(Pipeline),
    Parameter {
        ty: ScalarType,
        default: Option<Expr>,
    },
    Function(Box<Function>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub view: bool,
    pub parameters: Vec<FunctionParameter>,
    pub bindings: Vec<Binding>,
    pub body: FunctionBody,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionParameter {
    pub name: String,
    pub ty: ParameterType,
    pub default: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParameterType {
    Scalar(ScalarType),
    Tabular(Option<Vec<(String, ScalarType)>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionBody {
    Scalar(Expr),
    Tabular(Pipeline),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CallArgument {
    pub name: Option<String>,
    pub value: ArgumentValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArgumentValue {
    Scalar(Expr),
    Tabular(Box<Query>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub source: Source,
    pub operators: Vec<SpannedOperator>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Table {
        name: Vec<String>,
        span: Span,
    },
    Pattern {
        prefix: Vec<String>,
        pattern: String,
        span: Span,
    },
    Subquery(Box<Query>),
    Call {
        name: String,
        args: Vec<CallArgument>,
        span: Span,
    },
    Print(Vec<NamedExpr>),
    Datatable {
        columns: Vec<(String, ScalarType)>,
        values: Vec<Expr>,
    },
    Range {
        name: String,
        start: Box<Expr>,
        end: Box<Expr>,
        step: Box<Expr>,
    },
    Union {
        kind: UnionKind,
        withsource: Option<String>,
        inputs: Vec<Source>,
    },
    Search {
        inputs: Vec<Source>,
        predicate: Box<Expr>,
        case_sensitive: bool,
        find: Option<Find>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Find {
    pub source: String,
    pub project: Option<Vec<(String, Option<ScalarType>)>>,
    pub pack_all: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Bool,
    Int,
    Long,
    Real,
    String,
    Datetime,
    Timespan,
    Guid,
    Decimal,
    Dynamic,
}

impl ScalarType {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "bool" | "boolean" => Self::Bool,
            "int" => Self::Int,
            "long" => Self::Long,
            "real" | "double" => Self::Real,
            "string" => Self::String,
            "datetime" | "date" => Self::Datetime,
            "timespan" | "time" => Self::Timespan,
            "guid" | "uuid" | "uniqueid" => Self::Guid,
            "decimal" => Self::Decimal,
            "dynamic" => Self::Dynamic,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedOperator {
    pub kind: Operator,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Where(Expr),
    Search {
        predicate: Expr,
        case_sensitive: bool,
    },
    Evaluate {
        name: String,
        args: Vec<Expr>,
        schema: Option<OutputSchema>,
    },
    Take(Expr),
    Project(Vec<NamedExpr>),
    Extend(Vec<NamedExpr>),
    Serialize(Vec<NamedExpr>),
    ProjectAway(Vec<String>),
    ProjectKeep(Vec<String>),
    ProjectRename(Vec<(String, String)>),
    ProjectReorder(Vec<ReorderColumn>),
    GetSchema,
    As(String),
    AsMaterialized(String),
    Invoke {
        name: String,
        args: Vec<CallArgument>,
    },
    Sample {
        count: Expr,
        distinct: Option<Expr>,
    },
    MakeSeries(Box<MakeSeries>),
    MvExpand {
        array_mode: bool,
        arrays_only: bool,
        index: Option<String>,
        columns: Vec<ExpandColumn>,
        limit: usize,
    },
    MvApply {
        index: Option<String>,
        columns: Vec<ExpandColumn>,
        limit: usize,
        operators: Vec<SpannedOperator>,
    },
    Partition {
        key: String,
        strategy: PartitionStrategy,
        operators: Vec<SpannedOperator>,
    },
    Scan(Box<Scan>),
    Parse {
        input: Expr,
        kind: ParseKind,
        flags: String,
        pattern: Vec<ParsePart>,
        filter: bool,
    },
    Summarize {
        aggregates: Vec<NamedExpr>,
        by: Vec<NamedExpr>,
    },
    Sort(Vec<SortKey>),
    Top {
        count: Expr,
        key: SortKey,
    },
    TopHitters {
        count: Expr,
        value: Expr,
        weight: Option<Expr>,
    },
    TopNested(Vec<NestedTop>),
    Distinct(Vec<Expr>),
    Count,
    Join {
        kind: JoinKind,
        input: Box<Query>,
        keys: Vec<JoinKey>,
    },
    Lookup {
        kind: JoinKind,
        input: Box<Query>,
        keys: Vec<JoinKey>,
    },
    Union {
        kind: UnionKind,
        withsource: Option<String>,
        inputs: Vec<Source>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedExpr {
    pub name: Option<String>,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputSchema {
    pub include_input: bool,
    pub columns: Vec<(String, ScalarType)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NestedTop {
    pub count: Option<Expr>,
    pub value: NamedExpr,
    pub others: Option<Expr>,
    pub aggregate: NamedExpr,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpandColumn {
    pub value: NamedExpr,
    pub ty: Option<ScalarType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    Legacy,
    Native,
    Shuffle,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub match_id: Option<String>,
    pub declarations: Vec<ScanDeclaration>,
    pub steps: Vec<ScanStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanDeclaration {
    pub name: String,
    pub ty: ScalarType,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanStep {
    pub name: String,
    pub output: ScanOutput,
    pub condition: Expr,
    pub assignments: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOutput {
    All,
    Last,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorderColumn {
    pub pattern: String,
    pub order: Option<(bool, bool)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesAggregation {
    pub value: NamedExpr,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MakeSeries {
    pub aggregates: Vec<SeriesAggregation>,
    pub axis: Expr,
    pub from: Option<Expr>,
    pub to: Option<Expr>,
    pub step: Expr,
    pub by: Vec<NamedExpr>,
    pub nonempty: bool,
    pub inclusive: bool,
    pub align_to_start: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {
    Simple,
    Regex,
    Relaxed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsePart {
    Literal(String),
    Capture { name: String, ty: ScalarType },
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    InnerUnique,
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinKey {
    pub left: String,
    pub right: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnionKind {
    Outer,
    Inner,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    pub kind: ExprKind,
    pub span: Span,
    pub(crate) depth: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Identifier(Vec<String>),
    #[doc(hidden)]
    BindingReference(String),
    Literal(Literal),
    SearchTerm {
        column: Option<String>,
        term: String,
    },
    ToScalar(Box<ArgumentValue>),
    #[doc(hidden)]
    DeferredCall {
        name: String,
        args: Vec<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        ty: ScalarType,
    },
    NamedArgument {
        name: String,
        value: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Dynamic(Box<Expr>),
    Array(Vec<Expr>),
    Object(Vec<(String, Expr)>),
    Access {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    Type(ScalarType),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    InList {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    TermSet {
        expr: Box<Expr>,
        values: Vec<Expr>,
        tabular: Option<Box<Query>>,
        all: bool,
    },
    InTabular {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
        case_insensitive: bool,
    },
    #[doc(hidden)]
    InPrepared {
        expr: Box<Expr>,
        binding: String,
        negated: bool,
        case_insensitive: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Integer(i64),
    Real(f64),
    String(String),
    Timespan(String),
    Typed { ty: ScalarType, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    EqInsensitive,
    NotEqInsensitive,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Contains,
    NotContains,
    ContainsCs,
    NotContainsCs,
    Has,
    NotHas,
    HasCs,
    NotHasCs,
    HasPrefix,
    NotHasPrefix,
    HasPrefixCs,
    NotHasPrefixCs,
    HasSuffix,
    NotHasSuffix,
    HasSuffixCs,
    NotHasSuffixCs,
    StartsWith,
    NotStartsWith,
    StartsWithCs,
    NotStartsWithCs,
    EndsWith,
    NotEndsWith,
    EndsWithCs,
    NotEndsWithCs,
    MatchesRegex,
}

impl Expr {
    pub(crate) fn any(&self, predicate: impl Fn(&ExprKind) -> bool + Copy) -> bool {
        if predicate(&self.kind) {
            return true;
        }
        match &self.kind {
            ExprKind::Call { args, .. }
            | ExprKind::DeferredCall { args, .. }
            | ExprKind::Array(args) => args.iter().any(|expr| expr.any(predicate)),
            ExprKind::Dynamic(expr)
            | ExprKind::Cast { expr, .. }
            | ExprKind::Unary { expr, .. }
            | ExprKind::NamedArgument { value: expr, .. }
            | ExprKind::InPrepared { expr, .. }
            | ExprKind::InTabular { expr, .. } => expr.any(predicate),
            ExprKind::Binary { left, right, .. }
            | ExprKind::Access {
                expr: left,
                index: right,
            } => left.any(predicate) || right.any(predicate),
            ExprKind::Object(items) => items.iter().any(|(_, expr)| expr.any(predicate)),
            ExprKind::ToScalar(value) => match value.as_ref() {
                ArgumentValue::Scalar(expr) => expr.any(predicate),
                ArgumentValue::Tabular(_) => false,
            },
            ExprKind::InList { expr, values, .. } | ExprKind::TermSet { expr, values, .. } => {
                expr.any(predicate) || values.iter().any(|expr| expr.any(predicate))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => expr.any(predicate) || low.any(predicate) || high.any(predicate),
            _ => false,
        }
    }

    pub(crate) fn map_children(
        &self,
        mut map: impl FnMut(&Self) -> crate::Result<Self>,
    ) -> crate::Result<Self> {
        let kind = match &self.kind {
            ExprKind::Cast { expr, ty } => ExprKind::Cast {
                expr: Box::new(map(expr)?),
                ty: *ty,
            },
            ExprKind::NamedArgument { name, value } => ExprKind::NamedArgument {
                name: name.clone(),
                value: Box::new(map(value)?),
            },
            ExprKind::ToScalar(value) => ExprKind::ToScalar(Box::new(match value.as_ref() {
                ArgumentValue::Scalar(expr) => ArgumentValue::Scalar(map(expr)?),
                ArgumentValue::Tabular(query) => ArgumentValue::Tabular(query.clone()),
            })),
            ExprKind::DeferredCall { name, args } => ExprKind::DeferredCall {
                name: name.clone(),
                args: args.iter().map(&mut map).collect::<crate::Result<_>>()?,
            },
            ExprKind::Call { name, args } => ExprKind::Call {
                name: name.clone(),
                args: args.iter().map(&mut map).collect::<crate::Result<_>>()?,
            },
            ExprKind::Dynamic(expr) => ExprKind::Dynamic(Box::new(map(expr)?)),
            ExprKind::Array(items) => {
                ExprKind::Array(items.iter().map(&mut map).collect::<crate::Result<_>>()?)
            }
            ExprKind::Object(items) => ExprKind::Object(
                items
                    .iter()
                    .map(|(name, value)| Ok((name.clone(), map(value)?)))
                    .collect::<crate::Result<_>>()?,
            ),
            ExprKind::Access { expr, index } => ExprKind::Access {
                expr: Box::new(map(expr)?),
                index: Box::new(map(index)?),
            },
            ExprKind::Unary { op, expr } => ExprKind::Unary {
                op: *op,
                expr: Box::new(map(expr)?),
            },
            ExprKind::Binary { left, op, right } => ExprKind::Binary {
                left: Box::new(map(left)?),
                op: *op,
                right: Box::new(map(right)?),
            },
            ExprKind::InList {
                expr,
                values,
                negated,
                case_insensitive,
            } => ExprKind::InList {
                expr: Box::new(map(expr)?),
                values: values.iter().map(&mut map).collect::<crate::Result<_>>()?,
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            ExprKind::TermSet {
                expr,
                values,
                tabular,
                all,
            } => ExprKind::TermSet {
                expr: Box::new(map(expr)?),
                values: values.iter().map(&mut map).collect::<crate::Result<_>>()?,
                tabular: tabular.clone(),
                all: *all,
            },
            ExprKind::InTabular {
                expr,
                query,
                negated,
                case_insensitive,
            } => ExprKind::InTabular {
                expr: Box::new(map(expr)?),
                query: query.clone(),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            ExprKind::InPrepared {
                expr,
                binding,
                negated,
                case_insensitive,
            } => ExprKind::InPrepared {
                expr: Box::new(map(expr)?),
                binding: binding.clone(),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => ExprKind::Between {
                expr: Box::new(map(expr)?),
                low: Box::new(map(low)?),
                high: Box::new(map(high)?),
                negated: *negated,
            },
            _ => self.kind.clone(),
        };
        Ok(Self {
            kind,
            span: self.span,
            depth: self.depth,
        })
    }
}

impl Operator {
    pub(crate) fn expression_refs(&self) -> Vec<&Expr> {
        match self {
            Self::Where(expr)
            | Self::Search {
                predicate: expr, ..
            }
            | Self::Take(expr)
            | Self::Parse { input: expr, .. } => vec![expr],
            Self::Project(items) | Self::Extend(items) | Self::Serialize(items) => {
                items.iter().map(|i| &i.expr).collect()
            }
            Self::MvExpand { columns, .. } | Self::MvApply { columns, .. } => {
                columns.iter().map(|c| &c.value.expr).collect()
            }
            Self::Summarize { aggregates, by } => {
                aggregates.iter().chain(by).map(|i| &i.expr).collect()
            }
            Self::MakeSeries(series) => series
                .aggregates
                .iter()
                .flat_map(|a| std::iter::once(&a.value.expr).chain(a.default.iter()))
                .chain(series.by.iter().map(|b| &b.expr))
                .chain([&series.axis, &series.step])
                .chain(series.from.iter())
                .chain(series.to.iter())
                .collect(),
            Self::Sort(keys) => keys.iter().map(|k| &k.expr).collect(),
            Self::Top { count, key } => vec![count, &key.expr],
            Self::TopHitters {
                count,
                value,
                weight,
            } => std::iter::once(count)
                .chain(std::iter::once(value))
                .chain(weight.iter())
                .collect(),
            Self::TopNested(levels) => levels
                .iter()
                .flat_map(|level| {
                    level
                        .count
                        .iter()
                        .chain(std::iter::once(&level.value.expr))
                        .chain(level.others.iter())
                        .chain(std::iter::once(&level.aggregate.expr))
                })
                .collect(),
            Self::Evaluate { args, .. } => args.iter().collect(),
            Self::Sample { count, distinct } => {
                std::iter::once(count).chain(distinct.iter()).collect()
            }
            Self::Distinct(items) => items.iter().collect(),
            Self::Scan(scan) => scan
                .declarations
                .iter()
                .filter_map(|d| d.default.as_ref())
                .chain(scan.steps.iter().flat_map(|s| {
                    std::iter::once(&s.condition).chain(s.assignments.iter().map(|(_, e)| e))
                }))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub(crate) fn map_expressions(
        &self,
        mut map: impl FnMut(&Expr) -> crate::Result<Expr>,
    ) -> crate::Result<Self> {
        let mut operator = self.clone();
        match &mut operator {
            Self::Where(expr)
            | Self::Search {
                predicate: expr, ..
            }
            | Self::Take(expr)
            | Self::Parse { input: expr, .. } => {
                *expr = map(expr)?;
            }
            Self::Project(items) | Self::Extend(items) | Self::Serialize(items) => {
                for item in items {
                    item.expr = map(&item.expr)?;
                }
            }
            Self::MvExpand { columns, .. } | Self::MvApply { columns, .. } => {
                for column in columns {
                    column.value.expr = map(&column.value.expr)?;
                }
            }
            Self::Scan(scan) => {
                for declaration in &mut scan.declarations {
                    if let Some(default) = &mut declaration.default {
                        *default = map(default)?;
                    }
                }
                for step in &mut scan.steps {
                    step.condition = map(&step.condition)?;
                    for (_, value) in &mut step.assignments {
                        *value = map(value)?;
                    }
                }
            }
            Self::Evaluate { args, .. } => {
                for arg in args {
                    *arg = map(arg)?;
                }
            }
            Self::Summarize { aggregates, by } => {
                for item in aggregates.iter_mut().chain(by) {
                    item.expr = map(&item.expr)?;
                }
            }
            Self::MakeSeries(series) => {
                for aggregate in &mut series.aggregates {
                    aggregate.value.expr = map(&aggregate.value.expr)?;
                    if let Some(default) = &mut aggregate.default {
                        *default = map(default)?;
                    }
                }
                for group in &mut series.by {
                    group.expr = map(&group.expr)?;
                }
                series.axis = map(&series.axis)?;
                series.step = map(&series.step)?;
                if let Some(from) = &mut series.from {
                    *from = map(from)?;
                }
                if let Some(to) = &mut series.to {
                    *to = map(to)?;
                }
            }
            Self::Sort(keys) => {
                for key in keys {
                    key.expr = map(&key.expr)?;
                }
            }
            Self::Top { count, key } => {
                *count = map(count)?;
                key.expr = map(&key.expr)?;
            }
            Self::TopHitters {
                count,
                value,
                weight,
            } => {
                *count = map(count)?;
                *value = map(value)?;
                if let Some(weight) = weight {
                    *weight = map(weight)?;
                }
            }
            Self::TopNested(levels) => {
                for level in levels {
                    if let Some(count) = &mut level.count {
                        *count = map(count)?;
                    }
                    level.value.expr = map(&level.value.expr)?;
                    if let Some(others) = &mut level.others {
                        *others = map(others)?;
                    }
                    level.aggregate.expr = map(&level.aggregate.expr)?;
                }
            }
            Self::Sample { count, distinct } => {
                *count = map(count)?;
                if let Some(distinct) = distinct {
                    *distinct = map(distinct)?;
                }
            }
            Self::Distinct(items) => {
                for item in items {
                    *item = map(item)?;
                }
            }
            _ => {}
        }
        Ok(operator)
    }
}
