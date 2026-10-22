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
    Sample {
        count: Expr,
        distinct: Option<Expr>,
    },
    MakeSeries(Box<MakeSeries>),
    MvExpand {
        array_mode: bool,
        index: Option<String>,
        columns: Vec<ExpandColumn>,
        limit: usize,
    },
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
pub struct ExpandColumn {
    pub value: NamedExpr,
    pub ty: Option<ScalarType>,
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
    Literal(Literal),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub(crate) fn map_children(
        &self,
        mut map: impl FnMut(&Self) -> crate::Result<Self>,
    ) -> crate::Result<Self> {
        let kind = match &self.kind {
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
