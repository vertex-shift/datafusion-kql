use crate::ast::*;
use crate::error::{Error, Result};
use crate::lexer::{Kind, Token, lex};

/// Resource limits apply before DataFusion analysis and execution.
#[derive(Debug, Clone)]
pub struct ParserOptions {
    pub max_query_bytes: usize,
    pub max_nesting: usize,
    pub max_pipeline_operators: usize,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            max_query_bytes: 1 << 20,
            max_nesting: 64,
            max_pipeline_operators: 128,
        }
    }
}

/// Parse a KQL query without accessing a catalog or executing data.
pub fn parse(query: &str) -> Result<Query> {
    parse_with_options(query, &ParserOptions::default())
}

pub fn parse_with_options(query: &str, options: &ParserOptions) -> Result<Query> {
    if query.len() > options.max_query_bytes {
        return Err(Error::syntax(
            format!("query exceeds the {} byte limit", options.max_query_bytes),
            Span::new(0, query.len()),
        ));
    }
    if options.max_nesting == 0 || options.max_nesting > 128 {
        return Err(Error::Plan("max_nesting must be between 1 and 128".into()));
    }
    let mut parser = Parser {
        tokens: lex(query)?,
        pos: 0,
        nesting: 0,
        dynamic_depth: 0,
        options,
    };
    let result = parser.query()?;
    parser.expect(Kind::Eof)?;
    Ok(result)
}

struct Parser<'a, 'o> {
    tokens: Vec<Token<'a>>,
    pos: usize,
    nesting: usize,
    dynamic_depth: usize,
    options: &'o ParserOptions,
}

impl<'a> Parser<'a, '_> {
    fn current(&self) -> &Token<'a> {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token<'a> {
        let token = self.tokens[self.pos].clone();
        if !matches!(token.kind, Kind::Eof) {
            self.pos += 1;
        }
        token
    }

    fn at(&self, kind: &Kind<'_>) -> bool {
        self.current().kind == *kind
    }

    fn eat(&mut self, kind: Kind<'_>) -> bool {
        if self.at(&kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn keyword(&self, word: &str) -> bool {
        matches!(&self.current().kind, Kind::Ident(text) if *text == word)
    }

    fn eat_keyword(&mut self, word: &str) -> bool {
        if self.keyword(word) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, word: &str) -> Result<()> {
        if self.eat_keyword(word) {
            Ok(())
        } else {
            Err(self.expected(&format!("'{word}'")))
        }
    }

    fn expect(&mut self, kind: Kind<'_>) -> Result<()> {
        if self.eat(kind.clone()) {
            Ok(())
        } else {
            Err(self.expected(&format!("{kind:?}")))
        }
    }

    fn expected(&self, expected: &str) -> Error {
        Error::syntax(
            format!("expected {expected}, found {:?}", self.current().kind),
            self.current().span,
        )
    }

    fn enter(&mut self) -> Result<()> {
        self.nesting += 1;
        if self.nesting > self.options.max_nesting {
            return Err(Error::syntax(
                "query nesting limit exceeded",
                self.current().span,
            ));
        }
        Ok(())
    }

    fn identifier(&mut self) -> Result<String> {
        if self.eat(Kind::LBracket) {
            let token = self.advance();
            let Kind::String(value) = token.kind else {
                return Err(Error::syntax(
                    "escaped identifiers use ['column name']",
                    token.span,
                ));
            };
            let value = value.into_owned();
            self.expect(Kind::RBracket)?;
            return Ok(value);
        }
        let token = self.advance();
        match token.kind {
            Kind::Ident(value) => Ok(value.to_owned()),
            _ => Err(Error::syntax("expected an identifier", token.span)),
        }
    }

    fn path(&mut self) -> Result<Vec<String>> {
        let mut parts = vec![self.identifier()?];
        while self.eat(Kind::Dot) {
            parts.push(self.identifier()?);
        }
        Ok(parts)
    }

    fn query(&mut self) -> Result<Query> {
        self.enter()?;
        let bindings = self.bindings()?;
        let pipeline = self.pipeline()?;
        self.eat(Kind::Semi);
        if !matches!(self.current().kind, Kind::Eof | Kind::RParen | Kind::RBrace) {
            return Err(self.expected("'|' or the end of the query"));
        }
        self.nesting -= 1;
        Ok(Query { bindings, pipeline })
    }

    fn bindings(&mut self) -> Result<Vec<Binding>> {
        let mut bindings = Vec::new();
        while self.keyword("let") || self.keyword("declare") {
            let start = self.current().span.start;
            if self.eat_keyword("declare") {
                self.expect_keyword("query_parameters")?;
                self.expect(Kind::LParen)?;
                loop {
                    let name = self.identifier()?;
                    self.expect(Kind::Colon)?;
                    let ty = self.scalar_type()?;
                    let default = self.parameter_default(ty)?;
                    bindings.push(Binding {
                        name,
                        value: BindingValue::Parameter { ty, default },
                        span: Span::new(start, self.tokens[self.pos - 1].span.end),
                    });
                    if !self.eat(Kind::Comma) {
                        break;
                    }
                }
                self.expect(Kind::RParen)?;
                self.expect(Kind::Semi)?;
                if bindings.len() > self.options.max_pipeline_operators {
                    return Err(self.expected("fewer query declarations"));
                }
                continue;
            }
            self.expect_keyword("let")?;
            let name = self.identifier()?;
            self.expect(Kind::Assign)?;
            let value = if self.eat_keyword("view") {
                let mut function = self.function()?;
                if !function.parameters.is_empty() {
                    return Err(self.expected("a parameterless view"));
                }
                function.view = true;
                BindingValue::Function(Box::new(function))
            } else if self.looks_function() {
                BindingValue::Function(Box::new(self.function()?))
            } else if self.looks_tabular() {
                BindingValue::Tabular(self.pipeline()?)
            } else {
                BindingValue::Scalar(self.expr(0)?)
            };
            let end = self.current().span.end;
            self.expect(Kind::Semi)?;
            bindings.push(Binding {
                name,
                value,
                span: Span::new(start, end),
            });
            if bindings.len() > self.options.max_pipeline_operators {
                return Err(Error::syntax("too many let bindings", self.current().span));
            }
        }
        Ok(bindings)
    }

    fn scalar_type(&mut self) -> Result<ScalarType> {
        let span = self.current().span;
        let name = self.identifier()?;
        ScalarType::parse(&name)
            .ok_or_else(|| Error::unsupported(format!("scalar type '{name}'"), span))
    }

    fn parameter_default(&mut self, ty: ScalarType) -> Result<Option<Expr>> {
        if !self.eat(Kind::Assign) {
            return Ok(None);
        }
        let value = self.expr(0)?;
        fn literal(expr: &Expr) -> bool {
            match &expr.kind {
                ExprKind::Literal(_) => true,
                ExprKind::Call { name, args } => {
                    ScalarType::parse(name).is_some() && args.len() == 1 && literal(&args[0])
                }
                ExprKind::Unary {
                    op: UnaryOp::Negate | UnaryOp::Plus,
                    expr,
                } => literal(expr),
                _ => false,
            }
        }
        if ty == ScalarType::Dynamic || !literal(&value) {
            return Err(Error::syntax(
                "parameter defaults must be non-dynamic scalar literals",
                value.span,
            ));
        }
        Ok(Some(value))
    }

    fn looks_function(&self) -> bool {
        if !self.at(&Kind::LParen) {
            return false;
        }
        let mut depth = 0;
        for (i, token) in self.tokens[self.pos..].iter().enumerate() {
            match token.kind {
                Kind::LParen => depth += 1,
                Kind::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return self
                            .tokens
                            .get(self.pos + i + 1)
                            .is_some_and(|next| matches!(next.kind, Kind::LBrace));
                    }
                }
                Kind::Eof => break,
                _ => {}
            }
        }
        false
    }

    fn function(&mut self) -> Result<Function> {
        self.enter()?;
        self.expect(Kind::LParen)?;
        let mut parameters = Vec::new();
        let mut names = std::collections::HashSet::new();
        let mut scalar_seen = false;
        let mut optional_seen = false;
        if !self.at(&Kind::RParen) {
            loop {
                let start = self.current().span.start;
                let name = self.identifier()?;
                if !names.insert(name.clone()) {
                    return Err(self.expected("a unique function parameter name"));
                }
                self.expect(Kind::Colon)?;
                let (ty, default) = if self.eat(Kind::LParen) {
                    if scalar_seen {
                        return Err(self.expected("tabular parameters before scalar parameters"));
                    }
                    let columns = if self.eat(Kind::Star) {
                        None
                    } else {
                        let mut columns = Vec::new();
                        let mut names = std::collections::HashSet::new();
                        loop {
                            let name = self.identifier()?;
                            if !names.insert(name.clone()) {
                                return Err(self.expected("a unique parameter column name"));
                            }
                            self.expect(Kind::Colon)?;
                            columns.push((name, self.scalar_type()?));
                            if !self.eat(Kind::Comma) {
                                break;
                            }
                        }
                        Some(columns)
                    };
                    self.expect(Kind::RParen)?;
                    (ParameterType::Tabular(columns), None)
                } else {
                    scalar_seen = true;
                    let ty = self.scalar_type()?;
                    let default = self.parameter_default(ty)?;
                    if optional_seen && default.is_none() {
                        return Err(self.expected("required parameters before optional parameters"));
                    }
                    optional_seen |= default.is_some();
                    (ParameterType::Scalar(ty), default)
                };
                parameters.push(FunctionParameter {
                    name,
                    ty,
                    default,
                    span: Span::new(start, self.tokens[self.pos - 1].span.end),
                });
                if parameters.len() > 128 {
                    return Err(self.expected("at most 128 function parameters"));
                }
                if !self.eat(Kind::Comma) {
                    break;
                }
            }
        }
        self.expect(Kind::RParen)?;
        self.expect(Kind::LBrace)?;
        let bindings = self.bindings()?;
        let body = if self.looks_tabular() {
            FunctionBody::Tabular(self.pipeline()?)
        } else {
            FunctionBody::Scalar(self.expr(0)?)
        };
        self.eat(Kind::Semi);
        self.expect(Kind::RBrace)?;
        self.nesting -= 1;
        Ok(Function {
            view: false,
            parameters,
            bindings,
            body,
        })
    }

    fn looks_tabular(&self) -> bool {
        let mut at = self.pos;
        while matches!(self.tokens[at].kind, Kind::LParen) {
            at += 1;
        }
        if matches!(
            self.tokens[at].kind,
            Kind::Ident(
                "print"
                    | "datatable"
                    | "union"
                    | "let"
                    | "declare"
                    | "materialize"
                    | "search"
                    | "find"
            )
        ) {
            return true;
        }
        if matches!(
            self.tokens[at].kind,
            Kind::Ident("table" | "database" | "cluster")
        ) && matches!(
            self.tokens.get(at + 1).map(|token| &token.kind),
            Some(Kind::LParen)
        ) {
            return true;
        }
        if matches!(self.tokens[at].kind, Kind::Ident("range"))
            && !matches!(self.tokens.get(at + 1).map(|t| &t.kind), Some(Kind::LParen))
        {
            return true;
        }
        let mut depth = 0;
        for token in &self.tokens[at..] {
            match token.kind {
                Kind::Pipe if depth == 0 => return true,
                Kind::LParen | Kind::LBracket | Kind::LBrace => depth += 1,
                Kind::RParen | Kind::RBracket | Kind::RBrace if depth == 0 => break,
                Kind::RParen | Kind::RBracket | Kind::RBrace => depth -= 1,
                Kind::Semi | Kind::Eof => break,
                Kind::Comma if depth == 0 => break,
                _ => {}
            }
        }
        false
    }

    fn pipeline(&mut self) -> Result<Pipeline> {
        let source = self.source()?;
        let mut operators = Vec::new();
        while self.eat(Kind::Pipe) {
            if operators.len() == self.options.max_pipeline_operators {
                return Err(Error::syntax(
                    "pipeline operator limit exceeded",
                    self.current().span,
                ));
            }
            operators.push(self.operator()?);
        }
        Ok(Pipeline { source, operators })
    }

    fn source(&mut self) -> Result<Source> {
        self.enter()?;
        let source = if self.eat(Kind::LParen) {
            let query = self.query()?;
            self.expect(Kind::RParen)?;
            Source::Subquery(Box::new(query))
        } else if self.eat_keyword("print") {
            Source::Print(self.named_list()?)
        } else if self.eat_keyword("datatable") {
            self.datatable()?
        } else if self.eat_keyword("range") {
            let name = self.identifier()?;
            self.expect_keyword("from")?;
            let start = self.expr(0)?;
            self.expect_keyword("to")?;
            let end = self.expr(0)?;
            self.expect_keyword("step")?;
            let step = self.expr(0)?;
            Source::Range {
                name,
                start: Box::new(start),
                end: Box::new(end),
                step: Box::new(step),
            }
        } else if self.eat_keyword("union") {
            let (kind, withsource, inputs) = self.union_inputs()?;
            Source::Union {
                kind,
                withsource,
                inputs,
            }
        } else if self.keyword("search") || self.keyword("find") {
            let find = self.eat_keyword("find");
            if !find {
                self.expect_keyword("search")?;
            }
            let sensitive = if !find { self.search_kind()? } else { false };
            let source = if find && self.eat_keyword("withsource") {
                self.expect(Kind::Assign)?;
                self.identifier()?
            } else {
                "source_".into()
            };
            let inputs = if self.eat_keyword("in") {
                self.expect(Kind::LParen)?;
                let mut inputs = vec![self.source()?];
                while self.eat(Kind::Comma) {
                    inputs.push(self.source()?);
                }
                self.expect(Kind::RParen)?;
                inputs
            } else {
                vec![Source::Pattern {
                    prefix: Vec::new(),
                    pattern: "*".into(),
                    span: self.current().span,
                }]
            };
            if find {
                self.eat_keyword("where");
            }
            let predicate = Box::new(self.expr(0)?);
            let mut project = None;
            let mut pack_all = false;
            if find && self.eat_keyword("project") {
                if self.eat(Kind::Minus) {
                    self.expect_keyword("smart")?;
                } else {
                    let mut columns = Vec::new();
                    loop {
                        if self.eat_keyword("pack_all") {
                            self.expect(Kind::LParen)?;
                            self.expect(Kind::RParen)?;
                            pack_all = true;
                            break;
                        }
                        let name = self.identifier()?;
                        let ty = if self.eat(Kind::Colon) {
                            Some(self.scalar_type()?)
                        } else {
                            None
                        };
                        columns.push((name, ty));
                        if !self.eat(Kind::Comma) {
                            break;
                        }
                    }
                    project = Some(columns);
                }
            }
            Source::Search {
                inputs,
                predicate,
                case_sensitive: sensitive,
                find: find.then_some(Find {
                    source,
                    project,
                    pack_all,
                }),
            }
        } else if matches!(
            self.current().kind,
            Kind::Ident("table" | "database" | "cluster")
        ) && matches!(
            self.tokens.get(self.pos + 1).map(|token| &token.kind),
            Some(Kind::LParen)
        ) {
            let start = self.current().span.start;
            let mut name = Vec::new();
            if self.eat_keyword("cluster") {
                name.push(self.source_selector()?);
                self.expect(Kind::Dot)?;
                self.expect_keyword("database")?;
                name.push(self.source_selector()?);
                self.expect(Kind::Dot)?;
            } else if self.eat_keyword("database") {
                name.push(self.source_selector()?);
                self.expect(Kind::Dot)?;
            }
            if self.eat_keyword("table") {
                name.push(self.source_selector()?);
            } else {
                name.push(self.identifier()?);
            }
            Source::Table {
                name,
                span: Span::new(start, self.tokens[self.pos - 1].span.end),
            }
        } else {
            let start = self.current().span.start;
            if self.at(&Kind::Star) {
                let pattern = self.pattern_one()?;
                Source::Pattern {
                    prefix: Vec::new(),
                    pattern,
                    span: Span::new(start, self.tokens[self.pos - 1].span.end),
                }
            } else {
                let mut name = vec![self.identifier()?];
                while self.eat(Kind::Dot) {
                    if self.at(&Kind::Star) {
                        name.push(String::new());
                        break;
                    }
                    name.push(self.identifier()?);
                }
                if self.at(&Kind::Star) {
                    let mut pattern = name.pop().ok_or_else(|| self.expected("table pattern"))?;
                    pattern.push_str(&self.pattern_one()?);
                    Source::Pattern {
                        prefix: name,
                        pattern,
                        span: Span::new(start, self.tokens[self.pos - 1].span.end),
                    }
                } else if name.len() == 1 && self.at(&Kind::LParen) {
                    Source::Call {
                        name: name.remove(0),
                        args: self.source_arguments()?,
                        span: Span::new(start, self.tokens[self.pos - 1].span.end),
                    }
                } else {
                    Source::Table {
                        name,
                        span: Span::new(start, self.tokens[self.pos - 1].span.end),
                    }
                }
            }
        };
        self.nesting -= 1;
        Ok(source)
    }

    fn source_selector(&mut self) -> Result<String> {
        self.expect(Kind::LParen)?;
        let token = self.advance();
        let Kind::String(value) = token.kind else {
            return Err(Error::syntax(
                "local table selectors require a literal string name",
                token.span,
            ));
        };
        if value.is_empty() {
            return Err(Error::syntax(
                "local table selector names cannot be empty",
                token.span,
            ));
        }
        self.expect(Kind::RParen)?;
        Ok(value.into_owned())
    }

    fn source_arguments(&mut self) -> Result<Vec<CallArgument>> {
        self.expect(Kind::LParen)?;
        let mut args = Vec::new();
        if !self.at(&Kind::RParen) {
            loop {
                let name = if matches!(self.current().kind, Kind::Ident(_))
                    && self
                        .tokens
                        .get(self.pos + 1)
                        .is_some_and(|t| t.kind == Kind::Assign)
                {
                    let name = self.identifier()?;
                    self.expect(Kind::Assign)?;
                    Some(name)
                } else {
                    None
                };
                let value = if self.looks_tabular() {
                    let parenthesized = self.eat(Kind::LParen);
                    let query = self.query()?;
                    if parenthesized {
                        self.expect(Kind::RParen)?;
                    }
                    ArgumentValue::Tabular(Box::new(query))
                } else {
                    ArgumentValue::Scalar(self.expr(0)?)
                };
                args.push(CallArgument { name, value });
                if !self.eat(Kind::Comma) {
                    break;
                }
            }
        }
        self.expect(Kind::RParen)?;
        Ok(args)
    }

    fn datatable(&mut self) -> Result<Source> {
        self.expect(Kind::LParen)?;
        let mut columns = Vec::new();
        loop {
            let name = self.identifier()?;
            self.expect(Kind::Colon)?;
            let span = self.current().span;
            let ty_name = self.identifier()?;
            let ty = ScalarType::parse(&ty_name)
                .ok_or_else(|| Error::unsupported(format!("datatable type '{ty_name}'"), span))?;
            columns.push((name, ty));
            if !self.eat(Kind::Comma) {
                break;
            }
        }
        self.expect(Kind::RParen)?;
        self.expect(Kind::LBracket)?;
        let mut values = Vec::new();
        if !self.at(&Kind::RBracket) {
            loop {
                values.push(self.expr(0)?);
                if !self.eat(Kind::Comma) || self.at(&Kind::RBracket) {
                    break;
                }
            }
        }
        self.expect(Kind::RBracket)?;
        if !values.len().is_multiple_of(columns.len()) {
            return Err(Error::syntax(
                format!(
                    "{} datatable values do not fill rows of {} columns",
                    values.len(),
                    columns.len()
                ),
                self.tokens[self.pos - 1].span,
            ));
        }
        Ok(Source::Datatable { columns, values })
    }

    fn union_inputs(&mut self) -> Result<(UnionKind, Option<String>, Vec<Source>)> {
        let mut kind = UnionKind::Outer;
        let mut withsource = None;
        while self.keyword("kind") || self.keyword("withsource") || self.keyword("isfuzzy") {
            let parameter = self.identifier()?;
            self.expect(Kind::Assign)?;
            let span = self.current().span;
            if parameter == "withsource" {
                if withsource.is_some() {
                    return Err(Error::syntax("duplicate withsource parameter", span));
                }
                withsource = Some(self.identifier()?);
            } else if parameter == "isfuzzy" {
                if self.eat_keyword("true") {
                    return Err(Error::unsupported(
                        "fuzzy unions require a warning result channel",
                        span,
                    ));
                }
                self.expect_keyword("false")?;
            } else {
                kind = match self.identifier()?.as_str() {
                    "outer" => UnionKind::Outer,
                    "inner" => UnionKind::Inner,
                    other => {
                        return Err(Error::unsupported(format!("union kind '{other}'"), span));
                    }
                };
            }
        }
        let mut inputs = vec![self.source()?];
        while self.eat(Kind::Comma) {
            inputs.push(self.source()?);
        }
        Ok((kind, withsource, inputs))
    }

    fn operator(&mut self) -> Result<SpannedOperator> {
        let start = self.current().span.start;
        let mut name = self.identifier()?;
        while self.eat(Kind::Minus) {
            name.push('-');
            name.push_str(&self.identifier()?);
        }
        let kind = match name.as_str() {
            "where" | "filter" => Operator::Where(self.expr(0)?),
            "search" => {
                let case_sensitive = self.search_kind()?;
                Operator::Search {
                    predicate: self.expr(0)?,
                    case_sensitive,
                }
            }
            "top-hitters" => {
                let count = self.expr(0)?;
                self.expect_keyword("of")?;
                let value = self.expr(0)?;
                let weight = if self.eat_keyword("by") {
                    Some(self.expr(0)?)
                } else {
                    None
                };
                Operator::TopHitters {
                    count,
                    value,
                    weight,
                }
            }
            "evaluate" => {
                let call = self.prefix()?;
                let ExprKind::Call { name, args } = call.kind else {
                    return Err(Error::syntax("evaluate expects a plugin call", call.span));
                };
                let schema = if self.eat(Kind::Colon) {
                    self.expect(Kind::LParen)?;
                    let include_input = self.eat(Kind::Star);
                    let mut columns = Vec::new();
                    let mut names = std::collections::HashSet::new();
                    if include_input && !self.at(&Kind::RParen) {
                        self.expect(Kind::Comma)?;
                    }
                    if !self.at(&Kind::RParen) {
                        loop {
                            let name = self.identifier()?;
                            if !names.insert(name.clone()) {
                                return Err(self.expected("unique output schema column names"));
                            }
                            self.expect(Kind::Colon)?;
                            columns.push((name, self.scalar_type()?));
                            if columns.len() > 1024 {
                                return Err(self.expected("at most 1024 plugin output columns"));
                            }
                            if !self.eat(Kind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Kind::RParen)?;
                    Some(OutputSchema {
                        include_input,
                        columns,
                    })
                } else {
                    None
                };
                Operator::Evaluate { name, args, schema }
            }
            "top-nested" => {
                let mut levels = Vec::new();
                loop {
                    let count = if self.keyword("of") {
                        None
                    } else {
                        Some(self.expr(0)?)
                    };
                    self.expect_keyword("of")?;
                    let value = self.named_expr()?;
                    let others = if self.eat_keyword("with") {
                        self.expect_keyword("others")?;
                        self.expect(Kind::Assign)?;
                        Some(self.expr(0)?)
                    } else {
                        None
                    };
                    self.expect_keyword("by")?;
                    let aggregate = self.named_expr()?;
                    let ascending = self.eat_keyword("asc");
                    if !ascending {
                        self.eat_keyword("desc");
                    }
                    levels.push(NestedTop {
                        count,
                        value,
                        others,
                        aggregate,
                        ascending,
                    });
                    if levels.len() > 32 {
                        return Err(self.expected("at most 32 top-nested levels"));
                    }
                    if !self.eat(Kind::Comma) {
                        break;
                    }
                    self.expect_keyword("top")?;
                    self.expect(Kind::Minus)?;
                    self.expect_keyword("nested")?;
                }
                Operator::TopNested(levels)
            }
            "take" | "limit" => Operator::Take(self.row_count_expression()?),
            "project" => Operator::Project(self.named_list()?),
            "extend" => Operator::Extend(self.named_list()?),
            "serialize" => Operator::Serialize(
                if matches!(
                    self.current().kind,
                    Kind::Pipe | Kind::Semi | Kind::RParen | Kind::RBrace | Kind::Eof
                ) {
                    Vec::new()
                } else {
                    self.named_list()?
                },
            ),
            "invoke" => {
                let name = self.identifier()?;
                let args = self.source_arguments()?;
                Operator::Invoke { name, args }
            }
            "scan" => {
                let match_id = if self.eat_keyword("with_match_id") {
                    self.expect(Kind::Assign)?;
                    Some(self.identifier()?)
                } else {
                    None
                };
                let mut declarations = Vec::new();
                if self.eat_keyword("declare") {
                    self.expect(Kind::LParen)?;
                    loop {
                        let name = self.identifier()?;
                        self.expect(Kind::Colon)?;
                        let ty = self.scalar_type()?;
                        let default = if self.eat(Kind::Assign) {
                            Some(self.expr(0)?)
                        } else {
                            None
                        };
                        declarations.push(ScanDeclaration { name, ty, default });
                        if declarations.len() > 128 {
                            return Err(self.expected("at most 128 scan declarations"));
                        }
                        if !self.eat(Kind::Comma) {
                            break;
                        }
                    }
                    self.expect(Kind::RParen)?;
                }
                self.expect_keyword("with")?;
                self.expect(Kind::LParen)?;
                let mut steps = Vec::new();
                loop {
                    self.expect_keyword("step")?;
                    let name = self.identifier()?;
                    let output = if self.eat_keyword("output") {
                        self.expect(Kind::Assign)?;
                        match self.identifier()?.as_str() {
                            "all" => ScanOutput::All,
                            "last" => ScanOutput::Last,
                            "none" => ScanOutput::None,
                            _ => return Err(self.expected("output=all, last, or none")),
                        }
                    } else {
                        ScanOutput::All
                    };
                    self.expect(Kind::Colon)?;
                    let condition = self.expr(0)?;
                    let mut assignments = Vec::new();
                    if self.eat(Kind::Assign) {
                        self.expect(Kind::Gt)?;
                        loop {
                            let name = self.identifier()?;
                            self.expect(Kind::Assign)?;
                            assignments.push((name, self.expr(0)?));
                            if assignments.len() > 128 {
                                return Err(self.expected("at most 128 scan assignments per step"));
                            }
                            if !self.eat(Kind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Kind::Semi)?;
                    steps.push(ScanStep {
                        name,
                        output,
                        condition,
                        assignments,
                    });
                    if steps.len() > 128 {
                        return Err(self.expected("at most 128 scan steps"));
                    }
                    if self.at(&Kind::RParen) {
                        break;
                    }
                }
                self.expect(Kind::RParen)?;
                Operator::Scan(Box::new(Scan {
                    match_id,
                    declarations,
                    steps,
                }))
            }
            "parse" | "parse-where" => {
                let kind = if self.eat_keyword("kind") {
                    self.expect(Kind::Assign)?;
                    let span = self.current().span;
                    match self.identifier()?.as_str() {
                        "simple" => ParseKind::Simple,
                        "regex" => ParseKind::Regex,
                        "relaxed" => ParseKind::Relaxed,
                        _ => {
                            return Err(Error::syntax(
                                "parse kind must be simple, regex, or relaxed",
                                span,
                            ));
                        }
                    }
                } else {
                    ParseKind::Simple
                };
                let flags = if self.eat_keyword("flags") {
                    if kind != ParseKind::Regex {
                        return Err(self.expected("kind=regex for flags"));
                    }
                    self.expect(Kind::Assign)?;
                    if let Kind::String(value) = self.current().kind.clone() {
                        self.advance();
                        value.into_owned()
                    } else {
                        self.identifier()?
                    }
                } else {
                    String::new()
                };
                let input = self.expr(0)?;
                self.expect_keyword("with")?;
                let mut pattern = Vec::new();
                let mut names = std::collections::HashSet::new();
                while !matches!(
                    self.current().kind,
                    Kind::Pipe | Kind::Semi | Kind::RParen | Kind::RBrace | Kind::Eof
                ) {
                    let part = match self.current().kind.clone() {
                        Kind::Star => {
                            self.advance();
                            ParsePart::Wildcard
                        }
                        Kind::String(value) => {
                            self.advance();
                            ParsePart::Literal(value.into_owned())
                        }
                        _ => {
                            let span = self.current().span;
                            let name = self.identifier()?;
                            if !names.insert(name.clone()) {
                                return Err(Error::syntax("duplicate parse column", span));
                            }
                            let ty = if self.eat(Kind::Colon) {
                                let name = self.identifier()?;
                                ScalarType::parse(&name).ok_or_else(|| {
                                    Error::unsupported(format!("parse type '{name}'"), span)
                                })?
                            } else {
                                ScalarType::String
                            };
                            if ty == ScalarType::Dynamic {
                                return Err(Error::unsupported(
                                    "parse dynamic captures; use parse_json after extracting a string",
                                    span,
                                ));
                            }
                            ParsePart::Capture { name, ty }
                        }
                    };
                    pattern.push(part);
                    if pattern.len() > 256 {
                        return Err(Error::syntax(
                            "parse pattern limit exceeded",
                            self.current().span,
                        ));
                    }
                }
                if pattern.is_empty() {
                    return Err(self.expected("a parse pattern"));
                }
                Operator::Parse {
                    input,
                    kind,
                    flags,
                    pattern,
                    filter: name == "parse-where",
                }
            }
            "partition" => {
                let mut strategy = PartitionStrategy::Legacy;
                if self.eat_keyword("hint") {
                    self.expect(Kind::Dot)?;
                    self.expect_keyword("strategy")?;
                    self.expect(Kind::Assign)?;
                    strategy = match self.identifier()?.as_str() {
                        "legacy" => PartitionStrategy::Legacy,
                        "native" => PartitionStrategy::Native,
                        "shuffle" => PartitionStrategy::Shuffle,
                        _ => return Err(self.expected("legacy, native, or shuffle")),
                    };
                }
                self.expect_keyword("by")?;
                let key = self.identifier()?;
                let operators = self.transformation()?;
                Operator::Partition {
                    key,
                    strategy,
                    operators,
                }
            }
            "mv-expand" | "mvexpand" | "mv-apply" => {
                let mut array_mode = false;
                let mut index = None;
                while self.keyword("kind")
                    || self.keyword("bagexpansion")
                    || self.keyword("with_itemindex")
                {
                    let parameter = self.identifier()?;
                    if name == "mv-apply" && parameter != "with_itemindex" {
                        return Err(
                            self.expected("with_itemindex; mv-apply has no bag expansion mode")
                        );
                    }
                    self.expect(Kind::Assign)?;
                    if parameter == "with_itemindex" {
                        if index.is_some() {
                            return Err(self.expected("only one with_itemindex"));
                        }
                        index = Some(self.identifier()?);
                    } else {
                        let span = self.current().span;
                        array_mode = match self.identifier()?.as_str() {
                            "bag" => false,
                            "array" => true,
                            _ => {
                                return Err(Error::syntax(
                                    "mv-expand kind must be bag or array",
                                    span,
                                ));
                            }
                        };
                    }
                }
                let mut columns = Vec::new();
                loop {
                    let value = self.named_expr()?;
                    let ty = if self.eat_keyword("to") {
                        self.expect_keyword("typeof")?;
                        self.expect(Kind::LParen)?;
                        let span = self.current().span;
                        let name = self.identifier()?;
                        let ty = ScalarType::parse(&name).ok_or_else(|| {
                            Error::unsupported(format!("scalar type '{name}'"), span)
                        })?;
                        self.expect(Kind::RParen)?;
                        Some(ty)
                    } else {
                        None
                    };
                    columns.push(ExpandColumn { value, ty });
                    if columns.len() > 128 {
                        return Err(Error::syntax(
                            "mv-expand column limit exceeded",
                            self.current().span,
                        ));
                    }
                    if !self.eat(Kind::Comma) {
                        break;
                    }
                }
                let limit = if self.eat_keyword("limit") {
                    self.row_count()?
                } else if name == "mvexpand" {
                    128
                } else {
                    2_147_483_647
                };
                if name == "mv-apply" {
                    self.expect_keyword("on")?;
                    Operator::MvApply {
                        index,
                        columns,
                        limit,
                        operators: self.transformation()?,
                    }
                } else {
                    Operator::MvExpand {
                        array_mode,
                        arrays_only: false,
                        index,
                        columns,
                        limit,
                    }
                }
            }
            "project-away" => Operator::ProjectAway(self.pattern_list()?),
            "project-keep" => Operator::ProjectKeep(self.pattern_list()?),
            "project-reorder" => {
                let mut columns = Vec::new();
                if !matches!(
                    self.current().kind,
                    Kind::Pipe | Kind::Semi | Kind::RParen | Kind::Eof
                ) {
                    loop {
                        let pattern = self.pattern_one()?;
                        let natural = self.eat_keyword("granny");
                        if natural {
                            self.expect(Kind::Minus)?;
                        }
                        let order = if self.eat_keyword("asc") {
                            Some((true, natural))
                        } else if self.eat_keyword("desc") {
                            Some((false, natural))
                        } else if natural {
                            return Err(self.expected("asc or desc after granny-"));
                        } else {
                            None
                        };
                        columns.push(ReorderColumn { pattern, order });
                        if !self.eat(Kind::Comma) {
                            break;
                        }
                    }
                }
                Operator::ProjectReorder(columns)
            }
            "getschema" => Operator::GetSchema,
            "as" => {
                let materialized = if self.eat_keyword("hint") {
                    self.expect(Kind::Dot)?;
                    self.expect_keyword("materialized")?;
                    self.expect(Kind::Assign)?;
                    if self.eat_keyword("true") {
                        true
                    } else {
                        self.expect_keyword("false")?;
                        false
                    }
                } else {
                    false
                };
                let name = self.identifier()?;
                if materialized {
                    Operator::AsMaterialized(name)
                } else {
                    Operator::As(name)
                }
            }
            "sample" => Operator::Sample {
                count: self.row_count_expression()?,
                distinct: None,
            },
            "sample-distinct" => {
                let count = self.row_count_expression()?;
                self.expect_keyword("of")?;
                Operator::Sample {
                    count,
                    distinct: Some(self.expr(0)?),
                }
            }
            "project-rename" => {
                let mut renames = Vec::new();
                loop {
                    let new = self.identifier()?;
                    self.expect(Kind::Assign)?;
                    let old = self.identifier()?;
                    renames.push((new, old));
                    if !self.eat(Kind::Comma) {
                        break;
                    }
                }
                Operator::ProjectRename(renames)
            }
            "summarize" => {
                let aggregates = if self.keyword("by") {
                    Vec::new()
                } else {
                    self.named_list()?
                };
                let by = if self.eat_keyword("by") {
                    self.named_list()?
                } else {
                    Vec::new()
                };
                Operator::Summarize { aggregates, by }
            }
            "make-series" => {
                let nonempty = if self.eat_keyword("kind") {
                    self.expect(Kind::Assign)?;
                    self.expect_keyword("nonempty")?;
                    true
                } else {
                    false
                };
                let mut aggregates = Vec::new();
                loop {
                    let value = self.named_expr()?;
                    let default = if self.eat_keyword("default") {
                        self.expect(Kind::Assign)?;
                        Some(self.expr(0)?)
                    } else {
                        None
                    };
                    aggregates.push(SeriesAggregation { value, default });
                    if !self.eat(Kind::Comma) {
                        break;
                    }
                }
                self.expect_keyword("on")?;
                let axis = self.expr(4)?;
                let (from, to, step, inclusive, align_to_start) = if self.eat_keyword("in") {
                    self.expect_keyword("range")?;
                    self.expect(Kind::LParen)?;
                    let from = self.expr(0)?;
                    self.expect(Kind::Comma)?;
                    let to = self.expr(0)?;
                    self.expect(Kind::Comma)?;
                    let step = self.expr(0)?;
                    self.expect(Kind::RParen)?;
                    (Some(from), Some(to), step, true, false)
                } else {
                    let from = if self.eat_keyword("from") {
                        Some(self.expr(0)?)
                    } else {
                        None
                    };
                    let to = if self.eat_keyword("to") {
                        Some(self.expr(0)?)
                    } else {
                        None
                    };
                    self.expect_keyword("step")?;
                    let step = self.expr(0)?;
                    (from, to, step, false, true)
                };
                let by = if self.eat_keyword("by") {
                    self.named_list()?
                } else {
                    Vec::new()
                };
                Operator::MakeSeries(Box::new(MakeSeries {
                    aggregates,
                    axis,
                    from,
                    to,
                    step,
                    by,
                    nonempty,
                    inclusive,
                    align_to_start,
                }))
            }
            "sort" | "order" => {
                self.expect_keyword("by")?;
                let mut keys = vec![self.sort_key()?];
                while self.eat(Kind::Comma) {
                    keys.push(self.sort_key()?);
                }
                Operator::Sort(keys)
            }
            "top" => {
                let count = self.row_count_expression()?;
                self.expect_keyword("by")?;
                Operator::Top {
                    count,
                    key: self.sort_key()?,
                }
            }
            "distinct" => {
                let mut expressions = vec![self.expr(0)?];
                while self.eat(Kind::Comma) {
                    expressions.push(self.expr(0)?);
                }
                Operator::Distinct(expressions)
            }
            "count" => Operator::Count,
            "join" => self.join(false)?,
            "lookup" => self.join(true)?,
            "union" => {
                let (kind, withsource, inputs) = self.union_inputs()?;
                Operator::Union {
                    kind,
                    withsource,
                    inputs,
                }
            }
            _ => {
                return Err(Error::unsupported(
                    format!("tabular operator '{name}'"),
                    Span::new(start, self.tokens[self.pos - 1].span.end),
                ));
            }
        };
        Ok(SpannedOperator {
            kind,
            span: Span::new(start, self.tokens[self.pos - 1].span.end),
        })
    }

    fn row_count(&mut self) -> Result<usize> {
        let token = self.advance();
        match token.kind {
            Kind::Number(text) => text
                .parse()
                .map_err(|_| Error::syntax("row count must be a nonnegative integer", token.span)),
            _ => Err(Error::unsupported(
                "take/top currently require a literal nonnegative row count",
                token.span,
            )),
        }
    }

    fn row_count_expression(&mut self) -> Result<Expr> {
        let expr = self.expr(0)?;
        if matches!(
            &expr.kind,
            ExprKind::Literal(Literal::Real(_))
                | ExprKind::Unary {
                    op: UnaryOp::Negate,
                    ..
                }
        ) {
            return Err(Error::syntax(
                "row count must be a nonnegative integer",
                expr.span,
            ));
        }
        Ok(expr)
    }

    fn pattern_list(&mut self) -> Result<Vec<String>> {
        let mut patterns = Vec::new();
        loop {
            let pattern = self.pattern_one()?;
            patterns.push(pattern);
            if !self.eat(Kind::Comma) {
                break;
            }
        }
        Ok(patterns)
    }

    fn pattern_one(&mut self) -> Result<String> {
        let mut pattern = if self.at(&Kind::Star) {
            String::new()
        } else {
            self.identifier()?
        };
        while self.eat(Kind::Star) {
            pattern.push('*');
            if let Kind::Ident(text) = self.current().kind
                && self.current().span.start == self.tokens[self.pos - 1].span.end
            {
                pattern.push_str(text);
                self.advance();
            }
        }
        Ok(pattern)
    }

    fn named_list(&mut self) -> Result<Vec<NamedExpr>> {
        let mut expressions = vec![self.named_expr()?];
        while self.eat(Kind::Comma) {
            expressions.push(self.named_expr()?);
        }
        Ok(expressions)
    }

    fn named_expr(&mut self) -> Result<NamedExpr> {
        let alias = match &self.current().kind {
            Kind::Ident(_) => self
                .tokens
                .get(self.pos + 1)
                .is_some_and(|t| t.kind == Kind::Assign),
            Kind::LBracket => self
                .tokens
                .get(self.pos + 3)
                .is_some_and(|t| t.kind == Kind::Assign),
            _ => false,
        };
        let name = if alias {
            let name = self.identifier()?;
            self.expect(Kind::Assign)?;
            Some(name)
        } else {
            None
        };
        Ok(NamedExpr {
            name,
            expr: self.expr(0)?,
        })
    }

    fn sort_key(&mut self) -> Result<SortKey> {
        let expr = self.expr(0)?;
        let ascending = if self.eat_keyword("asc") {
            true
        } else {
            self.eat_keyword("desc");
            false
        };
        let nulls_first = if self.eat_keyword("nulls") {
            if self.eat_keyword("first") {
                true
            } else {
                self.expect_keyword("last")?;
                false
            }
        } else {
            ascending
        };
        Ok(SortKey {
            expr,
            ascending,
            nulls_first,
        })
    }

    fn join(&mut self, lookup: bool) -> Result<Operator> {
        let kind = if self.eat_keyword("kind") {
            self.expect(Kind::Assign)?;
            let span = self.current().span;
            match self.identifier()?.as_str() {
                "innerunique" => JoinKind::InnerUnique,
                "inner" => JoinKind::Inner,
                "leftouter" => JoinKind::LeftOuter,
                "rightouter" => JoinKind::RightOuter,
                "fullouter" => JoinKind::FullOuter,
                "leftsemi" => JoinKind::LeftSemi,
                "rightsemi" => JoinKind::RightSemi,
                "leftanti" | "anti" | "leftantisemi" => JoinKind::LeftAnti,
                "rightanti" | "rightantisemi" => JoinKind::RightAnti,
                other => {
                    return Err(Error::unsupported(format!("join kind '{other}'"), span));
                }
            }
        } else {
            if lookup {
                JoinKind::LeftOuter
            } else {
                JoinKind::InnerUnique
            }
        };
        if lookup && !matches!(kind, JoinKind::LeftOuter | JoinKind::Inner) {
            return Err(Error::syntax(
                "lookup kind must be leftouter or inner",
                self.current().span,
            ));
        }
        let input = if self.eat(Kind::LParen) {
            let input = self.query()?;
            self.expect(Kind::RParen)?;
            input
        } else {
            Query {
                bindings: Vec::new(),
                pipeline: Pipeline {
                    source: self.source()?,
                    operators: Vec::new(),
                },
            }
        };
        self.expect_keyword("on")?;
        let mut keys = Vec::new();
        loop {
            let span = self.current().span;
            let left = self.path()?;
            let key = if self.eat(Kind::Eq) {
                let right = self.path()?;
                if left.len() != 2 || left[0] != "$left" || right.len() != 2 || right[0] != "$right"
                {
                    return Err(Error::unsupported(
                        "join comparisons must be $left.Column == $right.Column",
                        span,
                    ));
                }
                JoinKey {
                    left: left[1].clone(),
                    right: right[1].clone(),
                }
            } else if left.len() == 1 {
                JoinKey {
                    left: left[0].clone(),
                    right: left[0].clone(),
                }
            } else {
                return Err(Error::syntax("expected a join key", span));
            };
            keys.push(key);
            if !self.eat(Kind::Comma) && !self.eat_keyword("and") {
                break;
            }
        }
        Ok(if lookup {
            Operator::Lookup {
                kind,
                input: Box::new(input),
                keys,
            }
        } else {
            Operator::Join {
                kind,
                input: Box::new(input),
                keys,
            }
        })
    }

    fn node(&self, kind: ExprKind, span: Span, depth: usize) -> Result<Expr> {
        if depth > self.options.max_nesting {
            return Err(Error::syntax("expression depth limit exceeded", span));
        }
        Ok(Expr { kind, span, depth })
    }

    fn expr(&mut self, minimum: u8) -> Result<Expr> {
        self.enter()?;
        let mut left = self.prefix()?;
        loop {
            if minimum <= 3 && self.eat(Kind::Colon) {
                let column = match &left.kind {
                    ExprKind::Identifier(parts) if parts.len() == 1 => Some(parts[0].clone()),
                    ExprKind::Wildcard => None,
                    _ => return Err(self.expected("a search column name before ':'")),
                };
                let token = self.advance();
                let Kind::String(term) = token.kind else {
                    return Err(Error::syntax(
                        "search terms after ':' must be strings",
                        token.span,
                    ));
                };
                left = self.node(
                    ExprKind::SearchTerm {
                        column,
                        term: term.into_owned(),
                    },
                    Span::new(left.span.start, token.span.end),
                    1,
                )?;
                continue;
            }
            if self.eat(Kind::LBracket) {
                let index = self.expr(0)?;
                self.expect(Kind::RBracket)?;
                let span = Span::new(left.span.start, self.tokens[self.pos - 1].span.end);
                let depth = left.depth.max(index.depth) + 1;
                left = self.node(
                    ExprKind::Access {
                        expr: Box::new(left),
                        index: Box::new(index),
                    },
                    span,
                    depth,
                )?;
                continue;
            }
            if self.eat(Kind::Dot) {
                let start = self.current().span.start;
                let name = self.identifier()?;
                let index = self.node(
                    ExprKind::Literal(Literal::String(name)),
                    Span::new(start, self.tokens[self.pos - 1].span.end),
                    1,
                )?;
                let span = Span::new(left.span.start, index.span.end);
                let depth = left.depth + 1;
                left = self.node(
                    ExprKind::Access {
                        expr: Box::new(left),
                        index: Box::new(index),
                    },
                    span,
                    depth,
                )?;
                continue;
            }
            let negated = self.at(&Kind::Bang);
            let next = if negated { self.pos + 1 } else { self.pos };
            let keyword = match self.tokens.get(next).map(|t| &t.kind) {
                Some(Kind::Ident(word)) => Some(*word),
                _ => None,
            };
            if minimum <= 3 && matches!(keyword, Some("has_any" | "has_all")) {
                if negated {
                    self.advance();
                }
                let all = self.eat_keyword("has_all");
                if !all {
                    self.expect_keyword("has_any")?;
                }
                self.expect(Kind::LParen)?;
                let mut values = Vec::new();
                let tabular = if self.looks_tabular() {
                    Some(Box::new(self.query()?))
                } else {
                    loop {
                        values.push(self.expr(0)?);
                        if !self.eat(Kind::Comma) {
                            break;
                        }
                    }
                    None
                };
                self.expect(Kind::RParen)?;
                let span = Span::new(left.span.start, self.tokens[self.pos - 1].span.end);
                let depth = left
                    .depth
                    .max(values.iter().map(|v| v.depth).max().unwrap_or(0))
                    + 1;
                left = self.node(
                    ExprKind::TermSet {
                        expr: Box::new(left),
                        values,
                        tabular,
                        all,
                    },
                    span,
                    depth,
                )?;
                if negated {
                    left = self.node(
                        ExprKind::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(left),
                        },
                        span,
                        depth + 1,
                    )?;
                }
                continue;
            }
            if minimum <= 3 && matches!(keyword, Some("in" | "between")) {
                if negated {
                    self.advance();
                }
                self.advance();
                let start = left.span.start;
                let depth = left.depth;
                if keyword == Some("in") {
                    let case_insensitive = self.eat(Kind::Tilde);
                    self.expect(Kind::LParen)?;
                    if self.looks_tabular() {
                        let query = self.query()?;
                        self.expect(Kind::RParen)?;
                        left = self.node(
                            ExprKind::InTabular {
                                expr: Box::new(left),
                                query: Box::new(query),
                                negated,
                                case_insensitive,
                            },
                            Span::new(start, self.tokens[self.pos - 1].span.end),
                            depth + 1,
                        )?;
                        continue;
                    }
                    let mut values = Vec::new();
                    if !self.at(&Kind::RParen) {
                        loop {
                            values.push(self.expr(0)?);
                            if !self.eat(Kind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Kind::RParen)?;
                    let depth = depth.max(values.iter().map(|v| v.depth).max().unwrap_or(0));
                    left = self.node(
                        ExprKind::InList {
                            expr: Box::new(left),
                            values,
                            negated,
                            case_insensitive,
                        },
                        Span::new(start, self.tokens[self.pos - 1].span.end),
                        depth + 1,
                    )?;
                } else {
                    self.expect(Kind::LParen)?;
                    let low = self.expr(0)?;
                    self.expect(Kind::DotDot)?;
                    let high = self.expr(0)?;
                    self.expect(Kind::RParen)?;
                    let depth = depth.max(low.depth).max(high.depth) + 1;
                    left = self.node(
                        ExprKind::Between {
                            expr: Box::new(left),
                            low: Box::new(low),
                            high: Box::new(high),
                            negated,
                        },
                        Span::new(start, self.tokens[self.pos - 1].span.end),
                        depth,
                    )?;
                }
                continue;
            }
            let Some((op, precedence, tokens)) = self.binary() else {
                break;
            };
            if precedence < minimum {
                break;
            }
            for _ in 0..tokens {
                self.advance();
            }
            let right = self.expr(precedence + 1)?;
            let span = Span::new(left.span.start, right.span.end);
            let depth = left.depth.max(right.depth) + 1;
            left = self.node(
                ExprKind::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
                span,
                depth,
            )?;
        }
        self.nesting -= 1;
        Ok(left)
    }

    fn search_kind(&mut self) -> Result<bool> {
        if !self.eat_keyword("kind") {
            return Ok(false);
        }
        self.expect(Kind::Assign)?;
        match self.identifier()?.as_str() {
            "case_sensitive" => Ok(true),
            "case_insensitive" | "default" => Ok(false),
            _ => Err(self.expected("default, case_sensitive, or case_insensitive")),
        }
    }

    fn transformation(&mut self) -> Result<Vec<SpannedOperator>> {
        self.enter()?;
        self.expect(Kind::LParen)?;
        let mut operators = vec![self.operator()?];
        while self.eat(Kind::Pipe) {
            if operators.len() >= self.options.max_pipeline_operators {
                return Err(self.expected("fewer subquery operators"));
            }
            operators.push(self.operator()?);
        }
        self.expect(Kind::RParen)?;
        self.nesting -= 1;
        Ok(operators)
    }

    fn prefix(&mut self) -> Result<Expr> {
        let token = self.advance();
        let span = token.span;
        let kind = match token.kind {
            Kind::Number(text) => {
                let literal = if text.starts_with("0x") || text.starts_with("0X") {
                    Literal::Integer(crate::literals::integer(text, span)?)
                } else if text.contains(['.', 'e', 'E']) {
                    let value: f64 = text
                        .parse()
                        .map_err(|_| Error::syntax("invalid real literal", span))?;
                    if !value.is_finite() {
                        return Err(Error::syntax("real literal is out of range", span));
                    }
                    Literal::Real(value)
                } else {
                    Literal::Integer(crate::literals::integer(text, span)?)
                };
                ExprKind::Literal(literal)
            }
            Kind::String(text) => ExprKind::Literal(Literal::String(text.into_owned())),
            Kind::Timespan(text) => ExprKind::Literal(Literal::Timespan(text.to_owned())),
            Kind::Typed { ty, value } => ExprKind::Literal(Literal::Typed {
                ty: ScalarType::parse(ty).expect("lexer only produces known literal types"),
                value: value.to_owned(),
            }),
            Kind::Ident("true") => ExprKind::Literal(Literal::Bool(true)),
            Kind::Ident("false") => ExprKind::Literal(Literal::Bool(false)),
            Kind::Ident("null") => ExprKind::Literal(Literal::Null),
            Kind::Ident("typeof") => {
                self.expect(Kind::LParen)?;
                let token = self.current().span;
                let name = self.identifier()?;
                let ty = ScalarType::parse(&name)
                    .ok_or_else(|| Error::unsupported(format!("scalar type '{name}'"), token))?;
                self.expect(Kind::RParen)?;
                ExprKind::Type(ty)
            }
            Kind::Ident("toscalar") => {
                let mut args = self.source_arguments()?;
                if args.len() != 1 || args[0].name.is_some() {
                    return Err(Error::syntax("toscalar expects one unnamed argument", span));
                }
                let value = args.remove(0).value;
                return self.node(
                    ExprKind::ToScalar(Box::new(value)),
                    Span::new(span.start, self.tokens[self.pos - 1].span.end),
                    1,
                );
            }
            Kind::Ident("dynamic") => {
                self.expect(Kind::LParen)?;
                self.dynamic_depth += 1;
                let value = self.expr(0)?;
                self.dynamic_depth -= 1;
                self.expect(Kind::RParen)?;
                let depth = value.depth + 1;
                return self.node(
                    ExprKind::Dynamic(Box::new(value)),
                    Span::new(span.start, self.tokens[self.pos - 1].span.end),
                    depth,
                );
            }
            Kind::Ident(name) => {
                let name = name.to_owned();
                if self.eat(Kind::LParen) {
                    let mut args = Vec::new();
                    if !self.at(&Kind::RParen) {
                        loop {
                            let argument = if matches!(name.as_str(), "bool" | "boolean")
                                && matches!(self.current().kind,Kind::Ident(word) if word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("false"))
                            {
                                let token = self.advance();
                                let Kind::Ident(word) = token.kind else {
                                    unreachable!("boolean token was checked");
                                };
                                self.node(
                                    ExprKind::Literal(Literal::Bool(
                                        word.eq_ignore_ascii_case("true"),
                                    )),
                                    token.span,
                                    1,
                                )?
                            } else if matches!(self.current().kind, Kind::Ident(_))
                                && self
                                    .tokens
                                    .get(self.pos + 1)
                                    .is_some_and(|t| t.kind == Kind::Assign)
                            {
                                let start = self.current().span.start;
                                let name = self.identifier()?;
                                self.expect(Kind::Assign)?;
                                let value = self.expr(0)?;
                                self.node(
                                    ExprKind::NamedArgument {
                                        name,
                                        value: Box::new(value.clone()),
                                    },
                                    Span::new(start, value.span.end),
                                    value.depth + 1,
                                )?
                            } else {
                                self.expr(0)?
                            };
                            args.push(argument);
                            if !self.eat(Kind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Kind::RParen)?;
                    let depth = args.iter().map(|a| a.depth).max().unwrap_or(0) + 1;
                    return self.node(
                        ExprKind::Call { name, args },
                        Span::new(span.start, self.tokens[self.pos - 1].span.end),
                        depth,
                    );
                }
                let mut parts = vec![name];
                while self.eat(Kind::Dot) {
                    parts.push(self.identifier()?);
                }
                ExprKind::Identifier(parts)
            }
            Kind::LBracket if self.dynamic_depth > 0 => {
                let mut items = Vec::new();
                if !self.at(&Kind::RBracket) {
                    loop {
                        items.push(self.expr(0)?);
                        if !self.eat(Kind::Comma) || self.at(&Kind::RBracket) {
                            break;
                        }
                    }
                }
                self.expect(Kind::RBracket)?;
                let depth = items.iter().map(|e| e.depth).max().unwrap_or(0) + 1;
                return self.node(
                    ExprKind::Array(items),
                    Span::new(span.start, self.tokens[self.pos - 1].span.end),
                    depth,
                );
            }
            Kind::LBrace if self.dynamic_depth > 0 => {
                let mut items = Vec::new();
                if !self.at(&Kind::RBrace) {
                    loop {
                        let token = self.advance();
                        let Kind::String(name) = token.kind else {
                            return Err(Error::syntax(
                                "dynamic object keys must be strings",
                                token.span,
                            ));
                        };
                        let name = name.into_owned();
                        self.expect(Kind::Colon)?;
                        items.push((name, self.expr(0)?));
                        if !self.eat(Kind::Comma) || self.at(&Kind::RBrace) {
                            break;
                        }
                    }
                }
                self.expect(Kind::RBrace)?;
                let depth = items.iter().map(|(_, e)| e.depth).max().unwrap_or(0) + 1;
                return self.node(
                    ExprKind::Object(items),
                    Span::new(span.start, self.tokens[self.pos - 1].span.end),
                    depth,
                );
            }
            Kind::LBracket => {
                self.pos -= 1;
                let parts = self.path()?;
                return self.node(
                    ExprKind::Identifier(parts),
                    Span::new(span.start, self.tokens[self.pos - 1].span.end),
                    1,
                );
            }
            Kind::LParen => {
                let mut expr = self.expr(0)?;
                self.expect(Kind::RParen)?;
                expr.span = Span::new(span.start, self.tokens[self.pos - 1].span.end);
                return Ok(expr);
            }
            Kind::Minus | Kind::Plus | Kind::Bang => {
                if matches!(token.kind, Kind::Minus)
                    && matches!(self.current().kind, Kind::Number(text) if text=="9223372036854775808" ||
                        text.strip_prefix("0x").or_else(||text.strip_prefix("0X"))
                            .and_then(|hex|u64::from_str_radix(hex,16).ok())==Some(1_u64<<63))
                {
                    let end = self.advance().span.end;
                    return self.node(
                        ExprKind::Literal(Literal::Integer(i64::MIN)),
                        Span::new(span.start, end),
                        1,
                    );
                }
                let op = match token.kind {
                    Kind::Minus => UnaryOp::Negate,
                    Kind::Plus => UnaryOp::Plus,
                    _ => UnaryOp::Not,
                };
                let expr = self.expr(6)?;
                let end = expr.span.end;
                let depth = expr.depth + 1;
                return self.node(
                    ExprKind::Unary {
                        op,
                        expr: Box::new(expr),
                    },
                    Span::new(span.start, end),
                    depth,
                );
            }
            Kind::Star => ExprKind::Wildcard,
            _ => return Err(Error::syntax("expected a scalar expression", span)),
        };
        self.node(kind, span, 1)
    }

    fn binary(&self) -> Option<(BinaryOp, u8, usize)> {
        use BinaryOp as B;
        let direct = match self.current().kind {
            Kind::Plus => Some((B::Add, 4)),
            Kind::Minus => Some((B::Subtract, 4)),
            Kind::Star => Some((B::Multiply, 5)),
            Kind::Slash => Some((B::Divide, 5)),
            Kind::Percent => Some((B::Modulo, 5)),
            Kind::Eq => Some((B::Eq, 3)),
            Kind::NotEq => Some((B::NotEq, 3)),
            Kind::EqInsensitive => Some((B::EqInsensitive, 3)),
            Kind::NotEqInsensitive => Some((B::NotEqInsensitive, 3)),
            Kind::Lt => Some((B::Lt, 3)),
            Kind::LtEq => Some((B::LtEq, 3)),
            Kind::Gt => Some((B::Gt, 3)),
            Kind::GtEq => Some((B::GtEq, 3)),
            _ => None,
        };
        if let Some((op, precedence)) = direct {
            return Some((op, precedence, 1));
        }
        let negated = self.at(&Kind::Bang);
        let at = self.pos + usize::from(negated);
        let Kind::Ident(word) = self.tokens.get(at)?.kind else {
            return None;
        };
        let op = match (word, negated) {
            ("or", false) => return Some((B::Or, 1, 1)),
            ("and", false) => return Some((B::And, 2, 1)),
            ("contains", false) => B::Contains,
            ("contains", true) => B::NotContains,
            ("contains_cs", false) => B::ContainsCs,
            ("contains_cs", true) => B::NotContainsCs,
            ("has", false) => B::Has,
            ("has", true) => B::NotHas,
            ("has_cs", false) => B::HasCs,
            ("hasprefix", false) => B::HasPrefix,
            ("hasprefix", true) => B::NotHasPrefix,
            ("hasprefix_cs", false) => B::HasPrefixCs,
            ("hasprefix_cs", true) => B::NotHasPrefixCs,
            ("hassuffix", false) => B::HasSuffix,
            ("hassuffix", true) => B::NotHasSuffix,
            ("hassuffix_cs", false) => B::HasSuffixCs,
            ("hassuffix_cs", true) => B::NotHasSuffixCs,
            ("has_cs", true) => B::NotHasCs,
            ("startswith", false) => B::StartsWith,
            ("startswith", true) => B::NotStartsWith,
            ("startswith_cs", false) => B::StartsWithCs,
            ("startswith_cs", true) => B::NotStartsWithCs,
            ("endswith", false) => B::EndsWith,
            ("endswith", true) => B::NotEndsWith,
            ("endswith_cs", false) => B::EndsWithCs,
            ("endswith_cs", true) => B::NotEndsWithCs,
            ("matches", false) if matches!(self.tokens.get(at + 1)?.kind, Kind::Ident("regex")) => {
                return Some((B::MatchesRegex, 3, 2));
            }
            _ => return None,
        };
        Some((op, 3, 1 + usize::from(negated)))
    }
}
