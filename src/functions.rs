use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::lit;

use crate::ast::{
    self, BindingValue, ExprKind, FunctionBody, FunctionParameter, ParameterType, Span,
};
use crate::{Error, QueryParameters, Result};

pub(crate) type Functions = HashMap<String, Arc<BoundFunction>>;

pub(crate) struct BoundFunction {
    pub definition: ast::Function,
    pub scalars: HashMap<String, Expr>,
    pub tables: HashMap<String, LogicalPlan>,
    pub functions: Functions,
    pub values: HashMap<String, ast::Expr>,
    tabular: bool,
    deferred: bool,
}

impl BoundFunction {
    pub fn new(
        definition: ast::Function,
        scalars: HashMap<String, Expr>,
        tables: HashMap<String, LogicalPlan>,
        functions: Functions,
        values: HashMap<String, ast::Expr>,
    ) -> Self {
        let mut symbols = HashMap::new();
        symbols.extend(
            scalars
                .keys()
                .chain(values.keys())
                .map(|name| (name.clone(), false)),
        );
        symbols.extend(tables.keys().map(|name| (name.clone(), true)));
        symbols.extend(
            functions
                .iter()
                .map(|(name, function)| (name.clone(), function.tabular)),
        );
        let tabular = infer_tabular(&definition, &symbols);
        let deferred = query_planning(&definition, &functions, &symbols);
        Self {
            definition,
            scalars,
            tables,
            functions,
            values,
            tabular,
            deferred,
        }
    }

    pub fn tabular(&self) -> bool {
        self.tabular
    }
}

pub(crate) fn arguments(
    parameters: &[FunctionParameter],
    names: &[Option<&str>],
    span: Span,
) -> Result<Vec<Option<usize>>> {
    let mut result = vec![None; parameters.len()];
    let mut named = false;
    for (index, name) in names.iter().enumerate() {
        let target = if let Some(name) = name {
            named = true;
            parameters
                .iter()
                .position(|parameter| parameter.name == *name)
                .ok_or_else(|| Error::syntax(format!("unknown argument '{name}'"), span))?
        } else {
            if named {
                return Err(Error::syntax(
                    "positional arguments must precede named arguments",
                    span,
                ));
            }
            index
        };
        let slot = result
            .get_mut(target)
            .ok_or_else(|| Error::syntax("too many function arguments", span))?;
        if slot.replace(index).is_some() {
            return Err(Error::syntax("duplicate function argument", span));
        }
    }
    for (parameter, value) in parameters.iter().zip(&result) {
        if value.is_none() && parameter.default.is_none() {
            return Err(Error::syntax(
                format!("missing function argument '{}'", parameter.name),
                span,
            ));
        }
    }
    Ok(result)
}

#[derive(Clone)]
struct Scope {
    values: HashMap<String, ast::Expr>,
    functions: Functions,
}

pub(crate) struct Expander<'a> {
    pub functions: &'a Functions,
    pub scalars: &'a mut HashMap<String, Expr>,
    pub parameters: &'a QueryParameters,
    pub next: &'a mut usize,
    pub schema: &'a DFSchema,
    deferred: &'a mut Functions,
    nodes: usize,
}

impl<'a> Expander<'a> {
    pub fn new(
        functions: &'a Functions,
        scalars: &'a mut HashMap<String, Expr>,
        parameters: &'a QueryParameters,
        next: &'a mut usize,
        schema: &'a DFSchema,
        deferred: &'a mut Functions,
    ) -> Self {
        Self {
            functions,
            scalars,
            parameters,
            next,
            schema,
            deferred,
            nodes: 0,
        }
    }

    fn capture(&mut self, value: Expr, span: Span) -> ast::Expr {
        let name = loop {
            let name = format!("__kql_function_capture_{}", self.next);
            *self.next += 1;
            if !self.scalars.contains_key(&name) {
                break name;
            }
        };
        self.scalars.insert(name.clone(), value);
        ast::Expr {
            kind: ExprKind::BindingReference(name),
            span,
            depth: 1,
        }
    }

    pub fn expression(&mut self, expr: &ast::Expr) -> Result<ast::Expr> {
        self.expand(expr, None, 0)
    }

    pub fn named(&mut self, items: &[ast::NamedExpr]) -> Result<Vec<ast::NamedExpr>> {
        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let expr = self.expression(&item.expr)?;
                Ok(ast::NamedExpr {
                    name: item.name.clone().or_else(|| {
                        let original = crate::planner::default_name(&item.expr, index);
                        (original != crate::planner::default_name(&expr, index)).then_some(original)
                    }),
                    expr,
                })
            })
            .collect()
    }

    pub fn operator(&mut self, operator: &ast::Operator) -> Result<ast::Operator> {
        let mut expanded = operator.map_expressions(|expr| self.expression(expr))?;
        match (operator, &mut expanded) {
            (ast::Operator::Project(before), ast::Operator::Project(after))
            | (ast::Operator::Extend(before), ast::Operator::Extend(after))
            | (ast::Operator::Serialize(before), ast::Operator::Serialize(after)) => {
                preserve_names(before, after);
            }
            (
                ast::Operator::Summarize {
                    aggregates: before,
                    by: before_by,
                },
                ast::Operator::Summarize {
                    aggregates: after,
                    by: after_by,
                },
            ) => {
                preserve_names(before, after);
                preserve_names(before_by, after_by);
            }
            _ => {}
        }
        Ok(expanded)
    }

    fn expand(
        &mut self,
        expr: &ast::Expr,
        scope: Option<&Scope>,
        depth: usize,
    ) -> Result<ast::Expr> {
        self.nodes += 1;
        if depth >= 128 || self.nodes > 16_384 {
            return Err(Error::syntax(
                "expanded function exceeds expression limits",
                expr.span,
            ));
        }
        if let ExprKind::Identifier(parts) = &expr.kind {
            if let Some(scope) = scope
                && let Some(value) = scope.values.get(&parts[0])
            {
                let mut value = value.clone();
                for property in &parts[1..] {
                    value = ast::Expr {
                        kind: ExprKind::Access {
                            expr: Box::new(value),
                            index: Box::new(ast::Expr {
                                kind: ExprKind::Literal(ast::Literal::String(property.clone())),
                                span: expr.span,
                                depth: 1,
                            }),
                        },
                        span: expr.span,
                        depth: expr.depth,
                    };
                }
                self.check_expanded(&value, depth)?;
                return Ok(value);
            }
            let functions = scope.map_or(self.functions, |scope| &scope.functions);
            if parts.len() == 1
                && (scope.is_some()
                    || self
                        .schema
                        .index_of_column_by_name(None, &parts[0])
                        .is_none())
                && let Some(function) = functions.get(&parts[0])
                && function.definition.parameters.is_empty()
            {
                return self.call(Arc::clone(function), &[], scope, expr.span, depth + 1);
            }
            if scope.is_some() {
                return Err(Error::syntax(
                    format!("unknown value '{}' in function scope", parts[0]),
                    expr.span,
                ));
            }
            return Ok(expr.clone());
        }
        if let ExprKind::Call { name, args } = &expr.kind {
            let functions = scope.map_or(self.functions, |scope| &scope.functions);
            if let Some(function) = functions.get(name) {
                return self.call(Arc::clone(function), args, scope, expr.span, depth + 1);
            }
        }
        if let ExprKind::TermSet {
            expr: value,
            values,
            tabular,
            all,
        } = &expr.kind
        {
            let tabular_values = values.len() == 1
                && match &values[0].kind {
                    ExprKind::Call { name, .. } => scope
                        .map_or(self.functions, |scope| &scope.functions)
                        .get(name)
                        .is_some_and(|function| function.tabular()),
                    ExprKind::Identifier(name) if name.len() == 1 => scope
                        .map_or(self.functions, |scope| &scope.functions)
                        .get(&name[0])
                        .is_some_and(|function| function.tabular()),
                    _ => false,
                };
            if tabular.is_some() || tabular_values {
                return Ok(ast::Expr {
                    kind: ExprKind::TermSet {
                        expr: Box::new(self.expand(value, scope, depth + 1)?),
                        values: values.clone(),
                        tabular: tabular.clone(),
                        all: *all,
                    },
                    span: expr.span,
                    depth: expr.depth,
                });
            }
        }
        if let ExprKind::InList {
            expr: value,
            values,
            negated,
            case_insensitive,
        } = &expr.kind
            && values.len() == 1
            && let ExprKind::Call { name, .. } = &values[0].kind
            && scope
                .map_or(self.functions, |scope| &scope.functions)
                .get(name)
                .is_some_and(|f| f.tabular())
        {
            let value = self.expand(value, scope, depth + 1)?;
            return Ok(ast::Expr {
                kind: ExprKind::InList {
                    expr: Box::new(value),
                    values: values.clone(),
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                },
                span: expr.span,
                depth: expr.depth,
            });
        }
        if matches!(
            expr.kind,
            ExprKind::ToScalar(_) | ExprKind::InTabular { .. }
        ) {
            if scope.is_some() {
                return Err(Error::unsupported(
                    "scalar subqueries in scalar functions require a row-independent closure",
                    expr.span,
                ));
            }
            return Ok(expr.clone());
        }
        expr.map_children(|child| self.expand(child, scope, depth + 1))
    }

    fn check_expanded(&mut self, expr: &ast::Expr, depth: usize) -> Result<()> {
        self.nodes += 1;
        if depth >= 128 || self.nodes > 16_384 {
            return Err(Error::syntax(
                "expanded function exceeds expression limits",
                expr.span,
            ));
        }
        expr.map_children(|child| {
            self.check_expanded(child, depth + 1)?;
            Ok(child.clone())
        })?;
        Ok(())
    }

    fn call(
        &mut self,
        function: Arc<BoundFunction>,
        args: &[ast::Expr],
        outer: Option<&Scope>,
        span: Span,
        depth: usize,
    ) -> Result<ast::Expr> {
        if depth >= 128 {
            return Err(Error::syntax("function nesting limit exceeded", span));
        }
        if function.tabular() {
            return Err(Error::syntax(
                "a tabular function cannot be used as a scalar",
                span,
            ));
        }
        if function.deferred {
            let args = args
                .iter()
                .map(|arg| match &arg.kind {
                    ExprKind::NamedArgument { name, value } => Ok(ast::Expr {
                        kind: ExprKind::NamedArgument {
                            name: name.clone(),
                            value: Box::new(self.expand(value, outer, depth + 1)?),
                        },
                        span: arg.span,
                        depth: arg.depth,
                    }),
                    _ => self.expand(arg, outer, depth + 1),
                })
                .collect::<Result<Vec<_>>>()?;
            let key = format!("__kql_deferred_function_{}", self.next);
            *self.next += 1;
            self.deferred.insert(key.clone(), function);
            return Ok(ast::Expr {
                kind: ExprKind::DeferredCall { name: key, args },
                span,
                depth: 1,
            });
        }
        let names = args
            .iter()
            .map(|arg| match &arg.kind {
                ExprKind::NamedArgument { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let slots = arguments(&function.definition.parameters, &names, span)?;
        let actuals = args
            .iter()
            .map(|arg| {
                let value = match &arg.kind {
                    ExprKind::NamedArgument { value, .. } => value,
                    _ => arg,
                };
                self.expand(value, outer, depth + 1)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut scope = Scope {
            values: function.values.clone(),
            functions: function.functions.clone(),
        };
        for (name, value) in &function.scalars {
            scope
                .values
                .insert(name.clone(), self.capture(value.clone(), span));
        }
        for (parameter, slot) in function.definition.parameters.iter().zip(slots) {
            let ParameterType::Scalar(ty) = parameter.ty else {
                return Err(Error::syntax("expected a scalar function parameter", span));
            };
            let value = slot
                .map(|index| actuals[index].clone())
                .or_else(|| parameter.default.clone())
                .ok_or_else(|| Error::syntax("missing function argument", span))?;
            scope.values.insert(
                parameter.name.clone(),
                ast::Expr {
                    depth: value.depth + 1,
                    kind: ExprKind::Cast {
                        expr: Box::new(value),
                        ty,
                    },
                    span,
                },
            );
        }
        let mut names = HashSet::new();
        for binding in &function.definition.bindings {
            if !names.insert(&binding.name) {
                return Err(Error::syntax("duplicate function binding", binding.span));
            }
            match &binding.value {
                BindingValue::Scalar(value) => {
                    let value = self.expand(value, Some(&scope), depth + 1)?;
                    scope.functions.remove(&binding.name);
                    scope.values.insert(binding.name.clone(), value);
                }
                BindingValue::Function(definition) => {
                    let nested = BoundFunction::new(
                        *definition.clone(),
                        HashMap::new(),
                        function.tables.clone(),
                        scope.functions.clone(),
                        scope.values.clone(),
                    );
                    scope.values.remove(&binding.name);
                    scope
                        .functions
                        .insert(binding.name.clone(), Arc::new(nested));
                }
                BindingValue::Parameter { ty, default } => {
                    let value = if let Some(value) = self.parameters.get(&binding.name) {
                        self.capture(lit(value.clone()), binding.span)
                    } else {
                        default.clone().ok_or_else(|| {
                            Error::syntax(
                                format!("missing query parameter '{}'", binding.name),
                                binding.span,
                            )
                        })?
                    };
                    scope.values.insert(
                        binding.name.clone(),
                        ast::Expr {
                            depth: value.depth + 1,
                            kind: ExprKind::Cast {
                                expr: Box::new(value),
                                ty: *ty,
                            },
                            span: binding.span,
                        },
                    );
                }
                BindingValue::Tabular(_) => {
                    return Err(Error::unsupported(
                        "tabular statements inside scalar functions require toscalar",
                        binding.span,
                    ));
                }
            }
        }
        let FunctionBody::Scalar(body) = &function.definition.body else {
            return Err(Error::syntax("expected a scalar function body", span));
        };
        self.expand(body, Some(&scope), depth + 1)
    }
}

fn query_planning(
    definition: &ast::Function,
    functions: &Functions,
    symbols: &HashMap<String, bool>,
) -> bool {
    fn expression(
        expr: &ast::Expr,
        functions: &Functions,
        symbols: &HashMap<String, bool>,
    ) -> bool {
        if matches!(
            expr.kind,
            ExprKind::ToScalar(_)
                | ExprKind::InTabular { .. }
                | ExprKind::TermSet {
                    tabular: Some(_),
                    ..
                }
        ) {
            return true;
        }
        if let ExprKind::Call { name, .. } = &expr.kind
            && functions
                .get(name)
                .is_some_and(|function| function.deferred || function.tabular)
        {
            return true;
        }
        if let ExprKind::InList { values, .. } | ExprKind::TermSet { values, .. } = &expr.kind
            && values.len() == 1
            && matches!(&values[0].kind,ExprKind::Identifier(name) if symbols.get(&name[0])==Some(&true))
        {
            return true;
        }
        let mut found = false;
        expr.map_children(|child| {
            found |= expression(child, functions, symbols);
            Ok(child.clone())
        })
        .expect("function-planning inspection does not fail");
        found
    }
    definition
        .bindings
        .iter()
        .any(|binding| match &binding.value {
            BindingValue::Tabular(_) => true,
            BindingValue::Scalar(value) => expression(value, functions, symbols),
            BindingValue::Function(function) => query_planning(function, functions, symbols),
            BindingValue::Parameter { .. } => false,
        })
        || match &definition.body {
            FunctionBody::Scalar(value) => expression(value, functions, symbols),
            FunctionBody::Tabular(_) => false,
        }
}

fn infer_tabular(definition: &ast::Function, outer: &HashMap<String, bool>) -> bool {
    fn expression(expr: &ast::Expr, symbols: &HashMap<String, bool>) -> bool {
        match &expr.kind {
            ExprKind::Identifier(parts) => symbols.get(&parts[0]).copied().unwrap_or(true),
            ExprKind::Call { name, .. } => {
                symbols.get(name).copied().unwrap_or(name == "materialize")
            }
            _ => false,
        }
    }
    let mut symbols = outer.clone();
    let mut tabular_parameter = false;
    for parameter in &definition.parameters {
        let tabular = matches!(parameter.ty, ParameterType::Tabular(_));
        tabular_parameter |= tabular;
        symbols.insert(parameter.name.clone(), tabular);
    }
    for binding in &definition.bindings {
        let tabular = match &binding.value {
            BindingValue::Scalar(expr) => expression(expr, &symbols),
            BindingValue::Tabular(_) => true,
            BindingValue::Function(function) => infer_tabular(function, &symbols),
            BindingValue::Parameter { .. } => false,
        };
        symbols.insert(binding.name.clone(), tabular);
    }
    tabular_parameter
        || match &definition.body {
            FunctionBody::Tabular(_) => true,
            FunctionBody::Scalar(expr) => expression(expr, &symbols),
        }
}

fn preserve_names(before: &[ast::NamedExpr], after: &mut [ast::NamedExpr]) {
    for (index, (before, after)) in before.iter().zip(after).enumerate() {
        if after.name.is_none() {
            let original = crate::planner::default_name(&before.expr, index);
            if original != crate::planner::default_name(&after.expr, index) {
                after.name = Some(original);
            }
        }
    }
}
