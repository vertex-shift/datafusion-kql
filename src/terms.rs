use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, BooleanBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use regex::{RegexSet, RegexSetBuilder};
use serde_json::Value as Json;

use crate::ast::Span;
use crate::dynamic::{self, Kernel};
use crate::expressions::{Expressions, is_string};
use crate::{Error, Result};

fn patterns(value: &Json, all: bool) -> DataFusionResult<Vec<String>> {
    fn collect(
        value: &Json,
        output: &mut BTreeSet<String>,
        all: bool,
        bytes: &mut usize,
    ) -> DataFusionResult<()> {
        if let Json::Array(items) = value {
            for item in items {
                collect(item, output, all, bytes)?;
            }
        } else {
            if value.as_str().is_some_and(|value| value.len() > 1_048_576) {
                return Err(DataFusionError::Execution(
                    "term search exceeds its 1 MiB text budget".into(),
                ));
            }
            let text = match value {
                Json::Null => String::new(),
                Json::String(value) => value.clone(),
                value => value.to_string(),
            }
            .to_lowercase();
            if output.insert(text.clone()) {
                *bytes = bytes.saturating_add(text.len());
                if output.len() > if all { 256 } else { 10_000 } || *bytes > 1_048_576 {
                    return Err(DataFusionError::Execution(
                        "term search exceeds its distinct-term or 1 MiB text budget".into(),
                    ));
                }
            }
        }
        Ok(())
    }
    let mut terms = BTreeSet::new();
    collect(value, &mut terms, all, &mut 0)?;
    Ok(terms.into_iter().collect())
}

#[derive(Debug)]
struct Matcher {
    key: String,
    regex: RegexSet,
}

fn matcher(key: String, terms: &[String]) -> DataFusionResult<Matcher> {
    let patterns = terms.iter().map(|term| pattern(term, true, true));
    let regex = RegexSetBuilder::new(patterns)
        .case_insensitive(true)
        .size_limit(16 << 20)
        .build()
        .map_err(|error| {
            DataFusionError::Execution(format!("cannot compile term search: {error}"))
        })?;
    Ok(Matcher { key, regex })
}

pub(crate) fn pattern(term: &str, start: bool, end: bool) -> String {
    if term.is_empty() {
        return r"\b\B".into();
    }
    format!(
        "{}{}{}",
        if start { r"(?:^|[^\p{L}\p{N}])" } else { "" },
        regex::escape(term),
        if end { r"(?:$|[^\p{L}\p{N}])" } else { "" }
    )
}

#[derive(Debug)]
struct Function {
    all: bool,
    constants: Option<Vec<String>>,
    signature: Signature,
    cache: Mutex<Option<Arc<Matcher>>>,
}

impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        self.all == other.all && self.constants == other.constants
    }
}
impl Eq for Function {}
impl Hash for Function {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.all.hash(state);
        self.constants.hash(state);
    }
}

pub(crate) fn plan(
    expressions: &Expressions<'_>,
    value: Expr,
    values: Vec<Expr>,
    all: bool,
    schema: &DFSchema,
    span: Span,
) -> Result<Expr> {
    let value = expressions.string_value(value, schema)?;
    if !is_string(&value.get_type(schema)?) {
        return Err(Error::syntax(
            "has_any/has_all require a string or dynamic source",
            span,
        ));
    }
    let runtime = values.iter().try_fold(false, |found, value| {
        Ok::<_, DataFusionError>(
            found || value.exists(|value| Ok(matches!(value, Expr::ScalarSubquery(_))))?,
        )
    })?;
    let (constants, cache, args) =
        if runtime {
            let [values] = values.as_slice() else {
                return Err(Error::unsupported(
                    "use a single tabular term source for runtime term search",
                    span,
                ));
            };
            (
                None,
                None,
                vec![
                    value,
                    dynamic::call(Kernel::EncodeJson, vec![values.clone()]),
                ],
            )
        } else {
            let mut json = Vec::new();
            for value in values {
                let value = dynamic::call(Kernel::EncodeJson, vec![value]);
                let text = expressions.constant_string(value, span)?;
                json.push(serde_json::from_str::<Json>(&text).map_err(|error| {
                    Error::Plan(format!("invalid constant term source: {error}"))
                })?);
            }
            let terms = patterns(&Json::Array(json), all)?;
            let cache = Arc::new(matcher(String::new(), &terms)?);
            (Some(terms), Some(cache), vec![value])
        };
    Ok(ScalarUDF::from(Function {
        all,
        constants,
        signature: Signature::exact(vec![DataType::Utf8; args.len()], Volatility::Immutable),
        cache: Mutex::new(cache),
    })
    .call(args))
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        if self.all {
            "__kql_has_all"
        } else {
            "__kql_has_any"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(1))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let arrays = arrays
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("term search expects UTF-8 strings".into())
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mut current = self
            .cache
            .lock()
            .map_err(|_| DataFusionError::Execution("term matcher cache is poisoned".into()))?
            .clone();
        let mut output = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            let index = if matches!(args.args[0], ColumnarValue::Scalar(_)) {
                0
            } else {
                row
            };
            if arrays[0].is_null(index) {
                output.append_null();
                continue;
            }
            let key = if arrays.len() == 2 {
                let index = if matches!(args.args[1], ColumnarValue::Scalar(_)) {
                    0
                } else {
                    row
                };
                if arrays[1].is_null(index) {
                    return Err(DataFusionError::Execution(
                        "tabular term source has no array value".into(),
                    ));
                }
                arrays[1].value(index)
            } else {
                ""
            };
            if current.as_ref().is_none_or(|matcher| matcher.key != key) {
                let value = serde_json::from_str(key).map_err(|error| {
                    DataFusionError::Execution(format!("invalid tabular term array: {error}"))
                })?;
                let terms = patterns(&value, self.all)?;
                let matcher = Arc::new(matcher(key.into(), &terms)?);
                *self.cache.lock().map_err(|_| {
                    DataFusionError::Execution("term matcher cache is poisoned".into())
                })? = Some(Arc::clone(&matcher));
                current = Some(matcher);
            }
            let matcher = current.as_ref().ok_or_else(|| {
                DataFusionError::Internal("term matcher was not initialized".into())
            })?;
            let matches = matcher.regex.matches(arrays[0].value(index));
            output.append_value(if self.all {
                matches.iter().count() == matcher.regex.len()
            } else {
                matches.matched_any()
            });
        }
        let array = Arc::new(output.finish());
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                array.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}
