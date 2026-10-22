use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;
use serde_json::Value as Json;

use crate::ast::Span;
use crate::dynamic;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Capture {
    Index(usize),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Mode {
    All(Vec<Capture>),
    Parse(Vec<String>),
}

#[derive(Debug, Clone)]
struct Function {
    pattern: String,
    compiled: Arc<Regex>,
    mode: Mode,
    signature: Signature,
}

impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.mode == other.mode
    }
}
impl Eq for Function {}
impl Hash for Function {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
        self.mode.hash(state);
    }
}

pub(crate) fn extract_all(
    pattern: String,
    groups: Option<Vec<Capture>>,
    source: Expr,
    span: Span,
) -> Result<Expr> {
    let compiled =
        Regex::new(&pattern).map_err(|e| Error::syntax(format!("invalid regex: {e}"), span))?;
    if !(2..=17).contains(&compiled.captures_len()) {
        return Err(Error::syntax(
            "extract_all requires between 1 and 16 capture groups",
            span,
        ));
    }
    let groups =
        groups.unwrap_or_else(|| (1..compiled.captures_len()).map(Capture::Index).collect());
    if groups.is_empty() || groups.len() > 16 {
        return Err(Error::syntax(
            "extract_all requires between 1 and 16 selected groups",
            span,
        ));
    }
    for group in &groups {
        let valid = match group {
            Capture::Index(index) => *index > 0 && *index < compiled.captures_len(),
            Capture::Name(name) => compiled.capture_names().flatten().any(|n| n == name),
        };
        if !valid {
            return Err(Error::syntax(
                "extract_all references an unknown capture group",
                span,
            ));
        }
    }
    Ok(ScalarUDF::from(Function {
        pattern,
        compiled: Arc::new(compiled),
        mode: Mode::All(groups),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![source]))
}

pub(crate) fn parse(
    pattern: String,
    fields: Vec<String>,
    source: Expr,
    span: Span,
) -> Result<Expr> {
    let compiled = Regex::new(&pattern)
        .map_err(|e| Error::syntax(format!("invalid parse regex: {e}"), span))?;
    Ok(ScalarUDF::from(Function {
        pattern,
        compiled: Arc::new(compiled),
        mode: Mode::Parse(fields),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![source]))
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.mode {
            Mode::All(_) => "__kql_extract_all",
            Mode::Parse(_) => "__kql_parse_fields",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(match &self.mode {
            Mode::All(_) => dynamic::data_type(),
            Mode::Parse(fields) => {
                let mut result = fields
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Arc::new(Field::new(format!("field_{i}"), DataType::Utf8, true)))
                    .collect::<Vec<_>>();
                result.push(Arc::new(Field::new("matched", DataType::Boolean, false)));
                DataType::Struct(result.into())
            }
        })
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0].to_array(1)?;
        let source = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("regex extraction expects UTF-8".into()))?;
        let output = match &self.mode {
            Mode::All(groups) => dynamic::from_json_rows(
                source.iter().map(|source| {
                    let Some(source) = source else {
                        return Ok(Json::Null);
                    };
                    let mut result = Vec::new();
                    for captures in self.compiled.captures_iter(source) {
                        if result.len() == dynamic::MAX_COLLECTION_SIZE {
                            return Err(DataFusionError::Execution(
                                "extract_all exceeds the collection limit".into(),
                            ));
                        }
                        let matched = groups
                            .iter()
                            .map(|group| {
                                let value = match group {
                                    Capture::Index(index) => captures.get(*index),
                                    Capture::Name(name) => captures.name(name),
                                };
                                value.map_or(Json::Null, |v| Json::String(v.as_str().to_owned()))
                            })
                            .collect::<Vec<_>>();
                        result.push(if matched.len() == 1 {
                            matched.into_iter().next().unwrap_or(Json::Null)
                        } else {
                            Json::Array(matched)
                        });
                    }
                    Ok(if result.is_empty() {
                        Json::Null
                    } else {
                        Json::Array(result)
                    })
                }),
                source.len(),
            )?,
            Mode::Parse(fields) => {
                let mut builders = fields
                    .iter()
                    .map(|_| StringBuilder::new())
                    .collect::<Vec<_>>();
                let mut matched = BooleanBuilder::with_capacity(source.len());
                for source in source.iter() {
                    let captures = source.and_then(|text| self.compiled.captures(text));
                    matched.append_value(captures.is_some());
                    for (field, builder) in fields.iter().zip(&mut builders) {
                        builder.append_option(
                            captures
                                .as_ref()
                                .and_then(|c| c.name(field))
                                .map(|m| m.as_str()),
                        );
                    }
                }
                let mut arrays = builders
                    .iter_mut()
                    .map(|builder| Arc::new(builder.finish()) as ArrayRef)
                    .collect::<Vec<_>>();
                arrays.push(Arc::new(matched.finish()));
                let DataType::Struct(fields) = self.return_type(&[])? else {
                    unreachable!("parse returns a struct");
                };
                Arc::new(StructArray::try_new(fields, arrays, None)?) as ArrayRef
            }
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}
