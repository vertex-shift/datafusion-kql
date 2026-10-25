use std::collections::BTreeMap;

use datafusion::common::{DataFusionError, Result};
use rust_decimal::Decimal;
use serde_json::{Map, Number, Value as Json, json};

use crate::dynamic::MAX_COLLECTION_SIZE;

const MAX_DEPTH: usize = 128;
const MAX_NODES: usize = MAX_COLLECTION_SIZE * 2 + 1;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Node {
    Null,
    Bool(bool),
    Long(i64),
    Real(f64),
    String(String),
    Array(Vec<Node>),
    Bag(BTreeMap<String, Node>),
    Datetime(i64),
    Timespan(i64),
    Guid([u8; 16]),
    Decimal(Decimal),
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

pub(crate) fn check_size(size: usize) -> Result<()> {
    if size > MAX_COLLECTION_SIZE {
        Err(error("dynamic collection exceeds 1048576 elements"))
    } else {
        Ok(())
    }
}

impl Node {
    pub(crate) fn from_json(value: &Json) -> Result<Self> {
        fn read(value: &Json, depth: usize, nodes: &mut usize) -> Result<Node> {
            *nodes += 1;
            if depth > MAX_DEPTH || *nodes > MAX_NODES {
                return Err(error("dynamic value exceeds nesting or node limits"));
            }
            Ok(match value {
                Json::Null => Node::Null,
                Json::Bool(value) => Node::Bool(*value),
                Json::Number(value) if value.is_i64() => Node::Long(
                    value
                        .as_i64()
                        .ok_or_else(|| error("invalid dynamic integer"))?,
                ),
                Json::Number(value) => Node::Real(
                    value
                        .as_f64()
                        .ok_or_else(|| error("invalid dynamic real"))?,
                ),
                Json::String(value) => Node::String(value.clone()),
                Json::Array(items) => {
                    check_size(items.len())?;
                    Node::Array(
                        items
                            .iter()
                            .map(|v| read(v, depth + 1, nodes))
                            .collect::<Result<_>>()?,
                    )
                }
                Json::Object(items) => {
                    check_size(items.len())?;
                    Node::Bag(
                        items
                            .iter()
                            .map(|(key, value)| Ok((key.clone(), read(value, depth + 1, nodes)?)))
                            .collect::<Result<_>>()?,
                    )
                }
            })
        }
        read(value, 0, &mut 0)
    }

    pub(crate) fn is_json(&self) -> bool {
        match self {
            Self::Datetime(_) | Self::Timespan(_) | Self::Guid(_) | Self::Decimal(_) => false,
            Self::Real(value) => value.is_finite(),
            Self::Array(items) => items.iter().all(Self::is_json),
            Self::Bag(items) => items.values().all(Self::is_json),
            _ => true,
        }
    }

    pub(crate) fn json(&self) -> Result<Json> {
        Ok(match self {
            Self::Null => Json::Null,
            Self::Bool(value) => (*value).into(),
            Self::Long(value) => (*value).into(),
            Self::Real(value) => Number::from_f64(*value)
                .map(Json::Number)
                .unwrap_or_else(|| Json::String(value.to_string())),
            Self::String(value) => value.clone().into(),
            Self::Array(items) => Json::Array(items.iter().map(Self::json).collect::<Result<_>>()?),
            Self::Bag(items) => Json::Object(
                items
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), value.json()?)))
                    .collect::<Result<Map<_, _>>>()?,
            ),
            Self::Datetime(value) => chrono::DateTime::from_timestamp_nanos(*value)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
                .into(),
            Self::Timespan(value) => timespan_text(*value).into(),
            Self::Guid(value) => crate::guid::format(value).into(),
            Self::Decimal(value) => crate::decimal::format(*value).into(),
        })
    }

    pub(crate) fn wire(&self) -> Result<String> {
        if self.is_json() {
            return Ok(format!("j{}", self.json()?));
        }
        fn write(value: &Node, depth: usize, nodes: &mut Vec<Json>) -> Result<usize> {
            if depth > MAX_DEPTH || nodes.len() >= MAX_NODES {
                return Err(error("typed dynamic value exceeds nesting or node limits"));
            }
            let (tag, payload) = match value {
                Node::Null => (0, Json::Null),
                Node::Bool(value) => (1, (*value).into()),
                Node::Long(value) => (2, (*value).into()),
                Node::Real(value) => {
                    let bits = if *value == 0.0 {
                        0.0_f64.to_bits()
                    } else {
                        value.to_bits()
                    };
                    (3, format!("{bits:016x}").into())
                }
                Node::String(value) => (4, value.clone().into()),
                Node::Array(items) => {
                    check_size(items.len())?;
                    (
                        5,
                        Json::Array(
                            items
                                .iter()
                                .map(|v| write(v, depth + 1, nodes).map(|index| json!(index)))
                                .collect::<Result<_>>()?,
                        ),
                    )
                }
                Node::Bag(items) => {
                    check_size(items.len())?;
                    (
                        6,
                        Json::Object(
                            items
                                .iter()
                                .map(|(key, value)| {
                                    Ok((key.clone(), json!(write(value, depth + 1, nodes)?)))
                                })
                                .collect::<Result<_>>()?,
                        ),
                    )
                }
                Node::Datetime(value) => (7, (*value).into()),
                Node::Timespan(value) => (8, (*value).into()),
                Node::Guid(value) => (9, crate::guid::format(value).into()),
                Node::Decimal(value) => (10, crate::decimal::format(*value).into()),
            };
            if nodes.len() >= MAX_NODES {
                return Err(error("typed dynamic value exceeds node limits"));
            }
            let index = nodes.len();
            nodes.push(json!([tag, payload]));
            Ok(index)
        }
        let mut nodes = Vec::new();
        let root = write(self, 0, &mut nodes)?;
        Ok(json!({"v":1,"root":root,"nodes":nodes}).to_string())
    }

    pub(crate) fn from_wire(text: &str) -> Result<Self> {
        if let Some(text) = text.strip_prefix('j') {
            let value = serde_json::from_str(text)
                .map_err(|cause| error(format!("corrupt dynamic JSON encoding: {cause}")))?;
            return Self::from_json(&value);
        }
        let encoded: Json = serde_json::from_str(text)
            .map_err(|e| error(format!("corrupt typed dynamic encoding: {e}")))?;
        if encoded.get("v").and_then(Json::as_u64) != Some(1) {
            return Err(error("unsupported typed dynamic encoding version"));
        }
        let values = encoded
            .get("nodes")
            .and_then(Json::as_array)
            .ok_or_else(|| error("typed dynamic encoding has no nodes"))?;
        if values.is_empty()
            || values.len() > MAX_NODES
            || encoded.get("root").and_then(Json::as_u64) != Some((values.len() - 1) as u64)
        {
            return Err(error("invalid typed dynamic root or node count"));
        }
        let mut nodes = Vec::<Option<(Node, usize)>>::with_capacity(values.len());
        for value in values {
            let pair = value
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| error("invalid typed dynamic node"))?;
            let tag = pair[0]
                .as_u64()
                .ok_or_else(|| error("invalid typed dynamic tag"))?;
            let payload = &pair[1];
            let mut depth = 0;
            let mut child = |index: &Json| -> Result<Node> {
                let index = index
                    .as_u64()
                    .and_then(|i| usize::try_from(i).ok())
                    .ok_or_else(|| error("invalid typed dynamic child index"))?;
                let (value, child_depth) =
                    nodes.get_mut(index).and_then(Option::take).ok_or_else(|| {
                        error("typed dynamic children must precede their parent and be used once")
                    })?;
                depth = depth.max(child_depth + 1);
                Ok(value)
            };
            let value = match tag {
                0 if payload.is_null() => Self::Null,
                1 => Self::Bool(
                    payload
                        .as_bool()
                        .ok_or_else(|| error("invalid typed bool"))?,
                ),
                2 => Self::Long(
                    payload
                        .as_i64()
                        .ok_or_else(|| error("invalid typed long"))?,
                ),
                3 => {
                    let text = payload
                        .as_str()
                        .filter(|text| text.len() == 16)
                        .ok_or_else(|| error("invalid typed real"))?;
                    Self::Real(f64::from_bits(
                        u64::from_str_radix(text, 16)
                            .map_err(|_| error("invalid typed real bits"))?,
                    ))
                }
                4 => Self::String(
                    payload
                        .as_str()
                        .ok_or_else(|| error("invalid typed string"))?
                        .into(),
                ),
                5 => {
                    let items = payload
                        .as_array()
                        .ok_or_else(|| error("invalid typed array"))?;
                    check_size(items.len())?;
                    Self::Array(items.iter().map(&mut child).collect::<Result<_>>()?)
                }
                6 => {
                    let items = payload
                        .as_object()
                        .ok_or_else(|| error("invalid typed bag"))?;
                    check_size(items.len())?;
                    Self::Bag(
                        items
                            .iter()
                            .map(|(key, index)| Ok((key.clone(), child(index)?)))
                            .collect::<Result<_>>()?,
                    )
                }
                7 => Self::Datetime(
                    payload
                        .as_i64()
                        .ok_or_else(|| error("invalid typed datetime"))?,
                ),
                8 => Self::Timespan(
                    payload
                        .as_i64()
                        .ok_or_else(|| error("invalid typed timespan"))?,
                ),
                9 => {
                    let text = payload
                        .as_str()
                        .ok_or_else(|| error("invalid typed guid"))?;
                    let (value, _) = crate::guid::parse_prefix(text)
                        .filter(|(_, end)| *end == text.len())
                        .ok_or_else(|| error("invalid typed guid value"))?;
                    Self::Guid(value)
                }
                10 => Self::Decimal(
                    payload
                        .as_str()
                        .and_then(crate::decimal::parse)
                        .ok_or_else(|| error("invalid typed decimal"))?,
                ),
                _ => return Err(error("unknown or invalid typed dynamic tag")),
            };
            if depth > MAX_DEPTH {
                return Err(error("typed dynamic value exceeds nesting limits"));
            }
            nodes.push(Some((value, depth)));
        }
        let (value, _) = nodes
            .pop()
            .and_then(|v| v)
            .ok_or_else(|| error("missing typed dynamic root"))?;
        if nodes.iter().any(Option::is_some) {
            return Err(error("typed dynamic encoding contains unused nodes"));
        }
        Ok(value)
    }
}

pub(crate) fn timespan_text(value: i64) -> String {
    let negative = if value < 0 { "-" } else { "" };
    let value = value.unsigned_abs();
    let base = format!(
        "{negative}{}.{:02}:{:02}:{:02}",
        value / 86_400_000_000_000,
        value / 3_600_000_000_000 % 24,
        value / 60_000_000_000 % 60,
        value / 1_000_000_000 % 60
    );
    if value.is_multiple_of(100) {
        format!("{base}.{:07}", value % 1_000_000_000 / 100)
    } else {
        format!("{base}.{:09}", value % 1_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_trees_round_trip_without_magic_json_objects() {
        let value = Node::Array(vec![
            Node::Datetime(123),
            Node::Timespan(-1),
            Node::Decimal(Decimal::MAX),
            Node::Guid([7; 16]),
            Node::Real(f64::INFINITY),
            Node::Bag(BTreeMap::from([("v".into(), Node::String("1".into()))])),
        ]);
        assert_eq!(Node::from_wire(&value.wire().unwrap()).unwrap(), value);
        let ordinary = Node::from_json(&json!({"v":1,"nodes":[[7,123]],"root":0})).unwrap();
        assert!(ordinary.is_json());
    }

    #[test]
    fn invalid_and_reused_node_references_are_rejected() {
        for text in [
            r#"{"v":1,"root":1,"nodes":[[2,1],[5,[0,0]]]}"#,
            r#"{"v":1,"root":0,"nodes":[[5,[0]]]}"#,
            r#"{"v":1,"root":1,"nodes":[[2,1],[2,2]]}"#,
            r#"{"v":2,"root":0,"nodes":[[2,1]]}"#,
        ] {
            assert!(Node::from_wire(text).is_err(), "{text}");
        }
    }
}
