use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::dynamic_value::Node;

const MAX_BYTES: usize = 16 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    UrlEncode,
    ComponentEncode,
    UrlDecode,
    ParseUrl,
    ParseQuery,
    Base64Encode,
    Base64Decode,
    Base64Array,
}

impl Op {
    pub(crate) fn lookup(name: &str) -> Option<Self> {
        Some(match name {
            "url_encode" => Self::UrlEncode,
            "url_encode_component" => Self::ComponentEncode,
            "url_decode" => Self::UrlDecode,
            "parse_url" | "parseurl" => Self::ParseUrl,
            "parse_urlquery" | "parseurlquery" => Self::ParseQuery,
            "base64_encode_tostring" | "base64_encodestring" => Self::Base64Encode,
            "base64_decode_tostring" | "base64_decodestring" => Self::Base64Decode,
            "base64_decode_toarray" => Self::Base64Array,
            _ => return None,
        })
    }
    fn name(self) -> &'static str {
        match self {
            Self::UrlEncode => "__kql_url_encode",
            Self::ComponentEncode => "__kql_url_encode_component",
            Self::UrlDecode => "__kql_url_decode",
            Self::ParseUrl => "__kql_parse_url",
            Self::ParseQuery => "__kql_parse_urlquery",
            Self::Base64Encode => "__kql_base64_encode_tostring",
            Self::Base64Decode => "__kql_base64_decode_tostring",
            Self::Base64Array => "__kql_base64_decode_toarray",
        }
    }
    fn dynamic(self) -> bool {
        matches!(self, Self::ParseUrl | Self::ParseQuery | Self::Base64Array)
    }
}

fn bounded(size: usize) -> Result<()> {
    if size > MAX_BYTES {
        Err(DataFusionError::Execution(
            "text conversion exceeds its 16 MiB per-value byte budget".into(),
        ))
    } else {
        Ok(())
    }
}

fn encode(text: &str, component: bool) -> Result<String> {
    let safe = |byte: u8| byte.is_ascii_alphanumeric() || b"-_.!*()".contains(&byte);
    let length = text.bytes().try_fold(0_usize, |count, byte| {
        let width = if safe(byte) || byte == b' ' && !component {
            1
        } else {
            3
        };
        count
            .checked_add(width)
            .ok_or_else(|| DataFusionError::Execution("URL output length overflow".into()))
    })?;
    bounded(length)?;
    let mut output = String::with_capacity(length);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in text.bytes() {
        if safe(byte) {
            output.push(byte as char);
        } else if byte == b' ' && !component {
            output.push('+');
        } else {
            output.push('%');
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 15) as usize] as char);
        }
    }
    Ok(output)
}

fn decode(text: &str, plus_as_space: bool) -> String {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = text.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let Some(high) = bytes.get(index + 1).copied().and_then(hex)
            && let Some(low) = bytes.get(index + 2).copied().and_then(hex)
        {
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(if plus_as_space && bytes[index] == b'+' {
                b' '
            } else {
                bytes[index]
            });
            index += 1;
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn query(text: &str) -> Result<Node> {
    let mut output = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(text.trim_start_matches('?').as_bytes()) {
        if output.len() >= crate::dynamic::MAX_COLLECTION_SIZE {
            return Err(DataFusionError::Execution(
                "URL query exceeds its parameter budget".into(),
            ));
        }
        output.insert(key.into_owned(), Node::String(value.into_owned()));
    }
    Ok(Node::Bag(output))
}

fn parse_url(text: &str) -> Result<Node> {
    let Ok(url) = url::Url::parse(text) else {
        return Ok(Node::Null);
    };
    if url.cannot_be_a_base() || url.host().is_none() {
        return Ok(Node::Null);
    }
    let host = match url.host() {
        Some(url::Host::Domain(value)) => value.to_owned(),
        Some(url::Host::Ipv4(value)) => value.to_string(),
        Some(url::Host::Ipv6(value)) => value.to_string(),
        None => return Ok(Node::Null),
    };
    let mut fields = BTreeMap::from([
        ("Scheme".into(), Node::String(url.scheme().into())),
        ("Host".into(), Node::String(host)),
        (
            "Port".into(),
            Node::String(
                url.port_or_known_default()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ),
        ),
        (
            "Path".into(),
            Node::String(url.path().strip_prefix('/').unwrap_or(url.path()).into()),
        ),
        (
            "Username".into(),
            Node::String(decode(url.username(), false)),
        ),
        (
            "Password".into(),
            Node::String(decode(url.password().unwrap_or(""), false)),
        ),
        (
            "Fragment".into(),
            Node::String(url.fragment().unwrap_or("").into()),
        ),
    ]);
    fields.insert("Query Parameters".into(), query(url.query().unwrap_or(""))?);
    Ok(Node::Bag(fields))
}

fn base64(text: &str, array: bool) -> Result<Option<Vec<u8>>> {
    let compact = text
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    let estimated = compact
        .len()
        .div_ceil(4)
        .checked_mul(3)
        .ok_or_else(|| DataFusionError::Execution("base64 decoded length overflow".into()))?;
    bounded(estimated)?;
    if array && estimated.saturating_sub(2) > crate::dynamic::MAX_COLLECTION_SIZE {
        return Err(DataFusionError::Execution(
            "base64 byte array exceeds its collection budget".into(),
        ));
    }
    match base64::engine::general_purpose::STANDARD.decode(compact) {
        Ok(output) => {
            if array {
                crate::dynamic_value::check_size(output.len())?;
            }
            Ok(Some(output))
        }
        Err(_) => Ok(None),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    op: Op,
    signature: Signature,
}

pub(crate) fn call(op: Op, value: Expr) -> Expr {
    ScalarUDF::from(Function {
        op,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
    .call(vec![value])
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        self.op.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.op.dynamic() {
            crate::dynamic::data_type()
        } else {
            DataType::Utf8
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0].to_array(1)?;
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("text conversion requires strings".into()))?;
        let mut output = StringBuilder::new();
        let mut nodes = Vec::new();
        for value in strings {
            let text = value.unwrap_or("");
            bounded(text.len())?;
            if self.op.dynamic() {
                nodes.push(match self.op {
                    Op::ParseUrl => parse_url(text)?,
                    Op::ParseQuery => {
                        Node::Bag(BTreeMap::from([("Query Parameters".into(), query(text)?)]))
                    }
                    Op::Base64Array => base64(text, true)?
                        .map(|values| {
                            Node::Array(
                                values
                                    .into_iter()
                                    .map(|value| Node::Long(i64::from(value)))
                                    .collect(),
                            )
                        })
                        .unwrap_or(Node::Null),
                    _ => unreachable!("only dynamic-returning text functions use this path"),
                });
            } else {
                match self.op {
                    Op::UrlEncode | Op::ComponentEncode => {
                        output.append_value(encode(text, self.op == Op::ComponentEncode)?)
                    }
                    Op::UrlDecode => output.append_value(decode(text, true)),
                    Op::Base64Encode => {
                        let length = base64::encoded_len(text.len(), true).ok_or_else(|| {
                            DataFusionError::Execution("base64 encoded length overflow".into())
                        })?;
                        bounded(length)?;
                        output.append_value(base64::engine::general_purpose::STANDARD.encode(text));
                    }
                    Op::Base64Decode => {
                        let decoded =
                            base64(text, false)?.and_then(|bytes| String::from_utf8(bytes).ok());
                        output.append_option(decoded);
                    }
                    _ => unreachable!("dynamic text functions are handled above"),
                }
            }
        }
        let output: ArrayRef = if self.op.dynamic() {
            crate::dynamic::from_node_rows(nodes.into_iter().map(Ok), strings.len())?
        } else {
            Arc::new(output.finish())
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
