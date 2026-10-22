use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::{can_cast_types, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn data_type() -> DataType {
    DataType::FixedSizeBinary(16)
}

pub(crate) fn check_comparison(
    left: &DataType,
    right: &DataType,
    span: crate::ast::Span,
) -> crate::Result<()> {
    if (left == &data_type() || right == &data_type())
        && left != right
        && *left != DataType::Null
        && *right != DataType::Null
    {
        return Err(crate::Error::syntax(
            "GUID comparisons require GUID or null operands; use toguid for strings",
            span,
        ));
    }
    Ok(())
}

pub(crate) fn parse_prefix(text: &str) -> Option<([u8; 16], usize)> {
    let mut value = [0_u8; 16];
    let (mut digits, mut position, mut hyphen) = (0, 0, false);
    while digits < 32 {
        let byte = *text.as_bytes().get(position)?;
        position += 1;
        if byte == b'-' {
            if hyphen || !matches!(digits, 8 | 12 | 16 | 20) {
                return None;
            }
            hyphen = true;
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        value[digits / 2] = (value[digits / 2] << 4) | nibble;
        digits += 1;
        hyphen = false;
    }
    Some((value, position))
}

pub(crate) fn format(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(36);
    for (index, &byte) in value.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 15) as usize] as char);
    }
    text
}

pub(crate) fn parse_array(input: &ArrayRef) -> Result<ArrayRef> {
    if input.data_type() == &data_type() {
        return Ok(Arc::clone(input));
    }
    if !can_cast_types(input.data_type(), &DataType::Utf8) {
        return Ok(datafusion::arrow::array::new_null_array(
            &data_type(),
            input.len(),
        ));
    }
    let strings = cast_with_options(input, &DataType::Utf8, &Default::default())?;
    let strings = strings
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("guid conversion did not produce strings".into())
        })?;
    let mut output = FixedSizeBinaryBuilder::with_capacity(strings.len(), 16);
    for value in strings {
        if let Some((value, _)) = value.and_then(parse_prefix) {
            output.append_value(value)?;
        } else {
            output.append_null();
        }
    }
    Ok(Arc::new(output.finish()))
}

pub(crate) fn format_array(input: &ArrayRef) -> Result<ArrayRef> {
    let values = input
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|values| values.value_length() == 16)
        .ok_or_else(|| {
            DataFusionError::Execution("guid formatting expects 128-bit values".into())
        })?;
    let mut output = StringBuilder::new();
    for index in 0..values.len() {
        output.append_value(if values.is_null(index) {
            String::new()
        } else {
            format(values.value(index))
        });
    }
    Ok(Arc::new(output.finish()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Guid {
    format: bool,
    signature: Signature,
}

pub(crate) fn call(value: Expr, format: bool) -> Expr {
    ScalarUDF::from(Guid {
        format,
        signature: Signature::any(1, Volatility::Immutable),
    })
    .call(vec![value])
}

impl ScalarUDFImpl for Guid {
    fn name(&self) -> &str {
        if self.format {
            "__kql_guid_string"
        } else {
            "__kql_toguid"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.format {
            DataType::Utf8
        } else {
            data_type()
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let input = args.args[0].to_array(1)?;
        let output = if self.format {
            format_array(&input)?
        } else {
            parse_array(&input)?
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
