use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use dec::{Context, Decimal128, Rounding};

pub(crate) type Arithmetic = Context<Decimal128>;

// Native struct comparison/hashing uses a canonical numeric key; the payload
// preserves decimal128 values without reparsing text during arithmetic.
static FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    [
        Arc::new(Field::new(
            "__kql_decimal_order_v1",
            DataType::FixedSizeBinary(16),
            false,
        )),
        Arc::new(Field::new(
            "__kql_decimal_payload_v1",
            DataType::FixedSizeBinary(16),
            false,
        )),
    ]
    .into_iter()
    .collect()
});

pub(crate) fn data_type() -> DataType {
    DataType::Struct(FIELDS.clone())
}

pub(crate) fn is_runtime(ty: &DataType) -> bool {
    matches!(ty, DataType::Struct(fields) if fields == &*FIELDS)
}

pub(crate) fn is_decimal(ty: &DataType) -> bool {
    is_runtime(ty) || matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
}

pub(crate) fn legacy_type() -> DataType {
    DataType::Decimal256(57, 28)
}

#[derive(Clone, Copy)]
pub(crate) struct Decimal(pub(crate) Decimal128);

impl Decimal {
    pub(crate) const ZERO: Self = Self(Decimal128::ZERO);
    pub(crate) const ONE: Self = Self(Decimal128::ONE);

    pub(crate) fn zero(negative: bool) -> Self {
        let mut bytes = Decimal128::ZERO.to_le_bytes();
        if negative {
            bytes[15] |= 0x80;
        }
        Self(Decimal128::from_le_bytes(bytes))
    }

    pub(crate) fn negate(self) -> Self {
        let mut bytes = self.0.to_le_bytes();
        bytes[15] ^= 0x80;
        Self(Decimal128::from_le_bytes(bytes))
    }

    pub(crate) fn new(context: &mut Arithmetic, value: Decimal128) -> Option<Self> {
        if value.is_signaling_nan() {
            return None;
        }
        Some(if value.is_nan() {
            Self(Decimal128::NAN)
        } else if value.is_zero() {
            Self::zero(value.is_signed())
        } else {
            Self(context.reduce(value).canonical())
        })
    }

    pub(crate) fn parse(context: &mut Arithmetic, text: &str) -> Option<Self> {
        context.clear_status();
        let value = context.parse(text.trim()).ok()?;
        if value.is_nan() {
            return None;
        }
        Self::new(context, value)
    }

    pub(crate) fn integral(self, context: &mut Arithmetic, rounding: Rounding) -> Self {
        let old = context.rounding();
        context.set_rounding(rounding);
        let value = context.round(self.0);
        context.set_rounding(old);
        Self::new(context, value).expect("rounding a decimal cannot create a signaling NaN")
    }

    pub(crate) fn integer(self, context: &mut Arithmetic) -> Option<i128> {
        if !self.0.is_finite() {
            return None;
        }
        let value = self.integral(context, Rounding::Down);
        let exponent = u32::try_from(value.0.exponent()).ok()?;
        value
            .0
            .coefficient()
            .checked_mul(10_i128.checked_pow(exponent)?)
    }

    pub(crate) fn real(self) -> Option<f64> {
        self.0.to_string().parse().ok()
    }

    pub(crate) fn key(self) -> [u8; 16] {
        if self.0.is_nan() {
            return [0; 16];
        }
        if self.0.is_infinite() {
            return if self.0.is_signed() {
                1_u128.to_be_bytes()
            } else {
                u128::MAX.to_be_bytes()
            };
        }
        if self.0.is_zero() {
            return ((1_u128 << 127) - u128::from(self.0.is_signed())).to_be_bytes();
        }
        let coefficient = self.0.coefficient().unsigned_abs();
        let digits = coefficient.ilog10() + 1;
        let significand = coefficient * 10_u128.pow(34 - digits);
        let exponent = self.0.exponent() + digits as i32 - 1 + 6177;
        let key = (1_u128 << 127) | ((exponent as u128) << 113) | significand;
        (if self.0.is_negative() { !key } else { key }).to_be_bytes()
    }
}

impl From<i64> for Decimal {
    fn from(value: i64) -> Self {
        Self::new(&mut Arithmetic::default(), Decimal128::from(value))
            .expect("all i64 values are finite exact decimal128 values")
    }
}

impl From<i32> for Decimal {
    fn from(value: i32) -> Self {
        Self::from(i64::from(value))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        !self.0.is_nan()
            && !other.0.is_nan()
            && (self.0.is_zero() && other.0.is_zero()
                || self.0.to_le_bytes() == other.0.to_le_bytes())
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_zero() {
            Decimal128::ZERO.to_le_bytes().hash(state);
        } else {
            self.0.to_le_bytes().hash(state);
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::fmt::Debug for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.0.is_finite() {
            return std::fmt::Display::fmt(&self.0, f);
        }
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.0.is_finite() {
            return std::fmt::Display::fmt(&self.0, f);
        }
        let mut digits = self.0.coefficient().unsigned_abs().to_string();
        let mut exponent = self.0.exponent();
        while digits.len() > 1 && digits.ends_with('0') {
            digits.pop();
            exponent += 1;
        }
        if self.0.is_signed() {
            f.write_str("-")?;
        }
        let adjusted = exponent + digits.len() as i32 - 1;
        if (-6..34).contains(&adjusted) {
            let point = digits.len() as i32 + exponent;
            if point <= 0 {
                f.write_str("0.")?;
                for _ in 0..-point {
                    f.write_str("0")?;
                }
                f.write_str(&digits)
            } else if point >= digits.len() as i32 {
                f.write_str(&digits)?;
                for _ in digits.len() as i32..point {
                    f.write_str("0")?;
                }
                Ok(())
            } else {
                let point = point as usize;
                write!(f, "{}.{}", &digits[..point], &digits[point..])
            }
        } else {
            f.write_str(&digits[..1])?;
            if digits.len() > 1 {
                write!(f, ".{}", &digits[1..])?;
            }
            write!(f, "E{adjusted:+}")
        }
    }
}

pub(crate) struct Builder {
    keys: FixedSizeBinaryBuilder,
    payloads: FixedSizeBinaryBuilder,
    validity: Vec<bool>,
}

impl Builder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            keys: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            payloads: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            validity: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn append(&mut self, value: Option<Decimal>) -> Result<()> {
        match value {
            Some(value) => {
                self.keys.append_value(value.key())?;
                self.payloads.append_value(value.0.to_le_bytes())?;
                self.validity.push(true);
            }
            None => {
                self.keys.append_value([0; 16])?;
                self.payloads.append_value([0; 16])?;
                self.validity.push(false);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<ArrayRef> {
        let nulls = self
            .validity
            .contains(&false)
            .then(|| NullBuffer::from(self.validity));
        Ok(Arc::new(StructArray::try_new(
            FIELDS.clone(),
            vec![
                Arc::new(self.keys.finish()),
                Arc::new(self.payloads.finish()),
            ],
            nulls,
        )?))
    }
}

pub(crate) fn read(array: &ArrayRef, row: usize) -> Result<Option<Decimal>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .filter(|a| is_runtime(a.data_type()))
        .ok_or_else(|| {
            DataFusionError::Execution("invalid decimal128 Arrow representation".into())
        })?;
    let payloads = array
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| DataFusionError::Execution("invalid decimal128 payload array".into()))?;
    let payload = payloads
        .value(row)
        .try_into()
        .map_err(|_| DataFusionError::Execution("invalid decimal128 payload width".into()))?;
    let inner = Decimal128::from_le_bytes(payload);
    let normalized_zero = Decimal::zero(inner.is_signed()).0;
    if !inner.is_canonical()
        || inner.is_signaling_nan()
        || inner.is_zero() && inner.to_le_bytes() != normalized_zero.to_le_bytes()
        || inner.is_finite()
            && !inner.is_zero()
            && inner.exponent() < 6111
            && inner.coefficient() % 10 == 0
        || inner.is_nan() && inner.to_le_bytes() != Decimal128::NAN.to_le_bytes()
    {
        return Err(DataFusionError::Execution(
            "noncanonical decimal128 payload".into(),
        ));
    }
    let value = Decimal(inner);
    let keys = array
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| DataFusionError::Execution("invalid decimal128 ordering array".into()))?;
    if keys.value(row) != value.key() {
        return Err(DataFusionError::Execution(
            "decimal128 ordering key does not match its value".into(),
        ));
    }
    Ok(Some(value))
}

pub(crate) fn scalar(value: Option<Decimal>) -> Result<ScalarValue> {
    let mut builder = Builder::new(1);
    builder.append(value)?;
    ScalarValue::try_from_array(&builder.finish()?, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_keys_preserve_numeric_order_and_equivalent_hashes() {
        let texts = [
            "-1E+100", "-10", "-1", "-1E-100", "0", "1E-100", "1", "1.25", "10", "1E+100",
        ];
        let mut context = Arithmetic::default();
        let values = texts.map(|text| Decimal::parse(&mut context, text).unwrap());
        for pair in values.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].key() < pair[1].key());
        }
        assert_eq!(
            Decimal::parse(&mut context, "1.2500"),
            Decimal::parse(&mut context, "1.25")
        );
        assert_eq!(Decimal::parse(&mut context, "-0.0"), Some(Decimal::ZERO));
        for value in values {
            let scalar = scalar(Some(value)).unwrap();
            assert_eq!(read(&scalar.to_array().unwrap(), 0).unwrap(), Some(value));
        }
    }
}
