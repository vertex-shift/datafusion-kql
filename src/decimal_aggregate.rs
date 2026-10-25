use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};

use crate::decimal::{self, Arithmetic, Decimal, data_type, scalar};

#[derive(Debug, PartialEq, Eq, Hash)]
struct Aggregate {
    kind: Kind,
    signature: Signature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Sum,
    Average,
    Minimum,
    Maximum,
}

pub(crate) fn aggregate(name: &str, value: Expr) -> Expr {
    AggregateUDF::from(Aggregate {
        kind: match name {
            "sum" => Kind::Sum,
            "avg" => Kind::Average,
            "min" => Kind::Minimum,
            "max" => Kind::Maximum,
            _ => unreachable!("only decimal numeric aggregates are routed here"),
        },
        signature: Signature::exact(vec![data_type()], Volatility::Immutable),
    })
    .call(vec![decimal::call(decimal::Op::Convert, vec![value])])
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Sum => "__kql_decimal_sum",
            Kind::Average => "__kql_decimal_avg",
            Kind::Minimum => "__kql_decimal_min",
            Kind::Maximum => "__kql_decimal_max",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(data_type())
    }
    fn default_value(&self, _: &DataType) -> Result<ScalarValue> {
        scalar((self.kind == Kind::Sum).then_some(Decimal::ZERO))
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DecimalState {
            kind: self.kind,
            context: Arithmetic::default(),
            total: Decimal::ZERO,
            count: 0,
            failed: false,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[sum]", args.name),
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new(
                format!("{}[count]", args.name),
                DataType::UInt64,
                false,
            )),
            Arc::new(Field::new(
                format!("{}[overflow]", args.name),
                DataType::Boolean,
                false,
            )),
        ])
    }
}

#[derive(Debug)]
struct DecimalState {
    kind: Kind,
    context: Arithmetic,
    total: Decimal,
    count: u64,
    failed: bool,
}

impl DecimalState {
    fn push(&mut self, value: Decimal, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let first = self.count == 0;
        self.count = self
            .count
            .checked_add(count)
            .ok_or_else(|| DataFusionError::Execution("decimal aggregate count overflow".into()))?;
        if first {
            self.total = value;
        } else if matches!(self.kind, Kind::Sum | Kind::Average) {
            let sum = self.context.add(self.total.0, value.0);
            if let Some(total) = Decimal::new(&mut self.context, sum) {
                self.total = total;
            } else {
                self.failed = true;
            }
        } else if self.total.0.is_nan() || value.0.is_nan() {
            self.total = Decimal(dec::Decimal128::NAN);
        } else if self.kind == Kind::Minimum && value < self.total
            || self.kind == Kind::Maximum && value > self.total
        {
            self.total = value;
        }
        Ok(())
    }
}

impl Accumulator for DecimalState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for row in 0..values[0].len() {
            if let Some(value) = decimal::value(&values[0], row)? {
                self.push(value, 1)?;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.failed || self.kind != Kind::Sum && self.count == 0 {
            None
        } else if self.kind == Kind::Average {
            let result = self
                .context
                .div(self.total.0, dec::Decimal128::from(self.count));
            Decimal::new(&mut self.context, result)
        } else {
            Some(self.total)
        };
        scalar(value)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::FixedSizeBinary(16, Some(self.total.0.to_le_bytes().to_vec())),
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Boolean(Some(self.failed)),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = states[0]
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("invalid decimal aggregate sum state".into())
            })?;
        let counts = states[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("invalid decimal aggregate count state".into())
            })?;
        let failed = states[2]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("invalid decimal aggregate overflow state".into())
            })?;
        for row in 0..sums.len() {
            if sums.is_null(row) || counts.is_null(row) || failed.is_null(row) {
                return Err(DataFusionError::Execution(
                    "null decimal aggregate state".into(),
                ));
            }
            let bytes = sums.value(row).try_into().map_err(|_| {
                DataFusionError::Internal("invalid decimal aggregate payload width".into())
            })?;
            let value = Decimal::new(&mut self.context, dec::Decimal128::from_le_bytes(bytes))
                .ok_or_else(|| {
                    DataFusionError::Execution("invalid decimal aggregate payload".into())
                })?;
            self.failed |= failed.value(row);
            self.push(value, counts.value(row))?;
        }
        Ok(())
    }
}
