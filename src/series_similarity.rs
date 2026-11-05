use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float64Array, Float64Builder, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::ast::Span;
use crate::dynamic;
use crate::dynamic_value::Node;

const UNIX_EPOCH_TICKS: i128 = 621_355_968_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Dot,
    Cosine,
    Pearson,
}

impl Op {
    pub(crate) fn lookup(name: &str) -> Option<Self> {
        Some(match name {
            "series_dot_product" => Self::Dot,
            "series_cosine_similarity" => Self::Cosine,
            "series_pearson_correlation" => Self::Pearson,
            _ => return None,
        })
    }

    fn name(self, native: bool) -> &'static str {
        match (self, native) {
            (Self::Dot, false) => "__kql_series_dot_product",
            (Self::Dot, true) => "__kql_series_dot_product_arrow",
            (Self::Cosine, false) => "__kql_series_cosine_similarity",
            (Self::Cosine, true) => "__kql_series_cosine_similarity_arrow",
            (Self::Pearson, false) => "__kql_series_pearson_correlation",
            (Self::Pearson, true) => "__kql_series_pearson_correlation_arrow",
        }
    }
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(format!("series comparison: {}", message.into()))
}

fn list_type(mut ty: &DataType) -> Option<&DataType> {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    match ty {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(field.data_type()),
        _ => None,
    }
}

fn native_numbers(mut ty: &DataType) -> bool {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn array_type(ty: &DataType) -> bool {
    dynamic::is_dynamic(ty) || crate::arrow_dynamic::is_collection(ty) || *ty == DataType::Null
}

pub(crate) fn plan(
    op: Op,
    mut values: Vec<Expr>,
    schema: &DFSchema,
    span: Span,
) -> crate::Result<Expr> {
    let valid = if op == Op::Cosine {
        (2..=4).contains(&values.len())
    } else {
        values.len() == 2
    };
    if !valid {
        return Err(crate::Error::syntax(
            "invalid series comparison argument count",
            span,
        ));
    }
    let types = values
        .iter()
        .map(|value| value.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    if op == Op::Dot && !types[..2].iter().any(array_type) {
        return Err(crate::Error::syntax(
            "series_dot_product requires at least one array",
            span,
        ));
    }
    let mut native = true;
    for (index, value) in values.iter_mut().enumerate() {
        let ty = &types[index];
        if index < 2 && array_type(ty) {
            native &= list_type(ty).is_some_and(native_numbers);
        } else if (index >= 2 || op == Op::Dot)
            && (ty.is_numeric() || crate::decimal::is_decimal(ty) || *ty == DataType::Null)
        {
            *value = if crate::decimal::is_decimal(ty) {
                crate::decimal::call(crate::decimal::Op::ImplicitReal, vec![value.clone()])
            } else {
                datafusion::logical_expr::expr_fn::cast(value.clone(), DataType::Float64)
            };
        } else {
            return Err(crate::Error::syntax(
                if index >= 2 {
                    "cosine magnitudes must be numeric"
                } else {
                    "series comparisons require arrays; only dot product broadcasts a numeric scalar"
                },
                span,
            ));
        }
    }
    let types = values
        .iter()
        .map(|value| value.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    Ok(ScalarUDF::from(Function {
        op,
        native,
        signature: Signature::exact(types, Volatility::Immutable),
    })
    .call(values))
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("inconsistent native Arrow input"))
}

fn decode(mut array: ArrayRef) -> Result<ArrayRef> {
    while let DataType::Dictionary(_, value) = array.data_type() {
        array = cast(&array, value)?;
    }
    Ok(array)
}

fn list_values(array: &ArrayRef) -> Result<Option<ArrayRef>> {
    Ok(match array.data_type() {
        DataType::List(_) => Some(Arc::clone(downcast::<ListArray>(array)?.values())),
        DataType::LargeList(_) => Some(Arc::clone(downcast::<LargeListArray>(array)?.values())),
        DataType::ListView(_) => Some(Arc::clone(downcast::<ListViewArray>(array)?.values())),
        DataType::LargeListView(_) => {
            Some(Arc::clone(downcast::<LargeListViewArray>(array)?.values()))
        }
        DataType::FixedSizeList(_, _) => {
            Some(Arc::clone(downcast::<FixedSizeListArray>(array)?.values()))
        }
        _ => None,
    })
}

fn list_range(array: &ArrayRef, row: usize) -> Result<(usize, usize)> {
    let (start, length) = match array.data_type() {
        DataType::List(_) => {
            let list = downcast::<ListArray>(array)?;
            (
                i64::from(list.value_offsets()[row]),
                i64::from(list.value_length(row)),
            )
        }
        DataType::LargeList(_) => {
            let list = downcast::<LargeListArray>(array)?;
            (list.value_offsets()[row], list.value_length(row))
        }
        DataType::ListView(_) => {
            let list = downcast::<ListViewArray>(array)?;
            (
                i64::from(list.value_offsets()[row]),
                i64::from(list.value_sizes()[row]),
            )
        }
        DataType::LargeListView(_) => {
            let list = downcast::<LargeListViewArray>(array)?;
            (list.value_offsets()[row], list.value_sizes()[row])
        }
        DataType::FixedSizeList(_, _) => {
            let list = downcast::<FixedSizeListArray>(array)?;
            (
                i64::from(list.value_offset(row)),
                i64::from(list.value_length()),
            )
        }
        _ => return Err(error("expected a native Arrow list")),
    };
    Ok((
        usize::try_from(start).map_err(|_| error("invalid list offset"))?,
        usize::try_from(length).map_err(|_| error("invalid list length"))?,
    ))
}

enum Source {
    Native {
        array: ArrayRef,
        values: ArrayRef,
        offset: usize,
    },
    Dynamic {
        array: ArrayRef,
        constant: Option<Node>,
    },
    Scalar(ArrayRef),
}

struct Input {
    source: Source,
    scalar: bool,
}

impl Input {
    fn new(value: &ColumnarValue) -> Result<Self> {
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let array = decode(value.to_array(1)?)?;
        let list = list_values(&array)?;
        let mut first = usize::MAX;
        let mut last = 0;
        if let Some(values) = &list {
            for row in 0..array.len() {
                if array.is_null(row) {
                    continue;
                }
                let (start, len) = list_range(&array, row)?;
                check_length(len)?;
                let end = start
                    .checked_add(len)
                    .filter(|end| *end <= values.len())
                    .ok_or_else(|| error("native list range exceeds its value buffer"))?;
                first = first.min(start);
                last = last.max(end);
            }
        }
        let first = first.min(last);
        let source = if *array.data_type() == DataType::Float64 {
            Source::Scalar(array)
        } else if let Some(values) = list.filter(|values| native_numbers(values.data_type())) {
            let (values, offset) = if *values.data_type() == DataType::Float64 {
                (values, 0)
            } else {
                let values = decode(values.slice(first, last - first))?;
                (cast(&values, &DataType::Float64)?, first)
            };
            Source::Native {
                array,
                values,
                offset,
            }
        } else {
            let array = if dynamic::is_dynamic(array.data_type()) {
                array
            } else if crate::arrow_dynamic::is_collection(array.data_type()) {
                crate::arrow_dynamic::convert(array)?
            } else if *array.data_type() == DataType::Null {
                dynamic::from_node(&Node::Null)?.to_array_of_size(array.len())?
            } else {
                return Err(error("unexpected series input type"));
            };
            let constant = if scalar {
                Some(dynamic::node_at(&array, 0)?)
            } else {
                None
            };
            Source::Dynamic { array, constant }
        };
        Ok(Self { source, scalar })
    }

    fn at(&self, row: usize) -> Result<Vector<'_>> {
        let row = if self.scalar { 0 } else { row };
        match &self.source {
            Source::Scalar(array) => {
                let values = downcast::<Float64Array>(array)?;
                Ok(Vector::Scalar(
                    (!values.is_null(row)).then(|| values.value(row)),
                ))
            }
            Source::Native {
                array,
                values,
                offset,
            } => {
                if array.is_null(row) {
                    return Ok(Vector::Missing);
                }
                let (start, len) = list_range(array, row)?;
                check_length(len)?;
                let start = start
                    .checked_sub(*offset)
                    .ok_or_else(|| error("native list precedes its prepared values"))?;
                if start.checked_add(len).is_none_or(|end| end > values.len()) {
                    return Err(error("native list range exceeds its value buffer"));
                }
                Ok(Vector::Native {
                    values: downcast(values)?,
                    start,
                    len,
                })
            }
            Source::Dynamic { array, constant } => {
                if let Some(value) = constant {
                    return Ok(match value {
                        Node::Array(values) => Vector::Nodes(Cow::Borrowed(values)),
                        _ => Vector::Missing,
                    });
                }
                Ok(match dynamic::node_at(array, row)? {
                    Node::Array(values) => Vector::Nodes(Cow::Owned(values)),
                    _ => Vector::Missing,
                })
            }
        }
    }

    fn magnitude(&self, row: usize) -> Result<Option<f64>> {
        let Vector::Scalar(value) = self.at(row)? else {
            return Err(error("cosine magnitudes must be real"));
        };
        Ok(value)
    }
}

fn check_length(len: usize) -> Result<()> {
    if len > dynamic::MAX_COLLECTION_SIZE {
        return Err(error("vector exceeds 1048576 elements"));
    }
    Ok(())
}

enum Vector<'a> {
    Native {
        values: &'a Float64Array,
        start: usize,
        len: usize,
    },
    Nodes(Cow<'a, [Node]>),
    Scalar(Option<f64>),
    Missing,
}

fn number(node: &Node) -> Option<f64> {
    match node {
        Node::Bool(value) => Some(f64::from(u8::from(*value))),
        Node::Long(value) => Some(*value as f64),
        Node::Real(value) => Some(*value),
        Node::Decimal(value) => value.real(),
        Node::Datetime(value) => {
            Some((i128::from(*value).div_euclid(100) + UNIX_EPOCH_TICKS) as f64)
        }
        Node::Timespan(value) => Some(*value as f64 / 100.0),
        _ => None,
    }
}

impl Vector<'_> {
    fn len(&self) -> Option<usize> {
        match self {
            Self::Native { len, .. } => Some(*len),
            Self::Nodes(values) => Some(values.len()),
            _ => None,
        }
    }

    fn value(&self, index: usize) -> Option<f64> {
        match self {
            Self::Native { values, start, .. } => {
                let index = start + index;
                (!values.is_null(index)).then(|| values.value(index))
            }
            Self::Nodes(values) => number(&values[index]),
            Self::Scalar(value) => *value,
            Self::Missing => None,
        }
    }
}

#[derive(Default)]
struct Sum {
    value: f64,
    correction: f64,
}

impl Sum {
    fn add(&mut self, value: f64) {
        let updated = self.value + value;
        if !updated.is_finite() {
            self.value = updated;
            self.correction = 0.0;
            return;
        }
        self.correction += if self.value.abs() >= value.abs() {
            (self.value - updated) + value
        } else {
            (value - updated) + self.value
        };
        self.value = updated;
    }

    fn total(&self) -> f64 {
        self.value + self.correction
    }
}

fn scaled(value: f64, factors: &[f64], divisors: &[f64]) -> f64 {
    if std::iter::once(&value)
        .chain(factors)
        .chain(divisors)
        .any(|value| !value.is_finite() || *value == 0.0)
    {
        return factors.iter().fold(value, |value, factor| value * factor)
            / divisors.iter().product::<f64>();
    }
    let (mut fraction, mut exponent) = libm::frexp(value);
    for &factor in factors {
        let (part, power) = libm::frexp(factor);
        fraction *= part;
        exponent += power;
    }
    for &divisor in divisors {
        let (part, power) = libm::frexp(divisor);
        fraction /= part;
        exponent -= power;
    }
    libm::scalbn(fraction, exponent)
}

#[derive(Default)]
struct Scale {
    maximum: f64,
    nonfinite: bool,
}

impl Scale {
    fn add(&mut self, value: f64) {
        self.maximum = self.maximum.max(value.abs());
        self.nonfinite |= !value.is_finite();
    }

    fn value(&self) -> f64 {
        if self.nonfinite || self.maximum == 0.0 {
            1.0
        } else {
            self.maximum
        }
    }
}

fn dot(left: &Vector<'_>, right: &Vector<'_>, len: usize) -> f64 {
    let (mut a, mut b) = (Scale::default(), Scale::default());
    for index in 0..len {
        if let (Some(x), Some(y)) = (left.value(index), right.value(index)) {
            a.add(x);
            b.add(y);
        }
    }
    let (a, b) = (a.value(), b.value());
    let mut sum = Sum::default();
    for index in 0..len {
        if let (Some(x), Some(y)) = (left.value(index), right.value(index)) {
            sum.add((x / a) * (y / b));
        }
    }
    scaled(sum.total(), &[a, b], &[])
}

fn cosine(left: &Vector<'_>, right: &Vector<'_>, magnitudes: [Option<f64>; 2]) -> f64 {
    let vectors = [left, right];
    let mut scales = [1.0; 2];
    let mut norms = [0.0; 2];
    for (which, vector) in vectors.iter().enumerate() {
        let len = vector.len().expect("cosine inputs are arrays");
        let mut scale = Scale::default();
        for index in 0..len {
            if let Some(value) = vector.value(index) {
                scale.add(value);
            }
        }
        scales[which] = scale.value();
        if magnitudes[which].is_none() {
            let mut sum = Sum::default();
            for index in 0..len {
                if let Some(value) = vector.value(index) {
                    let value = value / scales[which];
                    sum.add(value * value);
                }
            }
            norms[which] = sum.total().sqrt();
        }
    }
    let len = left.len().unwrap().min(right.len().unwrap());
    let mut sum = Sum::default();
    for index in 0..len {
        if let (Some(x), Some(y)) = (left.value(index), right.value(index)) {
            sum.add((x / scales[0]) * (y / scales[1]));
        }
    }
    let mut factors = [1.0; 2];
    let mut divisors = [0.0; 2];
    for which in 0..2 {
        divisors[which] = if let Some(magnitude) = magnitudes[which] {
            factors[which] = scales[which];
            magnitude
        } else {
            norms[which]
        };
    }
    let result = scaled(sum.total(), &factors, &divisors);
    if magnitudes == [None, None] && result.is_finite() {
        result.clamp(-1.0, 1.0)
    } else {
        result
    }
}

fn pearson(left: &Vector<'_>, right: &Vector<'_>, len: usize) -> Option<f64> {
    let origins = [left.value(0)?, right.value(0)?];
    let (mut a, mut b) = (Scale::default(), Scale::default());
    let mut distance = [0.0_f64; 2];
    for index in 0..len {
        let (x, y) = (left.value(index)?, right.value(index)?);
        if !x.is_finite() || !y.is_finite() {
            return None;
        }
        a.add(x);
        b.add(y);
        distance[0] = distance[0].max((x - origins[0]).abs());
        distance[1] = distance[1].max((y - origins[1]).abs());
    }
    let scales = [a.value(), b.value()];
    let normalize = |value: f64, which: usize| {
        let scale = if distance[which] == 0.0 {
            1.0
        } else {
            distance[which]
        };
        if scale.is_finite() {
            (value - origins[which]) / scale
        } else {
            value / scales[which] - origins[which] / scales[which]
        }
    };
    let (mut xmean, mut ymean) = (0.0, 0.0);
    let (mut xx, mut yy, mut xy) = (Sum::default(), Sum::default(), Sum::default());
    for index in 0..len {
        let x = normalize(left.value(index)?, 0);
        let y = normalize(right.value(index)?, 1);
        let (dx, dy) = (x - xmean, y - ymean);
        xmean += dx / (index + 1) as f64;
        ymean += dy / (index + 1) as f64;
        xx.add(dx * (x - xmean));
        yy.add(dy * (y - ymean));
        xy.add(dx * (y - ymean));
    }
    Some((xy.total() / xx.total().max(0.0).sqrt() / yy.total().max(0.0).sqrt()).clamp(-1.0, 1.0))
}

fn compare(
    op: Op,
    left: &Vector<'_>,
    right: &Vector<'_>,
    magnitudes: [Option<f64>; 2],
) -> Option<f64> {
    let len = match (left.len(), right.len()) {
        (Some(a), Some(b)) if op == Op::Pearson && a != b => return None,
        (Some(a), Some(b)) => a.min(b),
        (Some(len), None) if op == Op::Dot && matches!(right, Vector::Scalar(Some(_))) => len,
        (None, Some(len)) if op == Op::Dot && matches!(left, Vector::Scalar(Some(_))) => len,
        _ => return None,
    };
    if len == 0 {
        return None;
    }
    match op {
        Op::Dot => Some(dot(left, right, len)),
        Op::Cosine => Some(cosine(left, right, magnitudes)),
        Op::Pearson => pearson(left, right, len),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    op: Op,
    native: bool,
    signature: Signature,
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        self.op.name(self.native)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let inputs = args
            .args
            .iter()
            .map(Input::new)
            .collect::<Result<Vec<_>>>()?;
        let mut output = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let left = inputs[0].at(row)?;
            let right = inputs[1].at(row)?;
            if let Some(len) = left.len() {
                check_length(len)?;
            }
            if let Some(len) = right.len() {
                check_length(len)?;
            }
            let mut magnitudes = [None; 2];
            for (index, input) in inputs.iter().enumerate().skip(2) {
                magnitudes[index - 2] = input.magnitude(row)?;
            }
            output.append_option(compare(self.op, &left, &right, magnitudes));
        }
        let result = Arc::new(output.finish()) as ArrayRef;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(values: &[f64]) -> Vector<'_> {
        Vector::Nodes(Cow::Owned(
            values.iter().map(|value| Node::Real(*value)).collect(),
        ))
    }

    fn close(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance * expected.abs().max(1.0),
            "{actual} vs {expected}"
        );
    }

    #[test]
    fn similarities_match_independent_centered_and_geometric_oracles() {
        for size in [2, 3, 7, 32, 129] {
            let a = (0..size)
                .map(|index| (index as f64 * 0.3).sin() + 0.1 * index as f64)
                .collect::<Vec<_>>();
            let b = (0..size)
                .map(|index| (index as f64 * 0.7).cos() - 0.2 * index as f64)
                .collect::<Vec<_>>();
            let x = vector(&a);
            let y = vector(&b);
            let product = a.iter().zip(&b).map(|(x, y)| x * y).sum::<f64>();
            let cosine = product
                / a.iter().map(|v| v * v).sum::<f64>().sqrt()
                / b.iter().map(|v| v * v).sum::<f64>().sqrt();
            let xm = a.iter().sum::<f64>() / size as f64;
            let ym = b.iter().sum::<f64>() / size as f64;
            let xy = a
                .iter()
                .zip(&b)
                .map(|(x, y)| (x - xm) * (y - ym))
                .sum::<f64>();
            let xx = a.iter().map(|x| (x - xm).powi(2)).sum::<f64>();
            let yy = b.iter().map(|y| (y - ym).powi(2)).sum::<f64>();
            close(
                compare(Op::Dot, &x, &y, [None, None]).unwrap(),
                product,
                1E-12,
            );
            close(
                compare(Op::Cosine, &x, &y, [None, None]).unwrap(),
                cosine,
                1E-12,
            );
            close(
                compare(Op::Pearson, &x, &y, [None, None]).unwrap(),
                xy / xx.sqrt() / yy.sqrt(),
                1E-12,
            );
        }
    }

    #[test]
    fn cosine_uses_full_magnitudes_even_when_the_dot_product_truncates() {
        let a = vector(&[1.0, 2.0, 3.0]);
        let b = vector(&[4.0, 5.0]);
        assert_eq!(compare(Op::Dot, &a, &b, [None, None]), Some(14.0));
        close(
            compare(Op::Cosine, &a, &b, [None, None]).unwrap(),
            14.0 / (14.0_f64 * 41.0).sqrt(),
            1E-14,
        );
        assert!(compare(Op::Pearson, &a, &b, [None, None]).is_none());
    }

    #[test]
    fn optional_magnitudes_preserve_signed_zero_nan_and_unbounded_results() {
        let a = vector(&[1.0, 2.0, 3.0]);
        let b = vector(&[4.0, 5.0]);
        assert_eq!(
            compare(Op::Cosine, &a, &b, [Some(1.0), Some(1.0)]),
            Some(14.0)
        );
        assert_eq!(
            compare(Op::Cosine, &a, &b, [Some(-1.0), Some(1.0)]),
            Some(-14.0)
        );
        assert_eq!(
            compare(Op::Cosine, &a, &b, [Some(0.0), Some(1.0)]),
            Some(f64::INFINITY)
        );
        assert_eq!(
            compare(Op::Cosine, &a, &b, [Some(-0.0), Some(1.0)]),
            Some(f64::NEG_INFINITY)
        );
        assert!(
            compare(Op::Cosine, &a, &b, [Some(f64::NAN), None])
                .unwrap()
                .is_nan()
        );
        assert_eq!(
            compare(Op::Cosine, &a, &b, [Some(f64::INFINITY), None]),
            Some(0.0)
        );
    }

    #[test]
    fn numeric_coercions_and_missing_positions_follow_the_captured_reference() {
        let a = Vector::Nodes(Cow::Owned(vec![Node::Long(1), Node::Null, Node::Long(3)]));
        let b = vector(&[4.0, 5.0, 6.0]);
        assert_eq!(compare(Op::Dot, &a, &b, [None, None]), Some(22.0));
        close(
            compare(Op::Cosine, &a, &b, [None, None]).unwrap(),
            22.0 / (10.0_f64 * 77.0).sqrt(),
            1E-14,
        );
        assert!(compare(Op::Pearson, &a, &b, [None, None]).is_none());
        let a = Vector::Nodes(Cow::Owned(vec![
            Node::Long(1),
            Node::Bool(true),
            Node::Long(3),
        ]));
        assert_eq!(compare(Op::Dot, &a, &b, [None, None]), Some(27.0));
        close(
            compare(Op::Pearson, &a, &b, [None, None]).unwrap(),
            3.0_f64.sqrt() / 2.0,
            1E-14,
        );
        let missing = Vector::Nodes(Cow::Owned(vec![Node::Null, Node::String("x".into())]));
        assert_eq!(compare(Op::Dot, &missing, &b, [None, None]), Some(0.0));
        assert!(
            compare(Op::Cosine, &missing, &b, [None, None])
                .unwrap()
                .is_nan()
        );
    }

    #[test]
    fn scalar_broadcasting_is_limited_to_native_numeric_dot_operands() {
        let a = vector(&[1.0, 2.0, 3.0]);
        let ten = Vector::Scalar(Some(10.0));
        assert_eq!(compare(Op::Dot, &a, &ten, [None, None]), Some(60.0));
        assert_eq!(compare(Op::Dot, &ten, &a, [None, None]), Some(60.0));
        assert!(compare(Op::Dot, &a, &Vector::Scalar(None), [None, None]).is_none());
        assert!(compare(Op::Dot, &a, &Vector::Missing, [None, None]).is_none());
    }

    #[test]
    fn compensation_and_binary_scaling_avoid_unnecessary_intermediate_overflow() {
        let a = vector(&[1E16, 1.0, -1E16]);
        let b = vector(&[1.0, 1.0, 1.0]);
        close(compare(Op::Dot, &a, &b, [None, None]).unwrap(), 1.0, 1E-14);
        let huge = vector(&[1E308, -1E308]);
        let positive = vector(&[1E308, 1E308]);
        assert_eq!(compare(Op::Dot, &huge, &positive, [None, None]), Some(0.0));
        close(
            compare(Op::Cosine, &huge, &huge, [None, None]).unwrap(),
            1.0,
            1E-14,
        );
        let tiny = vector(&[1E-300, 2E-300, 3E-300]);
        close(
            compare(Op::Cosine, &tiny, &tiny, [None, None]).unwrap(),
            1.0,
            1E-14,
        );
        close(
            compare(Op::Pearson, &tiny, &tiny, [None, None]).unwrap(),
            1.0,
            1E-14,
        );
        close(scaled(1E-300, &[1E300, 1E300], &[1E300]), 1.0, 1E-14);
    }

    #[test]
    fn pearson_preserves_small_variations_on_large_origins() {
        let a = vector(&[1E15, 1E15 + 1.0, 1E15 + 2.0, 1E15 + 3.0]);
        let b = vector(&[6.0, 4.0, 2.0, 0.0]);
        close(
            compare(Op::Pearson, &a, &b, [None, None]).unwrap(),
            -1.0,
            1E-14,
        );
        let opposite = vector(&[-1E308, 1E308]);
        close(
            compare(Op::Pearson, &opposite, &opposite, [None, None]).unwrap(),
            1.0,
            1E-14,
        );
    }

    #[test]
    fn nonfinite_values_and_empty_or_constant_inputs_keep_native_real_semantics() {
        let a = vector(&[f64::NAN, 1.0, 2.0]);
        let b = vector(&[3.0, 4.0, 5.0]);
        assert!(compare(Op::Dot, &a, &b, [None, None]).unwrap().is_nan());
        assert!(compare(Op::Cosine, &a, &b, [None, None]).unwrap().is_nan());
        assert!(compare(Op::Pearson, &a, &b, [None, None]).is_none());
        let a = vector(&[f64::INFINITY, 1.0, 2.0]);
        assert_eq!(compare(Op::Dot, &a, &b, [None, None]), Some(f64::INFINITY));
        let single = vector(&[7.0]);
        let constant = vector(&[7.0; 3]);
        assert!(
            compare(Op::Pearson, &single, &single, [None, None])
                .unwrap()
                .is_nan()
        );
        assert!(
            compare(Op::Pearson, &constant, &b, [None, None])
                .unwrap()
                .is_nan()
        );
        let empty = vector(&[]);
        for op in [Op::Dot, Op::Cosine, Op::Pearson] {
            assert!(compare(op, &empty, &b, [None, None]).is_none());
        }
    }

    #[test]
    fn native_float_lists_keep_the_original_value_buffer() {
        use datafusion::arrow::datatypes::Float64Type;
        let list = ListArray::from_iter_primitive::<Float64Type, _, _>([Some(vec![
            Some(1.0),
            None,
            Some(3.0),
        ])]);
        let original = Arc::clone(list.values());
        let input = Input::new(&ColumnarValue::Array(Arc::new(list))).unwrap();
        let Source::Native { values, .. } = &input.source else {
            panic!("native lists must bypass dynamic conversion")
        };
        assert!(Arc::ptr_eq(values, &original));
        let vector = input.at(0).unwrap();
        assert_eq!(vector.value(0), Some(1.0));
        assert_eq!(vector.value(1), None);
        assert_eq!(vector.value(2), Some(3.0));
    }

    #[test]
    fn comparison_length_budget_checks_the_exact_boundary() {
        assert!(check_length(dynamic::MAX_COLLECTION_SIZE).is_ok());
        assert!(check_length(dynamic::MAX_COLLECTION_SIZE + 1).is_err());
    }
}
