use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use rustfft::num_complex::Complex;
use rustfft::{Fft, FftPlanner};

use crate::ast::{self, Span};
use crate::dynamic::{self, Kernel};
use crate::dynamic_value::Node;
use crate::expressions::Expressions;
use crate::series_functions::{self, Op};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Multi {
    Stats,
    FitLine,
    Fft,
    Ifft,
}

impl Multi {
    pub(crate) fn lookup(name: &str) -> Option<Self> {
        Some(match name {
            "series_stats" => Self::Stats,
            "series_fit_line" => Self::FitLine,
            "series_fft" => Self::Fft,
            "series_ifft" => Self::Ifft,
            _ => return None,
        })
    }

    fn names(self) -> &'static [&'static str] {
        match self {
            Self::Stats => &[
                "min", "min_idx", "max", "max_idx", "avg", "stdev", "variance",
            ],
            Self::FitLine => &[
                "rsquare",
                "slope",
                "variance",
                "rvariance",
                "interception",
                "line_fit",
            ],
            Self::Fft | Self::Ifft => &["real", "imag"],
        }
    }

    pub(crate) fn fields(self) -> Vec<FieldRef> {
        self.names()
            .iter()
            .map(|name| {
                let ty = match *name {
                    "min_idx" | "max_idx" => DataType::Int64,
                    "line_fit" | "real" | "imag" => dynamic::data_type(),
                    _ => DataType::Float64,
                };
                Arc::new(Field::new(*name, ty, true))
            })
            .collect()
    }
}

pub(crate) fn plan(
    op: Multi,
    expressions: &Expressions<'_>,
    args: &[ast::Expr],
    schema: &DFSchema,
    span: Span,
) -> crate::Result<Expr> {
    let valid = if op == Multi::FitLine {
        args.len() == 1
    } else {
        (1..=2).contains(&args.len())
    };
    if !valid {
        return Err(crate::Error::syntax(
            "invalid multi-output series argument count",
            span,
        ));
    }
    let mut values = Vec::with_capacity(args.len());
    for (index, argument) in args.iter().enumerate() {
        let value = expressions.scalar(argument, schema)?;
        if op == Multi::Stats && index == 1 {
            if value.get_type(schema)? != DataType::Boolean {
                return Err(crate::Error::syntax(
                    "series_stats ignore_nonfinite must be a constant bool",
                    argument.span,
                ));
            }
            let ScalarValue::Boolean(Some(flag)) = expressions.constant(value, argument.span)?
            else {
                return Err(crate::Error::syntax(
                    "series_stats ignore_nonfinite must be a nonnull constant bool",
                    argument.span,
                ));
            };
            values.push(datafusion::prelude::lit(flag));
        } else {
            let ty = value.get_type(schema)?;
            if !dynamic::is_dynamic(&ty)
                && !crate::arrow_dynamic::is_collection(&ty)
                && ty != DataType::Null
            {
                return Err(crate::Error::syntax(
                    "series functions require dynamic or native arrays",
                    argument.span,
                ));
            }
            values.push(dynamic::call(Kernel::IntoDynamic, vec![value]));
        }
    }
    let types = (0..values.len())
        .map(|index| {
            if op == Multi::Stats && index == 1 {
                DataType::Boolean
            } else {
                dynamic::data_type()
            }
        })
        .collect();
    Ok(ScalarUDF::from(Function {
        op,
        signature: Signature::exact(types, Volatility::Immutable),
    })
    .call(values))
}

pub(crate) fn projections(
    item: &ast::NamedExpr,
    expressions: &Expressions<'_>,
    schema: &DFSchema,
    used: &mut HashSet<String>,
    truncate_named: bool,
    temporary: &str,
    computations: &mut Vec<(String, Expr)>,
) -> crate::Result<Option<Vec<Expr>>> {
    let (names, value) = match &item.expr.kind {
        ast::ExprKind::ColumnNames { names, expr } => (Some(names), expr.as_ref()),
        _ => (None, &item.expr),
    };
    let op = match &value.kind {
        ast::ExprKind::Call { name, .. } => Multi::lookup(name),
        _ => None,
    };
    let Some(op) = op else {
        return if names.is_some() {
            Err(crate::Error::unsupported(
                "output-name lists require a supported multi-output series function",
                item.expr.span,
            ))
        } else {
            Ok(None)
        };
    };
    let ast::ExprKind::Call { name, args } = &value.kind else {
        unreachable!()
    };
    let fields = op.fields();
    if names.is_some_and(|names| names.is_empty() || names.len() > fields.len()) {
        return Err(crate::Error::syntax(
            format!(
                "this series function requires 1-{} output names",
                fields.len()
            ),
            item.expr.span,
        ));
    }
    let result = plan(op, expressions, args, schema, value.span)?;
    let temporary = format!("{temporary}_tuple_{}", computations.len());
    computations.push((temporary.clone(), result));
    let prefix = item.name.clone().unwrap_or_else(|| {
        let argument = args.first().and_then(|arg| match &arg.kind {
            ast::ExprKind::Identifier(parts) => parts.last(),
            _ => None,
        });
        argument.map_or_else(|| name.clone(), |argument| format!("{name}_{argument}"))
    });
    let count = if truncate_named {
        names.map_or(fields.len(), Vec::len)
    } else {
        fields.len()
    };
    fields
        .iter()
        .take(count)
        .enumerate()
        .map(|(index, field)| {
            let generated = format!("{prefix}_{}", field.name());
            let alias = crate::planner::output_name(
                &ast::NamedExpr {
                    name: names.and_then(|names| names.get(index)).cloned(),
                    expr: ast::Expr {
                        kind: ast::ExprKind::Identifier(vec![generated]),
                        span: item.expr.span,
                        depth: 1,
                    },
                },
                index,
                used,
            )?;
            Ok(
                Expr::Column(Column::from_name(format!("{temporary}.{}", field.name())))
                    .alias(alias),
            )
        })
        .collect::<crate::Result<Vec<_>>>()
        .map(Some)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    op: Multi,
    signature: Signature,
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn numeric(node: &Node) -> Option<f64> {
    let value = match node {
        Node::Long(value) => *value as f64,
        Node::Real(value) => *value,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

fn pack(fields: &[FieldRef], values: Vec<ScalarValue>) -> Result<ScalarValue> {
    let arrays = values
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
        fields.to_vec().into(),
        arrays,
        None,
    )?)))
}

fn nulls(fields: &[FieldRef]) -> Result<ScalarValue> {
    pack(
        fields,
        fields
            .iter()
            .map(|field| {
                if dynamic::is_dynamic(field.data_type()) {
                    dynamic::from_node(&Node::Null)
                } else {
                    ScalarValue::try_from(field.data_type())
                }
            })
            .collect::<Result<Vec<_>>>()?,
    )
}

struct Fourier {
    plan: Arc<dyn Fft<f64>>,
    len: usize,
    scratch: Vec<Complex<f64>>,
}

impl Fourier {
    fn new(len: usize, inverse: bool) -> Result<Self> {
        if len == 0 || len > dynamic::MAX_COLLECTION_SIZE {
            return Err(error(
                "series FFT length must be between 1 and 1048576 points",
            ));
        }
        let mut planner = FftPlanner::new();
        let plan = if inverse {
            planner.plan_fft_inverse(len)
        } else {
            planner.plan_fft_forward(len)
        };
        let scratch_len = plan.get_inplace_scratch_len();
        let bytes = len
            .checked_add(scratch_len)
            .and_then(|size| size.checked_mul(size_of::<Complex<f64>>()))
            .ok_or_else(|| error("series FFT working-buffer size overflow"))?;
        if bytes > 128 << 20 {
            return Err(error(
                "series FFT exceeds its 128 MiB numeric working-buffer budget",
            ));
        }
        Ok(Self {
            plan,
            len,
            scratch: vec![Complex::default(); scratch_len],
        })
    }

    fn apply(&mut self, values: &mut [Complex<f64>], inverse: bool) -> Result<()> {
        self.plan.process_with_scratch(values, &mut self.scratch);
        let scale = if inverse { 1.0 / self.len as f64 } else { 1.0 };
        for value in values {
            *value *= scale;
            if !value.re.is_finite() || !value.im.is_finite() {
                return Err(error(
                    "series FFT produced nonfinite output; rescale the input",
                ));
            }
        }
        Ok(())
    }
}

impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.op {
            Multi::Stats => "__kql_series_stats_columns",
            Multi::FitLine => "__kql_series_fit_line_columns",
            Multi::Fft => "__kql_series_fft_columns",
            Multi::Ifft => "__kql_series_ifft_columns",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(self.op.fields().into()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(1))
            .collect::<Result<Vec<_>>>()?;
        let at = |index, row| {
            if matches!(args.args[index], ColumnarValue::Scalar(_)) {
                0
            } else {
                row
            }
        };
        let fields = self.op.fields();
        let mut output = Vec::with_capacity(rows);
        let mut fourier: Option<Fourier> = None;
        for row in 0..rows {
            if matches!(self.op, Multi::Fft | Multi::Ifft) {
                let real = dynamic::node_at(&arrays[0], at(0, row))?;
                let Node::Array(real) = real else {
                    output.push(nulls(&fields)?);
                    continue;
                };
                if real.len() > dynamic::MAX_COLLECTION_SIZE {
                    return Err(error("series FFT exceeds 1048576 points"));
                }
                let imaginary = if arrays.len() == 2 {
                    let Node::Array(values) = dynamic::node_at(&arrays[1], at(1, row))? else {
                        output.push(nulls(&fields)?);
                        continue;
                    };
                    if values.len() != real.len() {
                        return Err(error(
                            "series FFT real and imaginary arrays must have the same length",
                        ));
                    }
                    Some(values)
                } else {
                    None
                };
                let values = real
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        Some(Complex::new(
                            numeric(value)?,
                            match &imaginary {
                                Some(values) => numeric(&values[index])?,
                                None => 0.0,
                            },
                        ))
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(mut values) = values else {
                    output.push(nulls(&fields)?);
                    continue;
                };
                if !values.is_empty() {
                    let inverse = self.op == Multi::Ifft;
                    if fourier.as_ref().is_none_or(|plan| plan.len != values.len()) {
                        fourier = Some(Fourier::new(values.len(), inverse)?);
                    }
                    fourier
                        .as_mut()
                        .ok_or_else(|| error("missing FFT plan"))?
                        .apply(&mut values, inverse)?;
                }
                output.push(pack(
                    &fields,
                    vec![
                        dynamic::from_node(&Node::Array(
                            values.iter().map(|value| Node::Real(value.re)).collect(),
                        ))?,
                        dynamic::from_node(&Node::Array(
                            values.iter().map(|value| Node::Real(value.im)).collect(),
                        ))?,
                    ],
                )?);
            } else {
                let series = dynamic::node_at(&arrays[0], at(0, row))?.json()?;
                let mut values = vec![series];
                if arrays.len() == 2 {
                    let flag = ScalarValue::try_from_array(&arrays[1], at(1, row))?;
                    let ScalarValue::Boolean(Some(flag)) = flag else {
                        return Err(error("series_stats ignore_nonfinite cannot be null"));
                    };
                    values.push(flag.into());
                }
                let result = series_functions::evaluate(
                    if self.op == Multi::Stats {
                        Op::Stats
                    } else {
                        Op::FitLine
                    },
                    &values,
                )?;
                let Some(result) = result.as_object() else {
                    output.push(nulls(&fields)?);
                    continue;
                };
                output.push(pack(
                    &fields,
                    fields
                        .iter()
                        .map(|field| {
                            let value =
                                result.get(field.name()).unwrap_or(&serde_json::Value::Null);
                            match field.data_type() {
                                DataType::Float64 => Ok(ScalarValue::Float64(value.as_f64())),
                                DataType::Int64 => Ok(ScalarValue::Int64(value.as_i64())),
                                _ => dynamic::from_json(value),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?,
                )?);
            }
        }
        let result: ArrayRef = if output.is_empty() {
            datafusion::arrow::array::new_empty_array(&DataType::Struct(fields.into()))
        } else {
            ScalarValue::iter_to_array(output)?
        };
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

    #[test]
    fn fft_matches_direct_complex_dft_and_normalized_inverse() {
        for len in [1, 2, 3, 4, 7, 16, 31, 64, 97] {
            let input = (0..len)
                .map(|index| Complex::new((index as f64 * 0.3).sin(), (index as f64 * 0.7).cos()))
                .collect::<Vec<_>>();
            let expected = (0..len)
                .map(|frequency| {
                    input
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            let angle = -2.0 * std::f64::consts::PI * (index * frequency) as f64
                                / len as f64;
                            *value * Complex::new(angle.cos(), angle.sin())
                        })
                        .sum::<Complex<f64>>()
                })
                .collect::<Vec<_>>();
            let mut actual = input.clone();
            Fourier::new(len, false)
                .unwrap()
                .apply(&mut actual, false)
                .unwrap();
            for (actual, expected) in actual.iter().zip(expected) {
                assert!(
                    (*actual - expected).norm() <= 1E-10 * len as f64,
                    "len={len}: {actual} vs {expected}"
                );
            }
            Fourier::new(len, true)
                .unwrap()
                .apply(&mut actual, true)
                .unwrap();
            for (actual, expected) in actual.iter().zip(input) {
                assert!((*actual - expected).norm() <= 1E-12 * len as f64);
            }
        }
    }

    #[test]
    fn fft_checks_the_exact_point_boundary_and_nonfinite_output() {
        assert!(Fourier::new(0, false).is_err());
        assert!(Fourier::new(dynamic::MAX_COLLECTION_SIZE + 1, false).is_err());
        let mut boundary = Fourier::new(dynamic::MAX_COLLECTION_SIZE, false).unwrap();
        let mut values = vec![Complex::default(); dynamic::MAX_COLLECTION_SIZE];
        boundary.apply(&mut values, false).unwrap();
        assert!(
            values
                .iter()
                .all(|value| value.re == 0.0 && value.im == 0.0)
        );
        let mut invalid = vec![Complex::new(f64::MAX, 0.0); 2];
        assert!(
            Fourier::new(2, false)
                .unwrap()
                .apply(&mut invalid, false)
                .is_err()
        );
    }
}
