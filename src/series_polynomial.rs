use datafusion::common::{DataFusionError, Result};
use nalgebra::{DMatrix, DVector, linalg::SVD};
use serde_json::{Number, Value as Json};

use crate::dynamic::MAX_COLLECTION_SIZE;
use crate::series_analytics::Normalization;

const MAX_WORK: usize = 67_108_864;
const MAX_NUMERIC_BYTES: usize = 128 << 20;

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(format!("series polynomial regression: {}", message.into()))
}

fn real(value: f64) -> Result<Json> {
    Number::from_f64(value)
        .map(Json::Number)
        .ok_or_else(|| error("nonfinite output; rescale the input or lower the degree"))
}

fn budget(rows: usize, width: usize) -> Result<()> {
    let squared = width.checked_mul(width);
    let work = squared.and_then(|squared| {
        rows.checked_mul(2)?
            .checked_add(width.checked_mul(64)?)?
            .checked_mul(squared)
    });
    if work.is_none_or(|work| work > MAX_WORK) {
        return Err(error("exceeds the 67108864-unit fitting-work budget"));
    }
    let bytes = squared.and_then(|squared| {
        squared
            .checked_mul(8)?
            .checked_add(rows.checked_mul(4)?)?
            .checked_add(width.checked_mul(16)?)?
            .checked_mul(size_of::<f64>())
    });
    if bytes.is_none_or(|bytes| bytes > MAX_NUMERIC_BYTES) {
        return Err(error("exceeds the 128 MiB numeric working-buffer budget"));
    }
    Ok(())
}

struct Qr {
    upper: DMatrix<f64>,
    projected: DVector<f64>,
}

impl Qr {
    fn new(width: usize) -> Self {
        Self {
            upper: DMatrix::zeros(width, width),
            projected: DVector::zeros(width),
        }
    }

    fn add(&mut self, row: &mut [f64], mut value: f64) {
        for column in 0..row.len() {
            let incoming = row[column];
            if incoming == 0.0 {
                continue;
            }
            let diagonal = self.upper[(column, column)];
            let norm = diagonal.hypot(incoming);
            let cosine = diagonal / norm;
            let sine = incoming / norm;
            self.upper[(column, column)] = norm;
            for (next, incoming) in row.iter_mut().enumerate().skip(column + 1) {
                let old = self.upper[(column, next)];
                self.upper[(column, next)] = cosine.mul_add(old, sine * *incoming);
                *incoming = (-sine).mul_add(old, cosine * *incoming);
            }
            let old = self.projected[column];
            self.projected[column] = cosine.mul_add(old, sine * value);
            value = (-sine).mul_add(old, cosine * value);
        }
    }

    fn solve(self, rows: usize) -> Result<(Vec<f64>, usize)> {
        let width = self.projected.len();
        let svd = SVD::try_new_unordered(self.upper, true, true, f64::EPSILON, 64 * width * width)
            .ok_or_else(|| error("the bounded SVD solver did not converge"))?;
        if svd.singular_values.iter().any(|value| !value.is_finite()) {
            return Err(error("nonfinite singular values; rescale the input"));
        }
        let largest = svd.singular_values.iter().copied().fold(0.0, f64::max);
        let tolerance = largest * rows.max(width) as f64 * f64::EPSILON;
        let rank = svd.rank(tolerance);
        let result = svd
            .solve(&self.projected, tolerance)
            .map_err(|message| error(format!("SVD solution failed: {message}")))?;
        if result.iter().any(|value| !value.is_finite()) {
            return Err(error("nonfinite coefficients; rescale the input"));
        }
        Ok((result.as_slice().to_vec(), rank))
    }
}

struct Axis {
    center: f64,
    scale: f64,
    constant: bool,
}

impl Axis {
    fn new(values: &[f64]) -> Self {
        let low = values.iter().copied().fold(f64::INFINITY, f64::min);
        let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let range = high - low;
        let center = if range.is_finite() {
            low + range / 2.0
        } else {
            low / 2.0 + high / 2.0
        };
        let scale = (high - center).abs().max((low - center).abs());
        Self {
            center,
            scale: if scale == 0.0 { 1.0 } else { scale },
            constant: scale == 0.0,
        }
    }

    fn normalize(&self, value: f64) -> f64 {
        (value - self.center) / self.scale
    }
}

fn chebyshev(row: &mut [f64], x: f64) {
    row[0] = 1.0;
    if row.len() > 1 {
        row[1] = x;
    }
    for index in 2..row.len() {
        row[index] = (2.0 * x).mul_add(row[index - 1], -row[index - 2]);
    }
}

fn evaluate(coefficients: &[f64], x: f64) -> f64 {
    let mut next = 0.0;
    let mut after = 0.0;
    for coefficient in coefficients.iter().skip(1).rev() {
        let value = (2.0 * x).mul_add(next, coefficient - after);
        after = next;
        next = value;
    }
    x.mul_add(next, coefficients[0] - after)
}

fn power_coefficients(coefficients: &[f64], axis: &Axis, normalization: Normalization) -> Vec<f64> {
    let width = coefficients.len();
    let mut powers = vec![0.0; width];
    let mut previous = vec![0.0; width];
    let mut current = vec![0.0; width];
    previous[0] = 1.0;
    powers[0] = coefficients[0];
    if width > 1 {
        current[1] = 1.0;
        powers[1] = coefficients[1];
    }
    for coefficient in coefficients.iter().skip(2) {
        let mut next = vec![0.0; width];
        for index in 0..width {
            next[index] = if index == 0 {
                0.0
            } else {
                2.0 * current[index - 1]
            } - previous[index];
            powers[index] = coefficient.mul_add(next[index], powers[index]);
        }
        previous = current;
        current = next;
    }
    let mut output = vec![powers[width - 1]];
    for coefficient in powers[..width - 1].iter().rev() {
        let mut next = vec![0.0; output.len() + 1];
        for (index, value) in output.iter().enumerate() {
            next[index] += -axis.center / axis.scale * value;
            next[index + 1] += value / axis.scale;
        }
        next[0] += coefficient;
        output = next;
    }
    for coefficient in output.iter_mut().skip(1) {
        *coefficient *= normalization.scale();
    }
    output[0] = normalization.restore(output[0]);
    output.reverse();
    output
}

fn minimum_norm(x: &[f64], y: &[f64], width: usize) -> Result<(Vec<f64>, Vec<f64>)> {
    let maximum = x.iter().map(|value| value.abs()).fold(1.0, f64::max);
    let degree = i32::try_from(width - 1).map_err(|_| error("degree is too large"))?;
    let design_scale = maximum.powi(degree);
    if !design_scale.is_finite() {
        return Err(error(
            "rank-deficient original-coordinate powers overflow; normalize x or lower the degree",
        ));
    }
    let response_scale = y.iter().map(|value| value.abs()).fold(0.0, f64::max);
    let response_scale = if response_scale == 0.0 {
        1.0
    } else {
        response_scale
    };
    let mut qr = Qr::new(width);
    let mut row = vec![0.0; width];
    let fill = |row: &mut [f64], value| {
        row[0] = 1.0 / design_scale;
        for index in 1..row.len() {
            row[index] = row[index - 1] * value;
        }
    };
    for (&x, &y) in x.iter().zip(y) {
        fill(&mut row, x);
        qr.add(&mut row, y / response_scale);
    }
    let (scaled, _) = qr.solve(x.len())?;
    let fitted = x
        .iter()
        .map(|&x| {
            fill(&mut row, x);
            row.iter().zip(&scaled).map(|(x, y)| x * y).sum::<f64>() * response_scale
        })
        .collect();
    let coefficients = scaled
        .iter()
        .rev()
        .map(|value| value / design_scale * response_scale)
        .collect();
    Ok((coefficients, fitted))
}

pub(crate) fn fit(series: &Json, coordinates: Option<&Json>, degree: i64) -> Result<Json> {
    let Some(series) = series.as_array() else {
        return Ok(Json::Null);
    };
    if series.len() > MAX_COLLECTION_SIZE {
        return Err(error("exceeds 1048576 points"));
    }
    if series.is_empty() {
        return Ok(Json::Null);
    }
    let degree = if degree < 0 { 1 } else { degree };
    let width = usize::try_from(degree)
        .ok()
        .and_then(|degree| degree.checked_add(1))
        .ok_or_else(|| error("degree is too large"))?;
    budget(series.len(), width)?;
    let Some(y) = series
        .iter()
        .map(|value| value.as_f64().filter(|value| value.is_finite()))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(Json::Null);
    };
    let x = if let Some(coordinates) = coordinates {
        let Some(coordinates) = coordinates.as_array() else {
            return Ok(Json::Null);
        };
        if coordinates.len() != y.len() {
            return Ok(Json::Null);
        }
        let Some(values) = coordinates
            .iter()
            .map(|value| value.as_f64().filter(|value| value.is_finite()))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(Json::Null);
        };
        values
    } else {
        (1..=y.len()).map(|value| value as f64).collect()
    };
    let normalization = Normalization::new(&y);
    let axis = Axis::new(&x);
    let mut qr = Qr::new(width);
    let mut row = vec![0.0; width];
    let mut mean = 0.0;
    let mut variance = 0.0;
    for (index, (&x, &y)) in x.iter().zip(&y).enumerate() {
        let normalized = normalization.normalize(y);
        let delta = normalized - mean;
        mean += delta / (index + 1) as f64;
        variance += delta * (normalized - mean);
        chebyshev(&mut row, axis.normalize(x));
        qr.add(&mut row, normalized);
    }
    let (normalized, rank) = qr.solve(y.len())?;
    let (coefficients, fitted, residual) = if rank == width {
        let mut residual = 0.0;
        let fitted = x
            .iter()
            .zip(&y)
            .map(|(&x, &y)| {
                let estimate = evaluate(&normalized, axis.normalize(x));
                let difference = normalization.normalize(y) - estimate;
                residual += difference * difference;
                normalization.restore(estimate)
            })
            .collect::<Vec<_>>();
        (
            power_coefficients(&normalized, &axis, normalization),
            fitted,
            residual,
        )
    } else {
        // A normalized basis changes the minimum-norm answer when rank is lost.
        let (coefficients, fitted) = minimum_norm(&x, &y, width)?;
        let residual = y
            .iter()
            .zip(&fitted)
            .map(|(&value, &fit)| {
                let difference = normalization.normalize(value) - normalization.normalize(fit);
                difference * difference
            })
            .sum();
        (coefficients, fitted, residual)
    };
    let rsquare = if variance <= 0.0 || width == 1 || axis.constant {
        0.0
    } else {
        (1.0 - residual / variance).clamp(0.0, 1.0)
    };
    let variance_of = |value| {
        if y.len() == 1 {
            0.0
        } else {
            normalization.variance(value, y.len())
        }
    };
    Ok(Json::Object(serde_json::Map::from_iter([
        ("rsquare".into(), real(rsquare)?),
        (
            "coefficients".into(),
            Json::Array(coefficients.into_iter().map(real).collect::<Result<_>>()?),
        ),
        ("variance".into(), real(variance_of(variance.max(0.0)))?),
        ("rvariance".into(), real(variance_of(residual))?),
        (
            "poly_fit".into(),
            Json::Array(fitted.into_iter().map(real).collect::<Result<_>>()?),
        ),
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn array(result: &Json, field: &str) -> Vec<f64> {
        result[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_f64().unwrap())
            .collect()
    }

    fn close(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance * expected.abs().max(1.0),
            "{actual} vs {expected}"
        );
    }

    #[test]
    fn polynomial_fits_use_one_based_default_x_and_descending_coefficients() {
        let line = fit(&json!([1, 4, 9, 16]), None, 1).unwrap();
        for (actual, expected) in array(&line, "coefficients").into_iter().zip([5.0, -5.0]) {
            close(actual, expected, 1E-12);
        }
        close(line["variance"].as_f64().unwrap(), 43.0, 1E-12);
        close(line["rvariance"].as_f64().unwrap(), 4.0 / 3.0, 1E-12);
        let quadratic = fit(&json!([1, 4, 9, 16]), Some(&json!([1, 2, 3, 4])), 2).unwrap();
        for (actual, expected) in array(&quadratic, "coefficients")
            .into_iter()
            .zip([1.0, 0.0, 0.0])
        {
            close(actual, expected, 1E-12);
        }
        for (actual, expected) in array(&quadratic, "poly_fit")
            .into_iter()
            .zip([1.0, 4.0, 9.0, 16.0])
        {
            close(actual, expected, 1E-12);
        }
    }

    #[test]
    fn uneven_noisy_coordinates_match_captured_kusto_regression() {
        let result = fit(&json!([3, 7, 43, 73]), Some(&json!([0, 1, 4, 5])), 2).unwrap();
        for (actual, expected) in array(&result, "coefficients").into_iter().zip([
            3.25,
            -2.7794117647059013,
            4.3235294117647225,
        ]) {
            close(actual, expected, 1E-12);
        }
        close(
            result["rsquare"].as_f64().unwrap(),
            0.9959487927402366,
            1E-12,
        );
        close(result["variance"].as_f64().unwrap(), 1089.0, 1E-12);
        close(
            result["rvariance"].as_f64().unwrap(),
            4.411764705882351,
            1E-12,
        );
    }

    #[test]
    fn rank_deficient_and_underdetermined_systems_use_original_coordinate_minimum_norm() {
        for (y, x, degree, coefficients) in [
            (
                json!([1, 2, 3, 4]),
                json!([1, 1, 1, 1]),
                1,
                vec![1.25, 1.25],
            ),
            (json!([7]), json!([1]), 1, vec![3.5, 3.5]),
            (
                json!([1, 2]),
                json!([1, 2]),
                2,
                vec![3.0 / 14.0, 5.0 / 14.0, 3.0 / 7.0],
            ),
        ] {
            let result = fit(&y, Some(&x), degree).unwrap();
            for (actual, expected) in array(&result, "coefficients").into_iter().zip(coefficients) {
                close(actual, expected, 1E-12);
            }
        }
        let single = fit(&json!([7]), None, 1).unwrap();
        assert_eq!(single["rsquare"], 0.0);
        assert_eq!(single["variance"], 0.0);
        assert_eq!(single["rvariance"], 0.0);
    }

    #[test]
    fn constant_and_degree_zero_fits_have_captured_zero_variance_rsquare() {
        let constant = fit(&json!([7, 7, 7, 7]), None, 1).unwrap();
        assert_eq!(constant["rsquare"], 0.0);
        assert_eq!(constant["variance"], 0.0);
        let zero = fit(&json!([1, 2, 3, 4]), None, 0).unwrap();
        close(array(&zero, "coefficients")[0], 2.5, 1E-12);
        for value in array(&zero, "poly_fit") {
            close(value, 2.5, 1E-12);
        }
        close(zero["variance"].as_f64().unwrap(), 5.0 / 3.0, 1E-12);
        close(zero["rvariance"].as_f64().unwrap(), 5.0 / 3.0, 1E-12);
        let negative = fit(&json!([1, 4, 9, 16]), None, -100).unwrap();
        assert_eq!(negative, fit(&json!([1, 4, 9, 16]), None, 1).unwrap());
    }

    #[test]
    fn normalized_coordinates_recover_high_degree_polynomials_and_narrow_large_origins() {
        let coefficients = [1.0, -0.5, 2.0, 0.25, -1.0, 0.75, 0.125, -0.25, 0.0625];
        let x = (0..33)
            .map(|index| (index as f64 / 16.0) - 1.0)
            .collect::<Vec<_>>();
        let y = x
            .iter()
            .map(|&x| {
                coefficients
                    .iter()
                    .fold(0.0_f64, |value, coefficient| value.mul_add(x, *coefficient))
            })
            .collect::<Vec<_>>();
        let result = fit(&json!(y), Some(&json!(x)), 8).unwrap();
        for (actual, expected) in array(&result, "coefficients").into_iter().zip(coefficients) {
            close(actual, expected, 1E-10);
        }
        let x = (0..9).map(|index| 1E9 + index as f64).collect::<Vec<_>>();
        let y = (0..9)
            .map(|index| (index as f64 - 4.0).powi(2) + 3.0)
            .collect::<Vec<_>>();
        let result = fit(&json!(y), Some(&json!(x)), 2).unwrap();
        for (actual, expected) in array(&result, "poly_fit").into_iter().zip(y) {
            close(actual, expected, 1E-12);
        }
        let huge = fit(&json!(vec![1E308; 8]), None, 1).unwrap();
        assert_eq!(array(&huge, "poly_fit"), [1E308; 8]);
        assert_eq!(huge["variance"], 0.0);
    }

    #[test]
    fn noisy_least_squares_residuals_are_orthogonal_to_each_fitted_basis() {
        for degree in 0..=8 {
            let x = (0..65)
                .map(|index| index as f64 / 32.0 - 1.0)
                .collect::<Vec<_>>();
            let y = x
                .iter()
                .map(|&x| x.powi(3) + 0.2 * (17.0 * x).sin())
                .collect::<Vec<_>>();
            let result = fit(&json!(y), Some(&json!(x)), degree).unwrap();
            let fitted = array(&result, "poly_fit");
            let mean = y.iter().sum::<f64>() / y.len() as f64;
            let variance = y.iter().map(|value| (value - mean).powi(2)).sum::<f64>();
            let residual = y
                .iter()
                .zip(&fitted)
                .map(|(value, fit)| (value - fit).powi(2))
                .sum::<f64>();
            close(result["variance"].as_f64().unwrap(), variance / 64.0, 1E-12);
            close(
                result["rvariance"].as_f64().unwrap(),
                residual / 64.0,
                1E-12,
            );
            for power in 0..=degree {
                let orthogonal = x
                    .iter()
                    .zip(y.iter().zip(&fitted))
                    .map(|(x, (value, fit))| (value - fit) * x.powi(power as i32))
                    .sum::<f64>();
                assert!(
                    orthogonal.abs() < 1E-10,
                    "degree={degree}, power={power}: {orthogonal}"
                );
            }
        }
    }

    #[test]
    fn streaming_reduction_matches_a_separate_full_vandermonde_pseudoinverse() {
        for count in [1, 2, 3, 7, 15] {
            for degree in 0..=6 {
                for duplicate in [false, true] {
                    let x = (0..count)
                        .map(|index| {
                            if duplicate {
                                (index % 2) as f64
                            } else {
                                index as f64 / count as f64 * 2.0 - 1.0
                            }
                        })
                        .collect::<Vec<_>>();
                    let y = (0..count)
                        .map(|index| (index as f64 * 0.7).sin() + 0.1 * index as f64)
                        .collect::<Vec<_>>();
                    let width = degree + 1;
                    let design =
                        DMatrix::from_fn(count, width, |row, column| x[row].powi(column as i32));
                    let svd = SVD::try_new_unordered(
                        design,
                        true,
                        true,
                        f64::EPSILON,
                        64 * width * width,
                    )
                    .unwrap();
                    let cutoff = svd.singular_values.iter().copied().fold(0.0, f64::max)
                        * count.max(width) as f64
                        * f64::EPSILON;
                    let expected = svd.solve(&DVector::from_column_slice(&y), cutoff).unwrap();
                    let result = fit(&json!(y), Some(&json!(x)), degree as i64).unwrap();
                    for (actual, expected) in array(&result, "coefficients")
                        .iter()
                        .rev()
                        .zip(expected.iter())
                    {
                        close(*actual, *expected, 1E-8);
                    }
                }
            }
        }
    }

    #[test]
    fn missing_invalid_and_mismatched_points_return_null_outputs() {
        for series in [
            Json::Null,
            json!([]),
            json!([1, null, 3]),
            json!([1, "2", 3]),
        ] {
            assert_eq!(fit(&series, None, 1).unwrap(), Json::Null);
        }
        for coordinates in [Json::Null, json!([]), json!([1, 2]), json!([1, "2", 3])] {
            assert_eq!(
                fit(&json!([1, 2, 3]), Some(&coordinates), 1).unwrap(),
                Json::Null
            );
        }
    }

    #[test]
    fn fitting_budgets_are_checked_at_exact_boundaries_before_allocations() {
        assert!(budget(MAX_COLLECTION_SIZE, 2).is_ok());
        let width = 8;
        let boundary = MAX_WORK / (width * width) / 2 - 32 * width;
        assert!(budget(boundary, width).is_ok());
        assert!(budget(boundary + 1, width).is_err());
        assert!(budget(usize::MAX, usize::MAX).is_err());
        assert!(
            fit(&json!([1, 2, 3]), None, i64::MAX)
                .unwrap_err()
                .to_string()
                .contains("budget")
        );
        let too_long = Json::Array(vec![Json::Null; MAX_COLLECTION_SIZE + 1]);
        assert!(
            fit(&too_long, None, 0)
                .unwrap_err()
                .to_string()
                .contains("1048576")
        );
    }

    #[test]
    fn unrepresentable_metrics_and_original_coordinate_solutions_fail_explicitly() {
        assert!(
            fit(&json!([1E308, -1E308, 1E308, -1E308]), None, 1)
                .unwrap_err()
                .to_string()
                .contains("nonfinite")
        );
        assert!(
            fit(&json!([1.0, 2.0]), Some(&json!([1E308, 1E308])), 2)
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
    }
}
