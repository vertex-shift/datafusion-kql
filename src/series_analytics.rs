use std::sync::LazyLock;

use datafusion::common::{DataFusionError, Result};
use serde_json::{Map, Number, Value as Json};

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn real(value: f64) -> Result<Json> {
    Number::from_f64(value)
        .map(Json::Number)
        .ok_or_else(|| error("series regression produced nonfinite output; rescale the input"))
}

#[derive(Clone, Copy)]
struct Normalization {
    origin: f64,
    scale: f64,
}

impl Normalization {
    fn new(values: &[f64]) -> Self {
        let origin = values[0];
        let delta = values
            .iter()
            .map(|value| (*value - origin).abs())
            .fold(0.0, f64::max);
        let scale = if delta.is_finite() {
            delta
        } else {
            values.iter().map(|value| value.abs()).fold(0.0, f64::max)
        };
        Self {
            origin,
            scale: if scale == 0.0 { 1.0 } else { scale },
        }
    }

    fn normalize(self, value: f64) -> f64 {
        let delta = value - self.origin;
        if delta.is_finite() {
            delta / self.scale
        } else {
            value / self.scale - self.origin / self.scale
        }
    }

    fn restore(self, value: f64) -> f64 {
        value.mul_add(self.scale, self.origin)
    }

    fn variance(self, sum_squares: f64, count: usize) -> f64 {
        sum_squares / (count - 1) as f64 * self.scale * self.scale
    }
}

#[derive(Default)]
struct Moments {
    count: usize,
    xmean: f64,
    ymean: f64,
    xx: f64,
    xy: f64,
    yy: f64,
}

impl Moments {
    fn add(&mut self, x: f64, y: f64) {
        self.count += 1;
        let dx = x - self.xmean;
        let dy = y - self.ymean;
        self.xmean += dx / self.count as f64;
        self.ymean += dy / self.count as f64;
        self.xx += dx * (x - self.xmean);
        self.xy += dx * (y - self.ymean);
        self.yy += dy * (y - self.ymean);
    }

    fn line(&self) -> Line {
        let slope = self.xy / self.xx;
        Line {
            mean: self.ymean,
            slope,
            yy: self.yy.max(0.0),
            residual: (-slope).mul_add(self.xy, self.yy).max(0.0),
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Line {
    mean: f64,
    slope: f64,
    yy: f64,
    residual: f64,
}

impl Line {
    fn intercept(self, count: usize) -> f64 {
        (-self.slope).mul_add((count - 1) as f64 / 2.0, self.mean)
    }

    fn fitted(self, index: usize, count: usize) -> f64 {
        self.slope
            .mul_add(index as f64 - (count - 1) as f64 / 2.0, self.mean)
    }

    fn fields(self, normalization: Normalization, count: usize) -> Result<Map<String, Json>> {
        Ok(Map::from_iter([
            ("rsquare".into(), real(rsquare(self.yy, self.residual))?),
            ("slope".into(), real(self.slope * normalization.scale)?),
            (
                "variance".into(),
                real(normalization.variance(self.yy, count))?,
            ),
            (
                "rvariance".into(),
                real(normalization.variance(self.residual, count))?,
            ),
            (
                "interception".into(),
                real(normalization.restore(self.intercept(count)))?,
            ),
        ]))
    }
}

fn rsquare(variance: f64, residual: f64) -> f64 {
    if variance == 0.0 {
        1.0
    } else {
        (1.0 - residual / variance).clamp(0.0, 1.0)
    }
}

fn values(series: &[Json], minimum: usize) -> Option<Vec<f64>> {
    if series.len() < minimum {
        return None;
    }
    series
        .iter()
        .map(|point| point.as_f64().filter(|value| value.is_finite()))
        .collect()
}

pub(crate) fn fit_line(series: &[Json]) -> Result<Json> {
    let Some(values) = values(series, 2) else {
        return Ok(Json::Null);
    };
    let normalization = Normalization::new(&values);
    let mut moments = Moments::default();
    for (index, value) in values.iter().enumerate() {
        moments.add(index as f64, normalization.normalize(*value));
    }
    let line = moments.line();
    let mut result = line.fields(normalization, values.len())?;
    result.insert(
        "line_fit".into(),
        Json::Array(
            (0..values.len())
                .map(|index| real(normalization.restore(line.fitted(index, values.len()))))
                .collect::<Result<Vec<_>>>()?,
        ),
    );
    Ok(Json::Object(result))
}

pub(crate) fn fit_two_lines(series: &[Json]) -> Result<Json> {
    let Some(mut values) = values(series, 4) else {
        return Ok(Json::Null);
    };
    let normalization = Normalization::new(&values);
    for value in &mut values {
        *value = normalization.normalize(*value);
    }
    let size = values.len();
    let mut suffix = vec![Line::default(); size];
    let mut moments = Moments::default();
    // Summarize suffixes once instead of refitting both sides at every split.
    for index in (0..size).rev() {
        moments.add(index as f64, values[index]);
        if moments.count >= 2 {
            suffix[index] = moments.line();
        }
    }
    let total_variance = moments.yy.max(0.0);
    let mut left = Moments::default();
    left.add(0.0, values[0]);
    let mut best = None;
    let mut best_residual = f64::INFINITY;
    for split in 2..=size - 2 {
        left.add((split - 1) as f64, values[split - 1]);
        let left_line = left.line();
        let right_line = suffix[split];
        let residual = left_line.residual + right_line.residual;
        if residual < best_residual {
            best_residual = residual;
            best = Some((split, left_line, right_line));
        }
    }

    let (split, left, right) = best.ok_or_else(|| error("series regression has no valid split"))?;
    let mut result = Map::from_iter([
        (
            "rsquare".into(),
            real(rsquare(total_variance, best_residual))?,
        ),
        ("split_idx".into(), Json::from(split)),
        (
            "variance".into(),
            real(normalization.variance(total_variance, size))?,
        ),
        (
            "rvariance".into(),
            real(normalization.variance(best_residual, size))?,
        ),
        (
            "line_fit".into(),
            Json::Array(
                (0..size)
                    .map(|index| {
                        let estimate = if index < split {
                            left.fitted(index, split)
                        } else {
                            right.fitted(index - split, size - split)
                        };
                        real(normalization.restore(estimate))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
    ]);
    for (name, line, count) in [("left", left, split), ("right", right, size - split)] {
        let fields = line.fields(normalization, count)?;
        for (field, value) in &fields {
            result.insert(format!("{name}_{field}"), value.clone());
        }
        result.insert(name.into(), Json::Object(fields));
    }
    Ok(Json::Object(result))
}

fn number(args: &[Json], index: usize, default: f64) -> Result<f64> {
    match args.get(index) {
        None => Ok(default),
        Some(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| error("outlier percentiles must be finite numeric values")),
    }
}

fn quantile(sorted: &[f64], percentile: f64) -> f64 {
    let position = percentile / 100.0 * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let fraction = position - lower as f64;
    let low = sorted[lower];
    let high = sorted[upper];
    // Weighted endpoints avoid overflow when the range spans both f64 extrema.
    if low.is_sign_negative() != high.is_sign_negative() {
        low * (1.0 - fraction) + high * fraction
    } else {
        (high - low).mul_add(fraction, low)
    }
}

fn normal_quantile(probability: f64) -> f64 {
    let mut low = -4.0;
    let mut high = 4.0;
    for _ in 0..64 {
        let middle = (low + high) / 2.0;
        let cdf = 0.5 * libm::erfc(-middle / std::f64::consts::SQRT_2);
        if cdf < probability {
            low = middle;
        } else {
            high = middle;
        }
    }
    (low + high) / 2.0
}

fn normalization_factors(low: f64, high: f64) -> (f64, f64) {
    let zlow = normal_quantile(low / 100.0);
    let zhigh = normal_quantile(high / 100.0);
    let cutoff = 4.0 * normal_quantile(0.75);
    (
        1.5 * (zhigh - zlow) / (zlow + cutoff),
        1.5 * (zhigh - zlow) / (cutoff - zhigh),
    )
}

static DEFAULT_FACTORS: LazyLock<(f64, f64)> = LazyLock::new(|| normalization_factors(10.0, 90.0));

pub(crate) fn outliers(series: &[Json], args: &[Json]) -> Result<Json> {
    let kind = match args.get(1) {
        None => "ctukey",
        Some(Json::String(value)) => value.as_str(),
        _ => return Err(error("series_outliers kind must be tukey or ctukey")),
    };
    let (low_percentile, high_percentile, low_factor, high_factor) = match kind {
        "tukey" => (25.0, 75.0, 1.0, 1.0),
        "ctukey" => {
            let low = number(args, 3, 10.0)?;
            let high = number(args, 4, 90.0)?;
            if !(2.0..=98.0).contains(&low) || !(2.0..=98.0).contains(&high) || low >= high {
                return Err(error("ctukey percentiles require 2 <= low < high <= 98"));
            }
            let (lower_factor, upper_factor) = if low == 10.0 && high == 90.0 {
                *DEFAULT_FACTORS
            } else {
                normalization_factors(low, high)
            };
            (low, high, lower_factor, upper_factor)
        }
        _ => return Err(error("series_outliers kind must be tukey or ctukey")),
    };
    let ignore = match args.get(2) {
        None | Some(Json::Null) => None,
        Some(value) => Some(
            value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| error("series_outliers ignore value must be numeric or null"))?,
        ),
    };
    let mut sorted = Vec::with_capacity(series.len());
    for point in series {
        if point.is_null() {
            continue;
        }
        let Some(value) = point.as_f64().filter(|value| value.is_finite()) else {
            return Ok(Json::Null);
        };
        if Some(value) != ignore {
            sorted.push(value);
        }
    }
    if sorted.is_empty() {
        return Ok(Json::Array(vec![Json::from(0.0); series.len()]));
    }
    sorted.sort_unstable_by(f64::total_cmp);
    let low = quantile(&sorted, low_percentile);
    let high = quantile(&sorted, high_percentile);
    if low == high && sorted.first() != sorted.last() {
        return Err(error(
            "series_outliers interquantile range is zero; choose wider percentiles",
        ));
    }
    let output = series
        .iter()
        .map(|point| {
            let Some(value) = point.as_f64() else {
                return Ok(Json::from(0.0));
            };
            if Some(value) == ignore || (low..=high).contains(&value) {
                return Ok(Json::from(0.0));
            }
            let bound = if value < low { low } else { high };
            let factor = if value < low { low_factor } else { high_factor };
            let width = high - low;
            let difference = value - bound;
            let ratio = if width.is_finite() && difference.is_finite() {
                difference / width
            } else {
                let scale = value.abs().max(low.abs()).max(high.abs());
                (value / scale - bound / scale) / (high / scale - low / scale)
            };
            Number::from_f64(ratio * factor)
                .map(Json::Number)
                .ok_or_else(|| {
                    error("series_outliers produced nonfinite scores; rescale the input")
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Json::Array(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_fit(values: &[f64]) -> (f64, f64, f64, f64) {
        let count = values.len() as f64;
        let xmean = (count - 1.0) / 2.0;
        let ymean = values.iter().sum::<f64>() / count;
        let covariance = values
            .iter()
            .enumerate()
            .map(|(index, value)| (index as f64 - xmean) * (value - ymean))
            .sum::<f64>();
        let xvariance = (0..values.len())
            .map(|index| (index as f64 - xmean).powi(2))
            .sum::<f64>();
        let variance = values
            .iter()
            .map(|value| (value - ymean).powi(2))
            .sum::<f64>();
        let slope = covariance / xvariance;
        let intercept = ymean - slope * xmean;
        let residual = values
            .iter()
            .enumerate()
            .map(|(index, value)| (value - (intercept + slope * index as f64)).powi(2))
            .sum::<f64>();
        (slope, intercept, variance, residual)
    }

    #[test]
    fn linear_time_split_search_matches_independent_exhaustive_regressions() {
        let mut state = 19_u64;
        for size in [4, 5, 8, 13, 32, 64] {
            for trial in 0..32 {
                let values = (0..size)
                    .map(|index| {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let noise = ((state >> 32) % 100) as f64 / 37.0;
                        noise
                            + if index < size / 2 {
                                2.0 * index as f64
                            } else {
                                30.0 - index as f64 * (trial % 5) as f64
                            }
                    })
                    .collect::<Vec<_>>();
                let json = values.iter().copied().map(Json::from).collect::<Vec<_>>();
                let fitted = fit_two_lines(&json).unwrap();
                let split = fitted["split_idx"].as_u64().unwrap() as usize;
                assert!((2..=size - 2).contains(&split));
                let left = reference_fit(&values[..split]);
                let right = reference_fit(&values[split..]);
                let minimum = (2..=size - 2)
                    .map(|split| {
                        reference_fit(&values[..split]).3 + reference_fit(&values[split..]).3
                    })
                    .fold(f64::INFINITY, f64::min);
                let total_variance = reference_fit(&values).2;
                let tolerance = 1E-10 * total_variance.max(1.0);
                assert!(
                    (left.3 + right.3 - minimum).abs() <= tolerance,
                    "size={size},trial={trial},split={split},residual={},best={minimum}",
                    left.3 + right.3
                );
                for (field, expected) in [
                    ("left_slope", left.0),
                    ("left_interception", left.1),
                    ("right_slope", right.0),
                    ("right_interception", right.1),
                    ("variance", total_variance / (size - 1) as f64),
                    ("rvariance", (left.3 + right.3) / (size - 1) as f64),
                ] {
                    let actual = fitted[field].as_f64().unwrap();
                    assert!(
                        (actual - expected).abs() <= tolerance,
                        "{field}: size={size},trial={trial},actual={actual},expected={expected}"
                    );
                }
                let fitted_values = fitted["line_fit"].as_array().unwrap();
                assert_eq!(fitted_values.len(), size);
                let actual_residual = values
                    .iter()
                    .zip(fitted_values)
                    .map(|(value, fit)| (value - fit.as_f64().unwrap()).powi(2))
                    .sum::<f64>();
                assert!((actual_residual - minimum).abs() <= tolerance);
            }
        }
    }

    #[test]
    fn normalized_line_fitting_preserves_narrow_large_offsets_and_subnormal_scales() {
        for scale in [1E-200, 1.0, 1E100] {
            let values = (0..10)
                .map(|index| Json::from(scale * (1.0 + 2.0 * index as f64)))
                .collect::<Vec<_>>();
            let fitted = fit_line(&values).unwrap();
            assert!((fitted["slope"].as_f64().unwrap() / scale - 2.0).abs() < 1E-12);
            assert!((fitted["rsquare"].as_f64().unwrap() - 1.0).abs() < 1E-12);
        }
        let values = (0..32)
            .map(|index| Json::from(1E100 + index as f64 * 1E85))
            .collect::<Vec<_>>();
        let fitted = fit_line(&values).unwrap();
        assert!(fitted["rsquare"].as_f64().unwrap() > 0.99);
        assert!(fitted["slope"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn two_lines_use_local_segment_intercepts_and_constant_series_have_an_earliest_split() {
        let values = [1.0, 3.0, 5.0, 7.0, 20.0, 19.0, 18.0, 17.0].map(Json::from);
        let fitted = fit_two_lines(&values).unwrap();
        assert_eq!(fitted["split_idx"], 4);
        assert!((fitted["left_slope"].as_f64().unwrap() - 2.0).abs() < 1E-12);
        assert!((fitted["right_slope"].as_f64().unwrap() + 1.0).abs() < 1E-12);
        assert!((fitted["left_interception"].as_f64().unwrap() - 1.0).abs() < 1E-12);
        assert!((fitted["right_interception"].as_f64().unwrap() - 20.0).abs() < 1E-12);
        assert_eq!(fitted["left"]["slope"], fitted["left_slope"]);
        assert_eq!(fit_two_lines(&[]).unwrap(), Json::Null);
        let fitted = fit_two_lines(&vec![Json::from(7.0); 100]).unwrap();
        assert_eq!(fitted["split_idx"], 2);
        assert_eq!(fitted["rsquare"], 1.0);
        assert_eq!(fitted["rvariance"], 0.0);
    }

    #[test]
    fn tukey_scores_use_interpolated_quartiles_and_original_positions() {
        let series = [-20.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 40.0].map(Json::from);
        let output = outliers(&series, &[Json::Null, Json::from("tukey")]).unwrap();
        // Type-7 linear quartiles are 1.75 and 5.25.
        assert_eq!(output[1], -0.75 / 3.5);
        assert_eq!(output[6], 0.75 / 3.5);
        assert!((output[0].as_f64().unwrap() + 21.75 / 3.5).abs() < 1E-12);
        assert!((output[7].as_f64().unwrap() - 34.75 / 3.5).abs() < 1E-12);
        let custom = outliers(
            &series,
            &[
                Json::Null,
                Json::from("ctukey"),
                Json::Null,
                25.0.into(),
                75.0.into(),
            ],
        )
        .unwrap();
        for (actual, expected) in custom
            .as_array()
            .unwrap()
            .iter()
            .zip(output.as_array().unwrap())
        {
            assert!((actual.as_f64().unwrap() - expected.as_f64().unwrap()).abs() < 1E-12);
        }
    }

    #[test]
    fn inverse_normal_and_percentile_normalization_follow_the_maintainer_equation() {
        assert!((normal_quantile(0.75) - 0.6744897501960817).abs() < 1E-15);
        assert!((normal_quantile(0.9) - 1.2815515655446004).abs() < 1E-15);
        for low in [2.0, 10.0, 25.0, 40.0] {
            for high in [60.0, 75.0, 90.0, 98.0] {
                let zlow = normal_quantile(low / 100.0);
                let zhigh = normal_quantile(high / 100.0);
                let cutoff = 4.0 * normal_quantile(0.75);
                let upper = 1.5 * (zhigh - zlow) / (cutoff - zhigh);
                let lower = 1.5 * (zhigh - zlow) / (zlow + cutoff);
                assert!((((cutoff - zhigh) / (zhigh - zlow)) * upper - 1.5).abs() < 1E-12);
                assert!((((-cutoff - zlow) / (zhigh - zlow)) * lower + 1.5).abs() < 1E-12);
            }
        }
    }

    #[test]
    fn extreme_values_and_degenerate_quantiles_have_explicit_finite_behavior() {
        let constant = vec![Json::from(f64::MAX); 8];
        for fit in [
            fit_line(&constant).unwrap(),
            fit_two_lines(&constant).unwrap(),
        ] {
            assert_eq!(fit["variance"], 0.0);
            assert_eq!(fit["rsquare"], 1.0);
        }
        assert!(fit_line(&[Json::from(-f64::MAX), Json::from(f64::MAX)]).is_err());
        let series = [-f64::MAX, -1.0, 0.0, 1.0, f64::MAX].map(Json::from);
        let scores = outliers(&series, &[Json::Null, "tukey".into()]).unwrap();
        assert!(
            scores
                .as_array()
                .unwrap()
                .iter()
                .all(|score| score.as_f64().unwrap().is_finite())
        );
        let all_missing = [Json::Null, Json::from(-999.0), Json::Null];
        assert_eq!(
            outliers(&all_missing, &[Json::Null, "tukey".into(), (-999.0).into()]).unwrap(),
            serde_json::json!([0.0, 0.0, 0.0])
        );
        assert!(
            outliers(
                serde_json::json!([0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                    .as_array()
                    .unwrap(),
                &[Json::Null, "tukey".into()]
            )
            .is_err()
        );
    }

    #[test]
    fn analytics_accept_the_exact_point_limit_and_reject_an_additional_point() {
        use crate::series_functions::{Op, evaluate};
        let maximum = crate::dynamic::MAX_COLLECTION_SIZE;
        let mut series = Json::Array(vec![Json::from(7.0); maximum]);
        let scores = evaluate(Op::Outliers, &[series.clone()]).unwrap();
        assert_eq!(scores.as_array().unwrap().len(), maximum);
        drop(scores);
        let fit = evaluate(Op::FitTwoLines, &[series.clone()]).unwrap();
        assert_eq!(fit["line_fit"].as_array().unwrap().len(), maximum);
        assert_eq!(fit["split_idx"], 2);
        assert_eq!(fit["rsquare"], 1.0);
        drop(fit);
        series.as_array_mut().unwrap().push(Json::from(7.0));
        for op in [Op::Outliers, Op::FitTwoLines] {
            assert!(evaluate(op, &[series.clone()]).is_err());
        }
    }
}
