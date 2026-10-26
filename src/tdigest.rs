use std::cmp::Ordering;
use std::mem::size_of;

use datafusion::arrow::datatypes::{DataType, TimeUnit, i256};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion_functions_aggregate_common::tdigest::{Centroid, TDigest};

use crate::dynamic_value::Node;

pub(crate) const EXACT_LIMIT: usize = 2048;
pub(crate) const CENTROIDS: usize = 1024;
const HEADER: usize = 8 + 2 + 16 + 64 + 16;
pub(crate) const MAX_BYTES: usize = HEADER + 16 + 4 + EXACT_LIMIT * 16;
const MAGIC: &[u8; 8] = b"DKTDG001";

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Long,
    Real,
    Datetime,
    Timespan,
}

impl Kind {
    pub(crate) fn from_type(ty: &DataType) -> Option<Self> {
        Some(match ty {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Self::Long,
            DataType::Float16 | DataType::Float32 | DataType::Float64 => Self::Real,
            DataType::Timestamp(_, _) => Self::Datetime,
            DataType::Duration(_) => Self::Timespan,
            ty if ty.is_numeric() => Self::Real,
            _ => return None,
        })
    }

    pub(crate) fn data_type(self) -> DataType {
        match self {
            Self::Long => DataType::Int64,
            Self::Real => DataType::Float64,
            Self::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, None),
            Self::Timespan => DataType::Duration(TimeUnit::Nanosecond),
        }
    }

    pub(crate) fn sample_type(self) -> DataType {
        if self == Self::Real {
            DataType::Float64
        } else {
            DataType::Int64
        }
    }

    fn compare(self, left: u64, right: u64) -> Ordering {
        if self == Self::Real {
            f64::from_bits(left).total_cmp(&f64::from_bits(right))
        } else {
            (left as i64).cmp(&(right as i64))
        }
    }

    fn special(self, value: u64) -> Option<usize> {
        if self != Self::Real {
            return None;
        }
        let value = f64::from_bits(value);
        if value.is_nan() {
            Some(if value.is_sign_negative() { 0 } else { 3 })
        } else if value.is_infinite() {
            Some(if value.is_sign_negative() { 1 } else { 2 })
        } else {
            None
        }
    }

    pub(crate) fn scalar(self, value: Option<u64>) -> ScalarValue {
        match self {
            Self::Long => ScalarValue::Int64(value.map(|value| value as i64)),
            Self::Real => ScalarValue::Float64(value.map(f64::from_bits)),
            Self::Datetime => {
                ScalarValue::TimestampNanosecond(value.map(|value| value as i64), None)
            }
            Self::Timespan => ScalarValue::DurationNanosecond(value.map(|value| value as i64)),
        }
    }

    pub(crate) fn node(self, value: Option<u64>) -> Node {
        let Some(value) = value else {
            return Node::Null;
        };
        match self {
            Self::Long => Node::Long(value as i64),
            Self::Real => Node::Real(f64::from_bits(value)),
            Self::Datetime => Node::Datetime(value as i64),
            Self::Timespan => Node::Timespan(value as i64),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    value: u64,
    weight: u64,
}

#[derive(Debug, Clone)]
struct Compressed {
    digest: TDigest,
    pending: Vec<Entry>,
    origin: i64,
    scale: f64,
}

#[derive(Debug, Clone)]
enum Storage {
    Exact(Vec<Entry>),
    Compressed(Box<Compressed>),
}

#[derive(Debug, Clone)]
pub(crate) struct Sketch {
    pub(crate) kind: Kind,
    total: u128,
    special: [u128; 4],
    minimum: Option<u64>,
    maximum: Option<u64>,
    storage: Storage,
}

// Compute ceil(percentile * total / 100) using the exact binary64 argument.
// Multiplying the count through f64 can move a weighted nearest rank by many rows.
pub(crate) fn rank(percentile: f64, total: u128) -> u128 {
    if total == 0 {
        return 0;
    }
    if percentile == 0.0 {
        return 1;
    }
    if percentile == 100.0 {
        return total;
    }
    let bits = percentile.to_bits();
    let raw_exponent = ((bits >> 52) & 0x7ff) as i32;
    let significand =
        (bits & ((1_u64 << 52) - 1)) | if raw_exponent == 0 { 0 } else { 1_u64 << 52 };
    let exponent = if raw_exponent == 0 {
        -1074
    } else {
        raw_exponent - 1023 - 52
    };
    if exponent < -248 {
        return 1;
    }
    let mut count = [0_u8; 32];
    count[..16].copy_from_slice(&total.to_le_bytes());
    let numerator = i256::from_le_bytes(count) * i256::from_i128(i128::from(significand));
    let denominator = i256::from_i128(100) << (-exponent as u32);
    if numerator <= denominator {
        return 1;
    }
    let rank = (numerator + denominator - i256::ONE) / denominator;
    let bytes = rank.to_le_bytes();
    let mut result = [0; 16];
    result.copy_from_slice(&bytes[..16]);
    u128::from_le_bytes(result).clamp(1, total)
}

impl Sketch {
    pub(crate) fn new(kind: Kind) -> Self {
        Self {
            kind,
            total: 0,
            special: [0; 4],
            minimum: None,
            maximum: None,
            storage: Storage::Exact(Vec::new()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn finite_count(&self) -> u128 {
        self.total - self.special.iter().sum::<u128>()
    }

    fn record(&mut self, value: u64, weight: u128) -> Result<()> {
        self.total = self
            .total
            .checked_add(weight)
            .ok_or_else(|| error("t-digest total weight exceeds u128"))?;
        if let Some(index) = self.kind.special(value) {
            self.special[index] = self.special[index]
                .checked_add(weight)
                .ok_or_else(|| error("t-digest special-value weight overflow"))?;
        } else {
            if self
                .minimum
                .is_none_or(|minimum| self.kind.compare(value, minimum).is_lt())
            {
                self.minimum = Some(value);
            }
            if self
                .maximum
                .is_none_or(|maximum| self.kind.compare(value, maximum).is_gt())
            {
                self.maximum = Some(value);
            }
        }
        Ok(())
    }

    pub(crate) fn insert(&mut self, value: u64, weight: u64) -> Result<()> {
        if weight == 0 {
            return Ok(());
        }
        self.record(value, u128::from(weight))?;
        let entry = Entry { value, weight };
        match &mut self.storage {
            Storage::Exact(entries) => {
                entries.push(entry);
                if entries.len() > EXACT_LIMIT {
                    self.promote()?;
                }
            }
            Storage::Compressed(state) if self.kind.special(value).is_none() => {
                state.pending.push(entry);
                if state.pending.len() >= EXACT_LIMIT {
                    self.flush()?;
                }
            }
            Storage::Compressed(_) => {}
        }
        Ok(())
    }

    fn promote(&mut self) -> Result<()> {
        if matches!(self.storage, Storage::Compressed(_)) {
            return Ok(());
        }
        let Storage::Exact(entries) =
            std::mem::replace(&mut self.storage, Storage::Exact(Vec::new()))
        else {
            unreachable!();
        };
        let origin = if self.kind == Kind::Real {
            0
        } else {
            entries.first().map_or(0, |entry| entry.value as i64)
        };
        let mut state = Compressed {
            digest: TDigest::new(CENTROIDS),
            pending: Vec::new(),
            origin,
            scale: 1.0,
        };
        for entry in entries {
            if self.kind.special(entry.value).is_none() {
                state.pending.push(entry);
            }
        }
        self.storage = Storage::Compressed(Box::new(state));
        self.flush()
    }

    fn coordinate(kind: Kind, value: u64, origin: i64, scale: f64) -> f64 {
        if kind == Kind::Real {
            f64::from_bits(value) / scale
        } else {
            ((value as i64) as i128 - i128::from(origin)) as f64 / scale
        }
    }

    fn update_scale(
        kind: Kind,
        minimum: Option<u64>,
        maximum: Option<u64>,
        state: &mut Compressed,
    ) -> Result<()> {
        let maximum = [minimum, maximum]
            .into_iter()
            .flatten()
            .map(|value| {
                if kind == Kind::Real {
                    f64::from_bits(value).abs()
                } else {
                    ((value as i64) as i128 - i128::from(state.origin)).unsigned_abs() as f64
                }
            })
            .fold(state.scale, f64::max);
        if maximum <= state.scale {
            return Ok(());
        }
        let mut scale = state.scale;
        while scale < maximum {
            scale = if scale > f64::MAX / 2.0 {
                maximum
            } else {
                scale * 2.0
            };
        }
        let factor = state.scale / scale;
        if state.digest.count() > 0.0 {
            state.digest = digest_parts(
                state
                    .digest
                    .centroids()
                    .iter()
                    .map(|centroid| Centroid::new(centroid.mean() * factor, centroid.weight()))
                    .collect(),
                state.digest.count(),
                state.digest.min() * factor,
                state.digest.max() * factor,
            )?;
        }
        state.scale = scale;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        let Storage::Compressed(state) = &mut self.storage else {
            return Ok(());
        };
        Self::update_scale(self.kind, self.minimum, self.maximum, state)?;
        if state.pending.is_empty() {
            return Ok(());
        }
        let mut values = state
            .pending
            .iter()
            .map(|entry| {
                Centroid::new(
                    Self::coordinate(self.kind, entry.value, state.origin, state.scale),
                    entry.weight as f64,
                )
            })
            .collect::<Vec<_>>();
        values.sort_unstable_by(|a, b| a.mean().total_cmp(&b.mean()));
        let count = values.iter().map(Centroid::weight).sum();
        let leaf = digest_parts(
            values,
            count,
            Self::coordinate(
                self.kind,
                self.minimum
                    .ok_or_else(|| error("missing t-digest minimum"))?,
                state.origin,
                state.scale,
            ),
            Self::coordinate(
                self.kind,
                self.maximum
                    .ok_or_else(|| error("missing t-digest maximum"))?,
                state.origin,
                state.scale,
            ),
        )?;
        state.digest = bounded_merge(&state.digest, &leaf)?;
        state.pending.clear();
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: &Self) -> Result<()> {
        if self.kind != other.kind {
            return Err(error("cannot merge t-digests of different logical types"));
        }
        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            *self = other.clone();
            return Ok(());
        }
        if let Storage::Exact(entries) = &other.storage {
            for entry in entries {
                self.insert(entry.value, entry.weight)?;
            }
            return Ok(());
        }
        let mut other = other.clone();
        other.flush()?;
        self.promote()?;
        self.flush()?;
        self.total = self
            .total
            .checked_add(other.total)
            .ok_or_else(|| error("t-digest total weight overflow"))?;
        for (left, right) in self.special.iter_mut().zip(other.special) {
            *left = left
                .checked_add(right)
                .ok_or_else(|| error("t-digest special-value weight overflow"))?;
        }
        if let Some(value) = other.minimum
            && self
                .minimum
                .is_none_or(|minimum| self.kind.compare(value, minimum).is_lt())
        {
            self.minimum = Some(value);
        }
        if let Some(value) = other.maximum
            && self
                .maximum
                .is_none_or(|maximum| self.kind.compare(value, maximum).is_gt())
        {
            self.maximum = Some(value);
        }
        let (Storage::Compressed(left), Storage::Compressed(right)) =
            (&mut self.storage, &other.storage)
        else {
            unreachable!();
        };
        Self::update_scale(self.kind, self.minimum, self.maximum, left)?;
        if right.digest.count() == 0.0 {
            return Ok(());
        }
        let shift = if self.kind == Kind::Real {
            0.0
        } else {
            (i128::from(right.origin) - i128::from(left.origin)) as f64 / left.scale
        };
        let factor = right.scale / left.scale;
        let transformed = digest_parts(
            right
                .digest
                .centroids()
                .iter()
                .map(|centroid| Centroid::new(centroid.mean() * factor + shift, centroid.weight()))
                .collect(),
            right.digest.count(),
            right.digest.min() * factor + shift,
            right.digest.max() * factor + shift,
        )?;
        left.digest = bounded_merge(&left.digest, &transformed)?;
        Ok(())
    }

    pub(crate) fn percentile(&mut self, percentile: f64) -> Result<Option<u64>> {
        if !percentile.is_finite() || !(0.0..=100.0).contains(&percentile) {
            return Err(error(
                "t-digest percentile must be finite and between 0 and 100",
            ));
        }
        if self.is_empty() {
            return Ok(None);
        }
        let mut rank = rank(percentile, self.total);
        if let Storage::Exact(entries) = &mut self.storage {
            entries.sort_unstable_by(|a, b| self.kind.compare(a.value, b.value));
            let mut cumulative = 0_u128;
            for entry in entries {
                cumulative += u128::from(entry.weight);
                if cumulative >= rank {
                    return Ok(Some(entry.value));
                }
            }
            return Err(error("invalid exact t-digest rank state"));
        }
        for (index, value) in [(0, -f64::NAN), (1, f64::NEG_INFINITY)] {
            if rank <= self.special[index] {
                return Ok(Some(value.to_bits()));
            }
            rank -= self.special[index];
        }
        let finite = self.finite_count();
        if rank > finite {
            return Ok(Some(if rank - finite <= self.special[2] {
                f64::INFINITY.to_bits()
            } else {
                f64::NAN.to_bits()
            }));
        }
        if rank == 1 {
            return Ok(self.minimum);
        }
        if rank == finite {
            return Ok(self.maximum);
        }
        self.flush()?;
        let Storage::Compressed(state) = &self.storage else {
            unreachable!();
        };
        let estimate = state.digest.estimate_quantile(rank as f64 / finite as f64);
        if !estimate.is_finite() {
            return Err(error(
                "t-digest produced a nonfinite finite-population estimate",
            ));
        }
        if self.kind == Kind::Real {
            let value = (estimate * state.scale).clamp(
                f64::from_bits(
                    self.minimum
                        .ok_or_else(|| error("missing t-digest minimum"))?,
                ),
                f64::from_bits(
                    self.maximum
                        .ok_or_else(|| error("missing t-digest maximum"))?,
                ),
            );
            Ok(Some(value.to_bits()))
        } else {
            let offset = (estimate * state.scale).round() as i128;
            let value = (i128::from(state.origin) + offset).clamp(
                i128::from(
                    self.minimum
                        .ok_or_else(|| error("missing t-digest minimum"))?
                        as i64,
                ),
                i128::from(
                    self.maximum
                        .ok_or_else(|| error("missing t-digest maximum"))?
                        as i64,
                ),
            );
            Ok(Some(value as i64 as u64))
        }
    }

    pub(crate) fn heap_size(&self) -> usize {
        match &self.storage {
            Storage::Exact(values) => values.capacity() * size_of::<Entry>(),
            Storage::Compressed(state) => {
                size_of::<Compressed>() + state.digest.size() - size_of::<TDigest>()
                    + state.pending.capacity() * size_of::<Entry>()
            }
        }
    }

    pub(crate) fn encode(&mut self) -> Result<Vec<u8>> {
        self.flush()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(self.kind as u8);
        bytes.push(u8::from(matches!(self.storage, Storage::Compressed(_))));
        bytes.extend_from_slice(&self.total.to_le_bytes());
        for weight in self.special {
            bytes.extend_from_slice(&weight.to_le_bytes());
        }
        bytes.extend_from_slice(&self.minimum.unwrap_or(0).to_le_bytes());
        bytes.extend_from_slice(&self.maximum.unwrap_or(0).to_le_bytes());
        match &mut self.storage {
            Storage::Exact(entries) => {
                entries.sort_unstable_by(|a, b| self.kind.compare(a.value, b.value));
                bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for entry in entries {
                    bytes.extend_from_slice(&entry.value.to_le_bytes());
                    bytes.extend_from_slice(&entry.weight.to_le_bytes());
                }
            }
            Storage::Compressed(state) => {
                bytes.extend_from_slice(&state.origin.to_le_bytes());
                bytes.extend_from_slice(&state.scale.to_le_bytes());
                bytes.extend_from_slice(&(state.digest.centroids().len() as u32).to_le_bytes());
                for centroid in state.digest.centroids() {
                    bytes.extend_from_slice(&centroid.mean().to_le_bytes());
                    bytes.extend_from_slice(&centroid.weight().to_le_bytes());
                }
            }
        }
        if bytes.len() > MAX_BYTES {
            return Err(error("t-digest state exceeds its byte budget"));
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER + 4 || bytes.len() > MAX_BYTES || bytes.get(..8) != Some(MAGIC) {
            return Err(error(
                "unsupported or oversized datafusion-kql t-digest format",
            ));
        }
        let mut reader = Reader { bytes: &bytes[8..] };
        let kind = match reader.take::<1>()?[0] {
            0 => Kind::Long,
            1 => Kind::Real,
            2 => Kind::Datetime,
            3 => Kind::Timespan,
            _ => return Err(error("invalid t-digest logical type")),
        };
        let mode = reader.take::<1>()?[0];
        let total = u128::from_le_bytes(reader.take()?);
        let mut special = [0_u128; 4];
        let mut special_total = 0_u128;
        for value in &mut special {
            *value = u128::from_le_bytes(reader.take()?);
            special_total = special_total
                .checked_add(*value)
                .ok_or_else(|| error("invalid t-digest special weights"))?;
        }
        if special_total > total || kind != Kind::Real && special_total != 0 {
            return Err(error("invalid t-digest type/weight state"));
        }
        let finite = total - special_total;
        let minimum = u64::from_le_bytes(reader.take()?);
        let maximum = u64::from_le_bytes(reader.take()?);
        if finite == 0 && (minimum != 0 || maximum != 0)
            || finite != 0
                && (kind.special(minimum).is_some()
                    || kind.special(maximum).is_some()
                    || kind.compare(minimum, maximum).is_gt())
        {
            return Err(error("invalid t-digest finite bounds"));
        }
        let mut sketch = Self {
            kind,
            total,
            special,
            minimum: (finite != 0).then_some(minimum),
            maximum: (finite != 0).then_some(maximum),
            storage: Storage::Exact(Vec::new()),
        };
        match mode {
            0 => {
                let count = u32::from_le_bytes(reader.take()?) as usize;
                if count > EXACT_LIMIT || reader.bytes.len() != count * 16 {
                    return Err(error("invalid exact t-digest length"));
                }
                let mut checked = Self::new(kind);
                let mut previous = None;
                for _ in 0..count {
                    let value = u64::from_le_bytes(reader.take()?);
                    let weight = u64::from_le_bytes(reader.take()?);
                    if weight == 0
                        || previous.is_some_and(|previous| kind.compare(previous, value).is_gt())
                    {
                        return Err(error("invalid exact t-digest sample or weight"));
                    }
                    checked.insert(value, weight)?;
                    previous = Some(value);
                }
                if checked.total != total
                    || checked.special != special
                    || checked.minimum != sketch.minimum
                    || checked.maximum != sketch.maximum
                {
                    return Err(error("t-digest metadata does not match its exact samples"));
                }
                return Ok(checked);
            }
            1 => {
                let origin = i64::from_le_bytes(reader.take()?);
                let scale = f64::from_le_bytes(reader.take()?);
                let count = u32::from_le_bytes(reader.take()?) as usize;
                if count > CENTROIDS
                    || reader.bytes.len() != count * 16
                    || !scale.is_finite()
                    || scale < 1.0
                    || kind == Kind::Real && origin != 0
                    || (finite == 0) != (count == 0)
                {
                    return Err(error("invalid compressed t-digest length, scale or count"));
                }
                let low = sketch
                    .minimum
                    .map_or(0.0, |value| Self::coordinate(kind, value, origin, scale));
                let high = sketch
                    .maximum
                    .map_or(0.0, |value| Self::coordinate(kind, value, origin, scale));
                if low.abs() > 1.0 + 1E-12 || high.abs() > 1.0 + 1E-12 {
                    return Err(error("t-digest bounds exceed its coordinate scale"));
                }
                let mut centroids = Vec::with_capacity(count);
                let mut weight_sum = 0.0;
                for _ in 0..count {
                    let mean = f64::from_le_bytes(reader.take()?);
                    let weight = f64::from_le_bytes(reader.take()?);
                    if !mean.is_finite()
                        || mean < low - 1E-12
                        || mean > high + 1E-12
                        || !weight.is_finite()
                        || weight <= 0.0
                    {
                        return Err(error("invalid t-digest centroid mean or weight"));
                    }
                    weight_sum += weight;
                    centroids.push(Centroid::new(mean, weight));
                }
                if (weight_sum - finite as f64).abs() > 1E-10 * (finite as f64).max(1.0) {
                    return Err(error("t-digest centroid weights do not match its count"));
                }
                let digest = if finite == 0 {
                    TDigest::new(CENTROIDS)
                } else {
                    digest_parts(centroids, finite as f64, low, high)?
                };
                sketch.storage = Storage::Compressed(Box::new(Compressed {
                    digest,
                    pending: Vec::new(),
                    origin,
                    scale,
                }));
            }
            _ => return Err(error("invalid t-digest storage mode")),
        }
        Ok(sketch)
    }
}

fn digest_parts(
    centroids: Vec<Centroid>,
    count: f64,
    minimum: f64,
    maximum: f64,
) -> Result<TDigest> {
    let sum = centroids
        .iter()
        .map(|centroid| centroid.mean() * centroid.weight())
        .sum();
    TDigest::try_from_parts(CENTROIDS, sum, count, maximum, minimum, centroids)
}

fn bounded_merge(left: &TDigest, right: &TDigest) -> Result<TDigest> {
    if left.count() == 0.0 {
        return compress(right.clone());
    }
    if right.count() == 0.0 {
        return compress(left.clone());
    }
    compress(TDigest::merge_digests([left, right]))
}

fn compress(digest: TDigest) -> Result<TDigest> {
    if digest.centroids().len() <= CENTROIDS {
        return Ok(digest);
    }
    // A very heavy weighted centroid can make the native merge exceed its
    // nominal size. Coalesce its ordered output at bounded quantile cuts.
    let count = digest.count();
    let mut output = Vec::with_capacity(CENTROIDS);
    let mut cumulative = 0.0;
    let mut mean = 0.0;
    let mut weight = 0.0;
    for centroid in digest.centroids() {
        let q = (output.len() + 1) as f64 / CENTROIDS as f64;
        let boundary = if q <= 0.5 {
            2.0 * q * q
        } else {
            1.0 - 2.0 * (1.0 - q).powi(2)
        };
        if weight > 0.0
            && cumulative + centroid.weight() > boundary * count
            && output.len() + 1 < CENTROIDS
        {
            output.push(Centroid::new(mean, weight));
            weight = 0.0;
        }
        let next = weight + centroid.weight();
        mean = if weight == 0.0 {
            centroid.mean()
        } else {
            mean + (centroid.mean() - mean) * (centroid.weight() / next)
        };
        weight = next;
        cumulative += centroid.weight();
    }
    if weight > 0.0 {
        output.push(Centroid::new(mean, weight));
    }
    digest_parts(output, count, digest.min(), digest.max())
}

struct Reader<'a> {
    bytes: &'a [u8],
}
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (prefix, remaining) = self
            .bytes
            .split_at_checked(N)
            .ok_or_else(|| error("truncated t-digest state"))?;
        let mut value = [0; N];
        value.copy_from_slice(prefix);
        self.bytes = remaining;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_ranks_do_not_round_large_counts_through_f64() {
        assert_eq!(rank(50.0, u128::from(u64::MAX)), 1_u128 << 63);
        assert_eq!(rank(100.0, u128::MAX), u128::MAX);
        assert_eq!(rank(0.0, u128::MAX), 1);
        assert_eq!(rank(1E-300, u128::MAX), 1);
        assert_eq!(rank(50.0, 3), 2);
        assert_eq!(rank(25.0, 100), 25);
    }

    #[test]
    fn exact_small_sets_and_large_integral_offsets_preserve_values() {
        let mut sketch = Sketch::new(Kind::Long);
        sketch.insert(9_007_199_254_740_993, 1).unwrap();
        sketch.insert(9_007_199_254_740_995, 1).unwrap();
        assert_eq!(
            sketch.percentile(50.0).unwrap(),
            Some(9_007_199_254_740_993)
        );
        for _ in 0..3000 {
            sketch.insert(9_007_199_254_740_993, 1).unwrap();
        }
        assert_eq!(sketch.percentile(0.0).unwrap(), Some(9_007_199_254_740_993));
        assert_eq!(
            sketch.percentile(50.0).unwrap(),
            Some(9_007_199_254_740_993)
        );
        assert_eq!(
            sketch.percentile(100.0).unwrap(),
            Some(9_007_199_254_740_995)
        );
    }

    #[test]
    fn compressed_integer_origins_survive_empty_and_partial_merges() {
        for kind in [Kind::Long, Kind::Datetime, Kind::Timespan] {
            for base in [i64::MAX - 20_000, i64::MIN + 20_000] {
                let mut source = Sketch::new(kind);
                for offset in 0..6000 {
                    source.insert((base + offset) as u64, 1).unwrap();
                }
                let mut target = Sketch::new(kind);
                target
                    .merge(&Sketch::decode(&source.encode().unwrap()).unwrap())
                    .unwrap();
                for p in [0.0, 1.0, 50.0, 99.0, 100.0] {
                    assert_eq!(target.percentile(p).unwrap(), source.percentile(p).unwrap());
                    let value = target.percentile(p).unwrap().unwrap() as i64;
                    let expected = base + (rank(p, 6000) - 1) as i64;
                    assert!(
                        (i128::from(value) - i128::from(expected)).abs() <= 60,
                        "kind={kind:?},base={base},p={p},value={value},expected={expected}"
                    );
                }
            }
        }
    }

    #[test]
    fn promotion_and_empty_special_only_states_have_checked_bounds() {
        for kind in [Kind::Long, Kind::Real, Kind::Datetime, Kind::Timespan] {
            let mut empty = Sketch::decode(&Sketch::new(kind).encode().unwrap()).unwrap();
            assert!(empty.is_empty());
            assert_eq!(empty.percentile(50.0).unwrap(), None);
            let mut sketch = Sketch::new(kind);
            for i in 0..EXACT_LIMIT {
                sketch
                    .insert(
                        if kind == Kind::Real {
                            (i as f64).to_bits()
                        } else {
                            i as u64
                        },
                        1,
                    )
                    .unwrap();
            }
            assert!(
                matches!(&sketch.storage,Storage::Exact(entries) if entries.len()==EXACT_LIMIT)
            );
            assert!(sketch.encode().unwrap().len() <= MAX_BYTES);
            sketch.insert(0, 1).unwrap();
            assert!(matches!(&sketch.storage, Storage::Compressed(_)));
            for _ in 0..100_000 {
                sketch.insert(0, u64::MAX).unwrap();
            }
            assert!(sketch.heap_size() < 100_000);
            assert!(sketch.encode().unwrap().len() <= MAX_BYTES);
            Sketch::decode(&sketch.encode().unwrap()).unwrap();
        }
        let mut special = Sketch::new(Kind::Real);
        for _ in 0..3000 {
            for value in [-f64::NAN, f64::NEG_INFINITY, f64::INFINITY, f64::NAN] {
                special.insert(value.to_bits(), u64::MAX).unwrap();
            }
        }
        let mut decoded = Sketch::decode(&special.encode().unwrap()).unwrap();
        assert!(f64::from_bits(decoded.percentile(0.0).unwrap().unwrap()).is_sign_negative());
        assert_eq!(
            decoded.percentile(50.0).unwrap(),
            Some(f64::NEG_INFINITY.to_bits())
        );
        assert_eq!(
            decoded.percentile(75.0).unwrap(),
            Some(f64::INFINITY.to_bits())
        );
        assert!(f64::from_bits(decoded.percentile(100.0).unwrap().unwrap()).is_nan());
    }

    #[test]
    fn compressed_wire_metadata_is_validated_before_native_construction() {
        let mut sketch = Sketch::new(Kind::Real);
        for i in 0..4000 {
            sketch.insert(f64::from(i).to_bits(), 1).unwrap();
        }
        let bytes = sketch.encode().unwrap();
        for cut in [0, 8, HEADER, HEADER + 8, HEADER + 16, bytes.len() - 1] {
            assert!(Sketch::decode(&bytes[..cut]).is_err());
        }
        for (offset, replacement) in [
            (HEADER, 1_i64.to_le_bytes()),
            (HEADER + 8, f64::NAN.to_le_bytes()),
            (HEADER + 8, 0.5_f64.to_le_bytes()),
            (HEADER + 20, f64::INFINITY.to_le_bytes()),
            (HEADER + 20, 2.0_f64.to_le_bytes()),
            (HEADER + 28, 0.0_f64.to_le_bytes()),
            (HEADER + 28, f64::NAN.to_le_bytes()),
            (HEADER + 28, f64::MAX.to_le_bytes()),
        ] {
            let mut invalid = bytes.clone();
            invalid[offset..offset + 8].copy_from_slice(&replacement);
            assert!(
                Sketch::decode(&invalid).is_err(),
                "offset={offset},replacement={replacement:?}"
            );
        }
        let mut invalid = bytes.clone();
        invalid[HEADER + 16..HEADER + 20].copy_from_slice(&((CENTROIDS + 1) as u32).to_le_bytes());
        assert!(Sketch::decode(&invalid).is_err());
        let mut invalid = bytes;
        invalid.extend_from_slice(&[0]);
        assert!(Sketch::decode(&invalid).is_err());
    }

    #[test]
    fn rank_error_is_measured_across_distributions_and_merge_orders() {
        let size = 100_000;
        for distribution in 0..4 {
            let mut values = (0..size)
                .map(|i| match distribution {
                    0 => f64::from(i),
                    1 => f64::from(size - i),
                    2 => (f64::from(i) / 1000.0).exp(),
                    _ => {
                        if i < size * 9 / 10 {
                            0.0
                        } else {
                            f64::from(i)
                        }
                    }
                })
                .collect::<Vec<_>>();
            let mut sketch = Sketch::new(Kind::Real);
            let mut parts = (0..8).map(|_| Sketch::new(Kind::Real)).collect::<Vec<_>>();
            for (index, value) in values.iter().enumerate() {
                sketch.insert(value.to_bits(), 1).unwrap();
                parts[index % 8].insert(value.to_bits(), 1).unwrap();
            }
            let mut merged = Sketch::new(Kind::Real);
            for part in parts.into_iter().rev() {
                merged.merge(&part).unwrap();
            }
            values.sort_by(f64::total_cmp);
            for sketch in [&mut sketch, &mut merged] {
                for p in [0.0, 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0, 100.0] {
                    let estimate = f64::from_bits(sketch.percentile(p).unwrap().unwrap());
                    let lower = values.partition_point(|value| *value < estimate);
                    let upper = values.partition_point(|value| *value <= estimate);
                    let target = p / 100.0 * size as f64;
                    let error = if target < lower as f64 {
                        lower as f64 - target
                    } else if target > upper as f64 {
                        target - upper as f64
                    } else {
                        0.0
                    };
                    assert!(
                        error <= size as f64 * 0.01,
                        "distribution={distribution},p={p},estimate={estimate},rank error={error}"
                    );
                }
                assert!(
                    sketch.heap_size() < 100_000,
                    "state is bounded independently of row count"
                );
                let bytes = sketch.encode().unwrap();
                let mut decoded = Sketch::decode(&bytes).unwrap();
                for p in [0.0, 50.0, 99.0, 100.0] {
                    assert_eq!(
                        decoded.percentile(p).unwrap(),
                        sketch.percentile(p).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn weights_special_values_and_extreme_floats_remain_bounded() {
        let mut sketch = Sketch::new(Kind::Real);
        for i in 0..20_000 {
            sketch
                .insert(
                    f64::from(i).to_bits(),
                    if i == 10_000 { u64::MAX } else { 1 },
                )
                .unwrap();
        }
        let estimate = f64::from_bits(sketch.percentile(50.0).unwrap().unwrap());
        assert_eq!(
            estimate, 10_000.0,
            "the median rank lies inside the dominant weighted atom"
        );
        assert!(sketch.encode().unwrap().len() <= MAX_BYTES);
        let mut huge = Sketch::new(Kind::Real);
        for _ in 0..5000 {
            huge.insert(f64::MAX.to_bits(), u64::MAX).unwrap();
        }
        assert_eq!(huge.percentile(50.0).unwrap(), Some(f64::MAX.to_bits()));
        let mut specials = Sketch::new(Kind::Real);
        for _ in 0..3000 {
            for value in [f64::NEG_INFINITY, 1.0, f64::INFINITY, f64::NAN] {
                specials.insert(value.to_bits(), 1).unwrap();
            }
        }
        let bytes = specials.encode().unwrap();
        let mut decoded = Sketch::decode(&bytes).unwrap();
        assert_eq!(
            decoded.percentile(0.0).unwrap(),
            Some(f64::NEG_INFINITY.to_bits())
        );
        assert!(f64::from_bits(decoded.percentile(100.0).unwrap().unwrap()).is_nan());
    }

    #[test]
    fn malformed_payloads_types_and_counts_are_not_accepted() {
        let mut sketch = Sketch::new(Kind::Long);
        sketch.insert(10, 2).unwrap();
        let bytes = sketch.encode().unwrap();
        for cut in 0..bytes.len() {
            assert!(Sketch::decode(&bytes[..cut]).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[10] = 99;
        assert!(Sketch::decode(&invalid).is_err());
        let mut invalid = bytes;
        invalid.extend_from_slice(&[0]);
        assert!(Sketch::decode(&invalid).is_err());
        assert!(sketch.merge(&Sketch::new(Kind::Real)).is_err());
        assert!(sketch.percentile(f64::NAN).is_err());
    }
}
