use std::collections::HashSet;

use datafusion::common::hash_utils::{HLL_RANDOM_STATE, QualityRandomState};
use datafusion::common::{DataFusionError, Result};

const MAGIC: &[u8; 8] = if cfg!(target_endian = "little") {
    b"DKHLL01L"
} else {
    b"DKHLL01B"
};
const HEADER: usize = MAGIC.len() + 2;
pub(crate) const MAX_BYTES: usize = HEADER + (1 << 18);
const PRECISION: [u8; 5] = [12, 14, 16, 17, 18];
const SPARSE_LIMIT: [usize; 5] = [256, 1000, 8000, 256, 256];

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Clone, Debug)]
enum Storage {
    Sparse(HashSet<u64, QualityRandomState>),
    Dense(Vec<u8>),
}

#[derive(Clone, Debug)]
pub(crate) struct Sketch {
    accuracy: u8,
    storage: Storage,
}

impl Sketch {
    pub(crate) fn new(accuracy: u8) -> Result<Self> {
        if usize::from(accuracy) >= PRECISION.len() {
            return Err(error("HLL accuracy must be an integer from 0 through 4"));
        }
        Ok(Self {
            accuracy,
            storage: Storage::Sparse(HashSet::with_hasher(HLL_RANDOM_STATE)),
        })
    }

    fn precision(&self) -> u8 {
        PRECISION[usize::from(self.accuracy)]
    }

    fn sparse_limit(&self) -> usize {
        SPARSE_LIMIT[usize::from(self.accuracy)]
    }

    fn register(registers: &mut [u8], precision: u8, hash: u64) {
        let index = hash as usize & (registers.len() - 1);
        let remainder = (hash >> precision) | (1_u64 << (64 - precision));
        let rank = remainder.trailing_zeros() as u8 + 1;
        registers[index] = registers[index].max(rank);
    }

    fn promote(&mut self) {
        if let Storage::Sparse(hashes) = &self.storage {
            let precision = self.precision();
            let mut registers = vec![0; 1 << precision];
            for hash in hashes {
                Self::register(&mut registers, precision, *hash);
            }
            self.storage = Storage::Dense(registers);
        }
    }

    pub(crate) fn insert(&mut self, hash: u64) {
        let precision = self.precision();
        let limit = self.sparse_limit();
        match &mut self.storage {
            Storage::Sparse(hashes) => {
                hashes.insert(hash);
                if hashes.len() > limit {
                    self.promote();
                }
            }
            Storage::Dense(registers) => Self::register(registers, precision, hash),
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) -> Result<()> {
        if self.accuracy != other.accuracy {
            return Err(error(
                "cannot merge HLL sketches with different accuracy values",
            ));
        }
        match &other.storage {
            Storage::Sparse(hashes) => {
                for hash in hashes {
                    self.insert(*hash);
                }
            }
            Storage::Dense(right) => {
                self.promote();
                let Storage::Dense(left) = &mut self.storage else {
                    unreachable!("promoting a sketch produces dense registers");
                };
                for (left, right) in left.iter_mut().zip(right) {
                    *left = (*left).max(*right);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn heap_size(&self) -> usize {
        match &self.storage {
            // Conservatively include hash-table bucket/control-byte overhead.
            Storage::Sparse(hashes) => hashes.capacity() * 16,
            Storage::Dense(registers) => registers.capacity(),
        }
    }

    pub(crate) fn count(&self) -> Result<i64> {
        let Storage::Dense(registers) = &self.storage else {
            let Storage::Sparse(hashes) = &self.storage else {
                unreachable!();
            };
            return Ok(hashes.len() as i64);
        };
        let mut histogram = [0_u32; 66];
        for rank in registers {
            histogram[usize::from(*rank)] += 1;
        }
        let bits = usize::from(64 - self.precision());
        let width = registers.len() as f64;
        // Ertl's histogram estimator avoids the intermediate-cardinality bias
        // of the original HLL estimator: https://arxiv.org/abs/1702.01284.
        let mut denominator = width * tau(1.0 - f64::from(histogram[bits + 1]) / width);
        for frequency in histogram[1..=bits].iter().rev() {
            denominator = (denominator + f64::from(*frequency)) * 0.5;
        }
        denominator += width * sigma(f64::from(histogram[0]) / width);
        let estimate = (width * width / (2.0 * std::f64::consts::LN_2 * denominator)).round();
        if !estimate.is_finite() || !(0.0..9_223_372_036_854_775_808.0).contains(&estimate) {
            return Err(error("HLL estimate exceeds the signed long result range"));
        }
        Ok(estimate as i64)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(
            HEADER
                + match &self.storage {
                    Storage::Sparse(hashes) => hashes.len() * 8,
                    Storage::Dense(registers) => registers.len(),
                },
        );
        output.extend_from_slice(MAGIC);
        output.push(self.accuracy);
        match &self.storage {
            Storage::Sparse(hashes) => {
                output.push(0);
                let mut hashes = hashes.iter().copied().collect::<Vec<_>>();
                hashes.sort_unstable();
                for hash in hashes {
                    output.extend_from_slice(&hash.to_le_bytes());
                }
            }
            Storage::Dense(registers) => {
                output.push(1);
                output.extend_from_slice(registers);
            }
        }
        output
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER || bytes.len() > MAX_BYTES || &bytes[..MAGIC.len()] != MAGIC {
            return Err(error(
                "unsupported or oversized datafusion-kql HLL sketch format",
            ));
        }
        let mut sketch = Self::new(bytes[MAGIC.len()])?;
        let payload = &bytes[HEADER..];
        match bytes[MAGIC.len() + 1] {
            0 => {
                if !payload.len().is_multiple_of(8) || payload.len() / 8 > sketch.sparse_limit() {
                    return Err(error("invalid sparse HLL payload length"));
                }
                let mut previous = None;
                for bytes in payload.as_chunks::<8>().0 {
                    let hash = u64::from_le_bytes(*bytes);
                    if previous.is_some_and(|previous| previous >= hash) {
                        return Err(error("sparse HLL hashes must be unique and ordered"));
                    }
                    sketch.insert(hash);
                    previous = Some(hash);
                }
            }
            1 => {
                if payload.len() != 1 << sketch.precision()
                    || payload.iter().any(|rank| *rank > 65 - sketch.precision())
                {
                    return Err(error("invalid dense HLL register length or rank"));
                }
                sketch.storage = Storage::Dense(payload.to_vec());
            }
            _ => return Err(error("unsupported HLL storage mode")),
        }
        Ok(sketch)
    }
}

fn sigma(mut probability: f64) -> f64 {
    if probability == 1.0 {
        return f64::INFINITY;
    }
    let mut total = probability;
    let mut weight = 1.0;
    for _ in 0..64 {
        probability *= probability;
        let next = total + probability * weight;
        if next == total {
            break;
        }
        total = next;
        weight *= 2.0;
    }
    total
}

fn tau(mut probability: f64) -> f64 {
    if probability == 0.0 || probability == 1.0 {
        return 0.0;
    }
    let mut total = 1.0 - probability;
    let mut weight = 0.5;
    for _ in 0..64 {
        probability = probability.sqrt();
        let next = total - (1.0 - probability).powi(2) * weight;
        if next == total {
            break;
        }
        total = next;
        weight *= 0.5;
    }
    total / 3.0
}

#[cfg(test)]
mod tests {
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn exact_thresholds_promote_to_bounded_dense_states() {
        for accuracy in 0..=4 {
            let mut sketch = Sketch::new(accuracy).unwrap();
            let limit = sketch.sparse_limit();
            for value in 0..limit {
                sketch.insert(HLL_RANDOM_STATE.hash_one(value));
                sketch.insert(HLL_RANDOM_STATE.hash_one(value));
            }
            assert_eq!(sketch.count().unwrap(), limit as i64);
            assert!(matches!(sketch.storage, Storage::Sparse(_)));
            sketch.insert(HLL_RANDOM_STATE.hash_one(limit));
            assert!(matches!(sketch.storage, Storage::Dense(_)));
            assert_eq!(sketch.heap_size(), 1 << sketch.precision());
            for value in limit..limit + 10_000 {
                sketch.insert(HLL_RANDOM_STATE.hash_one(value));
            }
            assert_eq!(sketch.heap_size(), 1 << sketch.precision());
        }
    }

    #[test]
    fn sketches_merge_and_roundtrip_without_changing_the_estimate() {
        for accuracy in 0..=4 {
            for size in [0, 5, 1000, 10_000] {
                let mut whole = Sketch::new(accuracy).unwrap();
                let mut left = Sketch::new(accuracy).unwrap();
                let mut right = Sketch::new(accuracy).unwrap();
                for value in 0..size {
                    let hash = HLL_RANDOM_STATE.hash_one(value);
                    whole.insert(hash);
                    if value % 3 != 0 {
                        left.insert(hash);
                    }
                    if value % 2 != 0 || value % 3 == 0 {
                        right.insert(hash);
                    }
                }
                left.merge(&Sketch::decode(&right.encode()).unwrap())
                    .unwrap();
                assert_eq!(left.encode(), whole.encode());
                assert_eq!(
                    Sketch::decode(&whole.encode()).unwrap().count().unwrap(),
                    whole.count().unwrap()
                );
                left.merge(&left.clone()).unwrap();
                assert_eq!(left.encode(), whole.encode());
            }
        }
        assert!(
            Sketch::new(1)
                .unwrap()
                .merge(&Sketch::new(2).unwrap())
                .is_err()
        );
    }

    #[test]
    fn approximation_stays_within_six_sigma_across_precisions_and_seeds() {
        let count = 100_000;
        for accuracy in 0..=4 {
            for seed in 0..8 {
                let mut sketch = Sketch::new(accuracy).unwrap();
                for value in 0..count {
                    sketch.insert(HLL_RANDOM_STATE.hash_one((seed, value)));
                }
                let relative = (sketch.count().unwrap() as f64 / f64::from(count) - 1.0).abs();
                let bound = 6.0 * 1.04 / ((1 << sketch.precision()) as f64).sqrt();
                assert!(
                    relative <= bound,
                    "accuracy={accuracy}, seed={seed}, error={relative}, bound={bound}"
                );
            }
        }
    }

    #[test]
    fn malformed_states_fail_before_unbounded_allocation() {
        assert!(Sketch::new(5).is_err());
        let empty = Sketch::new(1).unwrap().encode();
        for bytes in [&[][..], &empty[..8], b"foreign HLL", &[0; MAX_BYTES + 1]] {
            assert!(Sketch::decode(bytes).is_err());
        }
        for (index, value) in [(8, 5), (9, 2)] {
            let mut bytes = empty.clone();
            bytes[index] = value;
            assert!(Sketch::decode(&bytes).is_err());
        }
        let mut duplicate = empty.clone();
        duplicate.extend_from_slice(&1_u64.to_le_bytes());
        duplicate.extend_from_slice(&1_u64.to_le_bytes());
        assert!(Sketch::decode(&duplicate).is_err());
        let mut dense = empty;
        dense[9] = 1;
        dense.resize(HEADER + (1 << 14), 0);
        dense[HEADER] = 255;
        assert!(Sketch::decode(&dense).is_err());
    }
}
