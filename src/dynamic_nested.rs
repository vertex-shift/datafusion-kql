use std::collections::{BTreeMap, HashSet};

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::compute::{SortOptions, sort_to_indices};
use datafusion::common::{DataFusionError, Result};

use crate::dynamic::{Kernel, TopReduction};
use crate::dynamic_value::{Node, check_size};

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn integer(value: &Node) -> Option<i64> {
    match value {
        Node::Long(value) => Some(*value),
        Node::Bool(value) => Some(i64::from(*value)),
        _ => None,
    }
}

fn string(value: &Node) -> Result<&str> {
    match value {
        Node::String(value) => Ok(value),
        _ => Err(error("dynamic operation requires a string")),
    }
}

pub(crate) fn supports(kernel: &Kernel) -> bool {
    matches!(
        kernel,
        Kernel::Get
            | Kernel::Equal(_)
            | Kernel::Membership { .. }
            | Kernel::PackArray
            | Kernel::BagPack
            | Kernel::PackAll { .. }
            | Kernel::BagMerge
            | Kernel::BagSetKey
            | Kernel::BagRemoveKeys
            | Kernel::ArrayConcat
            | Kernel::ArraySlice
            | Kernel::ArrayIndexOf
            | Kernel::ArrayHas
            | Kernel::ArrayReverse
            | Kernel::ArraySum
            | Kernel::SetUnion
            | Kernel::SetIntersect
            | Kernel::SetDifference
            | Kernel::Zip
            | Kernel::Repeat
            | Kernel::Range
            | Kernel::TopValues { .. }
            | Kernel::TopReduce(_)
    )
}

pub(crate) fn expanded(
    value: &Node,
    array_mode: bool,
    arrays_only: bool,
    limit: usize,
) -> Result<Vec<Node>> {
    let mut items = match value {
        Node::Array(items) => items.clone(),
        Node::Bag(bag) if !arrays_only => bag
            .iter()
            .map(|(key, value)| {
                if array_mode {
                    Node::Array(vec![Node::String(key.clone()), value.clone()])
                } else {
                    Node::Bag(BTreeMap::from([(key.clone(), value.clone())]))
                }
            })
            .collect(),
        value => vec![value.clone()],
    };
    items.truncate(limit);
    check_size(items.len())?;
    Ok(items)
}

pub(crate) fn evaluate(kernel: &Kernel, values: &[Node]) -> Result<Node> {
    use Kernel as K;
    Ok(match kernel {
        K::Get => match (&values[0], &values[1]) {
            (Node::Bag(bag), Node::String(key)) => bag.get(key).cloned().unwrap_or(Node::Null),
            (Node::Array(items), index) => {
                let index = integer(index)
                    .ok_or_else(|| error("dynamic array index must be an integer"))?;
                let index = if index < 0 {
                    items.len() as i128 + i128::from(index)
                } else {
                    i128::from(index)
                };
                usize::try_from(index)
                    .ok()
                    .and_then(|i| items.get(i))
                    .cloned()
                    .unwrap_or(Node::Null)
            }
            _ => Node::Null,
        },
        K::Equal(negated) => {
            if values[0] == Node::Null && values[1] == Node::Null {
                Node::Null
            } else {
                Node::Bool((values[0] == values[1]) != *negated)
            }
        }
        K::Membership {
            negated,
            insensitive,
        } => {
            fn flatten(value: &Node, items: &mut Vec<Node>) -> Result<()> {
                if let Node::Array(values) = value {
                    for value in values {
                        flatten(value, items)?;
                    }
                } else {
                    check_size(items.len() + 1)?;
                    items.push(value.clone());
                }
                Ok(())
            }
            let mut needle = values[0].clone();
            let mut items = Vec::new();
            for value in &values[1..] {
                flatten(value, &mut items)?;
            }
            if *insensitive {
                if let Node::String(value) = &mut needle {
                    value.make_ascii_lowercase();
                }
                for item in &mut items {
                    if let Node::String(value) = item {
                        value.make_ascii_lowercase();
                    }
                }
            }
            if needle == Node::Null && items.contains(&Node::Null) {
                Node::Null
            } else {
                Node::Bool((needle != Node::Null && items.contains(&needle)) != *negated)
            }
        }
        K::PackArray => {
            check_size(values.len())?;
            Node::Array(values.to_vec())
        }
        K::BagPack => {
            let mut result = BTreeMap::new();
            for [key, value] in values.as_chunks::<2>().0 {
                result.insert(string(key)?.to_owned(), value.clone());
            }
            check_size(result.len())?;
            Node::Bag(result)
        }
        K::PackAll {
            names,
            ignore_empty,
        } => {
            let mut result = BTreeMap::new();
            for (name, value) in names.iter().zip(values) {
                if !*ignore_empty
                    || !(value == &Node::Null || matches!(value, Node::String(s) if s.is_empty()))
                {
                    result.insert(name.clone(), value.clone());
                }
            }
            check_size(result.len())?;
            Node::Bag(result)
        }
        K::BagMerge => {
            let mut result = BTreeMap::new();
            for value in values.iter().rev() {
                let Node::Bag(bag) = value else {
                    return Ok(Node::Null);
                };
                result.extend(bag.clone());
                check_size(result.len())?;
            }
            Node::Bag(result)
        }
        K::BagSetKey => {
            let Node::Bag(mut bag) = values[0].clone() else {
                return Ok(Node::Null);
            };
            let key = string(&values[1])?;
            if key.starts_with('$') {
                return Err(error(
                    "bag_set_key JSONPath updates are not implemented; use a root key",
                ));
            }
            bag.insert(key.into(), values[2].clone());
            check_size(bag.len())?;
            Node::Bag(bag)
        }
        K::BagRemoveKeys => {
            let (Node::Bag(mut bag), Node::Array(keys)) = (values[0].clone(), &values[1]) else {
                return Ok(Node::Null);
            };
            for key in keys {
                let key = string(key)?;
                if key.starts_with('$') {
                    return Err(error(
                        "bag_remove_keys JSONPath updates are not implemented; use root keys",
                    ));
                }
                bag.remove(key);
            }
            Node::Bag(bag)
        }
        K::ArrayConcat | K::SetUnion | K::SetIntersect | K::SetDifference => {
            let mut arrays = Vec::new();
            for value in values {
                let Node::Array(items) = value else {
                    return Ok(Node::Null);
                };
                arrays.push(items);
            }
            let mut result = Vec::new();
            if *kernel == K::ArrayConcat {
                for items in arrays {
                    check_size(result.len().saturating_add(items.len()))?;
                    result.extend_from_slice(items);
                }
            } else {
                let others = arrays[1..]
                    .iter()
                    .map(|items| items.iter().map(Node::wire).collect::<Result<HashSet<_>>>())
                    .collect::<Result<Vec<_>>>()?;
                let candidates = if *kernel == K::SetUnion {
                    arrays.into_iter().flatten().collect::<Vec<_>>()
                } else {
                    arrays[0].iter().collect()
                };
                let mut seen = HashSet::new();
                for item in candidates {
                    let key = item.wire()?;
                    let include = match kernel {
                        K::SetIntersect => others.iter().all(|set| set.contains(&key)),
                        K::SetDifference => !others[0].contains(&key),
                        _ => true,
                    };
                    if include && seen.insert(key) {
                        check_size(result.len() + 1)?;
                        result.push(item.clone());
                    }
                }
            }
            Node::Array(result)
        }
        K::ArraySlice => {
            let Node::Array(items) = &values[0] else {
                return Ok(Node::Null);
            };
            let (Some(start), Some(end)) = (integer(&values[1]), integer(&values[2])) else {
                return Ok(Node::Null);
            };
            let len = items.len() as i128;
            let offset = |value: i64| {
                if value < 0 {
                    len + i128::from(value)
                } else {
                    i128::from(value)
                }
            };
            let start = offset(start).clamp(0, len) as usize;
            let end = (offset(end) + 1).clamp(0, len) as usize;
            Node::Array(if end < start {
                Vec::new()
            } else {
                items[start..end].to_vec()
            })
        }
        K::ArrayIndexOf | K::ArrayHas => {
            let Node::Array(items) = &values[0] else {
                return Ok(Node::Null);
            };
            let start = values.get(2).and_then(integer).unwrap_or(0);
            let length = values.get(3).and_then(integer).unwrap_or(-1);
            let mut occurrence = values.get(4).and_then(integer).unwrap_or(1);
            if length < -1 || occurrence < 1 {
                return Ok(Node::Null);
            }
            let len = items.len() as i128;
            let start = if start < 0 {
                (len + i128::from(start)).max(0)
            } else {
                i128::from(start)
            };
            let end = if length == -1 {
                len
            } else {
                (start + i128::from(length)).min(len)
            };
            let mut found = -1;
            for i in start..end {
                if items[i as usize] == values[1] {
                    occurrence -= 1;
                    if occurrence == 0 {
                        found = i as i64;
                        break;
                    }
                }
            }
            if *kernel == K::ArrayHas {
                Node::Bool(found >= 0)
            } else {
                Node::Long(found)
            }
        }
        K::ArrayReverse => {
            let Node::Array(mut items) = values[0].clone() else {
                return Ok(Node::Null);
            };
            items.reverse();
            Node::Array(items)
        }
        K::ArraySum => {
            let Node::Array(items) = &values[0] else {
                return Ok(Node::Null);
            };
            let mut sum = 0.0;
            for item in items {
                sum += match item {
                    Node::Long(value) => *value as f64,
                    Node::Real(value) => *value,
                    Node::Decimal(value) => value
                        .to_string()
                        .parse::<f64>()
                        .map_err(|_| error("decimal array value is not representable as real"))?,
                    _ => return Ok(Node::Null),
                };
            }
            Node::Real(sum)
        }
        K::Zip => {
            let mut arrays = Vec::new();
            for value in values {
                let Node::Array(items) = value else {
                    return Ok(Node::Null);
                };
                arrays.push(items);
            }
            let len = arrays.iter().map(|items| items.len()).max().unwrap_or(0);
            check_size(len)?;
            Node::Array(
                (0..len)
                    .map(|i| {
                        Node::Array(
                            arrays
                                .iter()
                                .map(|items| items.get(i).cloned().unwrap_or(Node::Null))
                                .collect(),
                        )
                    })
                    .collect(),
            )
        }
        K::Repeat => {
            let Some(count) = integer(&values[1]).and_then(|value| usize::try_from(value).ok())
            else {
                return Ok(Node::Null);
            };
            check_size(count)?;
            Node::Array(vec![values[0].clone(); count])
        }
        K::Range => {
            let (start, end, step, datetime) = match (&values[0], &values[1], &values[2]) {
                (Node::Datetime(a), Node::Datetime(b), Node::Timespan(step)) => {
                    (*a, *b, *step, true)
                }
                (Node::Timespan(a), Node::Timespan(b), Node::Timespan(step)) => {
                    (*a, *b, *step, false)
                }
                _ => {
                    return Err(error(
                        "typed range requires matching datetime/timespan bounds and a timespan step",
                    ));
                }
            };
            if step == 0 {
                return Err(error("range() step cannot be zero"));
            }
            let distance = i128::from(end) - i128::from(start);
            let count = if distance != 0 && (distance > 0) != (step > 0) {
                0
            } else {
                distance / i128::from(step) + 1
            };
            let count = usize::try_from(count).map_err(|_| error("invalid range size"))?;
            check_size(count)?;
            Node::Array(
                (0..count)
                    .map(|i| {
                        let value = i64::try_from(i128::from(start) + i128::from(step) * i as i128)
                            .map_err(|_| error("range value overflow"))?;
                        Ok(if datetime {
                            Node::Datetime(value)
                        } else {
                            Node::Timespan(value)
                        })
                    })
                    .collect::<Result<_>>()?,
            )
        }
        K::TopValues {
            real,
            descending,
            nulls_first,
            expanded_limit,
            count,
        } => {
            let mut items = expanded(&values[0], false, true, *expanded_limit)?;
            for item in &mut items {
                if !matches!(
                    (&item, real),
                    (Node::Real(_), true) | (Node::Long(_), false)
                ) {
                    *item = Node::Null;
                }
            }
            let array: ArrayRef = if *real {
                std::sync::Arc::new(Float64Array::from_iter(items.iter().map(|item| {
                    if let Node::Real(value) = item {
                        Some(*value)
                    } else {
                        None
                    }
                })))
            } else {
                std::sync::Arc::new(Int64Array::from_iter(items.iter().map(|item| {
                    if let Node::Long(value) = item {
                        Some(*value)
                    } else {
                        None
                    }
                })))
            };
            let indices = sort_to_indices(
                array.as_ref(),
                Some(SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                }),
                Some(*count),
            )?;
            Node::Array(
                indices
                    .values()
                    .iter()
                    .map(|&i| items[i as usize].clone())
                    .collect(),
            )
        }
        K::TopReduce(reduction) => {
            let Node::Array(items) = &values[0] else {
                return Err(error("invalid optimized mv-apply input"));
            };
            match reduction {
                TopReduction::Sum => Node::Long(
                    datafusion::arrow::compute::sum(&Int64Array::from_iter(items.iter().map(
                        |value| {
                            if let Node::Long(value) = value {
                                Some(*value)
                            } else {
                                None
                            }
                        },
                    )))
                    .unwrap_or(0),
                ),
                TopReduction::Count => Node::Long(items.len() as i64),
                TopReduction::List {
                    limit,
                    include_nulls,
                } => Node::Array(
                    items
                        .iter()
                        .filter(|value| *include_nulls || **value != Node::Null)
                        .take(*limit)
                        .cloned()
                        .collect(),
                ),
            }
        }
        _ => return Err(error("invalid typed dynamic kernel dispatch")),
    })
}
