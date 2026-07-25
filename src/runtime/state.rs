use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use crate::{cli::RuntimeLimits, kafka::KafkaInput};

use super::Completion;

#[derive(Clone, Copy)]
pub(super) struct Release {
    pub partition: i32,
    pub retained_bytes: usize,
}

pub(super) struct SharedAdmission {
    records: AtomicUsize,
    bytes: AtomicUsize,
    admitted: AtomicU64,
    limits: RuntimeLimits,
    count_limit: Option<u64>,
}

impl SharedAdmission {
    pub fn new(limits: RuntimeLimits, count_limit: Option<u64>) -> Self {
        Self {
            records: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            admitted: AtomicU64::new(0),
            limits,
            count_limit,
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> bool {
        if self
            .records
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |records| {
                (records < self.limits.max_inflight_records).then_some(records + 1)
            })
            .is_err()
        {
            return false;
        }
        if self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(bytes).filter(|total| {
                    *total <= self.limits.max_inflight_bytes
                        || (current == 0 && bytes > self.limits.max_inflight_bytes)
                })
            })
            .is_err()
        {
            self.records.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        if self.count_limit.is_some_and(|limit| {
            self.admitted
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |admitted| {
                    (admitted < limit).then_some(admitted + 1)
                })
                .is_err()
        }) {
            self.bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.records.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub fn release(&self, records: usize, bytes: usize) -> Result<(), String> {
        self.records
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(records)
            })
            .map_err(|_| "shared in-flight record accounting underflow".to_owned())?;
        self.bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .map_err(|_| "shared retained-byte accounting underflow".to_owned())?;
        Ok(())
    }

    pub fn count_reached(&self) -> bool {
        self.count_limit
            .is_some_and(|limit| self.admitted.load(Ordering::Relaxed) >= limit)
    }
}

struct PartitionAdmission {
    next_sequence: u64,
    in_flight: usize,
    limited: bool,
    applied_paused: bool,
}

pub(super) struct Admission {
    pub total_records: usize,
    all_paused: bool,
    partition_pause_dirty: bool,
    partitions: BTreeMap<i32, PartitionAdmission>,
    max_inflight_per_partition: usize,
}

impl Admission {
    pub fn new(partitions: &[i32], max_inflight_per_partition: usize) -> Self {
        Self {
            total_records: 0,
            all_paused: false,
            partition_pause_dirty: false,
            partitions: partitions
                .iter()
                .map(|partition| {
                    (
                        *partition,
                        PartitionAdmission {
                            next_sequence: 0,
                            in_flight: 0,
                            limited: false,
                            applied_paused: false,
                        },
                    )
                })
                .collect(),
            max_inflight_per_partition,
        }
    }

    pub fn can_reserve(&self, partition: i32) -> bool {
        self.partitions
            .get(&partition)
            .is_some_and(|state| state.in_flight < self.max_inflight_per_partition)
    }

    pub fn reserve(&mut self, partition: i32) -> Result<u64, String> {
        if !self.can_reserve(partition) {
            return Err("record admitted without available capacity".to_owned());
        }
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| format!("record received for unknown partition {partition}"))?;
        let sequence = state.next_sequence;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| format!("partition {partition} sequence overflow"))?;
        state.in_flight += 1;
        self.partition_pause_dirty |=
            update_partition_limit(state, self.max_inflight_per_partition);
        self.total_records += 1;
        Ok(sequence)
    }

    pub fn release(&mut self, partition: i32) -> Result<(), String> {
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| format!("completion released unknown partition {partition}"))?;
        if state.in_flight == 0 || self.total_records == 0 {
            return Err(format!(
                "invalid completion release for partition {partition}"
            ));
        }
        state.in_flight -= 1;
        self.partition_pause_dirty |=
            update_partition_limit(state, self.max_inflight_per_partition);
        self.total_records -= 1;
        Ok(())
    }

    pub fn sync_pauses(&mut self, input: &mut KafkaInput, pause_all: bool) -> Result<(), String> {
        if pause_all == self.all_paused && !self.partition_pause_dirty {
            return Ok(());
        }
        for (partition, state) in &mut self.partitions {
            let pause = pause_all || state.limited;
            if pause != state.applied_paused {
                input.set_paused(*partition, pause)?;
                state.applied_paused = pause;
            }
        }
        self.all_paused = pause_all;
        self.partition_pause_dirty = false;
        Ok(())
    }
}

fn low_water(limit: usize) -> usize {
    limit.saturating_mul(3).div_ceil(4)
}

fn update_partition_limit(state: &mut PartitionAdmission, limit: usize) -> bool {
    let previous = state.limited;
    state.limited = if state.limited {
        state.in_flight >= low_water(limit)
    } else {
        state.in_flight >= limit
    };
    state.limited != previous
}

struct PartitionOrder {
    next_sequence: u64,
    pending: BTreeMap<u64, Completion>,
}

#[derive(Default)]
pub(super) struct Orderer {
    partitions: BTreeMap<i32, PartitionOrder>,
}

impl Orderer {
    pub fn insert(
        &mut self,
        completion: Completion,
        ready: &mut Vec<Completion>,
    ) -> Result<(), (String, Box<Completion>)> {
        let state = self
            .partitions
            .entry(completion.partition)
            .or_insert_with(|| PartitionOrder {
                next_sequence: 0,
                pending: BTreeMap::new(),
            });
        if completion.sequence < state.next_sequence
            || state.pending.contains_key(&completion.sequence)
        {
            return Err((
                format!(
                    "duplicate completion for partition {} sequence {}",
                    completion.partition, completion.sequence
                ),
                Box::new(completion),
            ));
        }
        if completion.sequence == state.next_sequence {
            ready.push(completion);
            state.next_sequence += 1;
        } else {
            state.pending.insert(completion.sequence, completion);
            return Ok(());
        }
        while let Some(completion) = state.pending.remove(&state.next_sequence) {
            ready.push(completion);
            state.next_sequence += 1;
        }
        Ok(())
    }

    pub fn take_pending(&mut self) -> Vec<Completion> {
        self.partitions
            .values_mut()
            .flat_map(|state| std::mem::take(&mut state.pending).into_values())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use crate::{
        output::Timestamp,
        transform::jsonata::{Action, PassPayload},
    };

    use super::*;
    use crate::runtime::{CompletionOutcome, SourceRecord};

    fn completion(partition: i32, sequence: u64, bytes: usize, action: Action) -> Completion {
        Completion {
            consumer: 0,
            partition,
            sequence,
            retained_bytes: bytes,
            source: SourceRecord {
                partition,
                offset: i64::try_from(sequence).unwrap(),
                timestamp: None::<Timestamp>,
                key: None,
                headers: Vec::new(),
            },
            outcome: CompletionOutcome::Action(action),
        }
    }

    #[test]
    fn completion_frontier_orders_each_partition_and_advances_across_drop() {
        let mut orderer = Orderer::default();
        let mut ready = Vec::new();
        orderer
            .insert(completion(0, 2, 1, Action::Tombstone), &mut ready)
            .unwrap();
        assert!(ready.is_empty());
        orderer
            .insert(completion(0, 1, 1, Action::Drop), &mut ready)
            .unwrap();
        assert!(ready.is_empty());
        orderer
            .insert(
                completion(
                    0,
                    0,
                    1,
                    Action::PassThrough(PassPayload::Exact(b"a".to_vec())),
                ),
                &mut ready,
            )
            .unwrap();
        assert_eq!(
            ready.iter().map(|item| item.sequence).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        ready.clear();
        orderer
            .insert(
                completion(
                    1,
                    0,
                    1,
                    Action::PassThrough(PassPayload::Exact(b"b".to_vec())),
                ),
                &mut ready,
            )
            .unwrap();
        assert_eq!(ready[0].partition, 1);
    }

    #[test]
    fn admission_enforces_per_partition_limits() {
        let mut admission = Admission::new(&[0, 1], 1);
        assert_eq!(admission.reserve(0).unwrap(), 0);
        assert!(!admission.can_reserve(0));
        assert!(admission.can_reserve(1));
        admission.release(0).unwrap();
        assert_eq!(admission.reserve(0).unwrap(), 1);
    }

    #[test]
    fn shared_admission_enforces_limits_across_consumers() {
        let admission = SharedAdmission::new(
            RuntimeLimits {
                max_inflight_records: 2,
                max_inflight_bytes: 10,
                max_inflight_per_partition: 2,
            },
            Some(3),
        );
        assert!(admission.try_reserve(6));
        assert!(!admission.try_reserve(5));
        admission.release(1, 6).unwrap();
        assert!(admission.try_reserve(11));
        assert!(!admission.try_reserve(1));
        admission.release(1, 11).unwrap();
        assert!(admission.try_reserve(1));
        admission.release(1, 1).unwrap();
        assert!(!admission.try_reserve(1));

        let admission = Arc::new(SharedAdmission::new(
            RuntimeLimits {
                max_inflight_records: 100,
                max_inflight_bytes: 100,
                max_inflight_per_partition: 100,
            },
            Some(50),
        ));
        let admitted = thread::scope(|scope| {
            let handles = (0..4)
                .map(|_| {
                    let admission = Arc::clone(&admission);
                    scope.spawn(move || {
                        (0..100)
                            .filter(|_| {
                                let reserved = admission.try_reserve(1);
                                if reserved {
                                    admission.release(1, 1).unwrap();
                                }
                                reserved
                            })
                            .count()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(admitted, 50);
    }

    #[test]
    fn partition_release_is_exactly_once_and_low_water_is_hysteretic() {
        let mut admission = Admission::new(&[0], 4);
        for _ in 0..4 {
            admission.reserve(0).unwrap();
        }
        assert!(admission.partitions[&0].limited);
        for expected_limited in [true, false] {
            admission.release(0).unwrap();
            assert_eq!(admission.partitions[&0].limited, expected_limited);
        }
        for _ in 0..2 {
            admission.release(0).unwrap();
        }
        assert!(admission.release(0).is_err());
    }
}
