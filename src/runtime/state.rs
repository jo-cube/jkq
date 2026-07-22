use std::collections::BTreeMap;

use crate::{cli::RuntimeLimits, kafka::KafkaInput};

use super::Completion;

#[derive(Clone, Copy)]
pub(super) struct Release {
    pub partition: i32,
    pub retained_bytes: usize,
}

struct PartitionAdmission {
    next_sequence: u64,
    in_flight: usize,
    limited: bool,
    applied_paused: bool,
}

pub(super) struct Admission {
    pub total_records: usize,
    total_bytes: usize,
    global_limited: bool,
    all_paused: bool,
    partition_pause_dirty: bool,
    partitions: BTreeMap<i32, PartitionAdmission>,
    limits: RuntimeLimits,
}

impl Admission {
    pub fn new(partitions: &[i32], limits: RuntimeLimits) -> Self {
        Self {
            total_records: 0,
            total_bytes: 0,
            global_limited: false,
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
            limits,
        }
    }

    pub fn can_reserve(&self, partition: i32, bytes: usize) -> bool {
        let Some(state) = self.partitions.get(&partition) else {
            return false;
        };
        if self.total_records >= self.limits.max_inflight_records
            || state.in_flight >= self.limits.max_inflight_per_partition
        {
            return false;
        }
        self.total_bytes
            .checked_add(bytes)
            .is_some_and(|total| total <= self.limits.max_inflight_bytes)
            || (self.total_bytes == 0 && bytes > self.limits.max_inflight_bytes)
    }

    pub fn should_wait_for_capacity(&self) -> bool {
        self.total_records >= self.limits.max_inflight_records
            || self.total_bytes >= self.limits.max_inflight_bytes
            || self.partitions.values().all(|state| state.limited)
    }

    pub fn reserve(&mut self, partition: i32, bytes: usize) -> Result<u64, String> {
        if !self.can_reserve(partition, bytes) {
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
            update_partition_limit(state, self.limits.max_inflight_per_partition);
        self.total_records += 1;
        self.total_bytes = self
            .total_bytes
            .checked_add(bytes)
            .ok_or_else(|| "retained-byte accounting overflow".to_owned())?;
        Ok(sequence)
    }

    pub fn release(&mut self, release: Release) -> Result<(), String> {
        let state = self.partitions.get_mut(&release.partition).ok_or_else(|| {
            format!(
                "completion released unknown partition {}",
                release.partition
            )
        })?;
        if state.in_flight == 0
            || self.total_records == 0
            || self.total_bytes < release.retained_bytes
        {
            return Err(format!(
                "invalid completion release for partition {}",
                release.partition
            ));
        }
        state.in_flight -= 1;
        self.partition_pause_dirty |=
            update_partition_limit(state, self.limits.max_inflight_per_partition);
        self.total_records -= 1;
        self.total_bytes -= release.retained_bytes;
        Ok(())
    }

    fn update_global_limit(&mut self, pending: bool) {
        if self.global_limited {
            self.global_limited = pending
                || self.total_records >= low_water(self.limits.max_inflight_records)
                || self.total_bytes >= low_water(self.limits.max_inflight_bytes);
        } else {
            self.global_limited = pending
                || self.total_records >= self.limits.max_inflight_records
                || self.total_bytes >= self.limits.max_inflight_bytes;
        }
    }

    pub fn sync_pauses(
        &mut self,
        input: &mut KafkaInput,
        stopping: bool,
        pending: bool,
    ) -> Result<(), String> {
        self.update_global_limit(pending);
        let all_paused = stopping || self.global_limited;
        if all_paused == self.all_paused && !self.partition_pause_dirty {
            return Ok(());
        }
        for (partition, state) in &mut self.partitions {
            let pause = all_paused || state.limited;
            if pause != state.applied_paused {
                input.set_paused(*partition, pause)?;
                state.applied_paused = pause;
            }
        }
        self.all_paused = all_paused;
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
    use crate::{
        output::Timestamp,
        transform::jsonata::{Action, PassPayload},
    };

    use super::*;
    use crate::runtime::{CompletionOutcome, SourceRecord};

    fn completion(partition: i32, sequence: u64, bytes: usize, action: Action) -> Completion {
        Completion {
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
    fn admission_enforces_records_bytes_partitions_and_oversized_rule() {
        let mut admission = Admission::new(
            &[0, 1],
            RuntimeLimits {
                max_inflight_records: 2,
                max_inflight_bytes: 10,
                max_inflight_per_partition: 1,
            },
        );
        assert_eq!(admission.reserve(0, 6).unwrap(), 0);
        assert!(!admission.can_reserve(0, 1));
        assert!(admission.can_reserve(1, 4));
        assert!(!admission.can_reserve(1, 5));
        admission
            .release(Release {
                partition: 0,
                retained_bytes: 6,
            })
            .unwrap();
        assert!(admission.can_reserve(1, 11));
        admission.reserve(1, 11).unwrap();
        assert!(!admission.can_reserve(0, 1));
    }

    #[test]
    fn release_is_exactly_once_and_low_water_is_hysteretic() {
        let mut admission = Admission::new(
            &[0],
            RuntimeLimits {
                max_inflight_records: 4,
                max_inflight_bytes: 100,
                max_inflight_per_partition: 4,
            },
        );
        for _ in 0..4 {
            admission.reserve(0, 10).unwrap();
        }
        admission.update_global_limit(false);
        assert!(admission.global_limited);
        assert!(admission.partitions[&0].limited);
        for expected_limited in [true, false] {
            admission
                .release(Release {
                    partition: 0,
                    retained_bytes: 10,
                })
                .unwrap();
            admission.update_global_limit(false);
            assert_eq!(admission.global_limited, expected_limited);
            assert_eq!(admission.should_wait_for_capacity(), expected_limited);
        }
        for _ in 0..2 {
            admission
                .release(Release {
                    partition: 0,
                    retained_bytes: 10,
                })
                .unwrap();
        }
        assert!(
            admission
                .release(Release {
                    partition: 0,
                    retained_bytes: 10,
                })
                .is_err()
        );
    }
}
