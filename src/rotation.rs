//! Request-count and elapsed-time policies for selecting the next proxy.

use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use rand::seq::SliceRandom;

/// How a replacement is chosen from the currently eligible proxies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelectionStrategy {
    /// Visit proxies in candidate-list order, wrapping at the end.
    #[default]
    RoundRobin,
    /// Choose uniformly among eligible proxies other than the current one.
    Random,
    /// Visit every eligible proxy once in a randomly shuffled round.
    /// New and recovered proxies join the next round; unavailable proxies are
    /// skipped. Consecutive rounds do not immediately repeat a proxy when there
    /// is an alternative. Request/time limits still apply to each selection.
    ShuffledRoundRobin,
}

/// Limits controlling how long the pool retains its current proxy.
///
/// Each successful acquisition consumes one request allowance, including an
/// acquisition whose eventual network request fails. When either enabled limit
/// is reached, the *next* acquisition selects a replacement. Time limits are
/// checked during acquisition; they do not schedule background work.
///
/// With both limits disabled, the current proxy is retained until it becomes
/// unavailable or a caller requests a manual rotation. A replacement avoids the
/// current proxy whenever another eligible proxy exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationPolicy {
    /// Maximum acquisitions per proxy selection; `None` disables this limit.
    pub requests_per_proxy: Option<NonZeroU64>,
    /// Maximum elapsed time per selection; `None` disables this limit.
    pub max_age: Option<Duration>,
    /// Method used to choose the next eligible proxy.
    pub strategy: SelectionStrategy,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            requests_per_proxy: NonZeroU64::new(1),
            max_age: None,
            strategy: SelectionStrategy::RoundRobin,
        }
    }
}

impl RotationPolicy {
    /// Keep each selection for this many acquisitions before rotating.
    pub fn every(requests: NonZeroU64) -> Self {
        Self {
            requests_per_proxy: Some(requests),
            ..Self::default()
        }
    }

    /// Keep a proxy until it becomes unavailable or rotation is requested.
    pub fn sticky() -> Self {
        Self {
            requests_per_proxy: None,
            ..Self::default()
        }
    }

    /// Keep each selection for a positive duration, regardless of request count.
    /// The time limit is checked on acquisition; no background timer is needed.
    pub fn for_duration(duration: Duration) -> crate::Result<Self> {
        let policy = Self {
            max_age: Some(duration),
            ..Self::sticky()
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Rejects a zero time limit; use a request limit of one to rotate every time.
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_age.is_some_and(|age| age.is_zero()) {
            return Err(crate::Error::Config(
                "rotation max_age must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// The pool serializes access to this state together with its eligible nodes.
#[derive(Debug, Default)]
pub(crate) struct RotationState {
    current: Option<String>,
    requests: u64,
    selected_at: Option<Instant>,
    rotate_next: bool,
    // The pool reuses this allocation while eligibility is unchanged. Keeping
    // its order also preserves the successor when the current node disappears.
    candidates: Option<Arc<[String]>>,
    positions: HashMap<String, usize>,
    shuffled_remaining: Vec<usize>,
    shuffled_started: bool,
    #[cfg(test)]
    snapshot_rebuilds: usize,
}

impl RotationState {
    #[cfg(test)]
    fn select(
        &mut self,
        candidates: &[String],
        policy: &RotationPolicy,
        now: Instant,
    ) -> Option<String> {
        // Unit tests may construct small slices; the production pool passes a
        // shared snapshot so stable acquisitions never copy or scan all IDs.
        let candidates = match &self.candidates {
            Some(previous) if previous.as_ref() == candidates => Arc::clone(previous),
            _ => Arc::from(candidates),
        };
        self.select_shared(&candidates, policy, now)
    }

    pub(crate) fn select_shared(
        &mut self,
        candidates: &Arc<[String]>,
        policy: &RotationPolicy,
        now: Instant,
    ) -> Option<String> {
        if candidates.is_empty() {
            // Preserve the previous position across temporary total outages.
            self.rotate_next = true;
            return None;
        }

        let removed_successor = self.update_candidates(candidates);

        let quota_reached = policy
            .requests_per_proxy
            .is_some_and(|limit| self.requests >= limit.get());
        let age_reached = policy.max_age.is_some_and(|limit| {
            self.selected_at
                .is_some_and(|started| now.saturating_duration_since(started) >= limit)
        });
        let current_index = self
            .current
            .as_ref()
            .and_then(|current| self.positions.get(current))
            .copied();

        if current_index.is_some() && !self.rotate_next && !quota_reached && !age_reached {
            if policy.strategy == SelectionStrategy::ShuffledRoundRobin && !self.shuffled_started {
                // A session can receive its first node from the pool's shared
                // allocator. That node counts as visited in this shuffled round.
                self.start_shuffled_round(candidates.len(), current_index);
            }
            self.requests = self.requests.saturating_add(1);
            return self.current.clone();
        }

        let index = match policy.strategy {
            SelectionStrategy::RoundRobin => current_index
                .map(|index| (index + 1) % candidates.len())
                .or(removed_successor)
                .unwrap_or(0),
            SelectionStrategy::Random => Self::next_random(candidates.len(), current_index),
            SelectionStrategy::ShuffledRoundRobin => {
                self.next_shuffled(candidates.len(), current_index)
            }
        };
        let selected = candidates[index].clone();
        self.current = Some(selected.clone());
        self.requests = 1;
        self.selected_at = Some(now);
        self.rotate_next = false;
        Some(selected)
    }

    pub(crate) fn has_current(&self) -> bool {
        self.current.is_some()
    }

    /// Start a new session at a globally allocated node without consuming quota.
    pub(crate) fn seed(&mut self, id: &str, now: Instant) {
        self.current = Some(id.to_owned());
        self.requests = 0;
        self.selected_at = Some(now);
        self.rotate_next = false;
        self.shuffled_remaining.clear();
        self.shuffled_started = false;
    }

    pub(crate) fn invalidate(&mut self, id: &str) {
        if self.current.as_deref() == Some(id) {
            self.force_rotate();
        }
    }

    pub(crate) fn force_rotate(&mut self) {
        self.rotate_next = true;
    }

    // Returns the removed current node's next surviving successor. All work
    // proportional to pool size happens only when the shared snapshot changes.
    fn update_candidates(&mut self, candidates: &Arc<[String]>) -> Option<usize> {
        if self
            .candidates
            .as_ref()
            .is_some_and(|previous| Arc::ptr_eq(previous, candidates))
        {
            return None;
        }
        let positions: HashMap<_, _> = candidates
            .iter()
            .enumerate()
            .map(|(index, id)| (id.clone(), index))
            .collect();
        let successor = self.current.as_ref().and_then(|current| {
            if positions.contains_key(current) {
                return None;
            }
            let index = *self.positions.get(current)?;
            let previous = self.candidates.as_ref()?;
            previous[index + 1..]
                .iter()
                .chain(&previous[..index])
                .find_map(|id| positions.get(id).copied())
        });
        if let Some(previous) = &self.candidates {
            self.shuffled_remaining = self
                .shuffled_remaining
                .iter()
                .filter_map(|index| positions.get(&previous[*index]).copied())
                .collect();
        }
        self.positions = positions;
        self.candidates = Some(Arc::clone(candidates));
        #[cfg(test)]
        {
            self.snapshot_rebuilds += 1;
        }
        successor
    }

    // Uniformly sample the index range with the current node removed, without
    // allocating or scanning an alternative-node collection.
    fn next_random(len: usize, current: Option<usize>) -> usize {
        if len == 1 {
            return 0;
        }
        if let Some(current) = current {
            let index = rand::random_range(0..len - 1);
            index + usize::from(index >= current)
        } else {
            rand::random_range(0..len)
        }
    }

    fn start_shuffled_round(&mut self, len: usize, already_visited: Option<usize>) {
        self.shuffled_remaining = (0..len)
            .filter(|index| Some(*index) != already_visited)
            .collect();
        self.shuffled_remaining.shuffle(&mut rand::rng());
        self.shuffled_started = true;
    }

    fn next_shuffled(&mut self, len: usize, current: Option<usize>) -> usize {
        if self.shuffled_remaining.is_empty() {
            self.start_shuffled_round(len, None);
            let last = self.shuffled_remaining.len() - 1;
            if len > 1 && self.shuffled_remaining.last().copied() == current {
                self.shuffled_remaining.swap(0, last);
            }
        }
        self.shuffled_remaining
            .pop()
            .expect("a shuffled round is nonempty for a nonempty candidate list")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        thread,
    };

    fn nodes(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| (*id).to_owned()).collect()
    }

    fn every(requests: u64) -> RotationPolicy {
        RotationPolicy {
            requests_per_proxy: NonZeroU64::new(requests),
            ..RotationPolicy::default()
        }
    }

    fn select(
        state: &mut RotationState,
        nodes: &[String],
        policy: &RotationPolicy,
        now: Instant,
    ) -> String {
        state.select(nodes, policy, now).expect("eligible nodes")
    }

    #[test]
    fn an_explicit_batch_rotates_on_twenty_first_acquisition() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c"]);
        let now = Instant::now();
        for expected in ["a", "b", "c", "a"] {
            for _ in 0..20 {
                assert_eq!(select(&mut state, &nodes, &every(20), now), expected);
            }
        }
    }

    #[test]
    fn default_visits_every_node_in_order_without_concentrating_a_batch() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c"]);
        let now = Instant::now();
        let actual: Vec<_> = (0..7)
            .map(|_| select(&mut state, &nodes, &RotationPolicy::default(), now))
            .collect();
        assert_eq!(actual, ["a", "b", "c", "a", "b", "c", "a"]);
    }

    #[test]
    fn elapsed_time_rotates_at_exact_boundary_and_resets_the_clock() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b"]);
        let start = Instant::now();
        let policy = RotationPolicy {
            requests_per_proxy: None,
            max_age: Some(Duration::from_secs(10)),
            ..RotationPolicy::default()
        };
        assert_eq!(select(&mut state, &nodes, &policy, start), "a");
        assert_eq!(
            select(
                &mut state,
                &nodes,
                &policy,
                start + Duration::from_millis(9_999)
            ),
            "a"
        );
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(10)),
            "b"
        );
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(19)),
            "b"
        );
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(20)),
            "a"
        );
    }

    #[test]
    fn either_limit_triggers_rotation() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c"]);
        let start = Instant::now();
        let policy = RotationPolicy {
            max_age: Some(Duration::from_secs(10)),
            ..every(2)
        };
        assert_eq!(select(&mut state, &nodes, &policy, start), "a");
        assert_eq!(select(&mut state, &nodes, &policy, start), "a");
        assert_eq!(select(&mut state, &nodes, &policy, start), "b");
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(10)),
            "c"
        );
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(10)),
            "c"
        );
        assert_eq!(
            select(&mut state, &nodes, &policy, start + Duration::from_secs(10)),
            "a"
        );
    }

    #[test]
    fn random_rotation_never_repeats_when_an_alternative_exists() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c", "d"]);
        let now = Instant::now();
        let policy = RotationPolicy {
            strategy: SelectionStrategy::Random,
            ..every(1)
        };
        let mut previous = None;
        for _ in 0..1_000 {
            let selected = select(&mut state, &nodes, &policy, now);
            assert!(nodes.contains(&selected));
            assert_ne!(previous.as_ref(), Some(&selected));
            previous = Some(selected);
        }
    }

    #[test]
    fn shuffled_rounds_cover_every_node_without_repeating_at_the_boundary() {
        let mut state = RotationState::default();
        let candidates: Arc<[String]> = (0..16).map(|index| index.to_string()).collect();
        let policy = RotationPolicy {
            strategy: SelectionStrategy::ShuffledRoundRobin,
            ..RotationPolicy::default()
        };
        let now = Instant::now();
        let mut previous = None;
        for _ in 0..20 {
            let mut visited = std::collections::HashSet::new();
            for _ in 0..candidates.len() {
                let selected = state.select_shared(&candidates, &policy, now).unwrap();
                assert_ne!(previous.as_ref(), Some(&selected));
                assert!(visited.insert(selected.clone()));
                previous = Some(selected);
            }
            assert_eq!(visited.len(), candidates.len());
        }
    }

    #[test]
    fn shuffled_rounds_apply_the_request_budget_to_each_visited_node() {
        let mut state = RotationState::default();
        let candidates: Arc<[String]> = nodes(&["a", "b", "c", "d"]).into();
        let policy = RotationPolicy {
            strategy: SelectionStrategy::ShuffledRoundRobin,
            ..every(3)
        };
        let now = Instant::now();
        for _ in 0..5 {
            let mut visited = std::collections::HashSet::new();
            for _ in 0..candidates.len() {
                let selected = state.select_shared(&candidates, &policy, now).unwrap();
                assert!(visited.insert(selected.clone()));
                for _ in 0..2 {
                    assert_eq!(
                        state.select_shared(&candidates, &policy, now),
                        Some(selected.clone())
                    );
                }
            }
        }
    }

    #[test]
    fn shuffled_updates_finish_surviving_nodes_before_adding_new_nodes() {
        let mut state = RotationState::default();
        let candidates: Arc<[String]> = nodes(&["a", "b", "c", "d", "e"]).into();
        let policy = RotationPolicy {
            strategy: SelectionStrategy::ShuffledRoundRobin,
            ..RotationPolicy::default()
        };
        let now = Instant::now();
        let first = state.select_shared(&candidates, &policy, now).unwrap();
        let removed = candidates[*state.shuffled_remaining.last().unwrap()].clone();
        let expected: std::collections::HashSet<_> = candidates
            .iter()
            .filter(|id| **id != first && **id != removed)
            .cloned()
            .collect();
        // Also reverse the list to verify the remaining round follows node
        // identities, rather than reusing indices from the old snapshot.
        let updated: Arc<[String]> = candidates
            .iter()
            .rev()
            .filter(|id| **id != removed)
            .cloned()
            .chain(["new".to_owned()])
            .collect();
        let actual: std::collections::HashSet<_> = (0..expected.len())
            .map(|_| state.select_shared(&updated, &policy, now).unwrap())
            .collect();
        assert_eq!(actual, expected);

        let next_round: std::collections::HashSet<_> = (0..updated.len())
            .map(|_| state.select_shared(&updated, &policy, now).unwrap())
            .collect();
        assert_eq!(next_round, updated.iter().cloned().collect());
        assert!(next_round.contains("new"));
        assert!(!next_round.contains(&removed));
    }

    #[test]
    fn seeded_sessions_start_at_the_assigned_node_without_spending_quota() {
        let candidates: Arc<[String]> = nodes(&["a", "b", "c", "d"]).into();
        let now = Instant::now();
        let mut state = RotationState::default();
        assert!(!state.has_current());
        state.seed("c", now);
        assert!(state.has_current());
        for _ in 0..3 {
            assert_eq!(
                state.select_shared(&candidates, &every(3), now).as_deref(),
                Some("c")
            );
        }
        assert_eq!(
            state.select_shared(&candidates, &every(3), now).as_deref(),
            Some("d")
        );

        let policy = RotationPolicy {
            strategy: SelectionStrategy::ShuffledRoundRobin,
            ..RotationPolicy::default()
        };
        state.seed("b", now);
        assert_eq!(
            state.select_shared(&candidates, &policy, now).as_deref(),
            Some("b")
        );
        let remaining: std::collections::HashSet<_> = (0..3)
            .map(|_| state.select_shared(&candidates, &policy, now).unwrap())
            .collect();
        assert_eq!(remaining, nodes(&["a", "c", "d"]).into_iter().collect());
    }

    #[test]
    fn large_shared_snapshots_are_indexed_only_when_the_allocation_changes() {
        let candidates: Arc<[String]> = (0..4_096).map(|index| index.to_string()).collect();
        let now = Instant::now();
        for strategy in [
            SelectionStrategy::RoundRobin,
            SelectionStrategy::Random,
            SelectionStrategy::ShuffledRoundRobin,
        ] {
            let mut state = RotationState::default();
            let policy = RotationPolicy {
                strategy,
                ..every(7)
            };
            for _ in 0..20_000 {
                state.select_shared(&candidates, &policy, now).unwrap();
            }
            assert_eq!(state.snapshot_rebuilds, 1);
            assert!(Arc::ptr_eq(state.candidates.as_ref().unwrap(), &candidates));

            let new_snapshot: Arc<[String]> = candidates.to_vec().into();
            state.select_shared(&new_snapshot, &policy, now).unwrap();
            assert_eq!(state.snapshot_rebuilds, 2);
            assert!(Arc::ptr_eq(
                state.candidates.as_ref().unwrap(),
                &new_snapshot
            ));
        }
    }

    #[test]
    fn manual_rotation_preserves_the_round_robin_position() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c"]);
        let now = Instant::now();
        let policy = every(0); // Both limits disabled.
        for expected in ["a", "b", "c", "a"] {
            for _ in 0..30 {
                assert_eq!(select(&mut state, &nodes, &policy, now), expected);
            }
            state.force_rotate();
        }
    }

    #[test]
    fn invalidation_only_forces_rotation_for_the_current_node() {
        let mut state = RotationState::default();
        let nodes = nodes(&["a", "b", "c"]);
        let now = Instant::now();
        let policy = every(20);
        assert_eq!(select(&mut state, &nodes, &policy, now), "a");
        state.invalidate("b");
        assert_eq!(select(&mut state, &nodes, &policy, now), "a");
        state.invalidate("a");
        assert_eq!(select(&mut state, &nodes, &policy, now), "b");
    }

    #[test]
    fn removal_continues_from_the_removed_nodes_successor_and_recovery_rejoins() {
        let mut state = RotationState::default();
        let all = nodes(&["a", "b", "c", "d"]);
        let now = Instant::now();
        let policy = every(1);
        assert_eq!(select(&mut state, &all, &policy, now), "a");
        assert_eq!(select(&mut state, &all, &policy, now), "b");
        state.invalidate("b");
        assert_eq!(
            select(&mut state, &nodes(&["a", "c", "d"]), &policy, now),
            "c"
        );
        assert_eq!(select(&mut state, &all, &policy, now), "d");
        assert_eq!(select(&mut state, &all, &policy, now), "a");
        assert_eq!(select(&mut state, &all, &policy, now), "b");
    }

    #[test]
    fn removal_skips_missing_successors_and_wraps() {
        let mut state = RotationState::default();
        let all = nodes(&["a", "b", "c", "d"]);
        let now = Instant::now();
        let policy = every(1);
        assert_eq!(select(&mut state, &all, &policy, now), "a");
        assert_eq!(select(&mut state, &all, &policy, now), "b");
        assert_eq!(select(&mut state, &nodes(&["a", "d"]), &policy, now), "d");
        assert_eq!(select(&mut state, &nodes(&["a", "new"]), &policy, now), "a");
        assert_eq!(select(&mut state, &nodes(&["new"]), &policy, now), "new");
    }

    #[test]
    fn list_changes_do_not_reset_a_valid_nodes_request_allowance() {
        let mut state = RotationState::default();
        let now = Instant::now();
        let policy = every(3);
        assert_eq!(select(&mut state, &nodes(&["a", "b"]), &policy, now), "a");
        assert_eq!(
            select(&mut state, &nodes(&["b", "a", "c"]), &policy, now),
            "a"
        );
        assert_eq!(select(&mut state, &nodes(&["a", "c"]), &policy, now), "a");
        assert_eq!(select(&mut state, &nodes(&["a", "c"]), &policy, now), "c");
    }

    #[test]
    fn empty_list_preserves_position_until_nodes_return() {
        let mut state = RotationState::default();
        let all = nodes(&["a", "b", "c"]);
        let now = Instant::now();
        let policy = every(20);
        assert_eq!(state.select(&[], &policy, now), None);
        assert_eq!(select(&mut state, &all, &policy, now), "a");
        state.force_rotate();
        assert_eq!(select(&mut state, &all, &policy, now), "b");
        assert_eq!(state.select(&[], &policy, now), None);
        assert_eq!(select(&mut state, &all, &policy, now), "c");
    }

    #[test]
    fn a_single_node_remains_usable_with_every_strategy() {
        for strategy in [
            SelectionStrategy::RoundRobin,
            SelectionStrategy::Random,
            SelectionStrategy::ShuffledRoundRobin,
        ] {
            let mut state = RotationState::default();
            let nodes = nodes(&["only"]);
            let now = Instant::now();
            let policy = RotationPolicy {
                strategy,
                ..every(1)
            };
            for _ in 0..50 {
                assert_eq!(select(&mut state, &nodes, &policy, now), "only");
            }
            state.invalidate("only");
            assert_eq!(select(&mut state, &nodes, &policy, now), "only");
            state.force_rotate();
            assert_eq!(select(&mut state, &nodes, &policy, now), "only");
        }
    }

    #[test]
    fn count_saturates_without_limits_and_rotates_at_the_maximum_limit() {
        let now = Instant::now();
        let nodes = nodes(&["a", "b"]);
        let mut state = RotationState::default();
        assert_eq!(select(&mut state, &nodes, &every(0), now), "a");
        state.requests = u64::MAX - 1;
        assert_eq!(select(&mut state, &nodes, &every(u64::MAX), now), "a");
        assert_eq!(select(&mut state, &nodes, &every(u64::MAX), now), "b");
        state.requests = u64::MAX;
        assert_eq!(select(&mut state, &nodes, &every(0), now), "b");
        assert_eq!(state.requests, u64::MAX);
    }

    #[test]
    fn configuration_rejects_zero_time_but_allows_manual_only_rotation() {
        assert!(
            RotationPolicy {
                max_age: Some(Duration::ZERO),
                ..every(1)
            }
            .validate()
            .is_err()
        );
        assert!(every(0).validate().is_ok());
        assert!(RotationPolicy::default().validate().is_ok());
    }

    #[test]
    fn constructors_select_independent_count_time_and_sticky_policies() {
        let count = NonZeroU64::new(20).unwrap();
        let counted = RotationPolicy::every(count);
        assert_eq!(counted.requests_per_proxy, Some(count));
        assert_eq!(counted.max_age, None);

        let sticky = RotationPolicy::sticky();
        assert_eq!(sticky.requests_per_proxy, None);
        assert_eq!(sticky.max_age, None);

        let duration = Duration::from_secs(30);
        let timed = RotationPolicy::for_duration(duration).unwrap();
        assert_eq!(timed.requests_per_proxy, None);
        assert_eq!(timed.max_age, Some(duration));
        assert!(RotationPolicy::for_duration(Duration::ZERO).is_err());
    }

    #[test]
    fn concurrent_acquisitions_receive_exact_serialized_batches() {
        let state = Arc::new(Mutex::new((RotationState::default(), Vec::new())));
        let nodes = Arc::new(nodes(&["a", "b", "c", "d"]));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let state = Arc::clone(&state);
            let nodes = Arc::clone(&nodes);
            threads.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut locked = state.lock().unwrap();
                    let selected = select(&mut locked.0, &nodes, &every(20), Instant::now());
                    locked.1.push(selected);
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let locked = state.lock().unwrap();
        let counts = locked.1.iter().fold(HashMap::new(), |mut counts, id| {
            *counts.entry(id.as_str()).or_insert(0) += 1;
            counts
        });
        for id in nodes.iter() {
            assert_eq!(counts[id.as_str()], 200);
        }
        for (index, batch) in locked.1.chunks_exact(20).enumerate() {
            assert!(batch.iter().all(|id| id == &nodes[index % nodes.len()]));
        }
    }
}
