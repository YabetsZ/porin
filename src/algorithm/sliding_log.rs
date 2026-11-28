//! Sliding log: remember every allowed hit within the last period.
//!
//! Exact, with no boundary effects, but stores one entry per allowed hit, so
//! memory grows with the limit. Python's `limits` calls this the moving
//! window.

use std::collections::VecDeque;

use super::{Algorithm, Check, Retry, Spec, Stats, until_end};

/// Sliding log: remembers the time of every unit spent in the last period
/// and allows a hit only if the period holds room for it. Exact, but keeps
/// up to `count` entries per key. Some libraries call it a moving window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SlidingLog;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SlidingLogState {
    /// `(nanoseconds, cost)` of allowed hits, oldest first, times non-decreasing.
    entries: VecDeque<(u64, u32)>,
    /// Sum of all entries' costs, including any that have expired but are not pruned yet.
    total: u32,
}

#[derive(Debug)]
pub(crate) struct SlidingLogCommit {
    now: u64,
    cost: u32,
    /// How many entries at the front had expired when the hit was checked.
    expired: usize,
}

impl Algorithm for SlidingLog {
    type State = SlidingLogState;
    type Commit = SlidingLogCommit;

    fn check_ticks(
        spec: Spec,
        state: &SlidingLogState,
        now: u64,
        cost: u32,
    ) -> Check<SlidingLogCommit> {
        let period = spec.period;
        let limit = spec.count;

        // A hit at time t counts while now < t + period.
        let is_live = |time: u64| until_end(time, period, now) > 0;
        let expired = state
            .entries
            .iter()
            .take_while(|(time, _)| !is_live(*time))
            .count();
        let expired_cost = state
            .entries
            .iter()
            .take(expired)
            .fold(0_u32, |sum, (_, cost)| sum.saturating_add(*cost));
        let used = state.total.saturating_sub(expired_cost);
        let live = || state.entries.iter().skip(expired);
        let until_expiry = |time: u64| spec.duration(until_end(time, period, now));
        let reset_after = live()
            .last()
            .map_or(spec.duration(0), |(time, _)| until_expiry(*time));

        if cost > limit {
            return Check::Denied {
                stats: Stats {
                    remaining: limit.saturating_sub(used),
                    reset_after,
                    ..Stats::default()
                },
                retry: Retry::Never,
            };
        }
        match used.checked_add(cost) {
            Some(total) if total <= limit => Check::Allowed {
                commit: SlidingLogCommit { now, cost, expired },
                stats: Stats {
                    remaining: limit.saturating_sub(total),
                    reset_after: if cost > 0 {
                        spec.duration(period)
                    } else {
                        reset_after
                    },
                    ..Stats::default()
                },
            },
            _ => {
                // Wait until enough of the oldest live entries expire. In
                // u64, as used + cost can pass u32::MAX.
                let must_free = u64::from(used)
                    .saturating_add(u64::from(cost))
                    .saturating_sub(u64::from(limit));
                let mut freed = 0_u64;
                let mut retry = reset_after;
                for (time, entry_cost) in live() {
                    freed = freed.saturating_add(u64::from(*entry_cost));
                    if freed >= must_free {
                        retry = until_expiry(*time);
                        break;
                    }
                }
                Check::Denied {
                    stats: Stats {
                        remaining: limit.saturating_sub(used),
                        reset_after,
                        ..Stats::default()
                    },
                    retry: Retry::After(retry),
                }
            }
        }
    }

    fn commit(state: &mut SlidingLogState, commit: SlidingLogCommit) {
        for (_, cost) in state.entries.drain(..commit.expired) {
            state.total = state.total.saturating_sub(cost);
        }
        if commit.cost == 0 {
            return;
        }
        // Keep times non-decreasing if the clock went backwards. The entry
        // then expires later than it would, which under-admits.
        let time = state
            .entries
            .back()
            .map_or(commit.now, |(last, _)| commit.now.max(*last));
        state.entries.push_back((time, commit.cost));
        state.total = state.total.saturating_add(commit.cost);
    }

    /// The newest entry is the last to leave the window.
    fn idle_at_ticks(spec: Spec, state: &SlidingLogState) -> u64 {
        state
            .entries
            .back()
            .map_or(0, |(time, _)| time.saturating_add(spec.period))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Rate, Timestamp};
    use std::num::NonZeroU32;
    use std::time::Duration;

    use super::*;

    fn per_second(count: u32) -> Rate {
        Rate::per_second(NonZeroU32::new(count).unwrap())
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::ZERO.saturating_add(Duration::from_millis(millis))
    }

    fn hit(rate: Rate, state: &mut SlidingLogState, now: Timestamp, cost: u32) -> Check<()> {
        match SlidingLog::check(rate, state, now, cost) {
            Check::Allowed { commit, stats } => {
                SlidingLog::commit(state, commit);
                Check::Allowed { commit: (), stats }
            }
            Check::Denied { stats, retry } => Check::Denied { stats, retry },
        }
    }

    #[test]
    fn no_edge_burst_across_a_boundary() {
        let rate = per_second(3);
        let mut state = SlidingLogState::default();
        let admitted = [999, 999, 999, 1_000, 1_998, 1_999]
            .map(|ms| matches!(hit(rate, &mut state, at(ms), 1), Check::Allowed { .. }));
        assert_eq!(admitted, [true, true, true, false, false, true]);
    }

    #[test]
    fn retry_waits_for_enough_entries_to_expire() {
        let rate = per_second(4);
        let mut state = SlidingLogState::default();
        for ms in [0, 100, 200, 300] {
            assert!(matches!(
                hit(rate, &mut state, at(ms), 1),
                Check::Allowed { .. }
            ));
        }
        // Cost 3 needs the three oldest entries gone: the one from 200 ms.
        assert_eq!(
            SlidingLog::check(rate, &state, at(500), 3).retry(),
            Some(Retry::After(Duration::from_millis(700)))
        );
    }

    #[test]
    fn retry_counts_past_u32_max() {
        let rate = per_second(u32::MAX);
        let mut state = SlidingLogState::default();
        hit(rate, &mut state, at(0), u32::MAX - 10);
        hit(rate, &mut state, at(100), 10);
        // Cost u32::MAX needs both entries gone, though used + cost
        // overflows a u32.
        assert_eq!(
            SlidingLog::check(rate, &state, at(200), u32::MAX).retry(),
            Some(Retry::After(Duration::from_millis(900)))
        );
    }

    #[test]
    fn commit_prunes_expired_entries() {
        let rate = per_second(2);
        let mut state = SlidingLogState::default();
        hit(rate, &mut state, at(0), 2);
        hit(rate, &mut state, at(5_000), 1);
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.total, 1);
    }

    #[test]
    fn hits_in_the_last_period_before_the_largest_timestamp_still_count() {
        // A hit at t counts until t + period, which can pass u64::MAX.
        // Saturating there made such hits expire at once, allowing every hit.
        let rate = per_second(2);
        let mut state = SlidingLogState::default();
        let half_second_before_the_end = Timestamp::from_nanos(u64::MAX - 500_000_000);
        hit(rate, &mut state, half_second_before_the_end, 2);
        let end = Timestamp::from_nanos(u64::MAX);
        assert_eq!(
            SlidingLog::check(rate, &state, end, 1).retry(),
            Some(Retry::After(Duration::from_millis(500))),
            "both units count until half a second past the end"
        );
    }

    impl<C> Check<C> {
        fn retry(&self) -> Option<Retry> {
            match self {
                Check::Allowed { .. } => None,
                Check::Denied { retry, .. } => Some(*retry),
            }
        }
    }
}
