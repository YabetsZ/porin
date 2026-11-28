//! Sliding window counter: two fixed-window counts, weighted.
//!
//! The estimate for the last period is the current window's count plus the
//! previous window's count scaled by how much of it still overlaps:
//!
//! ```text
//! estimate = ⌈previous × (period − elapsed) / period⌉ + current
//! ```
//!
//! It assumes the previous window's hits were spread evenly, so it can be
//! off when they weren't, but it needs only two counters per key. The
//! weighted part is rounded up, which errs towards denying.

use super::{
    Algorithm, Check, Retry, Spec, Stats, mul_div_ceil, mul_div_floor, saturate, until_end,
    window_start,
};

/// Sliding window counter: weights the previous window's count by how much
/// of it still overlaps the last period. Smooths fixed window's edge burst
/// with two counters per key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SlidingWindow;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SlidingWindowState {
    /// Nanoseconds, aligned to the period.
    start: u64,
    previous: u32,
    current: u32,
}

impl Algorithm for SlidingWindow {
    type State = SlidingWindowState;
    type Commit = SlidingWindowState;

    fn check_ticks(
        spec: Spec,
        state: &SlidingWindowState,
        now: u64,
        cost: u32,
    ) -> Check<SlidingWindowState> {
        let period = spec.period;
        let limit = spec.count;
        let window = roll(*state, window_start(now, period), period);

        // Time left in the current window. If the clock went backwards into
        // an earlier window, `now` is before `start`; treating it as the
        // window's beginning weights `previous` fully, which under-admits.
        let left = until_end(window.start, period, now).min(period);
        let estimate = weighted(window.previous, left, period).saturating_add(window.current);
        let reset_after = if window.current > 0 {
            spec.duration(left.saturating_add(period))
        } else if window.previous > 0 {
            spec.duration(left)
        } else {
            spec.duration(0)
        };
        let report = |estimate: u32| Stats {
            remaining: limit.saturating_sub(estimate),
            reset_after,
            ..Stats::default()
        };

        if cost > limit {
            return Check::Denied {
                stats: report(estimate),
                retry: Retry::Never,
            };
        }
        match estimate.checked_add(cost) {
            Some(total) if total <= limit => {
                let after = SlidingWindowState {
                    current: window.current.saturating_add(cost),
                    ..window
                };
                Check::Allowed {
                    commit: after,
                    stats: Stats {
                        remaining: limit.saturating_sub(total),
                        // A hit puts units in the current window.
                        reset_after: if cost > 0 {
                            spec.duration(left.saturating_add(period))
                        } else {
                            reset_after
                        },
                        ..Stats::default()
                    },
                }
            }
            _ => Check::Denied {
                stats: report(estimate),
                retry: Retry::After(spec.duration(retry_after(window, left, period, limit, cost))),
            },
        }
    }

    fn commit(state: &mut SlidingWindowState, commit: SlidingWindowState) {
        *state = commit;
    }

    /// The current window's count weighs on the estimate until a whole
    /// period after that window ends; the previous one's until the current
    /// window ends.
    fn idle_at_ticks(spec: Spec, state: &SlidingWindowState) -> u64 {
        let period = spec.period;
        if state.current > 0 {
            state.start.saturating_add(period.saturating_mul(2))
        } else if state.previous > 0 {
            state.start.saturating_add(period)
        } else {
            0
        }
    }
}

/// The state as seen from the window starting at `current_start`.
fn roll(state: SlidingWindowState, current_start: u64, period: u64) -> SlidingWindowState {
    if current_start <= state.start {
        state
    } else if state.start.checked_add(period) == Some(current_start) {
        SlidingWindowState {
            start: current_start,
            previous: state.current,
            current: 0,
        }
    } else {
        SlidingWindowState {
            start: current_start,
            previous: 0,
            current: 0,
        }
    }
}

/// `⌈count × left / period⌉`: the part of a window still overlapping.
fn weighted(count: u32, left: u64, period: u64) -> u32 {
    u32::try_from(mul_div_ceil(u64::from(count), left, period)).unwrap_or(u32::MAX)
}

/// Nanoseconds until a hit of `cost` fits, if nothing else is spent. The
/// caller has checked that it doesn't fit now and that `cost <= limit`.
fn retry_after(window: SlidingWindowState, left: u64, period: u64, limit: u32, cost: u32) -> u64 {
    // While still in this window, the previous window's weight shrinks. The
    // hit fits once ⌈previous × (left − wait) / period⌉ ≤ budget, that is
    // once left − wait ≤ ⌊budget × period / previous⌋.
    let budget = limit
        .checked_sub(window.current)
        .and_then(|room| room.checked_sub(cost));
    // A denied hit with room in this window implies previous > 0, so the
    // division succeeds whenever `budget` is `Some`.
    let overlap_allowed = budget.and_then(|budget| {
        u128::from(budget)
            .saturating_mul(u128::from(period))
            .checked_div(u128::from(window.previous))
            .map(saturate)
    });
    if let Some(overlap_allowed) = overlap_allowed {
        let wait = left.saturating_sub(overlap_allowed);
        // At `wait == left` this and the next-window case give the same
        // answer; `<` hands the boundary to the next-window case.
        if wait < left {
            return wait;
        }
    }
    // Otherwise wait for the next window, where today's count becomes the
    // previous count and decays the same way.
    if window.current == 0 {
        return left;
    }
    let budget = limit.saturating_sub(cost);
    let overlap_allowed = mul_div_floor(u64::from(budget), period, u64::from(window.current));
    left.saturating_add(period.saturating_sub(overlap_allowed))
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

    #[test]
    fn weights_the_previous_window_by_its_overlap() {
        // Cloudflare's example shape: 80 in the previous window, 50 in this
        // one, 70% elapsed. Estimate = ⌈80 × 0.3⌉ + 50 = 74.
        let rate = per_second(100);
        let state = SlidingWindowState {
            start: 1_000_000_000,
            previous: 80,
            current: 50,
        };
        let Check::Allowed { stats, .. } = SlidingWindow::check(rate, &state, at(1_700), 26) else {
            panic!("74 + 26 = 100 fits");
        };
        assert_eq!(stats.remaining, 0);
        assert!(matches!(
            SlidingWindow::check(rate, &state, at(1_700), 27),
            Check::Denied { .. }
        ));
    }

    #[test]
    fn previous_count_rolls_over_and_then_expires() {
        let rate = per_second(10);
        let state = SlidingWindowState {
            start: 0,
            previous: 0,
            current: 10,
        };
        // Right at the next window the old count weighs fully.
        assert!(matches!(
            SlidingWindow::check(rate, &state, at(1_000), 1),
            Check::Denied { .. }
        ));
        // Two windows later it no longer counts.
        assert!(matches!(
            SlidingWindow::check(rate, &state, at(2_000), 10),
            Check::Allowed { .. }
        ));
    }

    #[test]
    fn the_previous_window_still_weighs_in_the_last_window_before_the_largest_timestamp() {
        // The window holding u64::MAX ns starts 709,551,615 ns before it,
        // so at u64::MAX 290,448,385 ns of it are left and the previous
        // window's 2 units weigh ⌈2 × 0.29⌉ = 1. Saturating the window's end
        // at u64::MAX left nothing to weigh, allowing 2.
        let rate = per_second(2);
        let last_start = u64::MAX - 709_551_615;
        let state = SlidingWindowState {
            start: last_start - 1_000_000_000,
            previous: 0,
            current: 2,
        };
        let end = Timestamp::from_nanos(u64::MAX);
        assert!(matches!(
            SlidingWindow::check(rate, &state, end, 1),
            Check::Allowed { .. }
        ));
        assert!(matches!(
            SlidingWindow::check(rate, &state, end, 2),
            Check::Denied { .. }
        ));
    }

    #[test]
    fn retry_lands_when_the_overlap_has_decayed_enough() {
        let rate = per_second(10);
        let state = SlidingWindowState {
            start: 1_000_000_000,
            previous: 10,
            current: 0,
        };
        // At 1.0 s the estimate is 10. One unit fits once ⌈10 × left / 1 s⌉ ≤ 9,
        // that is after 100 ms.
        assert_eq!(
            SlidingWindow::check(rate, &state, at(1_000), 1),
            Check::Denied {
                stats: Stats::new(0, Duration::from_secs(1)),
                retry: Retry::After(Duration::from_millis(100)),
            }
        );
    }
}
