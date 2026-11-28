//! Fixed window: count hits in period-aligned windows.
//!
//! Cheapest of all, but a key can spend its limit at the end of one window
//! and again at the start of the next, so up to twice the limit can pass
//! within a short span around a boundary.

use super::{Algorithm, Check, Retry, Spec, Stats, until_end, window_start};

/// Fixed window: counts hits in windows aligned to the period, such as each
/// whole minute, and resets the count when a new window starts.
///
/// Cheapest to reason about, but a client can spend a full window at the end
/// of one window and another at the start of the next.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FixedWindow;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FixedWindowState {
    /// Nanoseconds, aligned to the period.
    start: u64,
    used: u32,
}

impl Algorithm for FixedWindow {
    type State = FixedWindowState;
    type Commit = FixedWindowState;

    fn check_ticks(
        spec: Spec,
        state: &FixedWindowState,
        now: u64,
        cost: u32,
    ) -> Check<FixedWindowState> {
        let period = spec.period;
        let limit = spec.count;

        // A later window starts empty. If the clock went backwards into an
        // earlier window, the stored window and its count are kept, which
        // under-admits.
        let current = window_start(now, period);
        let (start, used) = if current > state.start {
            (current, 0)
        } else {
            (state.start, state.used)
        };
        let window_end = spec.duration(until_end(start, period, now));
        let report = |used: u32| Stats {
            remaining: limit.saturating_sub(used),
            // Nothing spent in this window means nothing to reset.
            reset_after: if used == 0 {
                spec.duration(0)
            } else {
                window_end
            },
            ..Stats::default()
        };

        if cost > limit {
            return Check::Denied {
                stats: report(used),
                retry: Retry::Never,
            };
        }
        match used.checked_add(cost) {
            Some(total) if total <= limit => Check::Allowed {
                commit: FixedWindowState { start, used: total },
                stats: report(total),
            },
            _ => Check::Denied {
                stats: report(used),
                retry: Retry::After(window_end),
            },
        }
    }

    fn commit(state: &mut FixedWindowState, commit: FixedWindowState) {
        *state = commit;
    }

    fn idle_at_ticks(spec: Spec, state: &FixedWindowState) -> u64 {
        if state.used == 0 {
            0
        } else {
            state.start.saturating_add(spec.period)
        }
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

    fn hit(rate: Rate, state: &mut FixedWindowState, now: Timestamp, cost: u32) -> bool {
        match FixedWindow::check(rate, state, now, cost) {
            Check::Allowed { commit, .. } => {
                FixedWindow::commit(state, commit);
                true
            }
            Check::Denied { .. } => false,
        }
    }

    #[test]
    fn edge_burst_admits_twice_the_limit_across_a_boundary() {
        let rate = per_second(3);
        let mut state = FixedWindowState::default();
        let admitted =
            [999, 999, 999, 1_000, 1_000, 1_000, 1_000].map(|ms| hit(rate, &mut state, at(ms), 1));
        assert_eq!(admitted, [true, true, true, true, true, true, false]);
    }

    #[test]
    fn denial_waits_for_the_window_end() {
        let rate = per_second(1);
        let mut state = FixedWindowState::default();
        assert!(hit(rate, &mut state, at(1_250), 1));
        assert_eq!(
            FixedWindow::check(rate, &state, at(1_250), 1),
            Check::Denied {
                stats: Stats::new(0, Duration::from_millis(750)),
                retry: Retry::After(Duration::from_millis(750)),
            }
        );
    }

    #[test]
    fn the_last_window_before_the_largest_timestamp_reports_a_real_wait() {
        // The window holding u64::MAX ns starts 709,551,615 ns before it
        // and would end 290,448,385 ns after it. Saturating the end there
        // reported a wait of zero.
        let rate = per_second(1);
        let mut state = FixedWindowState::default();
        let end = Timestamp::from_nanos(u64::MAX);
        assert!(hit(rate, &mut state, end, 1));
        assert_eq!(
            FixedWindow::check(rate, &state, end, 1),
            Check::Denied {
                stats: Stats::new(0, Duration::from_nanos(290_448_385)),
                retry: Retry::After(Duration::from_nanos(290_448_385)),
            }
        );
    }

    #[test]
    fn clock_going_backwards_keeps_the_later_window() {
        let rate = per_second(2);
        let mut state = FixedWindowState::default();
        assert!(hit(rate, &mut state, at(5_100), 2));
        assert!(
            !hit(rate, &mut state, at(4_900), 1),
            "earlier window must not reset the count"
        );
    }
}
