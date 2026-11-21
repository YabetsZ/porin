//! Token bucket, implemented as GCRA (the generic cell rate algorithm).
//!
//! A bucket holds up to `burst` tokens and refills at `count` per period.
//! Instead of a token count and a refill time, GCRA stores one timestamp, the
//! theoretical arrival time (TAT): the moment the bucket would be full again.
//!
//! Times are whole nanoseconds, so the time to refill one token (the
//! emission interval) is rounded up once, and everything is built from it: a
//! cost of `n` takes `n` intervals and a full bucket is `burst` intervals.
//! The bucket then refills a fraction of a nanosecond per token slower than
//! the exact rate, never faster, and a full burst fits however it is split
//! into hits. Rates finer than one token per nanosecond are capped there.

use super::{Algorithm, Check, Retry, Stats, nanos};
use crate::{Rate, Timestamp};

#[derive(Debug)]
pub(crate) struct Gcra;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GcraState {
    /// Nanoseconds. A TAT at or before `now` means a full bucket.
    tat: u64,
}

impl Algorithm for Gcra {
    type State = GcraState;
    type Commit = GcraState;

    fn check(rate: Rate, state: &GcraState, now: Timestamp, cost: u32) -> Check<GcraState> {
        let now = now.as_nanos();
        let bucket = Bucket::new(rate);
        // If the clock went backwards, the stored TAT is kept: the bucket
        // looks emptier than it is, which under-admits.
        let tat = state.tat.max(now);

        if cost > bucket.burst {
            return Check::Denied {
                stats: bucket.stats(now, tat),
                retry: Retry::Never,
            };
        }
        let new_tat = tat.saturating_add(bucket.refill_time(cost));
        let allow_at = new_tat.saturating_sub(bucket.tolerance);
        if now >= allow_at {
            Check::Allowed {
                commit: GcraState { tat: new_tat },
                stats: bucket.stats(now, new_tat),
            }
        } else {
            Check::Denied {
                stats: bucket.stats(now, tat),
                retry: Retry::After(nanos(allow_at.saturating_sub(now))),
            }
        }
    }

    fn commit(state: &mut GcraState, commit: GcraState) {
        *state = commit;
    }
}

struct Bucket {
    /// Nanoseconds to refill one token, rounded up. At least 1.
    interval: u64,
    burst: u32,
    /// How far the TAT may run ahead of now: the refill time of a full bucket.
    tolerance: u64,
}

impl Bucket {
    fn new(rate: Rate) -> Self {
        let interval = rate
            .period_nanos()
            .div_ceil(u64::from(rate.count().get()))
            .max(1);
        let burst = rate.burst().get();
        Self {
            interval,
            burst,
            tolerance: interval.saturating_mul(u64::from(burst)),
        }
    }

    fn refill_time(&self, tokens: u32) -> u64 {
        self.interval.saturating_mul(u64::from(tokens))
    }

    fn stats(&self, now: u64, tat: u64) -> Stats {
        let spare = now.saturating_add(self.tolerance).saturating_sub(tat);
        let remaining = spare
            .checked_div(self.interval)
            .and_then(|tokens| u32::try_from(tokens).ok())
            .unwrap_or(0)
            .min(self.burst);
        Stats {
            remaining,
            reset_after: nanos(tat.saturating_sub(now)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use super::*;

    fn rate(count: u32, burst: u32) -> Rate {
        Rate::per_second(NonZeroU32::new(count).unwrap())
            .with_burst(NonZeroU32::new(burst).unwrap())
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::ZERO.saturating_add(Duration::from_millis(millis))
    }

    fn hit(rate: Rate, state: &mut GcraState, now: Timestamp, cost: u32) -> Check<GcraState> {
        let check = Gcra::check(rate, state, now, cost);
        if let Check::Allowed { commit, .. } = check {
            Gcra::commit(state, commit);
        }
        check
    }

    #[test]
    fn full_burst_then_one_token_per_interval() {
        let rate = rate(10, 10);
        let mut state = GcraState::default();
        for n in 0..10 {
            assert!(
                matches!(hit(rate, &mut state, at(1_000), 1), Check::Allowed { .. }),
                "hit {n} of the burst"
            );
        }
        assert_eq!(
            hit(rate, &mut state, at(1_000), 1),
            Check::Denied {
                stats: Stats {
                    remaining: 0,
                    reset_after: Duration::from_secs(1)
                },
                retry: Retry::After(Duration::from_millis(100)),
            },
            "11th hit waits one emission interval"
        );
        assert!(matches!(
            hit(rate, &mut state, at(1_100), 1),
            Check::Allowed { .. }
        ));
    }

    #[test]
    fn idle_key_refills_to_burst_not_beyond() {
        let rate = rate(10, 5);
        let mut state = GcraState::default();
        assert!(matches!(
            hit(rate, &mut state, at(0), 5),
            Check::Allowed { .. }
        ));
        let Check::Allowed { stats, .. } = Gcra::check(rate, &state, at(60_000), 0) else {
            panic!("a peek is always allowed");
        };
        assert_eq!(stats.remaining, 5);
        assert_eq!(stats.reset_after, Duration::ZERO);
    }

    #[test]
    fn cost_above_burst_is_never_allowed() {
        let check = Gcra::check(rate(10, 3), &GcraState::default(), at(0), 4);
        assert!(matches!(
            check,
            Check::Denied {
                retry: Retry::Never,
                ..
            }
        ));
    }

    #[test]
    fn non_integer_interval_still_allows_the_full_burst() {
        // 3 per second: 333.33... ms per token, rounded up.
        let rate = rate(3, 2);
        let check = Gcra::check(rate, &GcraState::default(), at(0), 2);
        assert!(matches!(check, Check::Allowed { .. }), "{check:?}");
    }

    #[test]
    fn full_burst_fits_as_single_hits_when_the_interval_rounds() {
        // 9 per second: 111,111,111.1 ns per token. Rounding each hit up
        // separately used to make the ninth unit hit miss the burst.
        let rate = rate(9, 9);
        let mut state = GcraState::default();
        for n in 0..9 {
            assert!(
                matches!(hit(rate, &mut state, at(0), 1), Check::Allowed { .. }),
                "unit hit {n} of a burst of 9"
            );
        }
    }

    #[test]
    fn clock_going_backwards_under_admits() {
        let rate = rate(1, 1);
        let mut state = GcraState::default();
        assert!(matches!(
            hit(rate, &mut state, at(10_000), 1),
            Check::Allowed { .. }
        ));
        // The clock jumps back 5 s. The TAT (11 s) is kept, so the next token
        // is 6 s away on the new reading: slower than real time, never faster.
        let check = Gcra::check(rate, &state, at(5_000), 1);
        assert_eq!(
            check,
            Check::Denied {
                stats: Stats {
                    remaining: 0,
                    reset_after: Duration::from_secs(6)
                },
                retry: Retry::After(Duration::from_secs(6)),
            }
        );
    }
}
