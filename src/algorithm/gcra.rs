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

use super::{Algorithm, Check, Retry, Spec, Stats};

/// Token bucket: allows bursts up to the rate's burst size, then refills
/// steadily at the rate. The default algorithm.
///
/// Implemented as GCRA, which keeps one timestamp per key and rate. Also
/// known as a leaky bucket used as a meter; see [`LeakyBucket`](crate::LeakyBucket).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TokenBucket;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GcraState {
    /// Nanoseconds. A TAT at or before `now` means a full bucket.
    tat: u64,
}

impl GcraState {
    /// The state as one word, so a store can keep it in an atomic.
    pub(crate) const fn to_word(self) -> u64 {
        self.tat
    }

    pub(crate) const fn from_word(word: u64) -> Self {
        Self { tat: word }
    }

    /// From this time on the bucket is full, which is the never-hit state,
    /// until it is hit again.
    pub(crate) const fn idle_at(self) -> u64 {
        self.tat
    }
}

impl Algorithm for TokenBucket {
    type State = GcraState;
    type Commit = GcraState;

    fn check_ticks(spec: Spec, state: &GcraState, now: u64, cost: u32) -> Check<GcraState> {
        let bucket = Bucket::new(spec);
        // If the clock went backwards, the stored TAT is kept: the bucket
        // looks emptier than it is, which under-admits.
        let tat = state.tat.max(now);

        if cost > bucket.burst {
            return Check::Denied {
                stats: bucket.stats(now, tat),
                retry: Retry::Never,
            };
        }
        // In u128: a bucket that takes centuries to refill overflows u64
        // nanoseconds, and saturating there would allow every hit.
        let new_tat = u128::from(tat).saturating_add(bucket.refill_time(cost));
        let allow_at = new_tat.saturating_sub(bucket.tolerance);
        let Ok(stored_tat) = u64::try_from(new_tat) else {
            // The new TAT doesn't fit, and it only grows as time passes.
            return Check::Denied {
                stats: bucket.stats(now, tat),
                retry: Retry::Never,
            };
        };
        if u128::from(now) >= allow_at {
            Check::Allowed {
                commit: GcraState { tat: stored_tat },
                stats: bucket.stats(now, stored_tat),
            }
        } else {
            let wait = allow_at.saturating_sub(u128::from(now));
            Check::Denied {
                stats: bucket.stats(now, tat),
                retry: Retry::After(spec.duration(u64::try_from(wait).unwrap_or(u64::MAX))),
            }
        }
    }

    fn commit(state: &mut GcraState, commit: GcraState) {
        *state = commit;
    }

    fn idle_at_ticks(_: Spec, state: &GcraState) -> u64 {
        state.idle_at()
    }
}

struct Bucket {
    spec: Spec,
    /// Ticks to refill one token, rounded up. At least 1.
    interval: u64,
    burst: u32,
    /// How far the TAT may run ahead of now: the refill time of a full
    /// bucket. Can exceed `u64::MAX`.
    tolerance: u128,
}

impl Bucket {
    fn new(spec: Spec) -> Self {
        let interval = spec.period.div_ceil(u64::from(spec.count)).max(1);
        let burst = spec.burst;
        Self {
            spec,
            interval,
            burst,
            tolerance: u128::from(interval).saturating_mul(u128::from(burst)),
        }
    }

    fn refill_time(&self, tokens: u32) -> u128 {
        u128::from(self.interval).saturating_mul(u128::from(tokens))
    }

    fn stats(&self, now: u64, tat: u64) -> Stats {
        let spare = u128::from(now)
            .saturating_add(self.tolerance)
            .saturating_sub(u128::from(tat));
        let remaining = spare
            .checked_div(u128::from(self.interval))
            .and_then(|tokens| u32::try_from(tokens).ok())
            .unwrap_or(0)
            .min(self.burst);
        Stats {
            remaining,
            reset_after: self.spec.duration(tat.saturating_sub(now)),
            ..Stats::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Rate, Timestamp};
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
        let check = TokenBucket::check(rate, state, now, cost);
        if let Check::Allowed { commit, .. } = check {
            TokenBucket::commit(state, commit);
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
                stats: Stats::new(0, Duration::from_secs(1)),
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
        let Check::Allowed { stats, .. } = TokenBucket::check(rate, &state, at(60_000), 0) else {
            panic!("a peek is always allowed");
        };
        assert_eq!(stats.remaining, 5);
        assert_eq!(stats.reset_after, Duration::ZERO);
    }

    #[test]
    fn cost_above_burst_is_never_allowed() {
        let check = TokenBucket::check(rate(10, 3), &GcraState::default(), at(0), 4);
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
        let check = TokenBucket::check(rate, &GcraState::default(), at(0), 2);
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
    fn a_burst_too_long_to_refill_in_nanoseconds_still_limits() {
        // A full bucket takes 300,000 days to refill: more nanoseconds than
        // a u64 holds. Saturating arithmetic used to allow every hit.
        let rate = Rate::per_day(NonZeroU32::new(1).unwrap())
            .with_burst(NonZeroU32::new(300_000).unwrap());
        let mut state = GcraState::default();
        let allowed = (0..400_000)
            .filter(|_| matches!(hit(rate, &mut state, at(0), 1), Check::Allowed { .. }))
            .count();
        assert!(
            allowed <= 300_000,
            "{allowed} allowed from a burst of 300,000"
        );
        assert!(allowed > 0);
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
        let check = TokenBucket::check(rate, &state, at(5_000), 1);
        assert_eq!(
            check,
            Check::Denied {
                stats: Stats::new(0, Duration::from_secs(6)),
                retry: Retry::After(Duration::from_secs(6)),
            }
        );
    }
}
