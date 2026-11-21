//! The rate limiting math, as pure functions.
//!
//! Each algorithm checks one [`Rate`] against one key's state at a given
//! time. Checking never mutates: [`Check::Allowed`] carries a commit that the
//! caller applies only after every rate in the quota has allowed the hit, so
//! a quota with several rates is all-or-nothing. Nothing here reads a clock,
//! does IO or takes a lock; stores own the state and the concurrency.

pub(crate) mod fixed_window;
pub(crate) mod gcra;
pub(crate) mod sliding_log;
pub(crate) mod sliding_window;
#[cfg(test)]
mod tests;

use std::time::Duration;

use crate::{Quota, Rate, Timestamp};

/// One rate limiting algorithm.
pub(crate) trait Algorithm {
    /// Per-key, per-rate state. `Default` is a key that has never been hit.
    type State: Default;
    /// What to write into the state once a hit is allowed.
    type Commit;

    /// Checks whether `cost` units may be spent at `now`, without changing
    /// anything. A cost of 0 asks for the current stats. Times are in
    /// `spec`'s ticks.
    fn check_ticks(spec: Spec, state: &Self::State, now: u64, cost: u32) -> Check<Self::Commit>;

    /// [`Algorithm::check_ticks`] in nanoseconds, the unit of [`Timestamp`].
    fn check(rate: Rate, state: &Self::State, now: Timestamp, cost: u32) -> Check<Self::Commit> {
        Self::check_ticks(Spec::new(rate, Unit::Nanos), state, now.as_nanos(), cost)
    }

    /// Records an allowed hit. `commit` must come from [`Algorithm::check`]
    /// on this same, unchanged state.
    fn commit(state: &mut Self::State, commit: Self::Commit);

    /// The time from which `state` decides exactly like a never-hit state,
    /// for every later time, if nothing more is recorded. Taken from what
    /// the state stores, never from the caller's clock, so a hit recorded
    /// with an older timestamp can't make a key look idle early. In
    /// `spec`'s ticks.
    fn idle_at_ticks(spec: Spec, state: &Self::State) -> u64;

    /// [`Algorithm::idle_at_ticks`] in nanoseconds.
    fn idle_at(rate: Rate, state: &Self::State) -> u64 {
        Self::idle_at_ticks(Spec::new(rate, Unit::Nanos), state)
    }
}

/// The unit an algorithm counts time in. The library runs in nanoseconds;
/// the Redis scripts run in microseconds, because Lua numbers are doubles,
/// and their tests compare against this same code run in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unit {
    Nanos,
    #[cfg_attr(
        feature = "memory",
        expect(dead_code, reason = "for the Redis store, which counts in microseconds")
    )]
    Micros,
}

/// One rate in a unit's ticks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Spec {
    pub(crate) period: u64,
    pub(crate) count: u32,
    pub(crate) burst: u32,
    unit: Unit,
}

impl Spec {
    pub(crate) const fn new(rate: Rate, unit: Unit) -> Self {
        let nanos = rate.period_nanos();
        Self {
            period: match unit {
                Unit::Nanos => nanos,
                // Periods are whole milliseconds, so this is exact.
                Unit::Micros => nanos / 1_000,
            },
            count: rate.count().get(),
            burst: rate.burst().get(),
            unit,
        }
    }

    /// `ticks` of this spec's unit as a duration.
    pub(crate) const fn duration(self, ticks: u64) -> Duration {
        match self.unit {
            Unit::Nanos => Duration::from_nanos(ticks),
            Unit::Micros => Duration::from_micros(ticks),
        }
    }
}

/// The answer for one rate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Check<C> {
    /// The hit fits. Apply `commit` to record it.
    Allowed { commit: C, stats: Stats },
    /// The hit doesn't fit and nothing should be recorded.
    Denied { stats: Stats, retry: Retry },
}

/// What a key has left, for `RateLimit-*` headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stats {
    /// Units that can still be spent now, counting this hit if it was allowed.
    pub(crate) remaining: u32,
    /// Time until the key is back to its never-hit state, if nothing more is spent.
    pub(crate) reset_after: Duration,
}

/// When a denied hit could succeed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Retry {
    /// The same hit would be allowed after this long, if nothing else is spent meanwhile.
    After(Duration),
    /// The cost is larger than the rate can ever allow at once.
    Never,
}

/// The answer for a whole quota.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Allowed(Stats),
    Denied { stats: Stats, retry: Retry },
}

/// Checks `cost` against every rate in `quota` and records it in all of
/// them only if all of them allow it. `states[i]` belongs to
/// `quota.rates()[i]`. Rates past `N` are not checked, so callers must keep
/// `quota.rates().len() <= N`.
///
/// The combined stats are the tightest across rates: the smallest
/// `remaining` and the largest `reset_after`. A denial waits for the slowest
/// denying rate; with no hits in between, every rate only relaxes over time,
/// so all of them allow by then.
pub(crate) fn hit_quota<A: Algorithm, const N: usize>(
    quota: &Quota,
    states: &mut [A::State; N],
    now: Timestamp,
    cost: u32,
) -> Outcome {
    let (outcome, commits) = check_quota::<A, N>(quota, states, now, cost);
    // A peek records nothing. Its commit would at most move the state to
    // `now`, which changes nothing going forward but makes a never-hit key
    // look hit if the clock then reads earlier.
    if let (Outcome::Allowed(_), 1..) = (outcome, cost) {
        for (state, commit) in states.iter_mut().zip(commits) {
            if let Some(commit) = commit {
                A::commit(state, commit);
            }
        }
    }
    outcome
}

/// What [`hit_quota`] would answer, without recording anything. On
/// [`Outcome::Allowed`], the commits record the hit when applied in order.
pub(crate) fn check_quota<A: Algorithm, const N: usize>(
    quota: &Quota,
    states: &[A::State; N],
    now: Timestamp,
    cost: u32,
) -> (Outcome, [Option<A::Commit>; N]) {
    check_quota_in::<A, N>(quota, Unit::Nanos, states, now.as_nanos(), cost)
}

/// [`check_quota`] with time in `unit`'s ticks.
pub(crate) fn check_quota_in<A: Algorithm, const N: usize>(
    quota: &Quota,
    unit: Unit,
    states: &[A::State; N],
    now: u64,
    cost: u32,
) -> (Outcome, [Option<A::Commit>; N]) {
    let rates = quota.rates();
    let mut commits: [Option<A::Commit>; N] = std::array::from_fn(|_| None);
    let mut summary = Stats::UNLIMITED;
    let mut retry = None;
    for ((rate, state), slot) in rates.iter().zip(states.iter()).zip(commits.iter_mut()) {
        match A::check_ticks(Spec::new(*rate, unit), state, now, cost) {
            Check::Allowed {
                commit,
                stats: rate_stats,
            } => {
                *slot = Some(commit);
                summary = summary.tightest(rate_stats);
            }
            Check::Denied {
                retry: rate_retry, ..
            } => {
                retry = Some(retry.map_or(rate_retry, |earlier: Retry| earlier.later(rate_retry)));
            }
        }
    }

    let Some(retry) = retry else {
        return (Outcome::Allowed(summary), commits);
    };
    // Allowed rates reported stats as if the hit were recorded. It isn't,
    // so report what every rate has without it.
    let unchanged =
        rates
            .iter()
            .zip(states.iter())
            .fold(Stats::UNLIMITED, |summary, (rate, state)| {
                summary.tightest(peek_stats::<A>(Spec::new(*rate, unit), state, now))
            });
    let denied = Outcome::Denied {
        stats: unchanged,
        retry,
    };
    (denied, std::array::from_fn(|_| None))
}

fn peek_stats<A: Algorithm>(spec: Spec, state: &A::State, now: u64) -> Stats {
    match A::check_ticks(spec, state, now, 0) {
        Check::Allowed { stats, .. } | Check::Denied { stats, .. } => stats,
    }
}

impl Stats {
    /// The identity for [`Stats::tightest`].
    const UNLIMITED: Self = Self {
        remaining: u32::MAX,
        reset_after: Duration::ZERO,
    };

    fn tightest(self, other: Self) -> Self {
        Self {
            remaining: self.remaining.min(other.remaining),
            reset_after: self.reset_after.max(other.reset_after),
        }
    }
}

impl Retry {
    /// Whichever comes later; `Never` is later than any wait.
    fn later(self, other: Self) -> Self {
        match (self, other) {
            (Self::After(a), Self::After(b)) => Self::After(a.max(b)),
            (Self::Never, _) | (_, Self::Never) => Self::Never,
        }
    }
}

/// `⌊a × b / divisor⌋`, saturating at `u64::MAX`. A zero divisor saturates,
/// which is the safe direction for every caller.
fn mul_div_floor(a: u64, b: u64, divisor: u64) -> u64 {
    let product = u128::from(a).saturating_mul(u128::from(b));
    product
        .checked_div(u128::from(divisor))
        .map_or(u64::MAX, saturate)
}

/// `⌈a × b / divisor⌉`, saturating at `u64::MAX`, with a zero divisor saturating.
fn mul_div_ceil(a: u64, b: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return u64::MAX;
    }
    let product = u128::from(a).saturating_mul(u128::from(b));
    saturate(product.div_ceil(u128::from(divisor)))
}

fn saturate(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The start of the period-aligned window that contains `now`.
fn window_start(now: u64, period: u64) -> u64 {
    now.saturating_sub(now.checked_rem(period).unwrap_or(0))
}
