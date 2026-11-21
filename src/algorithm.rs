//! The rate limiting math, as pure functions.
//!
//! Each algorithm checks one [`Rate`] against one key's state at a given
//! time. Checking never mutates: [`Check::Allowed`] carries a commit that the
//! caller applies only after every rate in the quota has allowed the hit, so
//! a quota with several rates is all-or-nothing. Nothing here reads a clock,
//! does IO or takes a lock; stores own the state and the concurrency.

mod fixed_window;
mod gcra;
mod sliding_log;
mod sliding_window;
#[cfg(test)]
mod tests;

use std::time::Duration;

use crate::{Rate, Timestamp};

/// One rate limiting algorithm.
pub(crate) trait Algorithm {
    /// Per-key, per-rate state. `Default` is a key that has never been hit.
    type State: Default;
    /// What to write into the state once a hit is allowed.
    type Commit;

    /// Checks whether `cost` units may be spent at `now`, without changing
    /// anything. A cost of 0 asks for the current stats.
    fn check(rate: Rate, state: &Self::State, now: Timestamp, cost: u32) -> Check<Self::Commit>;

    /// Records an allowed hit. `commit` must come from [`Algorithm::check`]
    /// on this same, unchanged state.
    fn commit(state: &mut Self::State, commit: Self::Commit);
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

fn nanos(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}
