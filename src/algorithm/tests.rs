//! Properties shared by every algorithm, plus an independent model per
//! algorithm that recomputes each decision from the full history of hits.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "test arithmetic on small generated values"
)]

use std::num::NonZeroU32;
use std::time::Duration;

use proptest::prelude::*;

use super::fixed_window::FixedWindow;
use super::gcra::TokenBucket;
use super::sliding_log::SlidingLog;
use super::sliding_window::SlidingWindow;
use super::{Algorithm, Check, Outcome, Retry, check_quota, hit_quota};
use crate::Quota;
use crate::quota::MAX_RATES;
use crate::{Rate, Timestamp};

#[derive(Clone, Copy, Debug)]
struct Event {
    at: u64,
    cost: u32,
    allowed: bool,
}

/// Runs `ops` (time advance in nanoseconds, cost) from `start`, checking the
/// shared properties at every step, and returns what happened.
fn simulate<A: Algorithm>(rate: Rate, capacity: u32, start: u64, ops: &[(u64, u32)]) -> Vec<Event>
where
    A::State: Clone,
{
    let mut state = A::State::default();
    let mut now = start;
    let mut events = Vec::with_capacity(ops.len());
    for &(advance, cost) in ops {
        now += advance;
        let at = Timestamp::from_nanos(now);
        let check = A::check(rate, &state, at, cost);
        if cost == 0 {
            assert!(
                matches!(check, Check::Allowed { .. }),
                "a peek is always allowed"
            );
        }
        let allowed = match check {
            Check::Allowed { commit, stats } => {
                A::commit(&mut state, commit);
                assert_remaining_is_exact::<A>(rate, &state, at, stats.remaining);
                assert_reset_is_exact::<A>(rate, &state, now, capacity, stats.reset_after);
                true
            }
            Check::Denied { retry, stats } => {
                assert_reset_is_exact::<A>(rate, &state, now, capacity, stats.reset_after);
                assert_eq!(
                    retry == Retry::Never,
                    cost > capacity,
                    "Never exactly when cost {cost} > capacity {capacity}"
                );
                if let Retry::After(wait) = retry {
                    assert_retry_is_exact::<A>(rate, &state, now, cost, wait);
                }
                false
            }
        };
        events.push(Event {
            at: now,
            cost,
            allowed,
        });
    }
    events
}

/// Exactly `remaining` more unit hits fit at the same instant.
fn assert_remaining_is_exact<A: Algorithm>(
    rate: Rate,
    state: &A::State,
    at: Timestamp,
    remaining: u32,
) where
    A::State: Clone,
{
    let mut probe = state.clone();
    for n in 0..remaining {
        match A::check(rate, &probe, at, 1) {
            Check::Allowed { commit, .. } => A::commit(&mut probe, commit),
            Check::Denied { .. } => panic!("unit hit {n} of {remaining} remaining was denied"),
        }
    }
    assert!(
        matches!(A::check(rate, &probe, at, 1), Check::Denied { .. }),
        "more than {remaining} unit hits fit"
    );
}

/// After exactly `reset_after` with no hits the key is back to full, and not
/// one nanosecond sooner.
fn assert_reset_is_exact<A: Algorithm>(
    rate: Rate,
    state: &A::State,
    now: u64,
    capacity: u32,
    reset_after: Duration,
) {
    let remaining_at = |nanos: u64| match A::check(rate, state, Timestamp::from_nanos(nanos), 0) {
        Check::Allowed { stats, .. } => stats.remaining,
        Check::Denied { .. } => panic!("a peek is always allowed"),
    };
    let reset_after = u64::try_from(reset_after.as_nanos()).unwrap();
    assert_eq!(
        remaining_at(now + reset_after),
        capacity,
        "not full after reset_after = {reset_after} ns"
    );
    if reset_after > 0 {
        assert!(
            remaining_at(now + reset_after - 1) < capacity,
            "already full 1 ns before reset_after = {reset_after} ns"
        );
    }
}

/// The hit is allowed after exactly `wait`, and not one nanosecond sooner.
fn assert_retry_is_exact<A: Algorithm>(
    rate: Rate,
    state: &A::State,
    now: u64,
    cost: u32,
    wait: Duration,
) {
    let wait = u64::try_from(wait.as_nanos()).unwrap();
    assert!(wait > 0, "a denied hit can't be allowed after zero wait");
    let then = Timestamp::from_nanos(now + wait);
    assert!(
        matches!(A::check(rate, state, then, cost), Check::Allowed { .. }),
        "still denied after retry_after = {wait} ns"
    );
    let just_before = Timestamp::from_nanos(now + wait - 1);
    assert!(
        matches!(
            A::check(rate, state, just_before, cost),
            Check::Denied { .. }
        ),
        "already allowed 1 ns before retry_after = {wait} ns"
    );
}

fn allowed_cost(events: &[Event], mut counts: impl FnMut(&Event) -> bool) -> u64 {
    events
        .iter()
        .filter(|event| event.allowed && counts(event))
        .map(|event| u64::from(event.cost))
        .sum()
}

fn rate(count: u32, period_ms: u64, burst: u32) -> Rate {
    Rate::new(
        NonZeroU32::new(count).unwrap(),
        Duration::from_millis(period_ms),
    )
    .unwrap()
    .with_burst(NonZeroU32::new(burst).unwrap())
}

/// Advances from zero up to 2.5 periods, in eighths, plus 0–2 ns of jitter
/// so exact boundaries and the instants next to them both occur.
fn ops(period_ms: u64, max_cost: u32) -> impl Strategy<Value = Vec<(u64, u32)>> {
    let period = period_ms * 1_000_000;
    let advance =
        (0..=20_u64, 0..=2_u64).prop_map(move |(eighths, jitter)| eighths * period / 8 + jitter);
    let cost = prop_oneof![4 => 1..=2_u32, 2 => 0..=max_cost + 1];
    prop::collection::vec((advance, cost), 1..60)
}

fn start(period_ms: u64) -> impl Strategy<Value = u64> {
    0..=3 * period_ms * 1_000_000
}

const PERIODS_MS: [u64; 4] = [1, 7, 1_000, 60_000];

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn fixed_window_matches_per_window_sums(
        (count, period_ms, start, ops) in (1..=12_u32, prop::sample::select(&PERIODS_MS[..]))
            .prop_flat_map(|(count, period_ms)| (Just(count), Just(period_ms), start(period_ms), ops(period_ms, count)))
    ) {
        let events = simulate::<FixedWindow>(rate(count, period_ms, count), count, start, &ops);
        let period = period_ms * 1_000_000;
        for (index, event) in events.iter().enumerate() {
            let window = event.at / period;
            let used = allowed_cost(&events[..index], |earlier| earlier.at / period == window);
            prop_assert_eq!(event.allowed, used + u64::from(event.cost) <= u64::from(count), "{:?}", event);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn sliding_log_matches_the_last_period(
        (count, period_ms, start, ops) in (1..=12_u32, prop::sample::select(&PERIODS_MS[..]))
            .prop_flat_map(|(count, period_ms)| (Just(count), Just(period_ms), start(period_ms), ops(period_ms, count)))
    ) {
        let events = simulate::<SlidingLog>(rate(count, period_ms, count), count, start, &ops);
        let period = period_ms * 1_000_000;
        for (index, event) in events.iter().enumerate() {
            let used = allowed_cost(&events[..index], |earlier| earlier.at + period > event.at);
            prop_assert_eq!(event.allowed, used + u64::from(event.cost) <= u64::from(count), "{:?}", event);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn sliding_window_matches_the_weighted_estimate(
        (count, period_ms, start, ops) in (1..=12_u32, prop::sample::select(&PERIODS_MS[..]))
            .prop_flat_map(|(count, period_ms)| (Just(count), Just(period_ms), start(period_ms), ops(period_ms, count)))
    ) {
        let events = simulate::<SlidingWindow>(rate(count, period_ms, count), count, start, &ops);
        let period = period_ms * 1_000_000;
        for (index, event) in events.iter().enumerate() {
            let window = event.at / period;
            let current = allowed_cost(&events[..index], |earlier| earlier.at / period == window);
            let previous = allowed_cost(&events[..index], |earlier| earlier.at / period + 1 == window);
            let left = u128::from((window + 1) * period - event.at);
            let weighted = (u128::from(previous) * left).div_ceil(u128::from(period));
            let estimate = u64::try_from(weighted).unwrap() + current;
            prop_assert_eq!(event.allowed, estimate + u64::from(event.cost) <= u64::from(count), "{:?}", event);
            // Independent of the estimate: no aligned window ever exceeds the limit.
            let spent_here = if event.allowed { u64::from(event.cost) } else { 0 };
            prop_assert!(current + spent_here <= u64::from(count), "window over limit at {:?}", event);
        }
    }

    /// When the period divides evenly by the count there is no rounding, so
    /// GCRA must match an exact token bucket decision for decision.
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn gcra_matches_an_exact_token_bucket(
        (count, burst, period_ms, start, ops) in (
            prop::sample::select(&[1_u32, 2, 4, 5, 8, 10][..]),
            1..=12_u32,
            prop::sample::select(&PERIODS_MS[..]),
        ).prop_flat_map(|(count, burst, period_ms)| (Just(count), Just(burst), Just(period_ms), start(period_ms), ops(period_ms, burst)))
    ) {
        let events = simulate::<TokenBucket>(rate(count, period_ms, burst), burst, start, &ops);
        // Token levels scaled by the period: one token = `period` units, and
        // each nanosecond adds `count` units. Integers throughout.
        let period = u128::from(period_ms) * 1_000_000;
        let capacity = u128::from(burst) * period;
        let mut level = capacity;
        let mut last = start;
        for event in &events {
            level = capacity.min(level + u128::from(event.at - last) * u128::from(count));
            last = event.at;
            let needed = u128::from(event.cost) * period;
            prop_assert_eq!(event.allowed, level >= needed, "{:?} at level {}", event, level);
            if event.allowed {
                level -= needed;
            }
        }
    }

    /// With rounding, GCRA still never admits more than a token bucket can:
    /// over any span, allowed cost ≤ burst + ⌈span × count / period⌉.
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn gcra_never_exceeds_the_token_bucket_bound(
        (count, burst, period_ms, start, ops) in (1..=12_u32, 1..=12_u32, prop::sample::select(&PERIODS_MS[..]))
            .prop_flat_map(|(count, burst, period_ms)| (Just(count), Just(burst), Just(period_ms), start(period_ms), ops(period_ms, burst)))
    ) {
        let events = simulate::<TokenBucket>(rate(count, period_ms, burst), burst, start, &ops);
        let period = u128::from(period_ms) * 1_000_000;
        let allowed: Vec<_> = events.iter().filter(|event| event.allowed).collect();
        for (i, first) in allowed.iter().enumerate() {
            let mut spent = 0_u128;
            for last in &allowed[i..] {
                spent += u128::from(last.cost);
                let span = u128::from(last.at - first.at);
                let bound = u128::from(burst) + (span * u128::from(count)).div_ceil(period);
                prop_assert!(spent <= bound, "spent {} > bound {} from {:?} to {:?}", spent, bound, first, last);
            }
        }
    }
}

/// A denied hit changes no rate's state, and an allowed one changes them
/// exactly as checking and committing each rate alone would.
fn quota_is_all_or_nothing<A: Algorithm>(
    quota: &Quota,
    ops: &[(u64, u32)],
) -> Result<(), TestCaseError>
where
    A::State: Clone + PartialEq + std::fmt::Debug,
{
    let mut states: [A::State; MAX_RATES] = std::array::from_fn(|_| A::State::default());
    let mut now = 0;
    for &(advance, cost) in ops {
        now += advance;
        let at = Timestamp::from_nanos(now);
        let before = states.clone();
        match hit_quota::<A, MAX_RATES>(quota, &mut states, at, cost) {
            Outcome::Denied { .. } => {
                prop_assert_eq!(&states, &before, "a denied hit changed state");
            }
            Outcome::Allowed(_) if cost == 0 => {
                prop_assert_eq!(&states, &before, "a peek changed state");
            }
            Outcome::Allowed(_) => {
                for ((rate, state), old) in quota.rates().iter().zip(&states).zip(&before) {
                    let mut alone = old.clone();
                    match A::check(*rate, &alone, at, cost) {
                        Check::Allowed { commit, .. } => A::commit(&mut alone, commit),
                        Check::Denied { .. } => {
                            prop_assert!(false, "allowed overall but {} denies", rate);
                        }
                    }
                    prop_assert_eq!(state, &alone);
                }
            }
        }
    }
    Ok(())
}

fn multi_rate_quota() -> impl Strategy<Value = Quota> {
    prop::collection::vec(
        (1..=6_u32, prop::sample::select(&PERIODS_MS[..])),
        1..=MAX_RATES,
    )
    .prop_map(|rates| {
        let mut quota = Quota::new(rate(rates[0].0, rates[0].1, rates[0].0));
        for &(count, period_ms) in &rates[1..] {
            if let Ok(with) = quota.with(rate(count, period_ms, count)) {
                quota = with;
            }
        }
        quota
    })
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn quotas_are_all_or_nothing(quota in multi_rate_quota(), ops in ops(1_000, 6)) {
        quota_is_all_or_nothing::<TokenBucket>(&quota, &ops)?;
        quota_is_all_or_nothing::<FixedWindow>(&quota, &ops)?;
        quota_is_all_or_nothing::<SlidingWindow>(&quota, &ops)?;
        quota_is_all_or_nothing::<SlidingLog>(&quota, &ops)?;
    }
}

/// What a check decided, without the commit, whose form can differ between
/// two states that decide alike.
fn decision<C>(check: &Check<C>) -> (bool, super::Stats, Option<Retry>) {
    match check {
        Check::Allowed { stats, .. } => (true, *stats, None),
        Check::Denied { stats, retry } => (false, *stats, Some(*retry)),
    }
}

/// Records `before` at arbitrary times, including backwards ones. From
/// `idle_at` on (and no earlier than the newest time recorded, since an
/// earlier time is a clock going backwards, where a state only ever allows
/// less), the state must decide every later hit exactly like a never-hit
/// state: evicting it then changes nothing.
fn idle_state_decides_like_a_new_one<A: Algorithm>(
    rate: Rate,
    before: &[(u64, u32)],
    after: &[(u64, u32)],
) -> Result<(), TestCaseError> {
    let mut state = A::State::default();
    for &(time, cost) in before {
        if let Check::Allowed { commit, .. } =
            A::check(rate, &state, Timestamp::from_nanos(time), cost)
        {
            A::commit(&mut state, commit);
        }
    }
    let idle = A::idle_at(rate, &state);
    let newest = before.iter().map(|&(time, _)| time).max().unwrap_or(0);
    let mut fresh = A::State::default();
    let mut now = idle.max(newest);
    for &(advance, cost) in after {
        now += advance;
        let at = Timestamp::from_nanos(now);
        let (old, new) = (
            A::check(rate, &state, at, cost),
            A::check(rate, &fresh, at, cost),
        );
        prop_assert_eq!(
            decision(&old),
            decision(&new),
            "at {} (idle at {}), cost {}",
            now,
            idle,
            cost
        );
        if let (Check::Allowed { commit: a, .. }, Check::Allowed { commit: b, .. }) = (old, new) {
            A::commit(&mut state, a);
            A::commit(&mut fresh, b);
        }
    }
    Ok(())
}

/// Hit times anywhere in a few periods, in any order.
fn shuffled_hits(period_ms: u64) -> impl Strategy<Value = Vec<(u64, u32)>> {
    let span = period_ms * 1_000_000 * 4;
    prop::collection::vec((0..span, 0..=5_u32), 0..40)
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn idle_states_decide_like_new_ones(
        count in 1..=6_u32,
        period_ms in prop::sample::select(&PERIODS_MS[..]),
        burst in 1..=8_u32,
        before in shuffled_hits(1_000),
        after in ops(1_000, 6),
    ) {
        let token = rate(count, period_ms, burst);
        let window = rate(count, period_ms, count);
        idle_state_decides_like_a_new_one::<TokenBucket>(token, &before, &after)?;
        idle_state_decides_like_a_new_one::<FixedWindow>(window, &before, &after)?;
        idle_state_decides_like_a_new_one::<SlidingWindow>(window, &before, &after)?;
        idle_state_decides_like_a_new_one::<SlidingLog>(window, &before, &after)?;
    }
}

/// On every denial with a wait `d`, the same hit is still denied at any
/// point before `d` (checked at 0, d/8 … 7d/8 and d − 1 ns) and allowed at
/// exactly `d`, with nothing spent between. Every algorithm's wait is exact
/// for each rate, and a quota waits for its slowest denying rate, so the
/// wait is exact for the quota too. `Never` comes exactly when the cost is
/// larger than the smallest of the rates' `capacity`.
fn retry_is_never_early_nor_late<A: Algorithm>(
    quota: &Quota,
    capacity: fn(Rate) -> u32,
    ops: &[(u64, u32)],
) -> Result<(), TestCaseError> {
    let smallest = quota.rates().iter().map(|rate| capacity(*rate)).min();
    let mut states: [A::State; MAX_RATES] = std::array::from_fn(|_| A::State::default());
    let mut now = 0;
    let allowed_at = |states: &[A::State; MAX_RATES], at: u64, cost: u32| {
        matches!(
            check_quota::<A, MAX_RATES>(quota, states, Timestamp::from_nanos(at), cost).0,
            Outcome::Allowed(_)
        )
    };
    for &(advance, cost) in ops {
        now += advance;
        match hit_quota::<A, MAX_RATES>(quota, &mut states, Timestamp::from_nanos(now), cost) {
            Outcome::Allowed(_) => {}
            Outcome::Denied {
                retry: Retry::Never,
                ..
            } => {
                prop_assert!(Some(cost) > smallest, "Never for cost {} at {}", cost, now);
            }
            Outcome::Denied {
                retry: Retry::After(wait),
                ..
            } => {
                prop_assert!(
                    Some(cost) <= smallest,
                    "a wait for cost {} that never fits",
                    cost
                );
                let wait = u64::try_from(wait.as_nanos()).unwrap();
                prop_assert!(wait > 0, "a denial with no wait at {}", now);
                for offset in (0..8).map(|eighths| wait * eighths / 8).chain([wait - 1]) {
                    prop_assert!(
                        !allowed_at(&states, now + offset, cost),
                        "cost {} allowed {} ns into a wait of {} ns at {}",
                        cost,
                        offset,
                        wait,
                        now
                    );
                }
                prop_assert!(
                    allowed_at(&states, now + wait, cost),
                    "cost {} still denied after its wait of {} ns at {}",
                    cost,
                    wait,
                    now
                );
            }
        }
    }
    Ok(())
}

/// Up to [`MAX_RATES`] rates with any counts, periods and bursts.
fn any_quota() -> impl Strategy<Value = Quota> {
    prop::collection::vec(
        (1..=8_u32, prop::sample::select(&PERIODS_MS[..]), 1..=10_u32),
        1..=MAX_RATES,
    )
    .prop_map(|rates| {
        let (count, period_ms, burst) = rates[0];
        let mut quota = Quota::new(rate(count, period_ms, burst));
        for &(count, period_ms, burst) in &rates[1..] {
            if let Ok(with) = quota.with(rate(count, period_ms, burst)) {
                quota = with;
            }
        }
        quota
    })
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn retry_after_is_never_early_nor_late(quota in any_quota(), ops in ops(1_000, 10)) {
        let burst = |rate: Rate| rate.burst().get();
        let count = |rate: Rate| rate.count().get();
        retry_is_never_early_nor_late::<TokenBucket>(&quota, burst, &ops)?;
        retry_is_never_early_nor_late::<FixedWindow>(&quota, count, &ops)?;
        retry_is_never_early_nor_late::<SlidingWindow>(&quota, count, &ops)?;
        retry_is_never_early_nor_late::<SlidingLog>(&quota, count, &ops)?;
    }
}
