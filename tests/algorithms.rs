//! Exact answers from every algorithm at the edges: golden tables, long
//! idles, uneven periods, extreme rates and clocks, concurrency, and cost 0.
#![cfg(test)]
#![cfg(feature = "memory")]
#![expect(
    clippy::arithmetic_side_effects,
    reason = "test arithmetic on small known values"
)]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use porin::{
    Clock, Decision, Denial, FixedWindow, Limiter, ManualClock, Quota, SlidingLog, SlidingWindow,
    Stats, Timestamp, TokenBucket, WhenFull, quota,
};

fn block_on<F: Future>(future: F) -> F::Output {
    pollster::block_on(future)
}

fn limiter<A: porin::Algorithm>(
    algorithm: A,
    quota: Quota,
    clock: &ManualClock,
) -> Limiter<u64, A> {
    Limiter::builder()
        .algorithm(algorithm)
        .quota(quota)
        .clock(clock.clone())
        .build()
}

fn allowed(decision: Result<Decision, porin::Error>) -> Stats {
    match decision {
        Ok(Decision::Allowed(stats)) => stats,
        other => panic!("expected an allowed hit, got {other:?}"),
    }
}

fn denied(decision: Result<Decision, porin::Error>) -> Denial {
    match decision {
        Ok(Decision::Denied(denial)) => denial,
        other => panic!("expected a denial, got {other:?}"),
    }
}

/// `cost` units for `key` are denied until exactly `wait` from now and
/// allowed from then on. Checks with `test_n`, so nothing is spent, and
/// leaves the clock where it was.
fn assert_allowed_after_exactly<A: porin::Algorithm>(
    limiter: &Limiter<u64, A>,
    clock: &ManualClock,
    key: u64,
    cost: u32,
    wait: Duration,
) {
    let now = clock.now();
    let wait_nanos = u64::try_from(wait.as_nanos()).unwrap();
    assert!(wait_nanos > 0, "a denial must wait");
    let is_allowed_at = |offset: u64| {
        clock.set(Timestamp::from_nanos(now.as_nanos() + offset));
        block_on(limiter.test_n(&key, cost)).unwrap().is_allowed()
    };
    for offset in [0, wait_nanos / 2, wait_nanos - 1] {
        assert!(
            !is_allowed_at(offset),
            "{cost} units allowed {offset} ns into a wait of {wait:?}"
        );
    }
    assert!(
        is_allowed_at(wait_nanos),
        "{cost} units still denied after the {wait:?} wait"
    );
    clock.set(now);
}

fn at_millis(millis: u64) -> Timestamp {
    Timestamp::from_nanos(millis * 1_000_000)
}

#[derive(Clone, Copy, Debug)]
enum Expect {
    Allowed,
    RetryAfterMillis(u64),
    Never,
}

/// redis-cell's `it_rate_limits` table, which comes from throttled's
/// `TestRateLimit`: 1 per second with a `max_burst` of 4. Their limit is
/// `max_burst + 1`, the units allowed at once, which is what porin calls
/// the burst, so the same bucket here is `1/second burst 5`.
///
/// porin's definitions agree with theirs on every row. `remaining` is how
/// many more units fit right now after this hit; `reset_after` is the time
/// until the bucket is full again, the TAT minus now; a denial reports the
/// key's state without the hit. A cost-0 hit spends nothing and reports the
/// current state.
#[test]
fn token_bucket_matches_the_gcra_golden_table() {
    use Expect::{Allowed, Never, RetryAfterMillis};

    let clock = ManualClock::new();
    let limiter = limiter(TokenBucket, quota!("1/second burst 5"), &clock);
    // (time ms, cost, expected, remaining, reset_after ms)
    let table = [
        // More than the burst can never fit, and spends nothing.
        (0, 6, Never, 5, 0),
        (0, 1, Allowed, 4, 1_000),
        (0, 1, Allowed, 3, 2_000),
        (0, 1, Allowed, 2, 3_000),
        (0, 1, Allowed, 1, 4_000),
        (0, 1, Allowed, 0, 5_000),
        (0, 1, RetryAfterMillis(1_000), 0, 5_000),
        // The TAT was 5 s; this hit moves it to 6 s, 3 s from now, leaving
        // room for 5 − 3 = 2 more.
        (3_000, 1, Allowed, 2, 3_000),
        // TAT 7 s: 3.9 s away, room for ⌊1.1⌋ = 1 more.
        (3_100, 1, Allowed, 1, 3_900),
        (4_000, 1, Allowed, 1, 4_000),
        // TAT 8 s was reached exactly; the hit moves it to 9 s.
        (8_000, 1, Allowed, 4, 1_000),
        // The bucket was full from 9 s; the hit moves the TAT to 10.5 s.
        (9_500, 1, Allowed, 4, 1_000),
        (9_500, 0, Allowed, 4, 1_000),
        (9_500, 2, Allowed, 2, 3_000),
        // TAT 12.5 s; 5 units need it at 5 s from now, 3 s away.
        (9_500, 5, RetryAfterMillis(3_000), 2, 3_000),
    ];
    for (row, (time, cost, expect, remaining, reset_ms)) in table.into_iter().enumerate() {
        clock.set(at_millis(time));
        let decision = block_on(limiter.hit_n(&1, cost)).unwrap();
        let context = format!("row {row}: cost {cost} at {time} ms");
        match (expect, decision) {
            (Allowed, Decision::Allowed(_)) => {}
            (RetryAfterMillis(wait), Decision::Denied(denial)) => {
                assert_eq!(
                    denial.retry_after(),
                    Some(Duration::from_millis(wait)),
                    "{context}"
                );
            }
            (Never, Decision::Denied(denial)) => {
                assert_eq!(denial.retry_after(), None, "{context}");
            }
            (expect, decision) => panic!("{context}: expected {expect:?}, got {decision:?}"),
        }
        assert_eq!(decision.stats().remaining(), remaining, "{context}");
        assert_eq!(
            decision.stats().reset_after(),
            Duration::from_millis(reset_ms),
            "{context}"
        );
    }
}

/// governor issues 107 and 249: after a long idle a key must get its full
/// capacity back, exactly, not one unit more or less.
#[test]
fn exactly_the_capacity_again_after_a_long_idle() {
    fn check<A: porin::Algorithm>(algorithm: A) {
        let clock = ManualClock::new();
        let limiter = limiter(algorithm, quota!("5/second"), &clock);
        allowed(block_on(limiter.hit_n(&1, 5)));
        denied(block_on(limiter.hit(&1)));
        clock.advance(Duration::from_secs(2));
        for n in 0..5 {
            assert!(
                block_on(limiter.hit(&1)).unwrap().is_allowed(),
                "{algorithm:?}: unit {n} after the idle"
            );
        }
        assert!(
            !block_on(limiter.hit(&1)).unwrap().is_allowed(),
            "{algorithm:?}: a 6th unit after the idle"
        );
    }
    check(TokenBucket);
    check(FixedWindow);
    check(SlidingWindow);
    check(SlidingLog);
}

/// Hits key 1 until it is denied at every multiple of `step` below `end`
/// nanoseconds, and returns how many units were allowed.
fn hammer<A: porin::Algorithm>(
    limiter: &Limiter<u64, A>,
    clock: &ManualClock,
    step: u64,
    end: u64,
) -> u64 {
    let mut admitted = 0;
    for time in (0..end).step_by(usize::try_from(step).unwrap()) {
        clock.set(Timestamp::from_nanos(time));
        while block_on(limiter.hit(&1)).unwrap().is_allowed() {
            admitted += 1;
        }
    }
    admitted
}

/// 3 per second and 7 per minute don't divide into whole nanoseconds per
/// unit. Under constant pressure for 300 periods, each algorithm admits an
/// exact total worked out by hand from its definition.
#[test]
#[cfg_attr(miri, ignore = "too slow under Miri")]
fn uneven_periods_admit_exact_totals() {
    const PERIODS: u64 = 300;
    // (quota, count, period ns, sampling step ns, token bucket lateness at
    // the end in ns)
    let cases = [
        (quota!("3/second"), 3, 1_000_000_000, 1_000_000, 600),
        (quota!("7/minute"), 7, 60_000_000_000, 60_000_000, 1_200),
    ];
    for (quota, count, period, step, late) in cases {
        let end = PERIODS * period;

        // Each aligned window admits exactly its count.
        let clock = ManualClock::new();
        let fixed = limiter(FixedWindow, quota, &clock);
        assert_eq!(
            hammer(&fixed, &clock, step, end),
            count * PERIODS,
            "{quota} fixed"
        );

        // A unit leaves the log exactly one period after it entered, so
        // each period admits exactly its count.
        let clock = ManualClock::new();
        let log = limiter(SlidingLog, quota, &clock);
        assert_eq!(
            hammer(&log, &clock, step, end),
            count * PERIODS,
            "{quota} log"
        );

        // The previous window's weight is rounded up, so it stays at least
        // 1 until the window ends: under constant pressure each window after
        // the first admits count − 1. The first window admits count; the
        // second, with `count` behind it, frees units until the weight is 1,
        // so count − 1; every later window then starts with count − 1
        // behind it and admits count − 1 again.
        let clock = ManualClock::new();
        let window = limiter(SlidingWindow, quota, &clock);
        assert_eq!(
            hammer(&window, &clock, step, end),
            count + (count - 1) * (PERIODS - 1),
            "{quota} sliding window"
        );

        // An exact bucket would admit the burst plus count per period, up
        // to the last sample one step before the end: count × periods − 1
        // refilled units, since a step is shorter than one unit's refill.
        // porin rounds the refill time of one unit up to whole nanoseconds
        // (333,333,334 ns and 8,571,428,572 ns here), so the bucket runs
        // slow by under one nanosecond per unit and never fast. Over 300
        // periods that adds up to `late` ns, too little to cost a unit
        // anywhere but right at a refill instant.
        let clock = ManualClock::new();
        let bucket = limiter(TokenBucket, quota, &clock);
        let admitted = hammer(&bucket, &clock, step, end);
        assert_eq!(
            admitted,
            count + count * PERIODS - 1,
            "{quota} token bucket"
        );
        // At the end itself an exact bucket has one more unit; porin's
        // arrives `late` ns after it.
        clock.set(Timestamp::from_nanos(end));
        let denial = denied(block_on(bucket.hit(&1)));
        assert_eq!(
            denial.retry_after(),
            Some(Duration::from_nanos(late)),
            "{quota} token bucket at the end"
        );
    }
}

/// `u32::MAX` per millisecond is far finer than one unit per nanosecond;
/// the windows still count it exactly.
#[test]
fn a_rate_finer_than_a_nanosecond_still_limits() {
    fn spend_all_then_refill<A: porin::Algorithm>(algorithm: A, quota: Quota, refill: Duration) {
        let clock = ManualClock::new();
        let limiter = limiter(algorithm, quota, &clock);
        allowed(block_on(limiter.hit_n(&1, u32::MAX)));
        let denial = denied(block_on(limiter.hit(&1)));
        let wait = denial.retry_after().unwrap();
        assert_allowed_after_exactly(&limiter, &clock, 1, 1, wait);
        clock.advance(refill);
        let stats = allowed(block_on(limiter.hit_n(&1, u32::MAX)));
        assert_eq!(stats.remaining(), 0, "{algorithm:?}");
    }
    let quota = quota!("4294967295/millisecond");
    spend_all_then_refill(FixedWindow, quota, Duration::from_millis(1));
    spend_all_then_refill(SlidingLog, quota, Duration::from_millis(1));
    // The spent window weighs on the next one until it ends.
    spend_all_then_refill(SlidingWindow, quota, Duration::from_millis(2));

    // The token bucket refills at most one unit per nanosecond, as its
    // documentation says: after 1 ms, a million units are back, and the
    // full burst takes u32::MAX ns to return.
    let clock = ManualClock::new();
    let bucket = limiter(TokenBucket, quota, &clock);
    let stats = allowed(block_on(bucket.hit_n(&1, u32::MAX)));
    assert_eq!(
        stats.reset_after(),
        Duration::from_nanos(u64::from(u32::MAX))
    );
    let denial = denied(block_on(bucket.hit(&1)));
    assert_eq!(denial.retry_after(), Some(Duration::from_nanos(1)));
    clock.advance(Duration::from_millis(1));
    assert_eq!(block_on(bucket.stats(&1)).unwrap().remaining(), 1_000_000);
}

/// 1 per 10,000 days, the longest period, waits exactly that long.
#[test]
fn the_longest_period_waits_exactly() {
    const PERIOD: Duration = Duration::from_secs(10_000 * 86_400);

    fn check<A: porin::Algorithm>(algorithm: A, wait: Duration) {
        let clock = ManualClock::new();
        let limiter = limiter(algorithm, quota!("1/10000 days"), &clock);
        allowed(block_on(limiter.hit(&1)));
        let denial = denied(block_on(limiter.hit(&1)));
        assert_eq!(denial.retry_after(), Some(wait), "{algorithm:?}");
        assert_allowed_after_exactly(&limiter, &clock, 1, 1, wait);
    }
    check(TokenBucket, PERIOD);
    check(FixedWindow, PERIOD);
    check(SlidingLog, PERIOD);
    // The unit weighs fully on the rest of its own window, then on the next
    // window, decaying to zero only as that window ends.
    check(SlidingWindow, PERIOD * 2);
}

/// Threads hitting one key at a frozen instant, where nothing refills,
/// admit exactly the limit: never more, and none lost to races.
#[test]
fn concurrent_hits_admit_exactly_the_limit() {
    fn check<A: porin::Algorithm>(algorithm: A, quota: Quota, limit: u32) {
        let clock = ManualClock::starting_at(at_millis(1_234));
        let limiter = limiter(algorithm, quota, &clock);
        let admitted = AtomicU32::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..40 {
                        if block_on(limiter.hit(&7)).unwrap().is_allowed() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(
            admitted.load(Ordering::Relaxed),
            limit,
            "{algorithm:?} with {quota}: 320 attempts at one instant"
        );
    }
    let one_rate = quota!("100/minute");
    // The per-minute rate binds; the per-second rate's state must still
    // stay consistent with it under contention.
    let two_rates = quota!("150/second; 100/minute");
    check(TokenBucket, one_rate, 100);
    check(FixedWindow, one_rate, 100);
    check(SlidingWindow, one_rate, 100);
    check(SlidingLog, one_rate, 100);
    check(TokenBucket, two_rates, 100);
    check(FixedWindow, two_rates, 100);
    check(SlidingWindow, two_rates, 100);
    check(SlidingLog, two_rates, 100);
}

#[test]
fn a_cost_of_u32_max_never_fits_a_smaller_quota() {
    fn check<A: porin::Algorithm>(algorithm: A) {
        let clock = ManualClock::new();
        let small = limiter(algorithm, quota!("5/second"), &clock);
        let denial = denied(block_on(small.hit_n(&1, u32::MAX)));
        assert_eq!(denial.retry_after(), None, "{algorithm:?}");
        assert_eq!(
            denial.stats().remaining(),
            5,
            "{algorithm:?}: nothing spent"
        );

        // A quota of u32::MAX allows it, and then allows it again after
        // an exact wait.
        let large = limiter(algorithm, quota!("4294967295/second"), &clock);
        allowed(block_on(large.hit_n(&1, u32::MAX)));
        let denial = denied(block_on(large.hit_n(&1, u32::MAX)));
        let wait = denial.retry_after().unwrap();
        assert_allowed_after_exactly(&large, &clock, 1, u32::MAX, wait);
    }
    check(TokenBucket);
    check(FixedWindow);
    check(SlidingWindow);
    check(SlidingLog);
}

/// A clock 10 s short of the largest timestamp, then at it: no panics, no
/// over-admission, and every wait the limiter reports is real.
#[test]
fn a_clock_near_the_end_of_time() {
    fn check<A: porin::Algorithm>(algorithm: A) {
        let clock = ManualClock::starting_at(Timestamp::from_nanos(u64::MAX - 10_000_000_000));
        let limiter = limiter(algorithm, quota!("5/second"), &clock);
        for n in 0..5 {
            assert!(
                block_on(limiter.hit(&1)).unwrap().is_allowed(),
                "{algorithm:?}: unit {n}"
            );
        }
        let denial = denied(block_on(limiter.hit(&1)));
        if let Some(wait) = denial.retry_after() {
            assert_allowed_after_exactly(&limiter, &clock, 1, 1, wait);
        }

        // At the largest timestamp time can't pass, so a denial may only
        // say never, or name a wait that is not zero.
        clock.advance(Duration::MAX);
        assert_eq!(clock.now(), Timestamp::from_nanos(u64::MAX));
        let mut admitted = 0;
        while block_on(limiter.hit(&1)).unwrap().is_allowed() {
            admitted += 1;
            assert!(
                admitted <= 5,
                "{algorithm:?}: over-admits at the end of time"
            );
        }
        let denial = denied(block_on(limiter.hit(&1)));
        assert_ne!(
            denial.retry_after(),
            Some(Duration::ZERO),
            "{algorithm:?}: a denial that says to retry at once"
        );
    }
    check(TokenBucket);
    check(FixedWindow);
    check(SlidingWindow);
    check(SlidingLog);
}

#[test]
fn one_per_day_after_years_idle() {
    fn check<A: porin::Algorithm>(algorithm: A, wait: Duration) {
        let clock = ManualClock::new();
        let limiter = limiter(algorithm, quota!("1/day"), &clock);
        allowed(block_on(limiter.hit(&1)));
        clock.advance(Duration::from_secs(5 * 365 * 86_400));
        let stats = allowed(block_on(limiter.hit(&1)));
        assert_eq!(stats.remaining(), 0, "{algorithm:?}");
        let denial = denied(block_on(limiter.hit(&1)));
        assert_eq!(denial.retry_after(), Some(wait), "{algorithm:?}");
    }
    const DAY: Duration = Duration::from_secs(86_400);
    check(TokenBucket, DAY);
    // Five years of days is a whole number of days, so the hit lands at
    // the start of a window.
    check(FixedWindow, DAY);
    check(SlidingLog, DAY);
    // The unit weighs on its own window and then on the next.
    check(SlidingWindow, DAY * 2);
}

/// A cost of 0 asks for the stats: it is allowed even on a spent key, and
/// it neither stores a key nor evicts one.
#[test]
fn a_cost_of_zero_spends_and_stores_nothing() {
    fn check<A: porin::Algorithm>(algorithm: A, when_full: WhenFull) {
        let clock = ManualClock::new();
        let limiter: Limiter<u64, A> = Limiter::builder()
            .algorithm(algorithm)
            .quota(quota!("2/minute"))
            .max_keys(NonZeroUsize::MIN)
            .when_full(when_full)
            .clock(clock.clone())
            .build();
        let context = format!("{algorithm:?}, {when_full:?}");
        allowed(block_on(limiter.hit_n(&1, 2)));
        clock.advance(Duration::from_secs(1));

        let peek = allowed(block_on(limiter.hit_n(&1, 0)));
        assert_eq!(peek, block_on(limiter.stats(&1)).unwrap(), "{context}");
        assert_eq!(peek.remaining(), 0, "{context}");

        for key in 2..10 {
            let peek = allowed(block_on(limiter.hit_n(&key, 0)));
            assert_eq!(peek, block_on(limiter.stats(&key)).unwrap(), "{context}");
            assert_eq!(peek.remaining(), 2, "{context}");
            assert_eq!(peek.reset_after(), Duration::ZERO, "{context}");
        }
        assert_eq!(
            limiter.full_hits(),
            0,
            "{context}: cost-0 hits filled the store"
        );
        let denial = denied(block_on(limiter.hit(&1)));
        assert!(
            !denial.is_store_full(),
            "{context}: key 1 was evicted or displaced"
        );

        // A real hit on another key does need the slot.
        let new_key = block_on(limiter.hit(&2)).unwrap();
        assert_eq!(limiter.full_hits(), 1, "{context}");
        match when_full {
            WhenFull::Refuse => assert!(denied(Ok(new_key)).is_store_full(), "{context}"),
            _ => assert!(new_key.is_allowed(), "{context}: evicts key 1"),
        }
    }
    let evict = WhenFull::EvictNearestIdle {
        within: Duration::from_secs(3_600),
    };
    for when_full in [WhenFull::Refuse, evict] {
        check(TokenBucket, when_full);
        check(FixedWindow, when_full);
        check(SlidingWindow, when_full);
        check(SlidingLog, when_full);
    }
}

/// After the window rolls, the previous window's units come back one at a
/// time as its weight decays. 3 per second, all 3 spent in the window
/// [0 s, 1 s), so the estimate in [1 s, 2 s) at 1 s + x is ⌈3 × (1 s − x) / 1 s⌉
/// plus whatever was spent since.
#[test]
fn sliding_window_units_return_one_at_a_time_after_the_roll() {
    const SECOND: u64 = 1_000_000_000;
    let clock = ManualClock::new();
    let limiter = limiter(SlidingWindow, quota!("3/second"), &clock);
    for (key, millis) in [(1, 0), (1, 500), (1, 900), (2, 0), (2, 500), (2, 900)] {
        clock.set(at_millis(millis));
        allowed(block_on(limiter.hit(&key)));
    }

    // Key 1 is only looked at. Its reset is when the previous window stops
    // counting: the end of the current window.
    for (millis, remaining, reset_ms) in [
        (1_000, 0, 1_000),
        (1_250, 0, 750), // ⌈2.25⌉ = 3
        (1_500, 1, 500), // ⌈1.5⌉ = 2
        (1_750, 2, 250), // ⌈0.75⌉ = 1
        (2_000, 3, 0),
    ] {
        clock.set(at_millis(millis));
        let stats = block_on(limiter.stats(&1)).unwrap();
        assert_eq!(stats.remaining(), remaining, "key 1 at {millis} ms");
        assert_eq!(
            stats.reset_after(),
            Duration::from_millis(reset_ms),
            "key 1 at {millis} ms"
        );
    }

    // Key 2 takes each unit as soon as it is back. A unit fits once
    // ⌈3 × left / 1 s⌉ + spent ≤ 2, where `left` is the time to 2 s.
    clock.set(at_millis(1_000));
    let denial = denied(block_on(limiter.hit(&2)));
    assert_eq!(denial.stats().remaining(), 0);
    assert_eq!(denial.stats().reset_after(), Duration::from_secs(1));
    // 3 × left ≤ 2 s first holds at left = 666,666,666 ns.
    let first = Duration::from_nanos(SECOND - 666_666_666);
    assert_eq!(denial.retry_after(), Some(first));
    assert_allowed_after_exactly(&limiter, &clock, 2, 1, first);
    clock.advance(first);
    let stats = allowed(block_on(limiter.hit(&2)));
    assert_eq!(stats.remaining(), 0, "⌈1.999999998⌉ + 1 = 3");
    // A spent unit counts until a whole window after this one ends.
    assert_eq!(
        stats.reset_after(),
        Duration::from_nanos(666_666_666 + SECOND)
    );

    // Now ⌈3 × left⌉ + 1 ≤ 2 needs left ≤ 333,333,333 ns.
    let denial = denied(block_on(limiter.hit(&2)));
    let second = Duration::from_nanos(666_666_666 - 333_333_333);
    assert_eq!(denial.retry_after(), Some(second));
    assert_allowed_after_exactly(&limiter, &clock, 2, 1, second);
    clock.advance(second);
    let stats = allowed(block_on(limiter.hit(&2)));
    assert_eq!(
        stats.reset_after(),
        Duration::from_nanos(333_333_333 + SECOND)
    );

    // The third unit would need the weight at 0, which only the next
    // window gives: there the 2 spent units are the previous window, and
    // one more fits at once.
    let denial = denied(block_on(limiter.hit(&2)));
    assert_eq!(
        denial.retry_after(),
        Some(Duration::from_nanos(333_333_333))
    );
    clock.set(at_millis(2_000));
    let stats = allowed(block_on(limiter.hit(&2)));
    assert_eq!(stats.remaining(), 0);
    assert_eq!(stats.reset_after(), Duration::from_secs(2));
}

/// A plain example of all-or-nothing costs: with 10 units and 5 + 4 spent,
/// 1 unit is left.
#[test]
fn two_units_are_denied_when_one_is_left() {
    fn check<A: porin::Algorithm>(algorithm: A) {
        let limiter = limiter(algorithm, quota!("10/minute"), &ManualClock::new());
        allowed(block_on(limiter.hit_n(&1, 5)));
        let stats = allowed(block_on(limiter.hit_n(&1, 4)));
        assert_eq!(stats.remaining(), 1, "{algorithm:?}");
        assert!(
            !block_on(limiter.test_n(&1, 2)).unwrap().is_allowed(),
            "{algorithm:?}: test_n(2)"
        );
        let denial = denied(block_on(limiter.hit_n(&1, 2)));
        assert_eq!(
            denial.stats().remaining(),
            1,
            "{algorithm:?}: nothing spent"
        );
        let stats = allowed(block_on(limiter.hit_n(&1, 1)));
        assert_eq!(stats.remaining(), 0, "{algorithm:?}");
    }
    check(TokenBucket);
    check(FixedWindow);
    check(SlidingWindow);
    check(SlidingLog);
}
