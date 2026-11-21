#![cfg(not(loom))]
#![expect(
    clippy::arithmetic_side_effects,
    reason = "test arithmetic on small generated values"
)]

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use proptest::prelude::*;

use super::MemoryStore;
use super::slot::Stored;
use crate::algorithm::fixed_window::FixedWindow;
use crate::algorithm::gcra::TokenBucket;
use crate::algorithm::sliding_log::SlidingLog;
use crate::algorithm::sliding_window::SlidingWindow;
use crate::algorithm::{Outcome, Retry, check_quota, hit_quota};
use crate::quota::MAX_RATES;
use crate::store::{Hit, WhenFull};
use crate::{Quota, Timestamp, quota};

fn store<A: Stored>(max_keys: usize, shards: usize) -> MemoryStore<u8, A> {
    MemoryStore::with_shards(NonZeroUsize::new(max_keys).unwrap(), shards)
}

fn at(millis: u64) -> Timestamp {
    Timestamp::ZERO.saturating_add(Duration::from_millis(millis))
}

fn allowed(hit: Hit) -> bool {
    matches!(hit, Hit::Allowed(_))
}

/// Runs random hits on the store and on a plain map with no shards and no
/// cap, and requires identical answers.
fn agrees_with_plain_map<A: Stored, const N: usize>(
    quota: &Quota,
    ops: &[(u8, i64, u32)],
) -> Result<(), TestCaseError> {
    let store = MemoryStore::<u8, A, N>::with_shards(NonZeroUsize::new(1_000).unwrap(), 3);
    let mut model: HashMap<u8, [A::State; MAX_RATES]> = HashMap::new();
    let mut now: u64 = 10_000_000_000;
    for &(key, step, cost) in ops {
        now = now.saturating_add_signed(step);
        let at = Timestamp::from_nanos(now);
        let states = model
            .entry(key)
            .or_insert_with(|| std::array::from_fn(|_| A::State::default()));
        let expected = Hit::from(hit_quota::<A, MAX_RATES>(quota, states, at, cost));
        prop_assert_eq!(
            store.hit(quota, &key, at, cost),
            expected,
            "key {} at {} cost {}",
            key,
            now,
            cost
        );
    }
    Ok(())
}

const QUOTAS: [Quota; 3] = [
    quota!("3/second"),
    quota!("2/second; 5/minute"),
    quota!("4/7 milliseconds burst 6; 10/second; 30/minute"),
];

/// Steps in time, mostly forwards but sometimes back, as hits that read the
/// clock before taking the lock, or a clock that jumps, deliver them.
fn ops() -> impl Strategy<Value = Vec<(u8, i64, u32)>> {
    let step = prop_oneof![
        4 => 0..=1_000_i64,
        4 => 0..=400_000_000_i64,
        1 => Just(1_000_000_000),
        2 => -400_000_000..=0_i64,
    ];
    prop::collection::vec((0..6_u8, step, 0..=4_u32), 1..80)
}

/// Room for 3 keys in one shard.
const CAP: usize = 3;

/// Runs random hits and resets on a store with room for [`CAP`] keys and on
/// a plain map with no cap. Every answer must match the map's, and the store
/// may refuse a key only if it was idle and at least [`CAP`] other keys are
/// active: it never forgets an active key, and never refuses one when an
/// idle key could make room.
///
/// Whether a model key is active comes from the model's own state (its
/// reset time is not zero), not from the store's idle-time arithmetic.
fn capped_store_agrees_with_a_plain_map<A: Stored>(
    quota: &Quota,
    ops: &[(u8, u64, u32, bool)],
) -> Result<(), TestCaseError> {
    let store = store::<A>(CAP, 1);
    let mut model: HashMap<u8, [A::State; MAX_RATES]> = HashMap::new();
    let mut now = 0;
    for &(key, advance, cost, reset) in ops {
        now += advance;
        if reset {
            store.reset(&key);
            model.remove(&key);
            continue;
        }
        let at = Timestamp::from_nanos(now);
        let reset_after = |states: &[A::State; MAX_RATES]| match check_quota::<A, MAX_RATES>(
            quota, states, at, 0,
        )
        .0
        {
            Outcome::Allowed(stats) | Outcome::Denied { stats, .. } => stats.reset_after,
        };
        let was_idle = model
            .get(&key)
            .is_none_or(|states| reset_after(states).is_zero());
        let states = model
            .entry(key)
            .or_insert_with(|| std::array::from_fn(|_| A::State::default()));
        let expected = hit_quota::<A, MAX_RATES>(quota, states, at, cost);
        match store.hit(quota, &key, at, cost) {
            Hit::Full { retry_after } => {
                prop_assert!(was_idle, "active key {} refused at {}", key, now);
                prop_assert!(
                    matches!(expected, Outcome::Allowed(summary) if !summary.reset_after.is_zero()),
                    "refused a hit that would store nothing: {:?}",
                    expected
                );
                model.remove(&key);
                let active: Vec<Duration> = model
                    .values()
                    .map(reset_after)
                    .filter(|wait| !wait.is_zero())
                    .collect();
                prop_assert!(active.len() >= CAP, "refused with room at {}", now);
                let wait = active.iter().min().copied().unwrap_or_default();
                prop_assert!(
                    retry_after <= wait,
                    "retry after {:?}, but a key is idle in {:?}",
                    retry_after,
                    wait
                );
            }
            hit => {
                prop_assert_eq!(
                    hit,
                    Hit::from(expected),
                    "key {} at {} cost {}",
                    key,
                    now,
                    cost
                );
            }
        }
    }
    Ok(())
}

fn capped_ops() -> impl Strategy<Value = Vec<(u8, u64, u32, bool)>> {
    let advance = prop_oneof![0..=1_000_u64, 0..=400_000_000_u64, Just(1_000_000_000)];
    let reset = prop::bool::weighted(0.1);
    prop::collection::vec((0..6_u8, advance, 0..=4_u32, reset), 1..120)
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn gcra_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in ops()) {
        agrees_with_plain_map::<TokenBucket, MAX_RATES>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn sliding_log_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in ops()) {
        agrees_with_plain_map::<SlidingLog, MAX_RATES>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn window_stores_agree_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in ops()) {
        agrees_with_plain_map::<FixedWindow, MAX_RATES>(&quota, &ops)?;
        agrees_with_plain_map::<SlidingWindow, MAX_RATES>(&quota, &ops)?;
    }

    /// Entries sized for one rate, as a single-rate limiter uses.
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn one_rate_stores_agree_with_a_plain_map(ops in ops()) {
        agrees_with_plain_map::<TokenBucket, 1>(&QUOTAS[0], &ops)?;
        agrees_with_plain_map::<SlidingLog, 1>(&QUOTAS[0], &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn capped_gcra_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in capped_ops()) {
        capped_store_agrees_with_a_plain_map::<TokenBucket>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn capped_sliding_log_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in capped_ops()) {
        capped_store_agrees_with_a_plain_map::<SlidingLog>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn capped_window_stores_agree_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in capped_ops()) {
        capped_store_agrees_with_a_plain_map::<FixedWindow>(&quota, &ops)?;
        capped_store_agrees_with_a_plain_map::<SlidingWindow>(&quota, &ops)?;
    }
}

#[test]
fn full_store_refuses_new_keys_until_one_goes_idle() {
    let quota = quota!("1/second");
    let store = store::<TokenBucket>(2, 1);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert!(allowed(store.hit(&quota, &2, at(500), 1)));
    assert_eq!(
        store.hit(&quota, &3, at(600), 1),
        Hit::Full {
            retry_after: Duration::from_millis(400)
        },
        "key 1 goes idle at 1 s"
    );
    assert_eq!(store.full_hits(), 1);
    // Known keys keep working while the store is full.
    assert!(!allowed(store.hit(&quota, &1, at(600), 1)));
    // Key 1 is idle at 1 s, so key 3 takes its place.
    assert!(allowed(store.hit(&quota, &3, at(1_000), 1)));
    assert!(
        !allowed(store.hit(&quota, &2, at(1_000), 1)),
        "key 2 is still active"
    );
}

#[test]
fn evicting_an_idle_key_changes_no_decision() {
    let quota = quota!("2/second");
    let store = store::<TokenBucket>(1, 1);
    assert!(allowed(store.hit(&quota, &1, at(0), 2)));
    assert!(
        allowed(store.hit(&quota, &2, at(1_000), 1)),
        "key 1 is idle and evicted"
    );
    // Key 1 comes back as a never-hit key, which is exactly what it was.
    assert_eq!(
        store.hit(&quota, &1, at(1_000), 2),
        Hit::Full {
            retry_after: Duration::from_millis(500)
        }
    );
    assert!(allowed(store.hit(&quota, &1, at(1_500), 2)));
}

#[test]
fn peeks_and_denied_hits_store_nothing() {
    let quota = quota!("2/second");
    let store = store::<TokenBucket>(1, 1);
    assert!(
        allowed(store.hit(&quota, &1, at(0), 0)),
        "a peek on an unknown key"
    );
    assert!(matches!(
        store.hit(&quota, &1, at(0), 3),
        Hit::Denied {
            retry: Retry::Never,
            ..
        }
    ));
    assert!(
        allowed(store.hit(&quota, &2, at(0), 1)),
        "neither the peek nor the denial took the only slot"
    );
}

#[test]
fn reset_forgets_a_key() {
    let quota = quota!("1/minute");
    let store = store::<TokenBucket>(10, 2);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert!(!allowed(store.hit(&quota, &1, at(0), 1)));
    store.reset(&1);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
}

#[test]
fn concurrent_hits_on_one_key_never_over_admit() {
    let quota = quota!("100/minute");
    let store = Arc::new(MemoryStore::<u8, TokenBucket>::new(
        NonZeroUsize::new(1_000).unwrap(),
    ));
    let admitted = Arc::new(AtomicU32::new(0));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let (store, admitted) = (Arc::clone(&store), Arc::clone(&admitted));
            scope.spawn(move || {
                for _ in 0..50 {
                    if allowed(store.hit(&quota, &7, at(0), 1)) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    assert_eq!(
        admitted.load(Ordering::Relaxed),
        100,
        "400 attempts at one instant, burst of 100"
    );
}

#[test]
fn shard_capacities_add_up_to_max_keys() {
    let store = store::<TokenBucket>(10, 4);
    let capacities: Vec<usize> = store
        .shards
        .iter()
        .map(|shard| shard.0.read().unwrap().capacity)
        .collect();
    assert_eq!(capacities, [3, 3, 2, 2]);
}

#[test]
fn a_sweep_that_frees_nothing_keeps_the_cap() {
    let quota = quota!("1/second");
    let store = store::<TokenBucket>(1, 1);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    // Key 1 is hit again after going idle, so the shard's earliest idle time
    // (1 s) is now stale; key 1 is really idle at 2.5 s.
    assert!(allowed(store.hit(&quota, &1, at(1_500), 1)));
    assert_eq!(
        store.hit(&quota, &2, at(1_600), 1),
        Hit::Full {
            retry_after: Duration::from_millis(900)
        }
    );
    assert_eq!(
        store.hit(&quota, &2, at(1_700), 1),
        Hit::Full {
            retry_after: Duration::from_millis(800)
        },
        "the second refusal skips the sweep: nothing can be idle before 2.5 s"
    );
    assert_eq!(store.full_hits(), 2);
    assert!(
        allowed(store.hit(&quota, &2, at(3_000), 1)),
        "well past key 1's idle time"
    );
}

#[test]
fn resets_keep_the_expiry_heap_bounded() {
    let quota = quota!("1/hour");
    let store = store::<TokenBucket>(4, 1);
    for _ in 0..1_000 {
        assert!(allowed(store.hit(&quota, &1, at(0), 1)));
        store.reset(&1);
    }
    let expiry = store.shards[0].0.read().unwrap().expiry.len();
    assert!(expiry <= 16, "{expiry} items for an empty shard");
}

#[test]
#[should_panic(expected = "a quota of 2 rates in a store sized for 1")]
fn more_rates_than_the_store_holds_panics() {
    let store = MemoryStore::<u8, TokenBucket, 1>::with_shards(NonZeroUsize::new(4).unwrap(), 1);
    store.hit(&quota!("2/second; 5/minute"), &1, at(0), 1);
}

fn store_when_full(max_keys: usize, policy: WhenFull) -> MemoryStore<u8, TokenBucket> {
    store::<TokenBucket>(max_keys, 1).when_full(policy)
}

#[test]
fn evict_nearest_idle_takes_the_key_nearest_to_idle() {
    let quota = quota!("1/second burst 2");
    // Both keys go idle within the window; only the nearest may go.
    let store = store_when_full(
        2,
        WhenFull::EvictNearestIdle {
            within: Duration::from_millis(1_500),
        },
    );
    assert!(allowed(store.hit(&quota, &1, at(0), 1)), "idle at 1 s");
    // Hit again before going idle: key 1's expiry item (1 s) is now stale,
    // and it is really idle at 2 s.
    assert!(allowed(store.hit(&quota, &1, at(500), 1)));
    assert!(allowed(store.hit(&quota, &2, at(600), 1)), "idle at 1.6 s");
    assert!(allowed(store.hit(&quota, &3, at(900), 1)), "evicts key 2");
    assert!(
        !allowed(store.hit(&quota, &1, at(900), 1)),
        "key 1 was kept, so its limit still holds"
    );
    assert_eq!(store.full_hits(), 1);
}

#[test]
fn evict_nearest_idle_refuses_keys_further_from_idle() {
    let quota = quota!("1/second");
    let store = store_when_full(
        1,
        WhenFull::EvictNearestIdle {
            within: Duration::from_millis(100),
        },
    );
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert_eq!(
        store.hit(&quota, &2, at(800), 1),
        Hit::Full {
            retry_after: Duration::from_millis(200)
        },
        "key 1 is 200 ms from idle, beyond the 100 ms allowed"
    );
    assert!(
        allowed(store.hit(&quota, &2, at(900), 1)),
        "now key 1 is within 100 ms"
    );
}

#[test]
fn overflow_counts_new_keys_against_one_shared_quota() {
    let quota = quota!("1/second");
    let store = store_when_full(1, WhenFull::Overflow(quota!("2/second")));
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert!(allowed(store.hit(&quota, &2, at(0), 1)), "overflow: 1 of 2");
    assert!(allowed(store.hit(&quota, &3, at(0), 1)), "overflow: 2 of 2");
    assert!(matches!(
        store.hit(&quota, &4, at(0), 1),
        Hit::Denied {
            retry: Retry::After(_),
            ..
        }
    ));
    assert!(
        !allowed(store.hit(&quota, &1, at(0), 1)),
        "the known key keeps its own limit"
    );
    assert_eq!(store.full_hits(), 3);
    assert!(
        allowed(store.hit(&quota, &5, at(1_000), 1)),
        "key 1 went idle, so key 5 is stored normally"
    );
}

#[test]
#[should_panic(expected = "an overflow quota of 2 rates in a store sized for 1")]
fn overflow_quota_with_more_rates_than_the_store_holds_panics() {
    let store = MemoryStore::<u8, TokenBucket, 1>::with_shards(NonZeroUsize::new(4).unwrap(), 1);
    drop(store.when_full(WhenFull::Overflow(quota!("2/second; 5/minute"))));
}

#[test]
fn debug_shows_the_policy_and_full_hits() {
    let store = store_when_full(4, WhenFull::Refuse);
    assert_eq!(
        format!("{store:?}"),
        "MemoryStore { shards: 1, when_full: Refuse, full_hits: 0, .. }"
    );
}

#[test]
fn a_timestamp_older_than_the_last_hit_never_evicts_an_active_key() {
    // Hits can commit out of order: the limiter reads the clock before it
    // takes the lock. The log stores the 900 ms hit at 1000 ms, so key 1
    // stays active until 2000 ms, not 1900 ms.
    let quota = quota!("3/second");
    let store = store::<SlidingLog>(1, 1);
    assert!(allowed(store.hit(&quota, &1, at(500), 1)));
    assert!(allowed(store.hit(&quota, &1, at(1_000), 1)));
    assert!(allowed(store.hit(&quota, &1, at(900), 1)));
    assert!(
        matches!(store.hit(&quota, &2, at(1_950), 1), Hit::Full { .. }),
        "key 1 still has two units in its window"
    );
    let admitted = (0..3)
        .filter(|_| allowed(store.hit(&quota, &1, at(1_960), 1)))
        .count();
    assert_eq!(admitted, 1, "only the 500 ms unit has expired");
}
