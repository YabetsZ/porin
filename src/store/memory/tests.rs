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
use crate::algorithm::gcra::Gcra;
use crate::algorithm::sliding_log::SlidingLog;
use crate::algorithm::{Outcome, Retry, hit_quota};
use crate::quota::MAX_RATES;
use crate::store::Hit;
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
    ops: &[(u8, u64, u32)],
) -> Result<(), TestCaseError> {
    let store = MemoryStore::<u8, A, N>::with_shards(NonZeroUsize::new(1_000).unwrap(), 3);
    let mut model: HashMap<u8, [A::State; MAX_RATES]> = HashMap::new();
    let mut now = 0;
    for &(key, advance, cost) in ops {
        now += advance;
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

fn ops() -> impl Strategy<Value = Vec<(u8, u64, u32)>> {
    let advance = prop_oneof![0..=1_000_u64, 0..=400_000_000_u64, Just(1_000_000_000)];
    prop::collection::vec((0..6_u8, advance, 0..=4_u32), 1..80)
}

/// Room for 3 keys in one shard.
const CAP: usize = 3;

/// Runs random hits and resets on a store with room for [`CAP`] keys and on
/// a plain map with no cap. Every answer must match the map's, and the store
/// may refuse a key only if it was idle and at least [`CAP`] other keys are
/// active: it never forgets an active key, and never refuses one when an
/// idle key could make room.
fn capped_store_agrees_with_a_plain_map<A: Stored>(
    quota: &Quota,
    ops: &[(u8, u64, u32, bool)],
) -> Result<(), TestCaseError> {
    let store = store::<A>(CAP, 1);
    let mut model: HashMap<u8, ([A::State; MAX_RATES], u64)> = HashMap::new();
    let mut now = 0;
    for &(key, advance, cost, reset) in ops {
        now += advance;
        if reset {
            store.reset(&key);
            model.remove(&key);
            continue;
        }
        let at = Timestamp::from_nanos(now);
        let was_idle = model.get(&key).is_none_or(|&(_, idle)| idle <= now);
        let (states, _) = model
            .entry(key)
            .or_insert_with(|| (std::array::from_fn(|_| A::State::default()), 0));
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
                let active: Vec<u64> = model
                    .values()
                    .map(|&(_, idle)| idle)
                    .filter(|&idle| idle > now)
                    .collect();
                prop_assert!(active.len() >= CAP, "refused with room at {}", now);
                let wait = active.iter().min().map_or(0, |idle| idle - now);
                prop_assert!(
                    retry_after <= Duration::from_nanos(wait),
                    "retry after {:?}, but a key is idle in {} ns",
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
                if let Outcome::Allowed(stats) = expected {
                    if let Some(entry) = model.get_mut(&key) {
                        entry.1 = now + u64::try_from(stats.reset_after.as_nanos()).unwrap();
                    }
                }
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
        agrees_with_plain_map::<Gcra, MAX_RATES>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn sliding_log_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in ops()) {
        agrees_with_plain_map::<SlidingLog, MAX_RATES>(&quota, &ops)?;
    }

    /// Entries sized for one rate, as a single-rate limiter uses.
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn one_rate_stores_agree_with_a_plain_map(ops in ops()) {
        agrees_with_plain_map::<Gcra, 1>(&QUOTAS[0], &ops)?;
        agrees_with_plain_map::<SlidingLog, 1>(&QUOTAS[0], &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn capped_gcra_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in capped_ops()) {
        capped_store_agrees_with_a_plain_map::<Gcra>(&quota, &ops)?;
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn capped_sliding_log_store_agrees_with_a_plain_map(quota in prop::sample::select(&QUOTAS[..]), ops in capped_ops()) {
        capped_store_agrees_with_a_plain_map::<SlidingLog>(&quota, &ops)?;
    }
}

#[test]
fn full_store_refuses_new_keys_until_one_goes_idle() {
    let quota = quota!("1/second");
    let store = store::<Gcra>(2, 1);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert!(allowed(store.hit(&quota, &2, at(500), 1)));
    assert_eq!(
        store.hit(&quota, &3, at(600), 1),
        Hit::Full {
            retry_after: Duration::from_millis(400)
        },
        "key 1 goes idle at 1 s"
    );
    assert_eq!(store.full_refusals(), 1);
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
    let store = store::<Gcra>(1, 1);
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
    let store = store::<Gcra>(1, 1);
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
    let store = store::<Gcra>(10, 2);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
    assert!(!allowed(store.hit(&quota, &1, at(0), 1)));
    store.reset(&1);
    assert!(allowed(store.hit(&quota, &1, at(0), 1)));
}

#[test]
fn concurrent_hits_on_one_key_never_over_admit() {
    let quota = quota!("100/minute");
    let store = Arc::new(MemoryStore::<u8, Gcra>::new(
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
    let store = store::<Gcra>(10, 4);
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
    let store = store::<Gcra>(1, 1);
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
    assert_eq!(store.full_refusals(), 2);
    assert!(
        allowed(store.hit(&quota, &2, at(3_000), 1)),
        "well past key 1's idle time"
    );
}

#[test]
fn resets_keep_the_expiry_heap_bounded() {
    let quota = quota!("1/hour");
    let store = store::<Gcra>(4, 1);
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
    let store = MemoryStore::<u8, Gcra, 1>::with_shards(NonZeroUsize::new(4).unwrap(), 1);
    store.hit(&quota!("2/second; 5/minute"), &1, at(0), 1);
}
