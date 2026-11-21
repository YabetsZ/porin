//! Run with `RUSTFLAGS="--cfg loom" cargo test --release --lib loom`.
#![cfg(loom)]

use std::num::NonZeroUsize;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, Ordering};

use super::MemoryStore;
use crate::algorithm::gcra::TokenBucket;
use crate::store::Hit;
use crate::{Timestamp, quota};

/// Two threads race for the last unit. In every interleaving loom can find,
/// exactly one gets it.
#[test]
fn loom_last_unit_goes_to_exactly_one_thread() {
    loom::model(|| {
        let quota = quota!("1/second");
        let store = Arc::new(MemoryStore::<u8, TokenBucket>::with_shards(
            NonZeroUsize::new(4).unwrap(),
            2,
        ));
        let admitted = Arc::new(AtomicU32::new(0));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let (store, admitted) = (Arc::clone(&store), Arc::clone(&admitted));
                loom::thread::spawn(move || {
                    if matches!(store.hit(&quota, &1, Timestamp::ZERO, 1), Hit::Allowed(_)) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(admitted.load(Ordering::Relaxed), 1);
    });
}

/// Races two hits for the last unit of a stored key, under `quota_a` and
/// `quota_b`, and requires exactly one to get it.
fn race_for_last_unit(quota_a: crate::Quota, quota_b: crate::Quota) {
    loom::model(move || {
        let store = Arc::new(MemoryStore::<u8, TokenBucket>::with_shards(
            NonZeroUsize::new(4).unwrap(),
            2,
        ));
        assert!(matches!(
            store.hit(&quota_a, &1, Timestamp::ZERO, 1),
            Hit::Allowed(_)
        ));
        let admitted = Arc::new(AtomicU32::new(0));
        let threads: Vec<_> = [quota_a, quota_b]
            .into_iter()
            .map(|quota| {
                let (store, admitted) = (Arc::clone(&store), Arc::clone(&admitted));
                loom::thread::spawn(move || {
                    if matches!(store.hit(&quota, &1, Timestamp::ZERO, 1), Hit::Allowed(_)) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(admitted.load(Ordering::Relaxed), 1);
    });
}

/// Both hits take the read lock and race on one compare-and-swap.
#[test]
fn loom_shared_hits_race_for_the_last_unit() {
    race_for_last_unit(quota!("2/second"), quota!("2/second"));
}

/// One hit takes the read lock, the other (a two-rate quota with the same
/// first rate) the write lock; they must still see each other's spend.
#[test]
fn loom_shared_and_exclusive_hits_race_for_the_last_unit() {
    race_for_last_unit(quota!("2/second"), quota!("2/second; 100/minute"));
}

/// A full shard: one thread hits the stored key through the read lock
/// while another brings a new key that needs that key's slot. Key 1 is idle
/// at exactly 1 s, so either the hit renews it and the new key is refused,
/// or the new key evicts it first and the hit finds no room. Never both.
#[test]
fn loom_a_read_path_hit_and_an_eviction_never_both_succeed() {
    loom::model(|| {
        let quota = quota!("1/second");
        let store = Arc::new(MemoryStore::<u8, TokenBucket, 1>::with_shards(
            NonZeroUsize::new(1).unwrap(),
            1,
        ));
        assert!(matches!(
            store.hit(&quota, &1, Timestamp::ZERO, 1),
            Hit::Allowed(_)
        ));
        let one_second = Timestamp::from_nanos(1_000_000_000);
        let admitted = Arc::new(AtomicU32::new(0));
        let threads: Vec<_> = [1_u8, 2]
            .into_iter()
            .map(|key| {
                let (store, admitted) = (Arc::clone(&store), Arc::clone(&admitted));
                loom::thread::spawn(move || {
                    if matches!(store.hit(&quota, &key, one_second, 1), Hit::Allowed(_)) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(admitted.load(Ordering::Relaxed), 1);
    });
}
