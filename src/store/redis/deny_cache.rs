//! The local deny cache: after Redis denies a key, this instance denies it
//! too until the denial's `retry_after` has passed, without asking Redis.
//!
//! It only ever denies, so it can't over-admit. It is bounded: when a shard
//! is full of live denials, a new one simply isn't cached.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::{PoisonError, RwLock};
use std::time::Duration;

use hashbrown::HashTable;

use crate::Timestamp;
use crate::algorithm::{Retry, Stats};
use crate::limiter::Decision;
use crate::store::Hit;

/// Each shard holds at least this many keys, so a small cache isn't cut
/// into shards so small that one fills up while others are empty.
const MIN_KEYS_PER_SHARD: usize = 64;
const MAX_SHARDS: usize = 64;

pub(super) struct DenyCache {
    shards: Box<[RwLock<Shard>]>,
    /// A randomly seeded `SipHash`, so attacker-chosen keys can't be
    /// crafted to collide.
    hasher: RandomState,
}

struct Shard {
    entries: HashTable<Entry>,
    capacity: usize,
    /// `(until, key hash)`, earliest first: one item per cached denial, so
    /// a full shard finds expired entries without scanning them all. An
    /// item goes stale when its entry is replaced or removed.
    expiry: BinaryHeap<Reverse<(u64, u64)>>,
}

struct Entry {
    /// The key's first Redis key name: it names the limiter, the key, the
    /// algorithm and the quota.
    key: Box<str>,
    /// The denied cost. Smaller costs are denied too; that only denies
    /// more than Redis would.
    cost: u32,
    /// What Redis reported, at `denied_at`.
    stats: Stats,
    denied_at: u64,
    /// Nanoseconds on the store's clock until which the key is denied.
    until: u64,
}

impl DenyCache {
    pub(super) fn new(max_keys: NonZeroUsize) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let shards = cpus
            .saturating_mul(4)
            .min(MAX_SHARDS)
            .min(max_keys.get() / MIN_KEYS_PER_SHARD)
            .max(1);
        let base = max_keys.get().checked_div(shards).unwrap_or(0);
        let extra = max_keys.get().checked_rem(shards).unwrap_or(0);
        Self {
            shards: (0..shards)
                .map(|index| {
                    RwLock::new(Shard {
                        entries: HashTable::new(),
                        capacity: base.saturating_add(usize::from(index < extra)),
                        expiry: BinaryHeap::new(),
                    })
                })
                .collect(),
            hasher: RandomState::new(),
        }
    }

    /// The cached denial of `cost` units for `key` at `now`, if Redis denied
    /// that key as many units or more and its `retry_after` hasn't passed.
    /// The stats and `retry_after` are Redis's, less the time since.
    pub(super) fn denial(&self, key: &str, cost: u32, now: Timestamp) -> Option<Decision> {
        let hash = self.hasher.hash_one(key);
        let now = now.as_nanos();
        // A panic can't leave an entry half written: each is replaced whole.
        let shard = self
            .shard(hash)
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = shard
            .entries
            .find(hash, |entry| &*entry.key == key)
            .filter(|entry| cost <= entry.cost && now < entry.until)?;
        let elapsed = Duration::from_nanos(now.saturating_sub(entry.denied_at));
        let stats = Stats {
            remaining: entry.stats.remaining,
            reset_after: entry.stats.reset_after.saturating_sub(elapsed),
            limiting: entry.stats.limiting,
            limiting_reset: entry.stats.limiting_reset.saturating_sub(elapsed),
        };
        Some(Decision::from_hit(Hit::Denied {
            stats,
            retry: Retry::After(Duration::from_nanos(entry.until.saturating_sub(now))),
        }))
    }

    /// Records that Redis denied `cost` units for `key` at `now`, to be
    /// retried after `retry_after`. Not cached if the shard is full of live
    /// denials.
    pub(super) fn insert(
        &self,
        key: &str,
        cost: u32,
        stats: Stats,
        retry_after: Duration,
        now: Timestamp,
    ) {
        let hash = self.hasher.hash_one(key);
        let now = now.as_nanos();
        let until = now.saturating_add(u64::try_from(retry_after.as_nanos()).unwrap_or(u64::MAX));
        let entry = Entry {
            key: key.into(),
            cost,
            stats,
            denied_at: now,
            until,
        };
        let mut shard = self
            .shard(hash)
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let shard = &mut *shard;
        if let Some(cached) = shard.entries.find_mut(hash, |cached| &*cached.key == key) {
            // A costlier hit, or one already on its way when the entry was
            // cached, reached Redis and was denied: the newer denial
            // replaces the old one. It may be weaker (a smaller cost or an
            // earlier end), which only sends more hits to Redis.
            *cached = entry;
        } else {
            if shard.entries.len() >= shard.capacity {
                shard.drop_expired(now);
            }
            if shard.entries.len() >= shard.capacity {
                return;
            }
            shard
                .entries
                .insert_unique(hash, entry, |cached| self.hasher.hash_one(&*cached.key));
        }
        shard.expiry.push(Reverse((until, hash)));
        if shard.expiry.len() > shard.capacity.saturating_mul(2).saturating_add(16) {
            shard.rebuild_expiry(&self.hasher);
        }
    }

    /// Forgets the cached denial of `key`, if any.
    pub(super) fn remove(&self, key: &str) {
        let hash = self.hasher.hash_one(key);
        let mut shard = self
            .shard(hash)
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Ok(entry) = shard.entries.find_entry(hash, |entry| &*entry.key == key) {
            entry.remove();
        }
    }

    fn shard(&self, hash: u64) -> &RwLock<Shard> {
        // Maps the hash's low 32 bits onto the shards with a multiply
        // instead of a division; the table inside picks slots from other
        // bits.
        let low = hash & u64::from(u32::MAX);
        let count = u64::try_from(self.shards.len()).unwrap_or(u64::MAX);
        let index = usize::try_from(low.saturating_mul(count) >> 32).unwrap_or(0);
        self.shards.get(index).unwrap_or_else(|| {
            // `low < 2^32`, so `index < shards.len()`, and there is always
            // at least one shard.
            unreachable!("shard index {index} out of {} shards", self.shards.len())
        })
    }
}

impl Shard {
    /// Removes every entry whose denial ended by `now`.
    fn drop_expired(&mut self, now: u64) {
        while let Some(&Reverse((until, hash))) = self.expiry.peek() {
            if until > now {
                break;
            }
            self.expiry.pop();
            // Any entry with this hash and time has expired, whichever key
            // it is; a replaced entry's old item matches nothing.
            if let Ok(entry) = self.entries.find_entry(hash, |entry| entry.until == until) {
                entry.remove();
            }
        }
    }

    /// Drops stale items, one per live entry left.
    fn rebuild_expiry(&mut self, hasher: &RandomState) {
        self.expiry = self
            .entries
            .iter()
            .map(|entry| Reverse((entry.until, hasher.hash_one(&*entry.key))))
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    fn at(nanos: u64) -> Timestamp {
        Timestamp::from_nanos(nanos)
    }

    fn stats() -> Stats {
        Stats {
            remaining: 3,
            reset_after: Duration::from_secs(10),
            limiting: 0,
            limiting_reset: Duration::from_secs(10),
        }
    }

    fn cache(max_keys: usize) -> DenyCache {
        DenyCache::new(NonZeroUsize::new(max_keys).unwrap())
    }

    #[test]
    fn denies_until_retry_after_exactly() {
        let cache = cache(100);
        cache.insert("k", 5, stats(), SECOND, at(1_000));
        assert!(cache.denial("k", 5, at(1_000)).is_some());
        let last = cache.denial("k", 5, at(1_000_000_999)).unwrap();
        let Decision::Denied(denial) = last else {
            panic!("a cached answer is a denial");
        };
        assert_eq!(denial.retry_after(), Some(Duration::from_nanos(1)));
        assert!(denial.stats().is_known());
        assert!(
            cache.denial("k", 5, at(1_000_001_000)).is_none(),
            "retry_after has passed"
        );
    }

    #[test]
    fn stats_count_down_from_the_denial() {
        let cache = cache(100);
        cache.insert("k", 1, stats(), Duration::from_secs(4), at(0));
        let Some(Decision::Denied(denial)) = cache.denial("k", 1, at(3_000_000_000)) else {
            panic!("cached");
        };
        assert_eq!(denial.retry_after(), Some(SECOND));
        assert_eq!(denial.stats().reset_after(), Duration::from_secs(7));
        assert_eq!(denial.stats().remaining(), 3);
    }

    #[test]
    fn denies_smaller_costs_but_not_larger_or_other_keys() {
        let cache = cache(100);
        cache.insert("k", 5, stats(), SECOND, at(0));
        assert!(cache.denial("k", 1, at(0)).is_some());
        assert!(cache.denial("k", 6, at(0)).is_none());
        assert!(cache.denial("j", 1, at(0)).is_none());
    }

    #[test]
    fn remove_forgets_the_key() {
        let cache = cache(100);
        cache.insert("k", 1, stats(), SECOND, at(0));
        cache.remove("k");
        assert!(cache.denial("k", 1, at(0)).is_none());
    }

    #[test]
    fn a_full_cache_drops_expired_entries_or_caches_nothing() {
        let cache = cache(1);
        cache.insert("a", 1, stats(), SECOND, at(0));
        cache.insert("b", 1, stats(), SECOND, at(0));
        assert!(cache.denial("a", 1, at(0)).is_some());
        assert!(cache.denial("b", 1, at(0)).is_none(), "full: not cached");
        cache.insert("b", 1, stats(), SECOND, at(2_000_000_000));
        assert!(
            cache.denial("b", 1, at(2_000_000_000)).is_some(),
            "a's expired entry made room"
        );
    }

    #[test]
    fn a_replaced_entry_keeps_the_newer_denial() {
        let cache = cache(1);
        cache.insert("k", 1, stats(), SECOND, at(0));
        cache.insert("k", 2, stats(), Duration::from_secs(3), at(0));
        // The first denial's expiry item is stale; it must not remove the
        // newer entry when it comes up.
        cache.insert("x", 1, stats(), SECOND, at(2_000_000_000));
        assert!(cache.denial("k", 2, at(2_000_000_000)).is_some());
    }

    #[test]
    fn stale_items_stay_bounded() {
        let cache = cache(1);
        for _ in 0..1_000 {
            cache.insert("k", 1, stats(), SECOND, at(0));
        }
        let shard = cache.shards.first().unwrap().read().unwrap();
        assert!(shard.expiry.len() <= 18, "{}", shard.expiry.len());
    }
}
