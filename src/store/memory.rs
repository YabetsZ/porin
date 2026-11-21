//! The in-memory store: a sharded map from key to per-rate state.
//!
//! Each shard is a hash table behind its own lock, so hits on different keys
//! rarely wait for each other. A hit checks and commits all of a quota's
//! rates under the shard's write lock, which makes multi-rate quotas
//! all-or-nothing. A single-rate token bucket on a known key needs only the
//! read lock and one compare-and-swap; see [`slot`].
//!
//! The number of tracked keys is capped. When a shard is full, keys that are
//! back to their never-hit state are evicted, earliest first; removing them
//! changes no decision. If every key in the shard is still active, the
//! store's [`WhenFull`] policy decides; by default the new key is refused
//! rather than evicting an active one, because forgetting an active key
//! would reset its limit and over-admit.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::time::Duration;

use hashbrown::HashTable;

use self::slot::{Slot, Stored};
use super::{Hit, WhenFull};
use crate::algorithm::Outcome;
use crate::quota::MAX_RATES;
use crate::sync::{AtomicU64, Mutex, RwLock};
use crate::{Quota, Timestamp};

mod slot;

/// Shards per available CPU, up to [`MAX_SHARDS`].
const SHARDS_PER_CPU: usize = 4;
const MAX_SHARDS: usize = 64;

/// Holds states for up to `N` rates per key. A store for a single-rate
/// quota sets `N` to 1, so each entry is a key and one word.
pub(crate) struct MemoryStore<K, A: Stored, const N: usize = MAX_RATES> {
    shards: Shards<K, A::Slot<N>>,
    /// Hashes each key once, for both the shard and the slot inside it. A
    /// randomly seeded `SipHash`, so attacker-chosen keys can't be crafted
    /// to collide.
    hasher: RandomState,
    when_full: WhenFull,
    /// The shared state for [`WhenFull::Overflow`].
    overflow: Option<Mutex<A::Slot<N>>>,
    /// New keys that found their shard full of active keys.
    full_hits: AtomicU64,
}

type Shards<K, S> = Box<[CacheLine<RwLock<Shard<K, S>>>]>;

/// Gives each shard its own cache lines. Without it, several shard locks
/// share a line, and threads locking different shards still slow each other
/// down by bouncing that line between cores (false sharing). 128 bytes
/// covers CPUs that fetch cache lines in adjacent pairs.
#[repr(align(128))]
struct CacheLine<T>(T);

struct Shard<K, S> {
    entries: HashTable<Entry<K, S>>,
    capacity: usize,
    /// `(idle time, key hash)`, earliest first: one item pushed per stored
    /// entry, so a full shard finds idle entries without scanning them all.
    /// Items go stale when an entry is hit again or reset; the sweep checks
    /// each item against the entry before evicting.
    expiry: BinaryHeap<Reverse<(u64, u64)>>,
}

struct Entry<K, S> {
    key: K,
    slot: S,
}

impl<K: Hash + Eq + Clone, A: Stored, const N: usize> MemoryStore<K, A, N> {
    /// A store that tracks at most `max_keys` keys, split evenly across shards.
    pub(crate) fn new(max_keys: NonZeroUsize) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let shards = cpus
            .saturating_mul(SHARDS_PER_CPU)
            .clamp(1, MAX_SHARDS)
            .min(max_keys.get());
        Self::with_shards(max_keys, shards)
    }

    fn with_shards(max_keys: NonZeroUsize, shards: usize) -> Self {
        let shards = shards.clamp(1, max_keys.get());
        let base = max_keys.get().checked_div(shards).unwrap_or(0);
        let extra = max_keys.get().checked_rem(shards).unwrap_or(0);
        let shards = (0..shards)
            .map(|index| {
                CacheLine(RwLock::new(Shard {
                    entries: HashTable::new(),
                    capacity: base.saturating_add(usize::from(index < extra)),
                    expiry: BinaryHeap::new(),
                }))
            })
            .collect();
        Self {
            shards,
            hasher: RandomState::new(),
            when_full: WhenFull::Refuse,
            overflow: None,
            full_hits: AtomicU64::new(0),
        }
    }

    /// Sets what happens to new keys when a shard has no room.
    ///
    /// # Panics
    ///
    /// If an overflow quota has more than `N` rates.
    pub(crate) fn when_full(mut self, policy: WhenFull) -> Self {
        self.overflow = match policy {
            WhenFull::Overflow(quota) => {
                assert!(
                    quota.rates().len() <= N,
                    "an overflow quota of {} rates in a store sized for {N}",
                    quota.rates().len()
                );
                Some(Mutex::new(A::Slot::<N>::new()))
            }
            WhenFull::Refuse | WhenFull::EvictNearestIdle { .. } => None,
        };
        self.when_full = policy;
        self
    }

    /// Spends `cost` units for `key` at `now`, if every rate allows it. A
    /// cost of 0 reports the key's stats without storing anything new.
    ///
    /// # Panics
    ///
    /// If `quota` has more than `N` rates.
    pub(crate) fn hit(&self, quota: &Quota, key: &K, now: Timestamp, cost: u32) -> Hit {
        // Callers size `N` from their quota, so this never fires. Checking
        // only the first `N` rates instead would over-admit.
        assert!(
            quota.rates().len() <= N,
            "a quota of {} rates in a store sized for {N}",
            quota.rates().len()
        );
        let hash = self.hasher.hash_one(key);
        let lock = &self.shard(hash).0;
        let now_nanos = now.as_nanos();

        if let ([rate], true) = (quota.rates(), A::Slot::<N>::SHARED) {
            // Read guards are never poisoned by a panicking reader.
            let shard = lock.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(outcome) = shard
                .entries
                .find(hash, |entry| entry.key == *key)
                .and_then(|entry| entry.slot.hit_shared(*rate, now, cost))
            {
                return outcome.into();
            }
        }

        // A panic while holding the write lock can only come from a user
        // `Hash` or `Eq` impl during lookup, before any state changes, so
        // the data is still consistent and the poison can be ignored.
        let mut shard = lock.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = shard.entries.find_mut(hash, |entry| entry.key == *key) {
            return entry.slot.hit(quota, now, cost).into();
        }

        // A key the store doesn't know is a never-hit key. Decide first and
        // store it only if the hit left something to remember.
        let mut slot = A::Slot::<N>::new();
        let outcome = slot.hit(quota, now, cost);
        let Outcome::Allowed(stats) = outcome else {
            return outcome.into();
        };
        if stats.reset_after.is_zero() {
            return outcome.into();
        }
        if !shard.make_room(now_nanos, now_nanos, &self.hasher) {
            // Relaxed: a statistics counter that orders nothing else.
            self.full_hits.fetch_add(1, Ordering::Relaxed);
            if let WhenFull::EvictNearestIdle { within } = self.when_full {
                let until =
                    now_nanos.saturating_add(u64::try_from(within.as_nanos()).unwrap_or(u64::MAX));
                if shard.make_room(now_nanos, until, &self.hasher) {
                    return self.insert(&mut shard, hash, key, slot, outcome);
                }
            }
            let retry_after = Duration::from_nanos(shard.next_idle().saturating_sub(now_nanos));
            drop(shard);
            return match (&self.overflow, self.when_full) {
                (Some(overflow), WhenFull::Overflow(overflow_quota)) => overflow
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .hit(&overflow_quota, now, cost)
                    .into(),
                _ => Hit::Full { retry_after },
            };
        }
        self.insert(&mut shard, hash, key, slot, outcome)
    }

    /// Stores a new key whose first hit was allowed, in a shard with room.
    fn insert(
        &self,
        shard: &mut Shard<K, A::Slot<N>>,
        hash: u64,
        key: &K,
        slot: A::Slot<N>,
        outcome: Outcome,
    ) -> Hit {
        shard.expiry.push(Reverse((slot.idle_at(), hash)));
        shard.entries.insert_unique(
            hash,
            Entry {
                key: key.clone(),
                slot,
            },
            |entry| self.hasher.hash_one(&entry.key),
        );
        outcome.into()
    }

    /// Forgets `key`, as if it had never been hit.
    pub(crate) fn reset(&self, key: &K) {
        let hash = self.hasher.hash_one(key);
        let mut shard = self
            .shard(hash)
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Ok(entry) = shard.entries.find_entry(hash, |entry| entry.key == *key) {
            entry.remove();
            shard.drop_stale_expiry(&self.hasher);
        }
    }

    /// New keys so far that found their shard full of active keys, whatever
    /// the [`WhenFull`] policy then did with them.
    pub(crate) fn full_hits(&self) -> u64 {
        self.full_hits.load(Ordering::Relaxed)
    }

    fn shard(&self, hash: u64) -> &CacheLine<RwLock<Shard<K, A::Slot<N>>>> {
        // Maps the hash's low 32 bits onto the shards with a multiply
        // instead of a division. The table inside a shard picks slots from
        // the lowest bits and the highest 7, so the two choices stay
        // independent for any table smaller than 2^26 slots.
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

impl<K, A: Stored, const N: usize> std::fmt::Debug for MemoryStore<K, A, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("shards", &self.shards.len())
            .field("when_full", &self.when_full)
            .field("full_hits", &self.full_hits.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<K: Hash + Eq, S: Slot> Shard<K, S> {
    /// Evicts entries until there is room for one more, and returns false
    /// if it can't. Idle entries go first, in any order; removing them
    /// changes no decision. Then, while room is still short, the entry
    /// nearest to idle goes, if it goes idle by `until`. With `until == now`
    /// only idle entries are evicted.
    fn make_room(&mut self, now: u64, until: u64, hasher: &RandomState) -> bool {
        while self.entries.len() >= self.capacity {
            let Some(&Reverse((idle_at, hash))) = self.expiry.peek() else {
                return false;
            };
            if idle_at > until {
                return false;
            }
            self.expiry.pop();
            // The item names a hash, not a key: rehashing tells apart keys
            // that share a slot group. Sweeps are rare, so the cost is fine.
            let has_hash = |entry: &Entry<K, S>| hasher.hash_one(&entry.key) == hash;
            // An entry is evicted if it is idle, or if this item is current
            // for it: items are lower bounds, so a current item at the top
            // of the heap belongs to the entry nearest to idle.
            let evictable = |entry: &Entry<K, S>| {
                let entry_idle = entry.slot.idle_at();
                (entry_idle <= now || entry_idle <= idle_at) && has_hash(entry)
            };
            if let Ok(victim) = self.entries.find_entry(hash, evictable) {
                victim.remove();
            } else if let Some(next) = self
                .entries
                .iter_hash(hash)
                .filter(|entry| has_hash(entry))
                .map(|entry| entry.slot.idle_at())
                .min()
            {
                // Hit again since the item was pushed: reschedule it. No
                // entry with this hash was evictable, so `next > idle_at`
                // and the item moves down the heap; the loop ends.
                self.expiry.push(Reverse((next, hash)));
            }
        }
        true
    }

    /// The earliest time an entry might go idle, if none is idle now.
    fn next_idle(&self) -> u64 {
        self.expiry
            .peek()
            .map_or(u64::MAX, |&Reverse((idle_at, _))| idle_at)
    }

    /// Resets leave their entries' items behind. Rebuilds the heap once
    /// stale items outnumber live entries, so it stays proportional to the
    /// shard however often keys are reset and stored again.
    fn drop_stale_expiry(&mut self, hasher: &RandomState) {
        let live = self.entries.len();
        if self.expiry.len() <= live.saturating_mul(2).saturating_add(16) {
            return;
        }
        self.expiry = self
            .entries
            .iter()
            .map(|entry| Reverse((entry.slot.idle_at(), hasher.hash_one(&entry.key))))
            .collect();
    }
}

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod tests;
