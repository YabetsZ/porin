//! Hooks for porin's own benchmarks, compiled only with `--cfg porin_bench`.
//! Not part of the API: these wrap crate-private types so the benchmarks
//! can time them without making them public.

use std::hash::Hash;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::algorithm::fixed_window::{FixedWindow, FixedWindowState};
use crate::algorithm::gcra::{Gcra, GcraState};
use crate::algorithm::sliding_log::{SlidingLog, SlidingLogState};
use crate::algorithm::sliding_window::{SlidingWindow, SlidingWindowState};
use crate::algorithm::{Algorithm, Check};
use crate::store::memory::MemoryStore;
use crate::store::{Hit, WhenFull};
use crate::{Quota, Rate, Timestamp};

/// The in-memory store with the token bucket, sized for single-rate quotas.
#[derive(Debug)]
pub struct Store<K>(MemoryStore<K, Gcra, 1>);

impl<K: Hash + Eq + Clone> Store<K> {
    /// A store that tracks at most `max_keys` keys.
    #[must_use]
    pub fn new(max_keys: usize) -> Self {
        Self(MemoryStore::new(
            NonZeroUsize::new(max_keys).unwrap_or(NonZeroUsize::MIN),
        ))
    }

    /// Like [`Store::new`], but a full store forgets the key nearest to
    /// idle if it goes idle within `within`.
    #[must_use]
    pub fn evicting(max_keys: usize, within: Duration) -> Self {
        let Self(store) = Self::new(max_keys);
        Self(store.when_full(WhenFull::EvictNearestIdle { within }))
    }

    /// Like [`Store::new`], but new keys that don't fit share `overflow`.
    ///
    /// # Panics
    ///
    /// If `overflow` has more than one rate.
    #[must_use]
    pub fn overflowing(max_keys: usize, overflow: Quota) -> Self {
        let Self(store) = Self::new(max_keys);
        Self(store.when_full(WhenFull::Overflow(overflow)))
    }

    /// Spends one unit; returns whether it was allowed.
    ///
    /// # Panics
    ///
    /// If `quota` has more than one rate.
    pub fn hit(&self, quota: &Quota, key: &K, now: Timestamp) -> bool {
        matches!(self.0.hit(quota, key, now, 1), Hit::Allowed(_))
    }

    /// Forgets `key`.
    pub fn reset(&self, key: &K) {
        self.0.reset(key);
    }

    /// New keys that found the store full.
    #[must_use]
    pub fn full_hits(&self) -> u64 {
        self.0.full_hits()
    }
}

/// One key's state under one algorithm, for timing the pure math.
#[derive(Debug)]
pub struct AlgorithmState(Inner);

#[derive(Debug)]
enum Inner {
    Gcra(GcraState),
    FixedWindow(FixedWindowState),
    SlidingWindow(SlidingWindowState),
    SlidingLog(SlidingLogState),
}

impl AlgorithmState {
    /// A never-hit state for `gcra`, `fixed_window`, `sliding_window` or
    /// `sliding_log`; anything else means `gcra`.
    #[must_use]
    pub fn new(algorithm: &str) -> Self {
        Self(match algorithm {
            "fixed_window" => Inner::FixedWindow(FixedWindowState::default()),
            "sliding_window" => Inner::SlidingWindow(SlidingWindowState::default()),
            "sliding_log" => Inner::SlidingLog(SlidingLogState::default()),
            _ => Inner::Gcra(GcraState::default()),
        })
    }

    /// Checks and commits one unit; returns whether it was allowed.
    pub fn hit(&mut self, rate: Rate, now: Timestamp) -> bool {
        match &mut self.0 {
            Inner::Gcra(state) => hit::<Gcra>(rate, state, now),
            Inner::FixedWindow(state) => hit::<FixedWindow>(rate, state, now),
            Inner::SlidingWindow(state) => hit::<SlidingWindow>(rate, state, now),
            Inner::SlidingLog(state) => hit::<SlidingLog>(rate, state, now),
        }
    }
}

fn hit<A: Algorithm>(rate: Rate, state: &mut A::State, now: Timestamp) -> bool {
    match A::check(rate, state, now, 1) {
        Check::Allowed { commit, .. } => {
            A::commit(state, commit);
            true
        }
        Check::Denied { .. } => false,
    }
}
