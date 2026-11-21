//! How the memory store keeps one key's states.
//!
//! Most algorithms keep plain states and are hit under the shard's write
//! lock. The token bucket's state is one word, so it lives in atomics: a
//! single-rate quota is then hit under the shared read lock with one
//! compare-and-swap, and hits on the same key don't queue behind each other.

use std::sync::atomic::Ordering;

use crate::algorithm::fixed_window::FixedWindow;
use crate::algorithm::gcra::{GcraState, TokenBucket};
use crate::algorithm::sliding_log::SlidingLog;
use crate::algorithm::sliding_window::SlidingWindow;
use crate::algorithm::{Algorithm, Check, Outcome, check_quota, hit_quota};
use crate::sync::AtomicU64;
use crate::{Quota, Rate, Timestamp};

/// An algorithm the memory store can hold, and how it holds its states.
pub(crate) trait Stored: Algorithm + Sized {
    /// States for up to `N` rates.
    type Slot<const N: usize>: Slot;
}

/// One key's states for every rate of a quota.
pub(crate) trait Slot {
    /// Whether [`Slot::hit_shared`] serves single-rate quotas. If false, the
    /// store doesn't try it and goes straight to the write lock.
    const SHARED: bool;

    /// A never-hit key.
    fn new() -> Self;

    /// Spends `cost` against every rate of `quota`, all or nothing.
    fn hit(&mut self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome;

    /// What [`Slot::hit`] would answer, without recording anything.
    fn test(&self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome;

    /// Spends `cost` against a single-rate quota through shared access, or
    /// returns `None` if the slot needs exclusive access.
    fn hit_shared(&self, rate: Rate, now: Timestamp, cost: u32) -> Option<Outcome>;

    /// From this time on the slot equals a never-hit key, if not hit again.
    fn idle_at(&self) -> u64;
}

/// Plain states, changed only under the write lock.
pub(crate) struct Locked<A: Algorithm, const N: usize> {
    states: [A::State; N],
    idle_at: u64,
}

impl<A: Algorithm, const N: usize> Slot for Locked<A, N> {
    const SHARED: bool = false;

    fn new() -> Self {
        Self {
            states: std::array::from_fn(|_| A::State::default()),
            idle_at: 0,
        }
    }

    fn hit(&mut self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome {
        let outcome = hit_quota::<A, N>(quota, &mut self.states, now, cost);
        if let Outcome::Allowed(_) = outcome {
            // From the stored states, not from `now`: a hit recorded with an
            // older timestamp than the last can be stored at the later time.
            self.idle_at = quota
                .rates()
                .iter()
                .zip(&self.states)
                .map(|(rate, state)| A::idle_at(*rate, state))
                .max()
                .unwrap_or(0);
        }
        outcome
    }

    fn test(&self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome {
        check_quota::<A, N>(quota, &self.states, now, cost).0
    }

    fn hit_shared(&self, _: Rate, _: Timestamp, _: u32) -> Option<Outcome> {
        None
    }

    fn idle_at(&self) -> u64 {
        self.idle_at
    }
}

/// Token bucket states as atomic words. The idle time is the latest TAT,
/// so it needs no field of its own.
///
/// Orderings are `Relaxed` throughout. Each word is the whole state of one
/// rate and guards no other data, so a compare-and-swap on it is atomic in
/// any ordering. Exclusive access always goes through the shard's write lock,
/// whose acquire sees every update made under earlier read locks.
pub(crate) struct Atomic<const N: usize> {
    tats: [AtomicU64; N],
}

impl<const N: usize> Atomic<N> {
    fn states(&self) -> [GcraState; N] {
        self.tats
            .each_ref()
            .map(|tat| GcraState::from_word(tat.load(Ordering::Relaxed)))
    }
}

impl<const N: usize> Slot for Atomic<N> {
    const SHARED: bool = true;

    fn new() -> Self {
        Self {
            tats: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn hit(&mut self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome {
        let mut states = self.states();
        let outcome = hit_quota::<TokenBucket, N>(quota, &mut states, now, cost);
        if let Outcome::Allowed(_) = outcome {
            for (tat, state) in self.tats.iter().zip(states) {
                tat.store(state.to_word(), Ordering::Relaxed);
            }
        }
        outcome
    }

    fn test(&self, quota: &Quota, now: Timestamp, cost: u32) -> Outcome {
        check_quota::<TokenBucket, N>(quota, &self.states(), now, cost).0
    }

    fn hit_shared(&self, rate: Rate, now: Timestamp, cost: u32) -> Option<Outcome> {
        let tat = self.tats.first()?;
        let mut current = tat.load(Ordering::Relaxed);
        loop {
            match TokenBucket::check(rate, &GcraState::from_word(current), now, cost) {
                Check::Allowed { stats, .. } if cost == 0 => {
                    return Some(Outcome::Allowed(stats));
                }
                Check::Allowed { commit, stats } => {
                    match tat.compare_exchange_weak(
                        current,
                        commit.to_word(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Outcome::Allowed(stats)),
                        // Another hit got in first: decide again on its result.
                        Err(actual) => current = actual,
                    }
                }
                Check::Denied { stats, retry } => return Some(Outcome::Denied { stats, retry }),
            }
        }
    }

    fn idle_at(&self) -> u64 {
        self.states()
            .iter()
            .map(|state| state.idle_at())
            .max()
            .unwrap_or(0)
    }
}

impl Stored for TokenBucket {
    type Slot<const N: usize> = Atomic<N>;
}

impl Stored for FixedWindow {
    type Slot<const N: usize> = Locked<Self, N>;
}

impl Stored for SlidingWindow {
    type Slot<const N: usize> = Locked<Self, N>;
}

impl Stored for SlidingLog {
    type Slot<const N: usize> = Locked<Self, N>;
}
