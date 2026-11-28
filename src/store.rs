//! Where limiter state lives: [`Memory`] (the default), or a Redis server
//! shared by every instance ([`RedisStore`], features `redis-tokio` and
//! `redis-smol`).

pub(crate) mod memory;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) mod redis;

use std::time::Duration;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub use self::redis::breaker::CircuitBreaker;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub use self::redis::event::Event;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub use self::redis::fallback::FailOver;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub use self::redis::{Direct, NoPolicy, RedisStore, RedisStoreBuilder, ViaSentinel};
use crate::Quota;
use crate::algorithm::{Outcome, Retry, Stats};

/// Where a [`Limiter`](crate::Limiter) keeps its state: [`Memory`] or
/// [`RedisStore`]. Sealed.
pub trait Store: sealed::Store {}

pub(crate) mod sealed {
    use crate::Algorithm;

    pub trait Store: 'static {
        /// What a limiter holds for this store.
        type Inner<K, A: Algorithm>;
        /// What a builder holds for this store.
        type Config;
    }

    pub trait FailurePolicy {
        #[expect(private_interfaces, reason = "sealed: outside crates can't name it")]
        fn policy(self) -> super::Policy;
    }
}

/// The in-memory store, private to one process. The default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Memory;

impl sealed::Store for Memory {
    type Inner<K, A: crate::Algorithm> = crate::limiter::MemoryInner<K, A>;
    type Config = crate::limiter::MemoryConfig;
}

impl Store for Memory {}

/// What a remote store's limiter answers when the store fails or doesn't
/// answer in time: [`FailOpen`], [`FailClosed`] or [`Propagate`]. There is
/// no default, because each is wrong for some service. Sealed.
pub trait FailurePolicy: sealed::FailurePolicy + Copy + Send + Sync + 'static {}

/// The failure policies a [`RedisStore`] takes: every [`FailurePolicy`],
/// and [`FailOver`], which only Redis supports. Sealed.
///
/// A store that takes any [`FailurePolicy`] can't be given `FailOver`:
///
/// ```compile_fail
/// use std::num::NonZeroU32;
///
/// fn general<P: porin::store::FailurePolicy>(_policy: P) {}
/// general(porin::store::FailOver::to_memory(NonZeroU32::MIN));
/// ```
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
pub trait RedisFailurePolicy: sealed::FailurePolicy + Copy + Send + Sync + 'static {}

/// Allow the hit when the store fails. The service stays up through a store
/// outage, unprotected. The decision's stats are not
/// [known](crate::Stats::is_known).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FailOpen;

/// Deny the hit when the store fails. Nothing gets through unchecked, so a
/// store outage takes the limited service down with it. The denial reports
/// [`is_store_unavailable`](crate::Denial::is_store_unavailable).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FailClosed;

/// Return the failure as [`Error::Store`](crate::Error::Store) and let the
/// caller decide.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Propagate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Policy {
    Open,
    Closed,
    Propagate,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    FailOver(self::redis::fallback::FailOver),
}

#[expect(private_interfaces, reason = "sealed: outside crates can't name it")]
impl sealed::FailurePolicy for FailOpen {
    fn policy(self) -> Policy {
        Policy::Open
    }
}
#[expect(private_interfaces, reason = "sealed: outside crates can't name it")]
impl sealed::FailurePolicy for FailClosed {
    fn policy(self) -> Policy {
        Policy::Closed
    }
}
#[expect(private_interfaces, reason = "sealed: outside crates can't name it")]
impl sealed::FailurePolicy for Propagate {
    fn policy(self) -> Policy {
        Policy::Propagate
    }
}
impl FailurePolicy for FailOpen {}
impl FailurePolicy for FailClosed {}
impl FailurePolicy for Propagate {}
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl RedisFailurePolicy for FailOpen {}
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl RedisFailurePolicy for FailClosed {}
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl RedisFailurePolicy for Propagate {}

/// What a store does with a new key when it has no room: every key it
/// tracks is still active, and forgetting an active key resets its limit.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use porin::{Limiter, WhenFull, quota};
///
/// // Each user gets 100 a minute. If a flood of new users fills the store,
/// // all the users it can't track share 20 a second between them.
/// let limiter: Limiter<u64> = Limiter::builder()
///     .quota(quota!("100/minute"))
///     .when_full(WhenFull::Overflow(quota!("20/second")))
///     .build();
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum WhenFull {
    /// Refuse the new key until a tracked key goes idle. Never over-admits;
    /// new keys wait during a flood of keys, while known keys keep working.
    #[default]
    Refuse,
    /// Forget the tracked key nearest to going idle, if it goes idle within
    /// `within`; otherwise refuse. That key starts over early, so it may
    /// over-admit by up to `within` worth of refill.
    EvictNearestIdle {
        /// How close to idle a key must be to be forgotten.
        within: Duration,
    },
    /// Count every new key that doesn't fit against this one shared quota.
    ///
    /// The store can't track those keys, so their own quota doesn't apply:
    /// the shared quota is their only limit, and a single key may use all
    /// of it. Keep it small, sized for the total you can afford to serve
    /// untracked clients, not for one client. Their denials report
    /// [`Denial::is_store_full`](crate::Denial::is_store_full). A key whose
    /// slot frees up later starts with its full own quota.
    ///
    /// Entries are sized for the larger of the two quotas' rate counts.
    Overflow(Quota),
}

/// A store's answer to one hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hit {
    Allowed(Stats),
    Denied {
        stats: Stats,
        retry: Retry,
    },
    /// The hit fit its quota, but the store has no room to remember a new
    /// key. It was not recorded. Room frees up after `retry_after`.
    Full {
        retry_after: Duration,
    },
}

impl From<Outcome> for Hit {
    fn from(outcome: Outcome) -> Self {
        match outcome {
            Outcome::Allowed(stats) => Self::Allowed(stats),
            Outcome::Denied { stats, retry } => Self::Denied { stats, retry },
        }
    }
}
