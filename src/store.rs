//! Where limiter state lives.

pub(crate) mod memory;

use std::time::Duration;

use crate::Quota;
use crate::algorithm::{Outcome, Retry, Stats};

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
