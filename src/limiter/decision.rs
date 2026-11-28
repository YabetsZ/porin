//! What a limiter answers.

use std::time::Duration;

use crate::algorithm::{Retry, Stats as CoreStats};
use crate::store::Hit;

/// Whether a hit was allowed, with what the key has left.
///
/// Denial is a normal answer, not an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use = "a decision that isn't checked doesn't limit anything"]
pub enum Decision {
    /// The hit was allowed and recorded.
    Allowed(Stats),
    /// The hit was denied and nothing was recorded.
    Denied(Denial),
}

impl Decision {
    /// Whether the hit was allowed.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed(_))
    }

    /// What the key has left, whether the hit was allowed or not.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        match self {
            Self::Allowed(stats) => *stats,
            Self::Denied(denial) => denial.stats,
        }
    }

    pub(crate) fn from_hit(hit: Hit) -> Self {
        match hit {
            Hit::Allowed(stats) => Self::Allowed(stats.into()),
            Hit::Denied { stats, retry } => Self::Denied(Denial {
                stats: stats.into(),
                retry_after: match retry {
                    Retry::After(wait) => Some(wait),
                    Retry::Never => None,
                },
                cause: Cause::Limit,
            }),
            Hit::Full { retry_after } => Self::Denied(Denial {
                stats: Stats {
                    remaining: 0,
                    reset_after: retry_after,
                    known: true,
                },
                retry_after: Some(retry_after),
                cause: Cause::StoreFull,
            }),
        }
    }

    /// An allowed hit the store couldn't confirm: [`FailOpen`](crate::store::FailOpen).
    #[cfg_attr(
        not(any(feature = "redis-tokio", feature = "redis-smol")),
        expect(dead_code, reason = "only remote stores can be unavailable")
    )]
    pub(crate) const fn allowed_unconfirmed() -> Self {
        Self::Allowed(Stats {
            remaining: 0,
            reset_after: Duration::ZERO,
            known: false,
        })
    }

    /// A hit denied because the store didn't answer: [`FailClosed`](crate::store::FailClosed).
    #[cfg_attr(
        not(any(feature = "redis-tokio", feature = "redis-smol")),
        expect(dead_code, reason = "only remote stores can be unavailable")
    )]
    pub(crate) const fn denied_unavailable(retry_after: Duration) -> Self {
        Self::Denied(Denial {
            stats: Stats {
                remaining: 0,
                reset_after: Duration::ZERO,
                known: false,
            },
            retry_after: Some(retry_after),
            cause: Cause::StoreUnavailable,
        })
    }
}

/// What a key has left, for `RateLimit-*` headers. With several rates, the
/// tightest one: the fewest units remaining and the longest reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Stats {
    remaining: u32,
    reset_after: Duration,
    known: bool,
}

impl Stats {
    /// Units that can still be spent now, after this hit if it was allowed.
    /// 0 when the stats aren't [known](Self::is_known).
    #[must_use]
    pub const fn remaining(&self) -> u32 {
        self.remaining
    }

    /// Time until the key is back to its full allowance, if nothing more is
    /// spent. Zero when the stats aren't [known](Self::is_known).
    #[must_use]
    pub const fn reset_after(&self) -> Duration {
        self.reset_after
    }

    /// Whether the store reported these stats. False when a remote store
    /// didn't answer and the limiter decided by its failure policy; skip
    /// `RateLimit-*` headers then.
    #[must_use]
    pub const fn is_known(&self) -> bool {
        self.known
    }
}

impl From<CoreStats> for Stats {
    fn from(stats: CoreStats) -> Self {
        Self {
            remaining: stats.remaining,
            reset_after: stats.reset_after,
            known: true,
        }
    }
}

/// Why a hit was denied, and when to try again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Denial {
    stats: Stats,
    retry_after: Option<Duration>,
    cause: Cause,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Cause {
    Limit,
    StoreFull,
    StoreUnavailable,
}

impl Denial {
    /// What the key has left.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    /// How long until the same hit would be allowed, if nothing else is
    /// spent meanwhile, for a `Retry-After` header. `None` if it never
    /// will: the hit costs more than the quota allows at once.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Whether the key was refused because the limiter's store was full of
    /// other active keys, rather than because the key used up its quota. A
    /// server may answer these with 503 instead of 429. See
    /// [`WhenFull`](crate::WhenFull).
    #[must_use]
    pub const fn is_store_full(&self) -> bool {
        matches!(self.cause, Cause::StoreFull)
    }

    /// Whether a remote store didn't answer and the limiter denied by its
    /// [`FailClosed`](crate::store::FailClosed) policy. A server may answer
    /// these with 503. `retry_after` is when the store will be tried again.
    #[must_use]
    pub const fn is_store_unavailable(&self) -> bool {
        matches!(self.cause, Cause::StoreUnavailable)
    }
}
