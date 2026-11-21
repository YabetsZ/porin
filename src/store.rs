//! Where limiter state lives.

pub(crate) mod memory;

use std::time::Duration;

use crate::algorithm::{Outcome, Retry, Stats};

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
