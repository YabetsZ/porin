//! What a [`RedisStore`](super::RedisStore) tells its
//! [`on_event`](super::RedisStoreBuilder::on_event) hook.

use std::time::Duration;

/// A change in a [`RedisStore`](super::RedisStore)'s connection to Redis,
/// for logs, metrics and alerts. See
/// [`RedisStoreBuilder::on_event`](super::RedisStoreBuilder::on_event).
///
/// Each opening and closing is reported once, but the hook runs after the
/// change, on whichever call made it: under concurrency an opening and the
/// closing that follows it can reach the hook in either order. Read the
/// breaker's state from the latest event's arrival with that in mind, or
/// treat events as hints for logs and alerts.
///
/// Answers the failure policy gives instead of Redis are counted by
/// [`RedisStore::policy_answers`](super::RedisStore::policy_answers) rather
/// than reported one by one: during an outage there is one per call.
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Event {
    /// Redis failed often enough in a row to open the
    /// [`CircuitBreaker`](super::CircuitBreaker): for `open_for`, the
    /// failure policy answers every call without asking Redis. If the call
    /// that tries Redis next fails too, the breaker opens again for longer
    /// and this event repeats.
    BreakerOpened {
        /// How long calls skip Redis.
        open_for: Duration,
    },
    /// Redis answered again after the breaker had opened, so calls go to
    /// Redis again.
    BreakerClosed,
    /// The store connected to Redis again, after the breaker opened or
    /// Redis said it had become a replica. A Sentinel store connected to
    /// the primary the Sentinels name now, which may be a new one.
    Reconnected,
}
