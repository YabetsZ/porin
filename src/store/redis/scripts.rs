//! The Lua scripts that run each algorithm inside Redis.
//!
//! A script is the shared exact-arithmetic helpers, one algorithm, and the
//! shared driver that checks every rate of a quota and records the hit only
//! if all of them allow it. Redis runs a script atomically, so instances
//! sharing a Redis can never both take the last unit.

use std::sync::{LazyLock, OnceLock};

use super::fallback::Fallback;
use crate::algorithm::Algorithm;
use crate::store::memory::MemoryStore;
use crate::{FixedWindow, Rate, SlidingLog, SlidingWindow, TokenBucket};

macro_rules! script {
    ($algorithm:literal) => {
        concat!(
            include_str!("lua/helpers.lua"),
            include_str!(concat!("lua/", $algorithm, ".lua")),
            include_str!("lua/driver.lua"),
        )
    };
}

static TOKEN_BUCKET: LazyLock<::redis::Script> =
    LazyLock::new(|| ::redis::Script::new(script!("token_bucket")));
static FIXED_WINDOW: LazyLock<::redis::Script> =
    LazyLock::new(|| ::redis::Script::new(script!("fixed_window")));
static SLIDING_WINDOW: LazyLock<::redis::Script> =
    LazyLock::new(|| ::redis::Script::new(script!("sliding_window")));
static SLIDING_LOG: LazyLock<::redis::Script> =
    LazyLock::new(|| ::redis::Script::new(script!("sliding_log")));

/// Every script, to load them all when connecting.
pub(crate) fn all() -> [&'static ::redis::Script; 4] {
    [&TOKEN_BUCKET, &FIXED_WINDOW, &SLIDING_WINDOW, &SLIDING_LOG]
}

/// An algorithm with a Redis script.
pub(crate) trait Scripted: Algorithm + crate::store::memory::Stored {
    /// The algorithm's name in Redis keys. Never change one: that would
    /// orphan every key in use.
    const NAME: &'static str;

    fn script() -> &'static ::redis::Script;

    /// This algorithm's in-memory fallback limiter, for
    /// [`FailOver`](crate::store::FailOver).
    fn fallback(fallback: &Fallback) -> &OnceLock<MemoryStore<String, Self>>;

    /// Whether the script counts `rate` exactly.
    fn fits(_rate: Rate) -> bool {
        true
    }
}

impl Scripted for TokenBucket {
    const NAME: &'static str = "token_bucket";

    fn script() -> &'static ::redis::Script {
        &TOKEN_BUCKET
    }

    fn fallback(fallback: &Fallback) -> &OnceLock<MemoryStore<String, Self>> {
        fallback.token_bucket()
    }

    fn fits(rate: Rate) -> bool {
        super::token_bucket_fits(rate)
    }
}

impl Scripted for FixedWindow {
    const NAME: &'static str = "fixed_window";

    fn script() -> &'static ::redis::Script {
        &FIXED_WINDOW
    }

    fn fallback(fallback: &Fallback) -> &OnceLock<MemoryStore<String, Self>> {
        fallback.fixed_window()
    }
}

impl Scripted for SlidingWindow {
    const NAME: &'static str = "sliding_window";

    fn script() -> &'static ::redis::Script {
        &SLIDING_WINDOW
    }

    fn fallback(fallback: &Fallback) -> &OnceLock<MemoryStore<String, Self>> {
        fallback.sliding_window()
    }
}

impl Scripted for SlidingLog {
    const NAME: &'static str = "sliding_log";

    fn script() -> &'static ::redis::Script {
        &SLIDING_LOG
    }

    fn fallback(fallback: &Fallback) -> &OnceLock<MemoryStore<String, Self>> {
        fallback.sliding_log()
    }
}
