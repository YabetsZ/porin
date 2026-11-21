//! # porin
//!
//! Rate limiting for Rust services. Pick an algorithm (fixed window, sliding
//! log, sliding window counter, token bucket), pick where the state lives (in
//! memory, Redis, or, from v0.2, shared between peers with no central store),
//! and plug it into any framework.
//!
//! Named after porins, the proteins that form channels in a cell membrane and
//! decide what passes through.
//!
//! Status: early development. The in-memory limiter works; Redis, the tower
//! layer and peer mode are next.
//!
//! ```
//! use porin::{Limiter, quota};
//!
//! # pollster::block_on(async {
//! // Checked at compile time: a typo in the notation is a compile error.
//! let limiter = Limiter::memory(quota!("10/second; 1000/day"));
//!
//! let user_id = 42_u64;
//! if limiter.hit(&user_id).await?.is_allowed() {
//!     // handle the request
//! }
//! # Ok::<(), porin::Error>(())
//! # }).unwrap();
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg_attr(
    not(feature = "memory"),
    expect(dead_code, reason = "only the memory store runs the algorithms so far")
)]
mod algorithm;
#[cfg(all(porin_bench, feature = "memory"))]
#[doc(hidden)]
pub mod bench;
#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;
pub mod clock;
pub mod key;
#[cfg(feature = "memory")]
mod limiter;
pub mod quota;
#[cfg(feature = "memory")]
mod store;
#[cfg(feature = "memory")]
mod sync;

pub use algorithm::fixed_window::FixedWindow;
pub use algorithm::gcra::TokenBucket;
pub use algorithm::sliding_log::SlidingLog;
pub use algorithm::sliding_window::SlidingWindow;
#[cfg(feature = "quanta")]
pub use clock::QuantaClock;
pub use clock::{Clock, CoarseClock, ManualClock, MonotonicClock, Timestamp};
pub use key::KeyEncode;
#[cfg(feature = "memory")]
pub use limiter::{Algorithm, Builder, Decision, Denial, Error, Limiter, NoQuota, Stats};
pub use quota::{Quota, QuotaError, QuotaErrorKind, Rate};
#[cfg(feature = "memory")]
pub use store::WhenFull;

/// A leaky bucket used as a meter allows exactly what a token bucket does,
/// so it is the same type.
pub type LeakyBucket = TokenBucket;
