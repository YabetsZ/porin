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
//! Status: the in-memory and Redis limiters and the tower layer are ready
//! to use; peer mode comes in v0.2.
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
//!
//! # Features
//!
//! | Feature | Adds |
//! |---|---|
//! | `memory` (default) | [`Limiter::memory`] and the in-memory store |
//! | `blocking` | `blocking::Limiter`, which waits by sleeping the thread |
//! | `quanta` | `QuantaClock`, a faster clock from the `quanta` crate |
//! | `redis-tokio`, `redis-smol` | the Redis store, on either runtime |
//! | `tower` | `tower::RateLimitLayer`, for axum, tonic and hyper |
//! | `axum_0_8`, `tonic_0_14` | client addresses from those servers' connection info |
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
pub mod store;
#[cfg(feature = "memory")]
mod sync;
#[cfg(feature = "tower")]
#[cfg_attr(docsrs, doc(cfg(feature = "tower")))]
pub mod tower;

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
