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
//! Status: early development. Quotas and clocks exist; algorithms and stores
//! are next.
//!
//! ```
//! use porin::quota;
//!
//! // Checked at compile time: a typo in the notation is a compile error.
//! let api = quota!("10/second; 1000/day");
//! assert_eq!(api.to_string(), "10/second; 1000/day");
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

mod algorithm;
#[cfg(porin_bench)]
#[doc(hidden)]
pub mod bench;
pub mod clock;
pub mod key;
pub mod quota;
#[cfg_attr(
    all(any(not(test), loom), not(porin_bench)),
    expect(
        dead_code,
        reason = "nothing public uses the stores yet; the limiter will"
    )
)]
mod store;
mod sync;

pub use clock::{Clock, ManualClock, MonotonicClock, Timestamp};
pub use key::KeyEncode;
pub use quota::{Quota, QuotaError, QuotaErrorKind, Rate};
