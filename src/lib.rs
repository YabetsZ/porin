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
//! Status: design phase, no usable code yet.
