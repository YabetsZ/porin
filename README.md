# porin

Rate limiting for Rust services: several algorithms, shared storage, any framework.

> **Status:** early development, not yet on crates.io. The in-memory limiter works; Redis, the tower layer and peer mode are next.

A porin is a protein that forms a channel through a cell membrane and controls what passes. This crate does the same for requests.

```rust
use porin::{Limiter, quota};

// The quota is checked at compile time: a typo is a build error.
let limiter = Limiter::memory(quota!("10/second; 1000/day"));

if limiter.hit(&user_id).await?.is_allowed() {
    // handle the request
}
```

## What works today

- **Algorithms:** token bucket (GCRA, the default), fixed window, sliding window counter and sliding log
- **Quotas:** up to four rates at once, all-or-nothing, with optional bursts (`10/second burst 20`)
- **Keys:** any `Hash + Eq` type, looked up by borrowed form, so a `Limiter<String>` takes a `&str`
- **Answers:** remaining units, reset time and an exact `retry_after` for `RateLimit-*` and `Retry-After` headers
- **Memory safety under floods:** at most 1,000,000 tracked keys by default; idle keys are evicted, and you choose what happens when every key is active (`WhenFull`)
- **Waiting:** `until_ready`, on any async runtime, and a blocking `Limiter` for code without one
- **Clocks:** the system clock by default, `CoarseClock` for the fastest hits, `QuantaClock` (feature `quanta`), and `ManualClock` for tests

## Planned

- **Storage:** Redis, and in v0.2 **peer mode**, which enforces a global limit with no central store
- **Frameworks:** a tower layer (axum, tonic, hyper), and actix in v0.2

## Prior art

- [limits](https://github.com/alisaifee/limits) (Python): several algorithms over pluggable storage
- [governor](https://github.com/boinkor-net/governor): GCRA in Rust
- [Counting things, a lot of different things](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/) (Cloudflare): the sliding window counter
- [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) (Das, Gupta, Motivala) and [foca](https://github.com/caio/foca): peer membership

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
