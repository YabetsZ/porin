# porin

Rate limiting for Rust services: several algorithms, shared storage, any framework.

> **Status:** design phase, no usable code yet.

A porin is a protein that forms a channel through a cell membrane and controls what passes. This crate does the same for requests.

## Planned

- **Algorithms:** fixed window, sliding log, sliding window counter, token bucket (GCRA)
- **Typed API:** quotas checked at compile time (`quota!("10/second; 1000/day")`), typed keys instead of formatted strings
- **Storage:** in memory, Redis, and in v0.2 **peer mode**, which enforces a global limit with no central store
- **Frameworks:** tower (axum, tonic, hyper), a blocking API, and actix in v0.2
- **Runtimes:** no runtime required in the core; tokio or smol for Redis

## Prior art

- [limits](https://github.com/alisaifee/limits) (Python): several algorithms over pluggable storage
- [governor](https://github.com/boinkor-net/governor): GCRA in Rust
- [Counting things, a lot of different things](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/) (Cloudflare): the sliding window counter
- [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) (Das, Gupta, Motivala) and [foca](https://github.com/caio/foca): peer membership

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
