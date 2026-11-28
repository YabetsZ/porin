# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `porin::tower` (feature `tower`): `RateLimitLayer`, a tower layer for axum, tonic and hyper, over a memory or Redis limiter. Requests are keyed by `PeerIp` (the TCP peer, or with `trusting_proxies` the one `ProxyHeader` the proxies write; IPv6 grouped by /64), `Header` (one value of at most 512 bytes) or `from_fn`. Limited requests get 429 with `Retry-After`; an unavailable or failing store 503; a missing or too long key 400, or a `fallback_key`. Responses carry the IETF draft `RateLimit-Policy` and `RateLimit` headers, or `X-RateLimit-*` with `Headers::Legacy`. A full store answers 429, or 503 with `FullStore::ServiceUnavailable`.
- Features `axum_0_8` and `tonic_0_14`: `PeerIp` reads axum's `ConnectInfo` or tonic's `TcpConnectInfo`. Named for the version they support, so a later one can be added beside them. An axum example.

## [0.1.0-alpha.2] - 2026-11-28

The Redis store. The API may still change before 0.1.0.

### Added

- `RedisStore` (features `redis-tokio` and `redis-smol`): limiters shared by every instance through a single Redis server, a Sentinel-managed primary or a Cluster. Each decision is one Lua script that Redis runs atomically, timed by Redis's own clock; keys expire once idle. A sliding log is a sorted set per rate, so a decision costs a few lookups however many entries the window holds.
- `Limiter::redis_builder`, which starts a limiter that keeps its state in a `RedisStore` under a limiter id. Keys need `KeyEncode`, and a key encoding to more than 512 bytes is refused with `Error::KeyTooLong`. Redis keys include the algorithm and a fingerprint of the quota, so changing either starts keys fresh.
- Failure policies, one of which a Redis store requires: `FailOpen`, `FailClosed` and `Propagate`. After 3 failures in a row the store stops asking Redis for up to 5 s and answers by its policy at once; a Sentinel store then looks for a new primary. `CircuitBreaker` changes those numbers; once the open time passes, one call at a time probes Redis. Calls wait at most 100 ms by default. Errors that retrying won't fix, such as refused credentials or permissions, are `Error::Store` under every policy, and `connect` fails on them, and on a replica, by loading the scripts and making a test write. A store whose server turns replica connects again, so one reached by an address that moves on failover follows it.
- `RedisStoreBuilder::primary_url`, for a Sentinel store only: the credentials, database and TLS (`rediss://`) that the Sentinel-managed primary needs, from a URL whose host is ignored. `RedisStore::sentinel` returns a `RedisStoreBuilder<_, ViaSentinel>`; the other builders are `RedisStoreBuilder<_, Direct>`, the default.
- `Stats::is_known`, false when a store didn't answer, and `Denial::is_store_unavailable`.
- `QuotaErrorKind::RefillTooLong`, for token buckets too slow to refill for a Redis store to count exactly (over about 71 years).
- `Limiter` and `Builder` take the store as a last type parameter, defaulting to `Memory`, so existing code keeps compiling.

### Changed

- `Error` has its first variants: `Error::Store` and `Error::KeyTooLong`.
- The `store` module is public, holding `Memory`, `RedisStore` and the failure policies.

### Fixed

- A sliding log denial whose units in use plus cost passed `u32::MAX` reported a `retry_after` too early.

## [0.1.0-alpha.1] - 2026-11-21

First release, to reserve the name. The in-memory limiter is usable; the API may still change before 0.1.0.

### Added

- `Quota` and `Rate`: up to four limits per quota, with optional token bucket bursts.
- `quota!`, which checks the quota notation at compile time, and `FromStr` for config files.
- `Clock` with `MonotonicClock` and `ManualClock`, and the `Timestamp` type.
- `CoarseClock`, read with one atomic load and advanced by a background thread, for the fastest hits; it may lag by a tick, which only under-admits.
- `QuantaClock` (feature `quanta`), which reads the processor's timestamp counter.
- `KeyEncode`: a stable, collision-free byte encoding of keys for stores shared between processes.
- `Limiter`, an in-memory rate limiter: `hit`, `hit_n`, `test`, `test_n`, `stats` and `reset`, with keys looked up by borrowed form (`&str` for `String` keys).
- Four algorithms: `TokenBucket` (the default, alias `LeakyBucket`), `FixedWindow`, `SlidingWindow` and `SlidingLog`.
- `Decision`, `Stats` and `Denial`, including the exact `retry_after` and whether a refusal came from a full store.
- `max_keys` (default 1,000,000) and `WhenFull`: refuse new keys (default), evict the key nearest to idle within a window, or share an overflow quota.
- `until_ready` and `until_ready_n`, which wait for the exact moment a hit fits, on any async runtime.
- Feature `memory` (on by default) for the in-memory limiter; without it only quotas, clocks and keys remain.
- `blocking::Limiter` (feature `blocking`) for code without an async runtime. It shares state with the async `Limiter` it converts from.

[Unreleased]: https://github.com/YabetsZ/porin/compare/v0.1.0-alpha.2...HEAD
[0.1.0-alpha.2]: https://github.com/YabetsZ/porin/compare/v0.1.0-alpha.1...v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/YabetsZ/porin/releases/tag/v0.1.0-alpha.1
