# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- `blocking::Limiter` (feature `blocking`) for code without an async runtime. It shares state with the async `Limiter` it converts from.
