# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Quota` and `Rate`: up to four limits per quota, with optional token bucket bursts.
- `quota!`, which checks the quota notation at compile time, and `FromStr` for config files.
- `Clock` with `MonotonicClock` and `ManualClock`, and the `Timestamp` type.
