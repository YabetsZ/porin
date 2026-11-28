//! When to stop asking a Redis that keeps failing.
//!
//! After [`CircuitBreaker::failures`] failures in a row the breaker opens:
//! calls answer by the failure policy at once instead of each waiting out a
//! timeout. When the open time is over, one call at a time probes Redis;
//! a success closes the breaker, a failure opens it for twice as long, up
//! to the longest open time. Failures of calls already in flight when it
//! opened don't open it again.
//!
//! The counters are advisory, so `Relaxed` is enough: a race can only let a
//! few extra calls through to Redis or answer a few by policy early, and no
//! other memory is published through them.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// When a [`RedisStore`](super::RedisStore) stops asking a Redis that keeps
/// failing, and for how long. Set with
/// [`RedisStoreBuilder::circuit_breaker`](super::RedisStoreBuilder::circuit_breaker).
///
/// After [`failures`](Self::failures) failures in a row, calls stop going
/// to Redis and the failure policy answers at once, for
/// [`first_open`](Self::first_open). Then one call at a time tries Redis:
/// a success closes the breaker, and a failure opens it for twice as long,
/// up to [`longest_open`](Self::longest_open).
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU32;
/// use std::time::Duration;
///
/// use porin::store::CircuitBreaker;
///
/// let breaker = CircuitBreaker::new()
///     .failures(NonZeroU32::new(5).ok_or("zero")?)
///     .longest_open(Duration::from_secs(30));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CircuitBreaker {
    failures: NonZeroU32,
    first_open: Duration,
    longest_open: Duration,
}

impl CircuitBreaker {
    /// The defaults: open after 3 failures, for 100 ms at first, at most
    /// 5 s.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            failures: match NonZeroU32::new(3) {
                Some(failures) => failures,
                None => NonZeroU32::MIN,
            },
            first_open: Duration::from_millis(100),
            longest_open: Duration::from_secs(5),
        }
    }

    /// Failures in a row that open the breaker. The default is 3.
    #[must_use]
    pub const fn failures(mut self, failures: NonZeroU32) -> Self {
        self.failures = failures;
        self
    }

    /// How long the breaker stays open the first time. The default is
    /// 100 ms.
    #[must_use]
    pub const fn first_open(mut self, open: Duration) -> Self {
        self.first_open = open;
        self
    }

    /// The longest the breaker stays open, however often it reopens. The
    /// default is 5 s. If it is shorter than
    /// [`first_open`](Self::first_open), every opening lasts `first_open`.
    #[must_use]
    pub const fn longest_open(mut self, open: Duration) -> Self {
        self.longest_open = open;
        self
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) struct Breaker {
    settings: CircuitBreaker,
    /// How long one probe may run before another caller may probe, in case
    /// the first was dropped before it finished.
    probe_lease_us: u64,
    start: Instant,
    /// Failures since the last success.
    failures: AtomicU32,
    /// How many times in a row the breaker has opened.
    trips: AtomicU32,
    /// 0 while closed. Otherwise microseconds after `start` until which
    /// calls are refused; once that has passed the breaker is half open,
    /// and one probe at a time goes through.
    open_until: AtomicU64,
    /// When the current probe started, in microseconds after `start`; 0 if
    /// there is none.
    probe: AtomicU64,
    /// Whether the next call should re-establish the connection first.
    reconnect: AtomicBool,
}

impl Breaker {
    pub(super) fn new(settings: CircuitBreaker, probe_lease: Duration) -> Self {
        Self {
            settings,
            probe_lease_us: micros(probe_lease),
            start: Instant::now(),
            failures: AtomicU32::new(0),
            trips: AtomicU32::new(0),
            open_until: AtomicU64::new(0),
            probe: AtomicU64::new(0),
            reconnect: AtomicBool::new(false),
        }
    }

    fn now_us(&self) -> u64 {
        // Never 0, which means "none" for `probe`.
        micros(self.start.elapsed()).max(1)
    }

    /// Whether a call may go to Redis now: always while closed, never while
    /// open, and for one probe at a time while half open. `Err` holds when
    /// to try again.
    pub(super) fn admit(&self) -> Result<(), Duration> {
        let until = self.open_until.load(Relaxed);
        if until == 0 {
            return Ok(());
        }
        let now = self.now_us();
        if let Some(left) = until.checked_sub(now).filter(|left| *left > 0) {
            return Err(Duration::from_micros(left));
        }
        let probe = self.probe.load(Relaxed);
        let lease_end = probe.saturating_add(self.probe_lease_us);
        if probe != 0 && now < lease_end {
            return Err(self.settings.first_open);
        }
        match self.probe.compare_exchange(probe, now, Relaxed, Relaxed) {
            Ok(_) => Ok(()),
            Err(_) => Err(self.settings.first_open),
        }
    }

    /// Whether this caller should re-establish the connection before
    /// trying: true for one caller after each time the breaker opened or a
    /// reconnect was requested.
    pub(super) fn should_reconnect(&self) -> bool {
        self.reconnect.swap(false, Relaxed)
    }

    /// Asks the next call to re-establish the connection, as when Redis
    /// says it is a replica now.
    pub(super) fn request_reconnect(&self) {
        self.reconnect.store(true, Relaxed);
    }

    /// Records a success; returns whether it closed an open breaker. Only
    /// one caller sees each closing, since it swaps the open time away.
    pub(super) fn succeeded(&self) -> bool {
        if self.failures.load(Relaxed) == 0 && self.open_until.load(Relaxed) == 0 {
            return false;
        }
        self.failures.store(0, Relaxed);
        self.trips.store(0, Relaxed);
        self.probe.store(0, Relaxed);
        self.open_until.swap(0, Relaxed) != 0
    }

    /// Records a failure.
    pub(super) fn failed(&self) -> Failed {
        let CircuitBreaker {
            failures: threshold,
            first_open,
            longest_open,
        } = self.settings;
        let failures = self.failures.fetch_add(1, Relaxed).saturating_add(1);
        if failures < threshold.get() {
            return Failed::retry_after(first_open);
        }
        let now = self.now_us();
        let until = self.open_until.load(Relaxed);
        if let Some(left) = until.checked_sub(now).filter(|left| *left > 0) {
            // Already open: calls that were in flight when it opened don't
            // open it again, or one burst would stretch it to the longest.
            return Failed::retry_after(Duration::from_micros(left));
        }
        let trips = self.trips.load(Relaxed).min(31);
        let open = first_open
            .saturating_mul(1 << trips)
            .min(longest_open.max(first_open));
        let reopen = now.saturating_add(micros(open));
        match self
            .open_until
            .compare_exchange(until, reopen, Relaxed, Relaxed)
        {
            Ok(_) => {
                self.trips.fetch_add(1, Relaxed);
                self.probe.store(0, Relaxed);
                self.reconnect.store(true, Relaxed);
                Failed {
                    retry_after: open,
                    opened: true,
                }
            }
            // Another failure opened it first.
            Err(current) => Failed::retry_after(Duration::from_micros(current.saturating_sub(now))),
        }
    }
}

/// What a failure did to the breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Failed {
    /// When the store is worth trying again.
    pub(super) retry_after: Duration,
    /// Whether this failure opened the breaker. Only one caller sees each
    /// opening, since it takes it with a compare-and-swap.
    pub(super) opened: bool,
}

impl Failed {
    const fn retry_after(retry_after: Duration) -> Self {
        Self {
            retry_after,
            opened: false,
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST: Duration = Duration::from_millis(100);
    const LEASE: Duration = Duration::from_secs(1);

    fn breaker(settings: CircuitBreaker) -> Breaker {
        Breaker::new(settings, LEASE)
    }

    /// Ends the open time, as if it had passed.
    fn expire(breaker: &Breaker) {
        breaker.open_until.store(1, Relaxed);
    }

    #[test]
    fn opens_after_three_failures_and_closes_on_success() {
        let breaker = breaker(CircuitBreaker::new());
        assert_eq!(breaker.failed().retry_after, FIRST);
        assert_eq!(breaker.failed().retry_after, FIRST);
        assert!(breaker.admit().is_ok());
        assert_eq!(breaker.failed().retry_after, FIRST);
        assert!(breaker.admit().is_err());
        assert!(breaker.should_reconnect());
        assert!(!breaker.should_reconnect());
        expire(&breaker);
        assert!(breaker.admit().is_ok(), "the probe");
        assert_eq!(
            breaker.failed().retry_after,
            FIRST * 2,
            "a failed probe reopens for longer"
        );
        breaker.succeeded();
        assert!(breaker.admit().is_ok());
        assert_eq!(breaker.failed().retry_after, FIRST);
        assert!(breaker.admit().is_ok(), "a success resets the count");
    }

    #[test]
    fn openings_and_closings_are_reported_once_each() {
        let breaker = breaker(CircuitBreaker::new());
        assert!(!breaker.succeeded(), "closed already");
        assert!(!breaker.failed().opened);
        assert!(!breaker.succeeded(), "a failure below the threshold");
        for _ in 0..2 {
            assert!(!breaker.failed().opened);
        }
        assert!(breaker.failed().opened, "the third failure in a row");
        assert!(!breaker.failed().opened, "already open");
        assert!(breaker.succeeded());
        assert!(!breaker.succeeded(), "already closed");
    }

    #[test]
    fn a_burst_of_failures_opens_it_once() {
        // Long enough not to pass during the loop, even under Miri.
        let open = Duration::from_secs(60);
        let breaker = breaker(CircuitBreaker::new().first_open(open));
        for _ in 0..100 {
            assert!(breaker.failed().retry_after <= open);
        }
        assert_eq!(breaker.trips.load(Relaxed), 1);
    }

    #[test]
    fn half_open_lets_one_probe_through() {
        let breaker = breaker(CircuitBreaker::new().failures(NonZeroU32::MIN));
        breaker.failed();
        expire(&breaker);
        assert!(breaker.admit().is_ok(), "the probe");
        assert!(breaker.admit().is_err(), "others wait for it");
        assert!(breaker.admit().is_err());
        breaker.succeeded();
        assert!(breaker.admit().is_ok());
        assert!(breaker.admit().is_ok(), "closed again");
    }

    #[test]
    fn a_dropped_probe_frees_its_lease() {
        let lease = Duration::from_millis(5);
        let breaker = Breaker::new(CircuitBreaker::new().failures(NonZeroU32::MIN), lease);
        breaker.failed();
        expire(&breaker);
        assert!(breaker.admit().is_ok(), "a probe that never reports back");
        assert!(breaker.admit().is_err());
        std::thread::sleep(lease * 2);
        assert!(
            breaker.admit().is_ok(),
            "its lease ended, so another caller probes"
        );
    }

    #[test]
    fn stays_open_at_most_the_longest_open_time() {
        let breaker = breaker(CircuitBreaker::new());
        let mut longest = Duration::ZERO;
        for _ in 0..40 {
            expire(&breaker);
            longest = longest.max(breaker.failed().retry_after);
        }
        assert_eq!(longest, Duration::from_secs(5));
    }

    #[test]
    fn settings_apply() {
        let settings = CircuitBreaker::new()
            .failures(NonZeroU32::MIN)
            .first_open(Duration::from_millis(10))
            .longest_open(Duration::from_millis(25));
        let breaker = breaker(settings);
        assert_eq!(
            breaker.failed().retry_after,
            Duration::from_millis(10),
            "one failure opens it"
        );
        assert!(breaker.admit().is_err());
        for expected in [20, 25, 25] {
            expire(&breaker);
            assert_eq!(
                breaker.failed().retry_after,
                Duration::from_millis(expected)
            );
        }
    }

    #[test]
    fn a_longest_open_below_the_first_is_the_first() {
        let settings = CircuitBreaker::new()
            .failures(NonZeroU32::MIN)
            .first_open(Duration::from_millis(50))
            .longest_open(Duration::from_millis(1));
        let breaker = breaker(settings);
        assert_eq!(breaker.failed().retry_after, Duration::from_millis(50));
        expire(&breaker);
        assert_eq!(breaker.failed().retry_after, Duration::from_millis(50));
    }
}
