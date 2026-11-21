//! Time sources.
//!
//! The rate limiting math never reads a clock. Stores ask a [`Clock`] for the
//! current [`Timestamp`] and pass it in, which keeps decisions deterministic
//! and lets tests drive time with a [`ManualClock`] instead of sleeping.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// A source of the current time.
///
/// Timestamps from one clock are comparable with each other. Timestamps from
/// two different clocks are not, because each clock picks its own origin.
pub trait Clock: Send + Sync {
    /// Returns the current time. Successive calls never go backwards.
    fn now(&self) -> Timestamp;
}

/// A point in time, as whole nanoseconds since its clock's origin.
///
/// A `u64` of nanoseconds covers about 584 years, so arithmetic saturates
/// instead of wrapping.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The clock's origin.
    pub const ZERO: Self = Self(0);

    /// Creates a timestamp `nanos` nanoseconds after the origin.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns the nanoseconds since the origin.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns this timestamp moved `duration` later, stopping at the
    /// largest representable time.
    #[must_use]
    pub const fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(saturating_nanos(duration)))
    }

    /// Returns the time elapsed since `earlier`, or zero if `earlier` is later.
    #[must_use]
    pub const fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

/// The system's monotonic clock, with its origin at the moment of creation.
///
/// Copies share the origin, so their timestamps are comparable.
#[derive(Clone, Copy, Debug)]
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    /// Creates a clock whose origin is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        Timestamp(saturating_nanos(self.origin.elapsed()))
    }
}

/// A clock that a background thread advances every `tick`, so reading it
/// is one atomic load instead of asking the operating system.
///
/// Reading the system clock costs little on its own, but it stops the
/// processor from overlapping the work around it, which makes a hit about
/// 15–40 ns slower. Use this clock when that matters more than precision.
///
/// The time it reports lags real time by up to one tick, and by more if the
/// thread is delayed, for example on an overloaded machine. A lagging clock
/// only under-admits: keys refill slightly late, and `retry_after` and
/// `reset_after` may read up to that lag longer than needed. It never
/// admits more than the quota.
///
/// Clones share one thread. The thread exits within a tick after the last
/// clone is dropped. A long tick makes all of this coarser: with a
/// one-second tick, keys refill a second late and `until_ready` may wait up
/// to that long.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use porin::{CoarseClock, Limiter, quota};
///
/// let clock = CoarseClock::new(Duration::from_millis(1))?;
/// let limiter: Limiter<u64> = Limiter::builder()
///     .quota(quota!("100/second"))
///     .clock(clock)
///     .build();
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct CoarseClock {
    // Relaxed: the value is the only data shared with the ticker thread,
    // and a slightly older value is always acceptable.
    nanos: Arc<AtomicU64>,
}

impl CoarseClock {
    /// The shortest tick. Shorter ticks are raised to this, so the thread
    /// can't spin.
    pub const MIN_TICK: Duration = Duration::from_micros(100);

    /// Starts a clock at [`Timestamp::ZERO`] whose thread advances it every
    /// `tick` (at least [`MIN_TICK`](Self::MIN_TICK)).
    ///
    /// # Errors
    ///
    /// If the operating system can't start the thread.
    pub fn new(tick: Duration) -> io::Result<Self> {
        let tick = tick.max(Self::MIN_TICK);
        let nanos = Arc::new(AtomicU64::new(0));
        let ticker = Arc::downgrade(&nanos);
        let origin = Instant::now();
        thread::Builder::new()
            .name("porin-coarse-clock".to_owned())
            .spawn(move || {
                loop {
                    thread::sleep(tick);
                    let Some(nanos) = ticker.upgrade() else {
                        return;
                    };
                    nanos.store(saturating_nanos(origin.elapsed()), Ordering::Relaxed);
                }
            })?;
        Ok(Self { nanos })
    }
}

impl Clock for CoarseClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.nanos.load(Ordering::Relaxed))
    }
}

/// A clock that reads the processor's timestamp counter through
/// [`quanta`](https://docs.rs/quanta), about 7 ns per read instead of about
/// 19 ns for the system clock, which makes a hit about 15 ns faster.
///
/// quanta calibrates the counter against the system clock at start-up and
/// falls back to the system clock where the counter isn't reliable. If the
/// counter were ever to step backwards between cores, porin under-admits:
/// a clock going backwards never resets a key's limit.
///
/// # Examples
///
/// ```
/// use porin::{Limiter, QuantaClock, quota};
///
/// # #[cfg(not(miri))] // quanta's inline assembly doesn't run under Miri
/// let limiter: Limiter<u64> = Limiter::builder()
///     .quota(quota!("100/second"))
///     .clock(QuantaClock::new())
///     .build();
/// ```
#[cfg(feature = "quanta")]
#[cfg_attr(docsrs, doc(cfg(feature = "quanta")))]
#[derive(Clone, Debug)]
pub struct QuantaClock {
    clock: quanta::Clock,
    origin: quanta::Instant,
}

#[cfg(feature = "quanta")]
impl QuantaClock {
    /// Creates a clock whose origin is now.
    #[must_use]
    pub fn new() -> Self {
        let clock = quanta::Clock::new();
        let origin = clock.now();
        Self { clock, origin }
    }
}

#[cfg(feature = "quanta")]
impl Default for QuantaClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "quanta")]
impl Clock for QuantaClock {
    fn now(&self) -> Timestamp {
        Timestamp(saturating_nanos(
            self.clock.now().saturating_duration_since(self.origin),
        ))
    }
}

/// A clock that moves only when told to, for tests and simulations.
///
/// Clones share the same time, so a test can keep one handle and give another
/// to the code under test.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use porin::{Clock, ManualClock, Timestamp};
///
/// let clock = ManualClock::new();
/// clock.advance(Duration::from_millis(1500));
/// assert_eq!(clock.now(), Timestamp::from_nanos(1_500_000_000));
/// ```
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    // Relaxed throughout: the clock's value is the only data it carries, so
    // it orders nothing else and needs no happens-before edge.
    nanos: Arc<AtomicU64>,
}

impl ManualClock {
    /// Creates a clock at [`Timestamp::ZERO`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a clock at `start`.
    #[must_use]
    pub fn starting_at(start: Timestamp) -> Self {
        Self {
            nanos: Arc::new(AtomicU64::new(start.0)),
        }
    }

    /// Moves the clock `duration` later, stopping at the largest representable time.
    pub fn advance(&self, duration: Duration) {
        let step = saturating_nanos(duration);
        // A CAS loop rather than `fetch_update`, which is deprecated on
        // current stable while its replacement is newer than the MSRV.
        let mut current = self.nanos.load(Ordering::Relaxed);
        loop {
            match self.nanos.compare_exchange_weak(
                current,
                current.saturating_add(step),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Moves the clock to `timestamp`. Moving it backwards is allowed, which
    /// lets tests check how stores handle a clock that jumps back.
    pub fn set(&self, timestamp: Timestamp) {
        self.nanos.store(timestamp.0, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.nanos.load(Ordering::Relaxed))
    }
}

/// Converts `duration` to whole nanoseconds, saturating at `u64::MAX`.
const fn saturating_nanos(duration: Duration) -> u64 {
    let nanos = duration.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "checked against u64::MAX just above"
        )]
        let nanos = nanos as u64;
        nanos
    }
}
