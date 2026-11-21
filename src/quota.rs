//! How much a key may spend, and over which period.
//!
//! A [`Rate`] is one limit, such as 10 per second. A [`Quota`] holds up to
//! [`MAX_RATES`] rates, and a hit must pass every one of them.
//!
//! # Notation
//!
//! [`quota!`](crate::quota!) and [`Quota`]'s `FromStr` accept the same text:
//!
//! ```text
//! quota      = rate *( ";" rate )
//! rate       = count separator [ multiplier ] unit [ "burst" count ]
//! separator  = "/" | "per"
//! unit       = millisecond[s] | second[s] | minute[s] | hour[s] | day[s]
//! ```
//!
//! Counts, multipliers and bursts are whole numbers of at least 1. Words are
//! case-insensitive and whitespace is optional between parts. Examples:
//! `10/second`, `100 per minute; 5000/day`, `10/5 seconds burst 20`.

mod parse;

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::Duration;

/// The most rates one [`Quota`] can hold.
pub const MAX_RATES: usize = 4;

const SECOND_MS: u64 = 1_000;
const MINUTE_MS: u64 = 60 * SECOND_MS;
const HOUR_MS: u64 = 60 * MINUTE_MS;
const DAY_MS: u64 = 24 * HOUR_MS;

/// The longest allowed period. Keeping it near 27 years leaves ample headroom
/// in nanosecond time arithmetic.
const MAX_PERIOD_MS: u64 = 10_000 * DAY_MS;

/// One limit: `count` units per period, with bursts of up to `burst` units.
///
/// The burst only matters to the token bucket. It defaults to the count, so
/// a full period's allowance can be spent at once. A smaller burst forces a
/// smoother pace; a larger one lets idle keys save up.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU32;
/// use porin::Rate;
///
/// let ten = NonZeroU32::new(10).ok_or("zero")?;
/// let rate = Rate::per_second(ten);
/// assert_eq!(rate.to_string(), "10/second");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rate {
    count: NonZeroU32,
    burst: NonZeroU32,
    /// Always in `1..=MAX_PERIOD_MS`.
    period_ms: u64,
}

impl Rate {
    /// `count` per second.
    #[must_use]
    pub const fn per_second(count: NonZeroU32) -> Self {
        Self::from_parts(count, SECOND_MS)
    }

    /// `count` per minute.
    #[must_use]
    pub const fn per_minute(count: NonZeroU32) -> Self {
        Self::from_parts(count, MINUTE_MS)
    }

    /// `count` per hour.
    #[must_use]
    pub const fn per_hour(count: NonZeroU32) -> Self {
        Self::from_parts(count, HOUR_MS)
    }

    /// `count` per day.
    #[must_use]
    pub const fn per_day(count: NonZeroU32) -> Self {
        Self::from_parts(count, DAY_MS)
    }

    /// `count` per `period`.
    ///
    /// # Errors
    ///
    /// Returns an error if `period` is zero, is not a whole number of
    /// milliseconds, or is longer than 10,000 days.
    pub fn new(count: NonZeroU32, period: Duration) -> Result<Self, QuotaError> {
        if period.subsec_nanos().checked_rem(1_000_000) != Some(0) {
            return Err(QuotaError::new(
                QuotaErrorKind::PeriodNotWholeMilliseconds,
                None,
            ));
        }
        let Ok(period_ms) = u64::try_from(period.as_millis()) else {
            return Err(QuotaError::new(QuotaErrorKind::PeriodTooLong, None));
        };
        if period_ms == 0 {
            return Err(QuotaError::new(QuotaErrorKind::ZeroPeriod, None));
        }
        if period_ms > MAX_PERIOD_MS {
            return Err(QuotaError::new(QuotaErrorKind::PeriodTooLong, None));
        }
        Ok(Self::from_parts(count, period_ms))
    }

    /// Returns this rate with a different burst size.
    #[must_use]
    pub const fn with_burst(self, burst: NonZeroU32) -> Self {
        Self { burst, ..self }
    }

    /// Units allowed per period.
    #[must_use]
    pub const fn count(self) -> NonZeroU32 {
        self.count
    }

    /// The most units that can be spent at once.
    #[must_use]
    pub const fn burst(self) -> NonZeroU32 {
        self.burst
    }

    /// The length of one period.
    #[must_use]
    pub const fn period(self) -> Duration {
        Duration::from_millis(self.period_ms)
    }

    const fn from_parts(count: NonZeroU32, period_ms: u64) -> Self {
        Self {
            count,
            burst: count,
            period_ms,
        }
    }
}

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (multiplier, unit) = largest_unit(self.period_ms);
        write!(f, "{}/", self.count)?;
        if multiplier == 1 {
            f.write_str(unit)?;
        } else {
            write!(f, "{multiplier} {unit}s")?;
        }
        if self.burst != self.count {
            write!(f, " burst {}", self.burst)?;
        }
        Ok(())
    }
}

/// Splits a period into the largest unit that divides it evenly.
fn largest_unit(period_ms: u64) -> (u64, &'static str) {
    for (unit_ms, name) in [
        (DAY_MS, "day"),
        (HOUR_MS, "hour"),
        (MINUTE_MS, "minute"),
        (SECOND_MS, "second"),
    ] {
        if let (Some(0), Some(multiplier)) = (
            period_ms.checked_rem(unit_ms),
            period_ms.checked_div(unit_ms),
        ) {
            return (multiplier, name);
        }
    }
    (period_ms, "millisecond")
}

/// Up to [`MAX_RATES`] rates that a key must stay within, all at once.
///
/// Build one with [`quota!`](crate::quota!) when the limits are known at
/// compile time, with [`Quota::new`] and [`Quota::with`] from runtime
/// values, or with `str::parse` from a config file. Rates are kept sorted by
/// period, so equal quotas compare and hash equal however they were built.
///
/// # Examples
///
/// ```
/// use porin::{quota, Quota};
///
/// let from_macro = quota!("1000/day; 10/second");
/// let from_config: Quota = "10/second; 1000/day".parse()?;
/// assert_eq!(from_macro, from_config);
/// assert_eq!(from_macro.to_string(), "10/second; 1000/day");
/// # Ok::<(), porin::QuotaError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Quota {
    /// Sorted by period. Slots from `len` on hold copies of `rates[0]`, so
    /// the derived `Eq` and `Hash` see only the real rates' values.
    rates: [Rate; MAX_RATES],
    /// Always in `1..=MAX_RATES`.
    len: usize,
}

impl Quota {
    /// A quota with a single rate.
    #[must_use]
    pub const fn new(rate: Rate) -> Self {
        Self {
            rates: [rate; MAX_RATES],
            len: 1,
        }
    }

    /// Returns this quota with one more rate.
    ///
    /// # Errors
    ///
    /// Returns an error if the quota already holds [`MAX_RATES`] rates, or
    /// already has a rate with the same period.
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "const fn cannot use iterators or get(); every index stays below MAX_RATES because len < MAX_RATES is checked first"
    )]
    pub const fn with(self, rate: Rate) -> Result<Self, QuotaError> {
        if self.len >= MAX_RATES {
            return Err(QuotaError::new(QuotaErrorKind::TooManyRates, None));
        }
        let mut rates = self.rates;

        // Find where the new rate goes to keep periods ascending.
        let mut index = 0;
        while index < self.len {
            let period_ms = rates[index].period_ms;
            if period_ms == rate.period_ms {
                return Err(QuotaError::new(QuotaErrorKind::DuplicatePeriod, None));
            }
            if period_ms > rate.period_ms {
                break;
            }
            index += 1;
        }

        let mut slot = self.len;
        while slot > index {
            rates[slot] = rates[slot - 1];
            slot -= 1;
        }
        rates[index] = rate;

        let len = self.len + 1;
        let mut unused = len;
        while unused < MAX_RATES {
            rates[unused] = rates[0];
            unused += 1;
        }
        Ok(Self { rates, len })
    }

    /// The rates, shortest period first.
    #[must_use]
    pub const fn rates(&self) -> &[Rate] {
        self.rates.split_at(self.len).0
    }

    /// Parses the quota notation (see the [module docs](self)).
    ///
    /// This is a `const fn`, so it also works in `const` items. Prefer
    /// [`quota!`](crate::quota!) for literals; it turns errors into compile
    /// errors.
    ///
    /// # Errors
    ///
    /// Returns an error describing the first problem and its byte position.
    pub const fn parse(notation: &str) -> Result<Self, QuotaError> {
        parse::parse(notation)
    }
}

impl From<Rate> for Quota {
    fn from(rate: Rate) -> Self {
        Self::new(rate)
    }
}

impl FromStr for Quota {
    type Err = QuotaError;

    fn from_str(notation: &str) -> Result<Self, Self::Err> {
        Self::parse(notation)
    }
}

impl fmt::Display for Quota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, rate) in self.rates().iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{rate}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Quota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Quota").field(&self.rates()).finish()
    }
}

/// Why a quota or rate could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuotaError {
    kind: QuotaErrorKind,
    position: Option<usize>,
}

impl QuotaError {
    const fn new(kind: QuotaErrorKind, position: Option<usize>) -> Self {
        Self { kind, position }
    }

    /// What went wrong.
    #[must_use]
    pub const fn kind(&self) -> QuotaErrorKind {
        self.kind
    }

    /// The byte offset in the notation where the problem starts, if the
    /// error came from parsing.
    #[must_use]
    pub const fn position(&self) -> Option<usize> {
        self.position
    }
}

impl fmt::Display for QuotaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.kind.message())?;
        if let Some(position) = self.position {
            write!(f, " at byte {position}")?;
        }
        Ok(())
    }
}

impl Error for QuotaError {}

/// The kinds of [`QuotaError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QuotaErrorKind {
    /// The notation is empty or only whitespace.
    Empty,
    /// A rate does not start with a count.
    ExpectedCount,
    /// A count is 0.
    ZeroCount,
    /// A number does not fit in a `u32`.
    NumberTooLarge,
    /// The count is not followed by `/` or `per`.
    ExpectedSeparator,
    /// The separator is not followed by a unit.
    ExpectedUnit,
    /// The unit is not one of the supported words.
    UnknownUnit,
    /// The period multiplier is 0.
    ZeroMultiplier,
    /// The period is zero.
    ZeroPeriod,
    /// The period has a fraction of a millisecond.
    PeriodNotWholeMilliseconds,
    /// The period is longer than 10,000 days.
    PeriodTooLong,
    /// `burst` is not followed by a number.
    ExpectedBurst,
    /// The burst is 0.
    ZeroBurst,
    /// A rate is followed by something other than `;` or the end.
    ExpectedSemicolon,
    /// The quota has more than [`MAX_RATES`] rates.
    TooManyRates,
    /// Two rates have the same period.
    DuplicatePeriod,
}

impl QuotaErrorKind {
    /// A short lowercase description, also used as the `quota!` compile error.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::Empty => "quota is empty",
            Self::ExpectedCount => "expected a count, such as `10` in `10/second`",
            Self::ZeroCount => "count must be at least 1",
            Self::NumberTooLarge => "number is larger than 4294967295",
            Self::ExpectedSeparator => "expected `/` or `per` after the count",
            Self::ExpectedUnit => "expected a unit: millisecond, second, minute, hour or day",
            Self::UnknownUnit => "unknown unit, expected millisecond, second, minute, hour or day",
            Self::ZeroMultiplier => "period multiplier must be at least 1",
            Self::ZeroPeriod => "period must be at least 1 millisecond",
            Self::PeriodNotWholeMilliseconds => "period must be a whole number of milliseconds",
            Self::PeriodTooLong => "period is longer than 10000 days",
            Self::ExpectedBurst => "expected a number after `burst`",
            Self::ZeroBurst => "burst must be at least 1",
            Self::ExpectedSemicolon => "expected `;` between rates",
            Self::TooManyRates => "a quota holds at most 4 rates",
            Self::DuplicatePeriod => "two rates have the same period",
        }
    }
}

/// Builds a [`Quota`] from the quota notation, checked at compile time.
///
/// A malformed literal is a compile error, so a typo can't reach production.
/// The notation is described in the [`quota` module](mod@crate::quota) docs. To
/// parse text that is only known at runtime, use `str::parse::<Quota>()`.
///
/// # Examples
///
/// ```
/// use porin::quota;
///
/// let api = quota!("100/minute; 5000/day");
/// let bursty = quota!("10/second burst 50");
/// assert_eq!(api.rates().len(), 2);
/// assert_eq!(bursty.rates()[0].burst().get(), 50);
/// ```
///
/// ```compile_fail
/// let typo = porin::quota!("10/secnd"); // unknown unit
/// ```
#[macro_export]
macro_rules! quota {
    ($notation:literal) => {{
        // A named `const` item, not an inline `const { }` block: inline
        // blocks are only evaluated during a full build, so a bad literal
        // would slip past `cargo check` and rust-analyzer.
        const QUOTA: $crate::Quota = match $crate::Quota::parse($notation) {
            ::core::result::Result::Ok(quota) => quota,
            ::core::result::Result::Err(error) => ::core::panic!("{}", error.kind().message()),
        };
        QUOTA
    }};
}
