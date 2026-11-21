//! The quota notation parser.
//!
//! It is a `const fn` so that `quota!` can run it at compile time, and
//! `FromStr` runs the same function at runtime. Const code can't use `?`,
//! iterators, trait methods or indexing without lint noise, so it walks byte
//! slices with `split_first`.

use std::num::NonZeroU32;

use super::{
    DAY_MS, HOUR_MS, MAX_PERIOD_MS, MINUTE_MS, Quota, QuotaError, QuotaErrorKind as Kind, Rate,
    SECOND_MS,
};

/// Unwraps `Ok`, or returns the `Err` early. Stands in for `?` in const code.
macro_rules! tri {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(error) => return Err(error),
        }
    };
}

const UNITS: [(&[u8], u64); 10] = [
    (b"millisecond", 1),
    (b"milliseconds", 1),
    (b"second", SECOND_MS),
    (b"seconds", SECOND_MS),
    (b"minute", MINUTE_MS),
    (b"minutes", MINUTE_MS),
    (b"hour", HOUR_MS),
    (b"hours", HOUR_MS),
    (b"day", DAY_MS),
    (b"days", DAY_MS),
];

pub(super) const fn parse(notation: &str) -> Result<Quota, QuotaError> {
    let input = notation.as_bytes();
    let start = skip_whitespace(input);
    if start.is_empty() {
        return Err(QuotaError::new(Kind::Empty, None));
    }

    let (first, mut rest) = tri!(parse_rate(input, start));
    let mut quota = Quota::new(first);
    loop {
        rest = skip_whitespace(rest);
        let after_semicolon = match rest.split_first() {
            None => return Ok(quota),
            Some((&b';', tail)) => tail,
            Some(_) => return Err(error_at(Kind::ExpectedSemicolon, input, rest)),
        };
        let rate_start = skip_whitespace(after_semicolon);
        let (rate, tail) = tri!(parse_rate(input, rate_start));
        quota = match quota.with(rate) {
            Ok(quota) => quota,
            Err(error) => return Err(error_at(error.kind(), input, rate_start)),
        };
        rest = tail;
    }
}

/// Parses one rate starting at `rest`. `input` is the whole notation, used
/// only to compute error positions.
const fn parse_rate<'a>(input: &[u8], rest: &'a [u8]) -> Result<(Rate, &'a [u8]), QuotaError> {
    let count_start = rest;
    let (count, rest) = match parse_u32(rest) {
        Number::Value(value, tail) => (value, tail),
        Number::Missing => return Err(error_at(Kind::ExpectedCount, input, count_start)),
        Number::TooLarge => return Err(error_at(Kind::NumberTooLarge, input, count_start)),
    };
    let Some(count) = NonZeroU32::new(count) else {
        return Err(error_at(Kind::ZeroCount, input, count_start));
    };

    let separator_start = skip_whitespace(rest);
    let Some(rest) = strip_separator(separator_start) else {
        return Err(error_at(Kind::ExpectedSeparator, input, separator_start));
    };

    let multiplier_start = skip_whitespace(rest);
    // A u64: a period is any whole number of milliseconds up to the
    // maximum, and Display writes long uneven ones in milliseconds.
    let (multiplier, unit_start) = match parse_u64(multiplier_start) {
        Number::Value(0, _) => {
            return Err(error_at(Kind::ZeroMultiplier, input, multiplier_start));
        }
        Number::Value(value, tail) => (value, skip_whitespace(tail)),
        Number::Missing => (1, multiplier_start),
        Number::TooLarge => return Err(error_at(Kind::PeriodTooLong, input, multiplier_start)),
    };

    let (unit, rest) = take_word(unit_start);
    if unit.is_empty() {
        return Err(error_at(Kind::ExpectedUnit, input, unit_start));
    }
    let Some(unit_ms) = unit_millis(unit) else {
        return Err(error_at(Kind::UnknownUnit, input, unit_start));
    };
    let period_ms = match multiplier.checked_mul(unit_ms) {
        Some(period_ms) if period_ms <= MAX_PERIOD_MS => period_ms,
        _ => return Err(error_at(Kind::PeriodTooLong, input, multiplier_start)),
    };

    let rate = Rate::from_parts(count, period_ms);
    let burst_keyword_start = skip_whitespace(rest);
    let (word, after_keyword) = take_word(burst_keyword_start);
    if !eq_ignore_ascii_case(word, b"burst") {
        return Ok((rate, rest));
    }

    let burst_start = skip_whitespace(after_keyword);
    match parse_u32(burst_start) {
        Number::Value(burst, tail) => match NonZeroU32::new(burst) {
            Some(burst) => Ok((rate.with_burst(burst), tail)),
            None => Err(error_at(Kind::ZeroBurst, input, burst_start)),
        },
        Number::Missing => Err(error_at(Kind::ExpectedBurst, input, burst_start)),
        Number::TooLarge => Err(error_at(Kind::NumberTooLarge, input, burst_start)),
    }
}

enum Number<'a, T> {
    Value(T, &'a [u8]),
    Missing,
    TooLarge,
}

const fn parse_u32(bytes: &[u8]) -> Number<'_, u32> {
    match parse_u64(bytes) {
        Number::Value(value, rest) if value <= u32::MAX as u64 => {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "checked against u32::MAX just above"
            )]
            let value = value as u32;
            Number::Value(value, rest)
        }
        Number::Value(..) | Number::TooLarge => Number::TooLarge,
        Number::Missing => Number::Missing,
    }
}

const fn parse_u64(bytes: &[u8]) -> Number<'_, u64> {
    let mut rest = bytes;
    let mut value: u64 = 0;
    let mut has_digits = false;
    while let Some((&byte, tail)) = rest.split_first() {
        let Some(digit) = digit_value(byte) else {
            break;
        };
        value = match value.checked_mul(10) {
            Some(shifted) => match shifted.checked_add(digit as u64) {
                Some(value) => value,
                None => return Number::TooLarge,
            },
            None => return Number::TooLarge,
        };
        has_digits = true;
        rest = tail;
    }
    if has_digits {
        Number::Value(value, rest)
    } else {
        Number::Missing
    }
}

const fn digit_value(byte: u8) -> Option<u32> {
    match byte {
        b'0' => Some(0),
        b'1' => Some(1),
        b'2' => Some(2),
        b'3' => Some(3),
        b'4' => Some(4),
        b'5' => Some(5),
        b'6' => Some(6),
        b'7' => Some(7),
        b'8' => Some(8),
        b'9' => Some(9),
        _ => None,
    }
}

/// Strips a leading `/` or the word `per`.
const fn strip_separator(bytes: &[u8]) -> Option<&[u8]> {
    if let Some((&b'/', tail)) = bytes.split_first() {
        return Some(tail);
    }
    let (word, tail) = take_word(bytes);
    if eq_ignore_ascii_case(word, b"per") {
        Some(tail)
    } else {
        None
    }
}

const fn unit_millis(word: &[u8]) -> Option<u64> {
    let mut units: &[(&[u8], u64)] = &UNITS;
    while let Some((&(name, unit_ms), tail)) = units.split_first() {
        if eq_ignore_ascii_case(word, name) {
            return Some(unit_ms);
        }
        units = tail;
    }
    None
}

/// Splits off the leading run of ASCII letters.
const fn take_word(bytes: &[u8]) -> (&[u8], &[u8]) {
    let mut rest = bytes;
    while let Some((byte, tail)) = rest.split_first() {
        if !byte.is_ascii_alphabetic() {
            break;
        }
        rest = tail;
    }
    let (word, _) = bytes.split_at(bytes.len().saturating_sub(rest.len()));
    (word, rest)
}

const fn skip_whitespace(bytes: &[u8]) -> &[u8] {
    let mut rest = bytes;
    while let Some((byte, tail)) = rest.split_first() {
        if !byte.is_ascii_whitespace() {
            break;
        }
        rest = tail;
    }
    rest
}

const fn eq_ignore_ascii_case(mut left: &[u8], mut right: &[u8]) -> bool {
    loop {
        match (left.split_first(), right.split_first()) {
            (None, None) => return true,
            (Some((a, left_tail)), Some((b, right_tail))) if a.eq_ignore_ascii_case(b) => {
                left = left_tail;
                right = right_tail;
            }
            _ => return false,
        }
    }
}

/// An error whose position is where `rest` starts within `input`.
const fn error_at(kind: Kind, input: &[u8], rest: &[u8]) -> QuotaError {
    QuotaError::new(kind, Some(input.len().saturating_sub(rest.len())))
}
