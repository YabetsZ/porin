//! Parsing, building and displaying quotas through the public API.
#![cfg(test)]

use std::num::NonZeroU32;
use std::time::Duration;

use porin::quota::MAX_RATES;
use porin::{Quota, QuotaErrorKind, Rate, quota};
use proptest::prelude::*;

fn nz(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("test values are non-zero")
}

fn parse(notation: &str) -> Quota {
    notation
        .parse()
        .unwrap_or_else(|error| panic!("{notation:?} should parse: {error}"))
}

fn parse_error(notation: &str) -> (QuotaErrorKind, Option<usize>) {
    let error = notation
        .parse::<Quota>()
        .expect_err(&format!("{notation:?} should not parse"));
    (error.kind(), error.position())
}

#[test]
fn parses_each_unit() {
    let cases = [
        ("5/millisecond", Duration::from_millis(1)),
        ("5/second", Duration::from_secs(1)),
        ("5/minute", Duration::from_secs(60)),
        ("5/hour", Duration::from_secs(3_600)),
        ("5/day", Duration::from_secs(86_400)),
    ];
    for (notation, period) in cases {
        let rates = parse(notation).rates().to_vec();
        assert_eq!(rates.len(), 1, "{notation}");
        assert_eq!(rates[0].count(), nz(5), "{notation}");
        assert_eq!(rates[0].period(), period, "{notation}");
        assert_eq!(
            rates[0].burst(),
            nz(5),
            "{notation}: burst defaults to the count"
        );
    }
}

#[test]
fn accepts_per_plurals_multipliers_case_and_spacing() {
    let expected = Rate::new(nz(10), Duration::from_secs(300)).expect("valid period");
    for notation in [
        "10/5 minutes",
        "10/5minutes",
        "10 per 5 minutes",
        "  10  /  5  MINUTES  ",
        "10 Per 5 Minute",
        "10/300 seconds",
    ] {
        assert_eq!(parse(notation), Quota::new(expected), "{notation:?}");
    }
}

#[test]
fn parses_burst() {
    let rate = parse("10/second burst 25").rates()[0];
    assert_eq!(rate.count(), nz(10));
    assert_eq!(rate.burst(), nz(25));
}

#[test]
fn sorts_rates_by_period_so_order_does_not_matter() {
    let forward = parse("10/second; 100/minute; 1000/day");
    let backward = parse("1000/day; 100/minute; 10/second");
    assert_eq!(forward, backward);
    assert_eq!(forward.to_string(), "10/second; 100/minute; 1000/day");
}

#[test]
fn reports_each_error_with_its_position() {
    let cases = [
        ("", QuotaErrorKind::Empty, None),
        ("   ", QuotaErrorKind::Empty, None),
        ("/second", QuotaErrorKind::ExpectedCount, Some(0)),
        ("0/second", QuotaErrorKind::ZeroCount, Some(0)),
        ("4294967296/second", QuotaErrorKind::NumberTooLarge, Some(0)),
        ("10 second", QuotaErrorKind::ExpectedSeparator, Some(3)),
        ("10/", QuotaErrorKind::ExpectedUnit, Some(3)),
        ("10/5", QuotaErrorKind::ExpectedUnit, Some(4)),
        ("10/secnd", QuotaErrorKind::UnknownUnit, Some(3)),
        ("10/0 seconds", QuotaErrorKind::ZeroMultiplier, Some(3)),
        ("10/10001 days", QuotaErrorKind::PeriodTooLong, Some(3)),
        ("10/second burst", QuotaErrorKind::ExpectedBurst, Some(15)),
        ("10/second burst 0", QuotaErrorKind::ZeroBurst, Some(16)),
        (
            "10/second, 5/day",
            QuotaErrorKind::ExpectedSemicolon,
            Some(9),
        ),
        ("10/second;", QuotaErrorKind::ExpectedCount, Some(10)),
        (
            "10/second; 20/second",
            QuotaErrorKind::DuplicatePeriod,
            Some(11),
        ),
        (
            "1/minute; 2/60 seconds",
            QuotaErrorKind::DuplicatePeriod,
            Some(10),
        ),
        (
            "1/second; 1/minute; 1/hour; 1/day; 1/2 days",
            QuotaErrorKind::TooManyRates,
            Some(35),
        ),
    ];
    for (notation, kind, position) in cases {
        assert_eq!(parse_error(notation), (kind, position), "{notation:?}");
    }
}

#[test]
fn error_display_includes_position() {
    let error = "10/secnd".parse::<Quota>().expect_err("unknown unit");
    assert_eq!(
        error.to_string(),
        "unknown unit, expected millisecond, second, minute, hour or day at byte 3"
    );
}

#[test]
fn macro_builds_the_same_quota_as_the_typed_api() {
    let typed = Quota::new(Rate::per_second(nz(10)))
        .with(Rate::per_day(nz(1000)))
        .expect("distinct periods");
    assert_eq!(quota!("10/second; 1000/day"), typed);
}

#[test]
fn macro_works_in_const_items() {
    const API: Quota = quota!("100 per minute");
    assert_eq!(API.rates()[0].period(), Duration::from_secs(60));
}

#[test]
fn rate_new_validates_the_period() {
    let cases = [
        (Duration::ZERO, QuotaErrorKind::ZeroPeriod),
        (
            Duration::from_micros(1500),
            QuotaErrorKind::PeriodNotWholeMilliseconds,
        ),
        (
            Duration::from_secs(10_001 * 86_400),
            QuotaErrorKind::PeriodTooLong,
        ),
        (Duration::MAX, QuotaErrorKind::PeriodNotWholeMilliseconds),
    ];
    for (period, kind) in cases {
        let error = Rate::new(nz(1), period).expect_err("invalid period");
        assert_eq!(error.kind(), kind, "{period:?}");
        assert_eq!(error.position(), None);
    }
}

#[test]
fn with_rejects_a_fifth_rate_and_duplicate_periods() {
    let mut quota = Quota::new(Rate::per_second(nz(1)));
    for rate in [
        Rate::per_minute(nz(1)),
        Rate::per_hour(nz(1)),
        Rate::per_day(nz(1)),
    ] {
        quota = quota.with(rate).expect("distinct periods");
    }
    assert_eq!(quota.rates().len(), MAX_RATES);
    let fifth = Rate::new(nz(1), Duration::from_secs(7 * 86_400)).expect("valid period");
    assert_eq!(
        quota.with(fifth).map_err(|e| e.kind()),
        Err(QuotaErrorKind::TooManyRates)
    );

    let single = Quota::new(Rate::per_second(nz(1)));
    assert_eq!(
        single.with(Rate::per_second(nz(2))).map_err(|e| e.kind()),
        Err(QuotaErrorKind::DuplicatePeriod)
    );
}

#[test]
fn equal_quotas_hash_equal() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash(quota: &Quota) -> u64 {
        let mut hasher = DefaultHasher::new();
        quota.hash(&mut hasher);
        hasher.finish()
    }
    let built_up = Quota::new(Rate::per_day(nz(9)))
        .with(Rate::per_second(nz(1)))
        .expect("distinct periods");
    let parsed = parse("1/second; 9/day");
    assert_eq!(built_up, parsed);
    assert_eq!(hash(&built_up), hash(&parsed));
}

const UNIT_MS: [u64; 5] = [1, 1_000, 60_000, 3_600_000, 86_400_000];
const MAX_PERIOD_MS: u64 = 10_000 * 86_400_000;

fn rate_strategy() -> impl Strategy<Value = Rate> {
    (
        1..=u32::MAX,
        1..=u32::MAX,
        prop::sample::select(&UNIT_MS[..]),
        1..=10_000_u64,
        any::<bool>(),
    )
        .prop_filter_map(
            "period within limit",
            |(count, burst, unit_ms, multiplier, custom_burst)| {
                let period_ms = unit_ms
                    .checked_mul(multiplier)
                    .filter(|ms| *ms <= MAX_PERIOD_MS)?;
                let rate = Rate::new(nz(count), Duration::from_millis(period_ms)).ok()?;
                Some(if custom_burst {
                    rate.with_burst(nz(burst))
                } else {
                    rate
                })
            },
        )
}

fn quota_strategy() -> impl Strategy<Value = Quota> {
    prop::collection::vec(rate_strategy(), 1..=MAX_RATES).prop_map(|rates| {
        let mut quota = Quota::new(rates[0]);
        for rate in &rates[1..] {
            // Duplicate periods are rejected; keeping the first one is fine here.
            if let Ok(with) = quota.with(*rate) {
                quota = with;
            }
        }
        quota
    })
}

proptest! {
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn display_then_parse_roundtrips(quota in quota_strategy()) {
        let text = quota.to_string();
        prop_assert_eq!(text.parse::<Quota>(), Ok(quota), "via {:?}", text);
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn parsing_arbitrary_text_never_panics_and_roundtrips_on_success(
        text in "[0-9a-zA-Z/;, ]{0,40}|\\PC{0,40}"
    ) {
        if let Ok(quota) = text.parse::<Quota>() {
            prop_assert_eq!(quota.to_string().parse::<Quota>(), Ok(quota));
        }
    }
}

#[test]
fn long_periods_that_display_in_milliseconds_parse_back() {
    // 5,000,000,001 ms is not a whole number of any larger unit, so it
    // displays in milliseconds with a multiplier past u32::MAX.
    let rate = Rate::new(
        NonZeroU32::new(10).unwrap(),
        Duration::from_millis(5_000_000_001),
    )
    .unwrap();
    let text = rate.to_string();
    assert_eq!(text, "10/5000000001 milliseconds");
    assert_eq!(text.parse::<Quota>().unwrap(), Quota::new(rate));
}
