//! The clocks and timestamp arithmetic.
#![cfg(test)]

use std::time::Duration;

use porin::{Clock, ManualClock, MonotonicClock, Timestamp};

#[test]
fn manual_clock_moves_only_when_told() {
    let clock = ManualClock::new();
    assert_eq!(clock.now(), Timestamp::ZERO);
    assert_eq!(clock.now(), Timestamp::ZERO);

    clock.advance(Duration::from_secs(2));
    assert_eq!(clock.now(), Timestamp::from_nanos(2_000_000_000));

    clock.set(Timestamp::from_nanos(5));
    assert_eq!(
        clock.now(),
        Timestamp::from_nanos(5),
        "set may move backwards"
    );
}

#[test]
fn manual_clock_clones_share_time() {
    let test_handle = ManualClock::starting_at(Timestamp::from_nanos(100));
    let code_handle = test_handle.clone();
    test_handle.advance(Duration::from_nanos(50));
    assert_eq!(code_handle.now(), Timestamp::from_nanos(150));
}

#[test]
fn manual_clock_saturates_instead_of_wrapping() {
    let clock = ManualClock::starting_at(Timestamp::from_nanos(u64::MAX - 1));
    clock.advance(Duration::from_secs(1));
    assert_eq!(clock.now(), Timestamp::from_nanos(u64::MAX));
    clock.advance(Duration::MAX);
    assert_eq!(clock.now(), Timestamp::from_nanos(u64::MAX));
}

#[test]
fn manual_clock_advances_from_many_threads_without_losing_steps() {
    let clock = ManualClock::new();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let clock = clock.clone();
            scope.spawn(move || {
                for _ in 0..1_000 {
                    clock.advance(Duration::from_nanos(1));
                }
            });
        }
    });
    assert_eq!(clock.now(), Timestamp::from_nanos(4_000));
}

#[test]
fn monotonic_clock_never_goes_backwards() {
    let clock = MonotonicClock::new();
    let mut previous = clock.now();
    for _ in 0..1_000 {
        let now = clock.now();
        assert!(now >= previous, "{now:?} < {previous:?}");
        previous = now;
    }
}

#[test]
fn timestamp_arithmetic_saturates() {
    let late = Timestamp::from_nanos(u64::MAX);
    assert_eq!(late.saturating_add(Duration::from_secs(1)), late);
    let early = Timestamp::from_nanos(10);
    assert_eq!(early.saturating_duration_since(late), Duration::ZERO);
    assert_eq!(
        late.saturating_duration_since(early),
        Duration::from_nanos(u64::MAX - 10)
    );
}
