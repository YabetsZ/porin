//! The blocking `Limiter`.
#![cfg(feature = "blocking")]

use std::time::{Duration, Instant};

use porin::blocking::Limiter;
use porin::{Decision, ManualClock, SlidingWindow, quota};

#[test]
fn hits_tests_stats_and_resets_without_a_runtime() {
    let limiter: Limiter<String> = Limiter::memory(quota!("2/minute"));
    assert!(limiter.test("a").unwrap().is_allowed());
    assert!(limiter.hit("a").unwrap().is_allowed());
    assert_eq!(limiter.stats("a").unwrap().remaining(), 1);
    assert!(limiter.hit_n("a", 1).unwrap().is_allowed());
    assert!(!limiter.hit("a").unwrap().is_allowed());
    assert!(!limiter.test_n("a", 1).unwrap().is_allowed());
    limiter.reset("a").unwrap();
    assert!(limiter.hit("a").unwrap().is_allowed());
}

#[test]
fn shares_state_with_the_async_limiter_it_came_from() {
    let clock = ManualClock::new();
    let limiter: porin::Limiter<u64, SlidingWindow> = porin::Limiter::builder()
        .algorithm(SlidingWindow)
        .quota(quota!("1/second"))
        .clock(clock)
        .build();
    let blocking = Limiter::from(limiter.clone());
    assert!(blocking.hit(&1).unwrap().is_allowed());
    let async_side = pollster::block_on(limiter.hit(&1)).unwrap();
    assert!(!async_side.is_allowed(), "the blocking hit spent the unit");
    assert_eq!(blocking.as_async().quota(), &quota!("1/second"));
}

#[test]
fn until_ready_sleeps_until_there_is_room() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("100/second"));
    assert!(limiter.hit_n(&1, 100).unwrap().is_allowed());
    let start = Instant::now();
    assert!(limiter.until_ready(&1).unwrap().is_allowed());
    assert!(start.elapsed() >= Duration::from_millis(9));
    let Ok(Decision::Denied(denial)) = limiter.until_ready_n(&1, 101) else {
        panic!("101 units never fit a burst of 100");
    };
    assert_eq!(denial.retry_after(), None);
}

#[test]
fn is_send_sync_and_clone() {
    fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
    assert_send_sync_clone::<Limiter<String>>();
}
