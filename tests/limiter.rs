//! The public `Limiter` API, driven by a manual clock.

use std::num::NonZeroUsize;
use std::time::Duration;

use porin::{
    Decision, FixedWindow, Limiter, ManualClock, SlidingLog, SlidingWindow, TokenBucket, WhenFull,
    quota,
};

fn block_on<F: Future>(future: F) -> F::Output {
    pollster::block_on(future)
}

fn allowed(decision: Result<Decision, porin::Error>) -> bool {
    decision.is_ok_and(|decision| decision.is_allowed())
}

fn max_keys(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap_or(NonZeroUsize::MIN)
}

#[test]
fn a_burst_is_allowed_then_the_wait_is_exact() {
    let clock = ManualClock::new();
    let limiter: Limiter<u64> = Limiter::builder()
        .quota(quota!("2/second"))
        .clock(clock.clone())
        .build();
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        assert!(allowed(limiter.hit(&1).await));
        let Ok(Decision::Denied(denial)) = limiter.hit(&1).await else {
            panic!("the third hit in a burst of 2 must be denied");
        };
        assert_eq!(denial.retry_after(), Some(Duration::from_millis(500)));
        assert_eq!(denial.stats().remaining(), 0);
        assert!(!denial.is_store_full());
        assert!(allowed(limiter.hit(&2).await), "other keys are separate");
        clock.advance(Duration::from_millis(500));
        assert!(allowed(limiter.hit(&1).await));
    });
}

#[test]
fn a_cost_above_the_burst_is_never_allowed() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("5/second"));
    block_on(async {
        let Ok(Decision::Denied(denial)) = limiter.hit_n(&1, 6).await else {
            panic!("6 units can never fit a burst of 5");
        };
        assert_eq!(denial.retry_after(), None);
        assert!(
            allowed(limiter.hit_n(&1, 5).await),
            "the denial spent nothing"
        );
    });
}

#[test]
fn several_rates_are_all_or_nothing() {
    let clock = ManualClock::new();
    let limiter: Limiter<u64> = Limiter::builder()
        .quota(quota!("2/second; 3/minute"))
        .clock(clock.clone())
        .build();
    block_on(async {
        assert!(allowed(limiter.hit_n(&1, 2).await));
        clock.advance(Duration::from_secs(1));
        assert!(
            !allowed(limiter.hit_n(&1, 2).await),
            "the per-second rate allows 2, the per-minute rate only 1"
        );
        assert!(
            allowed(limiter.hit(&1).await),
            "the denied hit spent nothing from the per-second rate"
        );
    });
}

#[test]
fn test_and_stats_spend_nothing() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("1/minute"));
    block_on(async {
        assert!(allowed(limiter.test(&1).await));
        assert!(allowed(limiter.test(&1).await));
        assert_eq!(limiter.stats(&1).await.unwrap().remaining(), 1);
        assert!(allowed(limiter.hit(&1).await));
        let stats = limiter.stats(&1).await.unwrap();
        assert_eq!(stats.remaining(), 0);
        assert!(stats.reset_after() > Duration::from_secs(59));
        assert!(!allowed(limiter.test(&1).await));
        assert!(!allowed(limiter.test_n(&1, 2).await));
    });
}

#[test]
fn reset_gives_the_full_allowance_back() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("1/hour"));
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        assert!(!allowed(limiter.hit(&1).await));
        limiter.reset(&1).await.unwrap();
        assert!(allowed(limiter.hit(&1).await));
    });
}

#[test]
fn string_keys_are_hit_with_borrowed_strs() {
    let limiter: Limiter<String> = Limiter::memory(quota!("1/minute"));
    block_on(async {
        assert!(allowed(limiter.hit("user-1").await));
        assert!(!allowed(limiter.hit(&String::from("user-1")).await));
        assert!(allowed(limiter.hit("user-2").await));
    });
}

#[test]
fn every_algorithm_limits() {
    fn check<A: porin::Algorithm>(algorithm: A) {
        let limiter: Limiter<u64, A> = Limiter::builder()
            .algorithm(algorithm)
            .quota(quota!("3/minute"))
            .clock(ManualClock::new())
            .build();
        block_on(async {
            for n in 0..3 {
                assert!(allowed(limiter.hit(&1).await), "{algorithm:?}: hit {n}");
            }
            assert!(!allowed(limiter.hit(&1).await), "{algorithm:?}: 4th hit");
        });
    }
    check(TokenBucket);
    check(FixedWindow);
    check(SlidingWindow);
    check(SlidingLog);
}

#[test]
fn a_full_store_refuses_new_keys_by_default() {
    let clock = ManualClock::new();
    let limiter: Limiter<u64> = Limiter::builder()
        .quota(quota!("1/second"))
        .max_keys(max_keys(1))
        .clock(clock.clone())
        .build();
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        let Ok(Decision::Denied(denial)) = limiter.hit(&2).await else {
            panic!("no room for key 2");
        };
        assert!(denial.is_store_full());
        assert_eq!(denial.retry_after(), Some(Duration::from_secs(1)));
        assert_eq!(limiter.full_hits(), 1);
        clock.advance(Duration::from_secs(1));
        assert!(allowed(limiter.hit(&2).await), "key 1 went idle");
    });
}

#[test]
fn a_full_store_can_share_an_overflow_quota() {
    let limiter: Limiter<u64> = Limiter::builder()
        .quota(quota!("1/second"))
        .max_keys(max_keys(1))
        .when_full(WhenFull::Overflow(quota!("2/second; 10/minute")))
        .clock(ManualClock::new())
        .build();
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        assert!(allowed(limiter.hit(&2).await));
        assert!(allowed(limiter.hit(&3).await));
        assert!(
            !allowed(limiter.hit(&4).await),
            "the overflow quota is spent"
        );
    });
}

#[test]
fn a_full_store_can_evict_the_key_nearest_to_idle() {
    let clock = ManualClock::new();
    let limiter: Limiter<u64> = Limiter::builder()
        .quota(quota!("1/second"))
        .max_keys(max_keys(1))
        .when_full(WhenFull::EvictNearestIdle {
            within: Duration::from_millis(100),
        })
        .clock(clock.clone())
        .build();
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        clock.advance(Duration::from_millis(950));
        assert!(allowed(limiter.hit(&2).await), "key 1 is 50 ms from idle");
    });
}

#[test]
fn clones_share_state() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("1/minute"));
    let clone = limiter.clone();
    block_on(async {
        assert!(allowed(limiter.hit(&1).await));
        assert!(!allowed(clone.hit(&1).await));
    });
}

#[test]
fn limiters_and_their_futures_cross_threads() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    fn assert_send<T: Send>(_: &T) {}
    let limiter: Limiter<String, SlidingLog> = Limiter::builder()
        .algorithm(SlidingLog)
        .quota(quota!("1/second"))
        .build();
    assert_send_sync(&limiter);
    let future = limiter.hit("key");
    assert_send(&future);
    drop(future);
}

#[test]
fn debug_names_the_algorithm_and_quota() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("10/second"));
    let debug = format!("{limiter:?}");
    assert!(
        debug.starts_with("Limiter { algorithm: TokenBucket, quota: "),
        "{debug}"
    );
}

#[test]
fn until_ready_waits_for_room() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("100/second"));
    block_on(async {
        assert!(allowed(limiter.hit_n(&1, 100).await), "spend the burst");
        let start = std::time::Instant::now();
        assert!(allowed(limiter.until_ready(&1).await));
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(9),
            "one token refills in 10 ms; waited {waited:?}"
        );
    });
}

#[test]
fn until_ready_returns_at_once_when_the_cost_never_fits() {
    let limiter: Limiter<u64> = Limiter::memory(quota!("5/hour"));
    block_on(async {
        let Ok(Decision::Denied(denial)) = limiter.until_ready_n(&1, 6).await else {
            panic!("6 units never fit a burst of 5");
        };
        assert_eq!(denial.retry_after(), None);
    });
}
