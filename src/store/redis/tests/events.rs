//! The store's event hook and its count of policy answers.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{free_port, spawn_server};
use crate::store::{CircuitBreaker, Event, FailClosed, RedisStore};
use crate::{Limiter, quota};

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn breaker_and_reconnect_events_fire_once_each_in_order() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    // Open long enough for the server to restart before the probe.
    let open = Duration::from_secs(2);
    let redis = RedisStore::builder(server.url.as_str())
        .on_failure(FailClosed)
        .circuit_breaker(CircuitBreaker::new().first_open(open))
        .on_event(move |event| seen.lock().unwrap().push(event))
        .connect()
        .await
        .unwrap();
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "events")
        .quota(quota!("100/hour"))
        .build()
        .unwrap();
    let recorded = || events.lock().unwrap().clone();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());
    assert_eq!(recorded(), [], "a healthy store says nothing");

    server.stop();
    for _ in 0..10 {
        assert!(!limiter.hit(&1).await.unwrap().stats().is_known());
    }
    assert_eq!(recorded(), [Event::BreakerOpened { open_for: open }]);
    assert_eq!(redis.policy_answers(), 10);

    server.start();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !limiter.hit(&1).await.unwrap().stats().is_known() {
        assert!(Instant::now() < deadline, "the store came back");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for _ in 0..5 {
        assert!(limiter.hit(&1).await.unwrap().stats().is_known());
    }
    assert_eq!(
        recorded(),
        [
            Event::BreakerOpened { open_for: open },
            Event::Reconnected,
            Event::BreakerClosed,
        ]
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_panicking_hook_leaves_the_store_working() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = RedisStore::builder(server.url.as_str())
        .on_failure(FailClosed)
        .circuit_breaker(
            CircuitBreaker::new()
                .failures(std::num::NonZeroU32::MIN)
                .first_open(Duration::from_secs(60)),
        )
        .on_event(|event| {
            if matches!(event, Event::BreakerOpened { .. }) {
                panic!("a buggy hook");
            }
        })
        .connect()
        .await
        .unwrap();
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "panics")
        .quota(quota!("100/hour"))
        .build()
        .unwrap();
    server.stop();
    let limiter_in_task = limiter.clone();
    let opened = tokio::spawn(async move { limiter_in_task.hit(&1).await }).await;
    assert!(opened.is_err(), "the panic reached the call");
    // The breaker opened before the hook ran, so it answers from now on.
    let crate::Decision::Denied(denial) = limiter.hit(&1).await.unwrap() else {
        panic!("fail closed");
    };
    assert!(denial.retry_after().unwrap() > Duration::from_secs(55));
}
