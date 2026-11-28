//! One global limit on Redis, and bounded waits.

use std::time::{Duration, Instant};

use super::{free_port, spawn_server};
use crate::store::{Propagate, RedisStore};
use crate::{Limiter, quota};

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_global_limit_is_shared_by_every_instance() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let connect = || async {
        RedisStore::builder(server.url.as_str())
            .on_failure(Propagate)
            .connect()
            .await
            .unwrap()
    };
    // Two stores, as two instances of a service calling one outside API.
    let one: Limiter<(), _, _> = Limiter::redis_builder(&connect().await, "partner-api")
        .quota(quota!("3/minute"))
        .build()
        .unwrap();
    let two: Limiter<(), _, _> = Limiter::redis_builder(&connect().await, "partner-api")
        .quota(quota!("3/minute"))
        .build()
        .unwrap();
    assert!(one.hit(&()).await.unwrap().is_allowed());
    assert!(two.hit(&()).await.unwrap().is_allowed());
    assert!(one.hit(&()).await.unwrap().is_allowed());
    assert!(
        !two.hit(&()).await.unwrap().is_allowed(),
        "the third unit was the last"
    );

    let start = Instant::now();
    let decision = two
        .until_ready_within(&(), Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!decision.is_allowed(), "the next unit is 20 s away");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "and it doesn't wait for it"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_bounded_wait_gives_up_during_an_outage() {
    use crate::store::FailClosed;

    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = RedisStore::builder(server.url.as_str())
        .on_failure(FailClosed)
        .connect()
        .await
        .unwrap();
    let limiter: Limiter<(), _, _> = Limiter::redis_builder(&redis, "outage")
        .quota(quota!("10/second"))
        .build()
        .unwrap();
    server.stop();
    // Fail closed denies with a retry time; waiting for Redis to come back
    // would take longer than allowed, so it gives up instead of sleeping on.
    let start = Instant::now();
    let decision = limiter
        .until_ready_within(&(), Duration::from_millis(50))
        .await
        .unwrap();
    let crate::Decision::Denied(denial) = decision else {
        panic!("fail closed denies while Redis is down");
    };
    assert!(denial.is_store_unavailable());
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
}
