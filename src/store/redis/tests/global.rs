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
