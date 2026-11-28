//! [`FailOver`]: the per-instance fallback answers outages, and only them.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use super::{Server, free_port, spawn_server};
use crate::store::{CircuitBreaker, FailOver, RedisStore};
use crate::{Decision, Error, Limiter, Quota, SlidingWindow, quota};

fn instances(count: u32) -> FailOver {
    FailOver::to_memory(NonZeroU32::new(count).unwrap())
}

/// A store whose breaker opens on the first failure and stays open for a
/// minute, so a test sees one failed call and then the open breaker.
async fn store(server: &Server, policy: FailOver) -> RedisStore<FailOver> {
    RedisStore::builder(server.url.as_str())
        .on_failure(policy)
        .circuit_breaker(
            CircuitBreaker::new()
                .failures(NonZeroU32::MIN)
                .first_open(Duration::from_secs(60)),
        )
        .connect()
        .await
        .unwrap()
}

fn limiter<P: crate::store::FailurePolicy>(
    redis: &RedisStore<P>,
    id: &str,
    quota: Quota,
) -> Limiter<u64, crate::TokenBucket, RedisStore<P>> {
    Limiter::redis_builder(redis, id)
        .quota(quota)
        .build()
        .unwrap()
}

fn sync_connection(url: &str) -> ::redis::Connection {
    ::redis::Client::open(url)
        .and_then(|client| client.get_connection())
        .unwrap()
}

/// Sets `maxmemory`: 1 makes every write fail with OOM, an outage that
/// keeps Redis's state; 0 lifts it.
fn max_memory(admin: &mut ::redis::Connection, bytes: &str) {
    ::redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory-policy", "noeviction"])
        .exec(admin)
        .unwrap();
    ::redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", bytes])
        .exec(admin)
        .unwrap();
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn outages_get_the_divided_quota_exactly_and_marked() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, instances(4)).await;
    // 8 an hour over 4 instances: 2 each.
    let limiter = limiter(&redis, "divided", quota!("8/hour"));
    let first = limiter.hit(&1).await.unwrap();
    assert!(first.stats().is_known() && !first.stats().is_fallback());

    server.stop();
    // The first call fails at Redis, opening the breaker; the rest are
    // answered by the open breaker. Both go to the fallback.
    let stats = limiter.stats(&1).await.unwrap();
    assert!(stats.is_fallback());
    assert_eq!(stats.remaining(), 2, "the share, whatever Redis had");
    for remaining in [1, 0] {
        let decision = limiter.hit(&1).await.unwrap();
        assert!(decision.is_allowed(), "within the share");
        assert!(decision.stats().is_fallback());
        assert!(!decision.stats().is_known());
        assert_eq!(decision.stats().remaining(), remaining);
    }
    for _ in 0..20 {
        let Decision::Denied(denial) = limiter.hit(&1).await.unwrap() else {
            panic!("past the share of 2");
        };
        assert!(denial.stats().is_fallback());
        assert!(!denial.is_store_unavailable(), "the fallback's own limit");
        let retry = denial.retry_after().unwrap();
        assert!(
            retry > Duration::from_secs(29 * 60) && retry <= Duration::from_secs(30 * 60),
            "one unit of 2/hour: {retry:?}"
        );
    }
    assert!(
        limiter.hit(&2).await.unwrap().is_allowed(),
        "other keys have their own share"
    );
    assert_eq!(redis.policy_answers(), 24);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn racing_threads_never_pass_the_share() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let redis = runtime.block_on(store(&server, instances(3)));
    // 100 a minute over 3 instances: 33 each, in a sliding window.
    let limiter: Limiter<u64, SlidingWindow, _> = Limiter::redis_builder(&redis, "race")
        .algorithm(SlidingWindow)
        .quota(quota!("100/minute"))
        .build()
        .unwrap();
    server.stop();
    let allowed = runtime.block_on(async {
        let tasks: Vec<_> = (0..400)
            .map(|_| {
                let limiter = limiter.clone();
                tokio::spawn(async move { limiter.hit(&7).await.unwrap() })
            })
            .collect();
        let mut allowed = 0;
        for task in tasks {
            let decision = task.await.unwrap();
            assert!(decision.stats().is_fallback());
            allowed += usize::from(decision.is_allowed());
        }
        allowed
    });
    assert_eq!(allowed, 33);
}

/// Redis's answers are final: its denials never reach the fallback, and
/// nothing moves between the two in either direction.
#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn redis_answers_never_touch_the_fallback() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = RedisStore::builder(server.url.as_str())
        .on_failure(instances(1))
        .circuit_breaker(CircuitBreaker::new().failures(NonZeroU32::new(1_000).unwrap()))
        .connect()
        .await
        .unwrap();
    let limiter = limiter(&redis, "final", quota!("3/hour"));
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    assert!(limiter.hit(&1).await.unwrap().is_allowed());

    let mut admin = sync_connection(&server.url);
    max_memory(&mut admin, "1");
    // The fallback starts from nothing: the two hits Redis took aren't in
    // it, so it allows its whole share of 3.
    for _ in 0..3 {
        let decision = limiter.hit(&1).await.unwrap();
        assert!(decision.is_allowed() && decision.stats().is_fallback());
    }
    assert!(!limiter.hit(&1).await.unwrap().is_allowed());

    max_memory(&mut admin, "0");
    // Back to Redis, which still has the two hits and none of the
    // fallback's three.
    let back = limiter.hit(&1).await.unwrap();
    assert!(back.is_allowed() && back.stats().is_known());
    assert_eq!(back.stats().remaining(), 0);
    let answers = redis.policy_answers();
    let Decision::Denied(denial) = limiter.hit(&1).await.unwrap() else {
        panic!("Redis denies the fourth hit");
    };
    assert!(denial.stats().is_known() && !denial.stats().is_fallback());
    assert_eq!(redis.policy_answers(), answers, "Redis answered it");
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn permanent_errors_and_resets_stay_errors() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, instances(1)).await;
    let limiter = limiter(&redis, "errors", quota!("5/hour"));
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());

    // A key of the wrong type where the state should be: Redis answers
    // WRONGTYPE, which retrying won't fix.
    let mut admin = sync_connection(&server.url);
    let names: Vec<String> = ::redis::cmd("KEYS")
        .arg("porin:*")
        .query::<Vec<String>>(&mut admin)
        .unwrap()
        .into_iter()
        .filter(|key| !key.contains("{connect}"))
        .collect();
    let [name] = names.as_slice() else {
        panic!("one key: {names:?}");
    };
    ::redis::cmd("DEL").arg(name).exec(&mut admin).unwrap();
    ::redis::cmd("LPUSH")
        .arg(name)
        .arg("x")
        .exec(&mut admin)
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(limiter.hit(&1).await, Err(Error::Store(_))));
    }
    assert_eq!(redis.policy_answers(), 0, "the fallback never answered");

    server.stop();
    assert!(limiter.hit(&2).await.unwrap().stats().is_fallback());
    assert!(
        matches!(limiter.reset(&2).await, Err(Error::Store(_))),
        "reset is an error during an outage"
    );
    let after = limiter.hit(&2).await.unwrap();
    assert_eq!(
        after.stats().remaining(),
        3,
        "the failed reset left the fallback's count alone"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_full_fallback_refuses_new_keys() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(
        &server,
        instances(1).max_keys(NonZeroUsize::new(1).unwrap()),
    )
    .await;
    let limiter = limiter(&redis, "full", quota!("5/hour"));
    server.stop();
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    let Decision::Denied(denial) = limiter.hit(&2).await.unwrap() else {
        panic!("no room for a second key");
    };
    assert!(denial.is_store_full() && denial.stats().is_fallback());
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_reset_after_recovery_clears_the_fallback_too() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = RedisStore::builder(server.url.as_str())
        .on_failure(instances(1))
        .circuit_breaker(CircuitBreaker::new().failures(NonZeroU32::new(1_000).unwrap()))
        .connect()
        .await
        .unwrap();
    let limiter = limiter(&redis, "reset", quota!("1/hour"));
    let mut admin = sync_connection(&server.url);
    max_memory(&mut admin, "1");
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    assert!(!limiter.hit(&1).await.unwrap().is_allowed());
    max_memory(&mut admin, "0");
    limiter.reset(&1).await.unwrap();
    max_memory(&mut admin, "1");
    let decision = limiter.hit(&1).await.unwrap();
    assert!(
        decision.is_allowed() && decision.stats().is_fallback(),
        "the reset reached the fallback"
    );
}
