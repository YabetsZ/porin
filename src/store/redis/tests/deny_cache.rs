//! The deny cache: Redis's denials answered locally, without a round trip,
//! and never anything Redis would deny allowed.

use std::cell::Cell;
use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::{Server, free_port, runtime, spawn_server};
use crate::store::{CircuitBreaker, FailClosed, RedisStore};
use crate::{Decision, Limiter, SlidingLog, quota};

async fn store(server: &Server, max_keys: usize) -> RedisStore<FailClosed> {
    RedisStore::builder(server.url.as_str())
        .on_failure(FailClosed)
        .deny_cache(NonZeroUsize::new(max_keys).unwrap())
        // Never open, so every call reaches Redis unless the cache answers.
        .circuit_breaker(CircuitBreaker::new().failures(NonZeroU32::MAX))
        .connect()
        .await
        .unwrap()
}

/// Scripts Redis has run so far, from `INFO commandstats`.
fn script_calls(url: &str) -> u64 {
    let mut connection = ::redis::Client::open(url)
        .and_then(|client| client.get_connection())
        .unwrap();
    let info: String = ::redis::cmd("INFO")
        .arg("commandstats")
        .query(&mut connection)
        .unwrap();
    ["cmdstat_evalsha:", "cmdstat_eval:"]
        .iter()
        .filter_map(|name| {
            let line = info.lines().find(|line| line.starts_with(name))?;
            let calls = line.split("calls=").nth(1)?.split(',').next()?;
            calls.parse::<u64>().ok()
        })
        .sum()
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn cached_denials_skip_redis_and_outlast_an_outage() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, 1_000).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "skip")
        .quota(quota!("2/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    let Decision::Denied(first) = limiter.hit(&1).await.unwrap() else {
        panic!("Redis denies the third hit");
    };
    let calls = script_calls(&server.url);
    let mut retry = first.retry_after().unwrap();
    for _ in 0..50 {
        let Decision::Denied(cached) = limiter.hit(&1).await.unwrap() else {
            panic!("the cache denies");
        };
        assert!(cached.stats().is_known() && !cached.is_store_unavailable());
        let next = cached.retry_after().unwrap();
        assert!(next <= retry, "counts down: {next:?} after {retry:?}");
        retry = next;
    }
    assert!(!limiter.test(&1).await.unwrap().is_allowed());
    assert_eq!(script_calls(&server.url), calls, "no script ran");

    assert!(limiter.hit(&2).await.unwrap().is_allowed());
    assert_eq!(script_calls(&server.url), calls + 1, "other keys ask Redis");
    assert!(
        limiter.stats(&1).await.unwrap().is_known(),
        "stats come from Redis"
    );

    server.stop();
    let Decision::Denied(during) = limiter.hit(&1).await.unwrap() else {
        panic!("the cache denies during an outage");
    };
    assert!(
        !during.is_store_unavailable(),
        "denied as limited, not as unavailable"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_cached_denial_ends_at_retry_after() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, 1_000).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "ends")
        .quota(quota!("1/300 milliseconds"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    let Decision::Denied(denial) = limiter.hit(&1).await.unwrap() else {
        panic!("denied");
    };
    let calls = script_calls(&server.url);
    let Decision::Denied(cached) = limiter.hit(&1).await.unwrap() else {
        panic!("cached");
    };
    assert_eq!(script_calls(&server.url), calls);
    tokio::time::sleep(cached.retry_after().unwrap() + Duration::from_millis(5)).await;
    assert!(
        limiter.hit(&1).await.unwrap().is_allowed(),
        "after {:?} the key asks Redis again",
        denial.retry_after()
    );
    assert_eq!(script_calls(&server.url), calls + 1);
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn only_denials_that_end_are_cached() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, 1_000).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "only")
        .quota(quota!("5/hour"))
        .build()
        .unwrap();
    // More than the quota ever allows: denied with no retry_after.
    let calls = script_calls(&server.url);
    for _ in 0..3 {
        let Decision::Denied(never) = limiter.hit_n(&1, 6).await.unwrap() else {
            panic!("6 never fit in 5");
        };
        assert_eq!(never.retry_after(), None);
    }
    assert_eq!(script_calls(&server.url), calls + 3, "each asked Redis");
    assert!(
        limiter.hit_n(&1, 5).await.unwrap().is_allowed(),
        "the never-fitting denial didn't deny smaller costs"
    );

    // Failure policy denials aren't cached either.
    server.stop();
    let Decision::Denied(down) = limiter.hit(&2).await.unwrap() else {
        panic!("fail closed");
    };
    assert!(down.is_store_unavailable());
    server.start();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let decision = limiter.hit(&2).await.unwrap();
        if decision.stats().is_known() {
            assert!(decision.is_allowed(), "Redis allows key 2");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stuck on {decision:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn reset_clears_the_cached_denial() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, 1_000).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "reset")
        .quota(quota!("1/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
    assert!(!limiter.hit(&1).await.unwrap().is_allowed());
    limiter.reset(&1).await.unwrap();
    assert!(limiter.hit(&1).await.unwrap().is_allowed());
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_full_cache_asks_redis() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let redis = store(&server, 1).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "full")
        .quota(quota!("1/hour"))
        .build()
        .unwrap();
    for key in [1, 2] {
        assert!(limiter.hit(&key).await.unwrap().is_allowed());
        assert!(!limiter.hit(&key).await.unwrap().is_allowed());
    }
    let calls = script_calls(&server.url);
    assert!(!limiter.hit(&1).await.unwrap().is_allowed());
    assert_eq!(script_calls(&server.url), calls, "key 1 is cached");
    for _ in 0..3 {
        assert!(!limiter.hit(&2).await.unwrap().is_allowed());
    }
    assert_eq!(
        script_calls(&server.url),
        calls + 3,
        "no room for key 2: Redis denies it each time"
    );
}

/// Over random hits, tests and resets on a few keys, the cached limiter
/// allows only what an exact model of the quota allows. A sliding log of
/// 20 a day forgets nothing during a test, so the model is a sum.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn never_allows_what_redis_would_deny() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let runtime = runtime();
    let redis = runtime.block_on(store(&server, 1_000));
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(64);
    let ops = prop::collection::vec((0..3_u64, 0..25_u32, 0..10_u8), 1..80);
    let mut runner = TestRunner::new(Config::with_cases(cases));
    let (case, allowed, denied) = (Cell::new(0_u32), Cell::new(0_u32), Cell::new(0_u32));
    // Calls that would each run a script without the cache.
    let calls = Cell::new(0_u64);
    let scripts_before = script_calls(&server.url);
    let result = runner.run(&ops, |ops| {
        case.set(case.get() + 1);
        let limiter: Limiter<u64, SlidingLog, _> =
            Limiter::redis_builder(&redis, &format!("model{}", case.get()))
                .algorithm(SlidingLog)
                .quota(quota!("20/day"))
                .build()
                .unwrap();
        let mut used: HashMap<u64, u32> = HashMap::new();
        for (key, cost, kind) in ops {
            let spent = used.entry(key).or_default();
            let fits = *spent + cost <= 20;
            match kind {
                0 => {
                    runtime.block_on(limiter.reset(&key)).unwrap();
                    *spent = 0;
                }
                1 | 2 => {
                    calls.set(calls.get() + 1);
                    let decision = runtime.block_on(limiter.test_n(&key, cost)).unwrap();
                    prop_assert!(!decision.is_allowed() || fits, "test {key} {cost}");
                }
                _ => {
                    calls.set(calls.get() + 1);
                    let decision = runtime.block_on(limiter.hit_n(&key, cost)).unwrap();
                    prop_assert!(
                        !decision.is_allowed() || fits,
                        "hit {} {} after {}",
                        key,
                        cost,
                        spent
                    );
                    if decision.is_allowed() {
                        *spent += cost;
                        allowed.set(allowed.get() + 1);
                    } else {
                        denied.set(denied.get() + 1);
                    }
                }
            }
        }
        Ok::<(), TestCaseError>(())
    });
    if let Err(error) = result {
        panic!("{error}");
    }
    let (allowed, denied) = (allowed.get(), denied.get());
    assert!(
        allowed > 0 && denied > 0,
        "{allowed} allowed, {denied} denied"
    );
    // And the cache answered some of them: without it, every call would
    // have run a script.
    let scripts = script_calls(&server.url) - scripts_before;
    assert!(
        scripts < calls.get(),
        "{scripts} scripts for {} calls: the cache never answered",
        calls.get()
    );
}
