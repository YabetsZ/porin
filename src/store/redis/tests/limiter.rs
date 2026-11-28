//! Limiters on a Redis store, through the public API: decisions, key
//! layout, atomicity across clients, outages, Cluster and Sentinel.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ::redis::{ConnectionAddr, TlsMode};

use super::super::tls_mode;
use super::{Server, binary, free_port, server, spawn_server};
use crate::store::{CircuitBreaker, FailClosed, FailOpen, FailurePolicy, Propagate, RedisStore};
use crate::{
    Decision, Error, FixedWindow, KeyEncode, Limiter, QuotaErrorKind, Rate, SlidingLog,
    SlidingWindow, quota,
};

async fn store<P: FailurePolicy>(server: &Server, policy: P) -> RedisStore<P> {
    RedisStore::builder(server.url.as_str())
        .on_failure(policy)
        .connect()
        .await
        .expect("connects")
}

/// The keys limiters wrote, leaving out the one a connect check writes
/// (it expires within a second).
fn limiter_keys(connection: &mut ::redis::Connection) -> Vec<String> {
    let keys: Vec<String> = ::redis::cmd("KEYS")
        .arg("porin:*")
        .query(connection)
        .unwrap();
    keys.into_iter()
        .filter(|key| !key.contains("{connect}"))
        .collect()
}

fn sync_connection(url: &str) -> ::redis::Connection {
    ::redis::Client::open(url)
        .and_then(|client| client.get_connection())
        .expect("a connection")
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn hits_spend_and_tests_dont() {
    let Some(server) = server() else { return };
    let redis = store(&server, Propagate).await;
    let limiter: Limiter<String, _, _> =
        Limiter::redis_builder(&redis, "hits_spend_and_tests_dont")
            .quota(quota!("2/minute"))
            .build()
            .unwrap();
    let alice = "alice";
    assert!(limiter.test(alice).await.unwrap().is_allowed());
    assert_eq!(limiter.stats(alice).await.unwrap().remaining(), 2);
    let first = limiter.hit(alice).await.unwrap();
    assert!(first.is_allowed());
    assert_eq!(first.stats().remaining(), 1);
    assert!(first.stats().is_known());
    assert!(limiter.hit(alice).await.unwrap().is_allowed());
    let denied = limiter.hit(alice).await.unwrap();
    let crate::Decision::Denied(denial) = denied else {
        panic!("a third hit in a minute is denied");
    };
    let retry = denial.retry_after().unwrap();
    assert!(
        retry > Duration::from_secs(25) && retry <= Duration::from_secs(30),
        "{retry:?}"
    );
    assert!(!denial.is_store_unavailable());
    assert!(
        limiter.hit("bob").await.unwrap().is_allowed(),
        "keys don't share"
    );
    limiter.reset(alice).await.unwrap();
    assert_eq!(limiter.stats(alice).await.unwrap().remaining(), 2);
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn keys_are_namespaced_hash_tagged_and_expire() {
    let Some(server) = server() else { return };
    let redis = store(&server, Propagate).await;
    let limiter: Limiter<(u64, String), FixedWindow, _> = Limiter::redis_builder(&redis, "api")
        .algorithm(FixedWindow)
        .quota(quota!("5/second; 100/hour"))
        .build()
        .unwrap();
    let other: Limiter<(u64, String), FixedWindow, _> = Limiter::redis_builder(&redis, "admin")
        .algorithm(FixedWindow)
        .quota(quota!("5/second; 100/hour"))
        .build()
        .unwrap();
    let key = (7, "a}b".to_owned());
    assert!(limiter.hit_n(&key, 5).await.unwrap().is_allowed());
    assert!(!limiter.hit(&key).await.unwrap().is_allowed());
    assert!(
        other.hit(&key).await.unwrap().is_allowed(),
        "limiters with different ids don't share"
    );

    let mut connection = sync_connection(&server.url);
    // "api", then 7 as 8 bytes and "a}b" with its length, all in hex; then
    // the algorithm and the quota's fingerprint, computed by hand.
    let tag = "porin:v1:{617069:000000000000000703617d62}:fixed_window:53e55081";
    for rate in 0..2 {
        let name = format!("{tag}:{rate}");
        let expires: i64 = ::redis::cmd("PEXPIRETIME")
            .arg(&name)
            .query(&mut connection)
            .unwrap();
        assert!(expires > 0, "{name} exists and expires");
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_changed_quota_or_algorithm_starts_keys_fresh() {
    let Some(server) = server() else { return };
    let redis = store(&server, Propagate).await;
    let two: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "change")
        .quota(quota!("2/hour"))
        .build()
        .unwrap();
    assert!(two.hit_n(&1, 2).await.unwrap().is_allowed());
    assert!(!two.hit(&1).await.unwrap().is_allowed());

    // Same id, same number of rates, a different rate: old state would
    // leave 1 of 3; a fresh key has all 3.
    let three: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "change")
        .quota(quota!("3/hour"))
        .build()
        .unwrap();
    assert_eq!(three.stats(&1).await.unwrap().remaining(), 3);

    // Same quota, another algorithm: its script must not read the token
    // bucket's state, which it couldn't parse.
    let window: Limiter<u64, SlidingWindow, _> = Limiter::redis_builder(&redis, "change")
        .algorithm(SlidingWindow)
        .quota(quota!("2/hour"))
        .build()
        .unwrap();
    assert!(window.hit_n(&1, 2).await.unwrap().is_allowed());
    assert!(
        !two.hit(&1).await.unwrap().is_allowed(),
        "the old limiter's key is untouched"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn keys_longer_than_512_bytes_are_refused_under_every_policy() {
    let Some(server) = server() else { return };
    let redis = store(&server, FailOpen).await;
    let limiter: Limiter<Vec<u8>, _, _> = Limiter::redis_builder(&redis, "long")
        .quota(quota!("1/hour"))
        .build()
        .unwrap();
    // A byte vector encodes as its length (2 bytes here) and its bytes.
    let longest = vec![7_u8; 510];
    assert_eq!(longest.key_bytes().len(), 512);
    let decision = limiter.hit(&longest).await.unwrap();
    assert!(decision.is_allowed() && decision.stats().is_known());

    let too_long = vec![7_u8; 511];
    assert!(matches!(
        limiter.hit(&too_long).await,
        Err(Error::KeyTooLong)
    ));
    assert!(matches!(
        limiter.test(&too_long).await,
        Err(Error::KeyTooLong)
    ));
    assert!(matches!(
        limiter.stats(&too_long).await,
        Err(Error::KeyTooLong)
    ));
    assert!(matches!(
        limiter.reset(&too_long).await,
        Err(Error::KeyTooLong)
    ));
    assert!(matches!(
        limiter.until_ready(&too_long).await,
        Err(Error::KeyTooLong)
    ));
    let mut connection = sync_connection(&server.url);
    assert_eq!(
        limiter_keys(&mut connection).len(),
        1,
        "nothing was written for the long key"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn the_circuit_breaker_settings_apply() {
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
        .connect()
        .await
        .unwrap();
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "breaker")
        .quota(quota!("10/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());
    server.stop();
    let Decision::Denied(first) = limiter.hit(&1).await.unwrap() else {
        panic!("fail closed denies");
    };
    assert!(first.is_store_unavailable());
    // One failure opened it, for a minute: even with Redis back, the
    // breaker answers.
    server.start();
    let Decision::Denied(second) = limiter.hit(&1).await.unwrap() else {
        panic!("the open breaker denies");
    };
    let retry = second.retry_after().unwrap();
    assert!(retry > Duration::from_secs(55), "{retry:?}");
}

#[test]
fn fingerprints_are_stable() {
    // Changing these orphans every key in use: they are part of the
    // stored format.
    assert_eq!(
        super::super::fingerprint(&quota!("5/second; 100/hour")),
        0x53e5_5081
    );
    assert_eq!(
        super::super::fingerprint(&quota!("100/hour; 5/second")),
        0x53e5_5081
    );
    assert_ne!(
        super::super::fingerprint(&quota!("5/second")),
        super::super::fingerprint(&quota!("5/second burst 6"))
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn refuses_a_token_bucket_redis_cant_count() {
    let Some(server) = server() else { return };
    let redis = store(&server, Propagate).await;
    let rate = Rate::per_day(std::num::NonZeroU32::MIN).with_burst(std::num::NonZeroU32::MAX);
    let built = Limiter::<u64>::redis_builder(&redis, "huge")
        .quota(crate::Quota::new(rate))
        .build();
    assert_eq!(built.unwrap_err().kind(), QuotaErrorKind::RefillTooLong);
    let windows = Limiter::<u64>::redis_builder(&redis, "huge")
        .algorithm(SlidingLog)
        .quota(crate::Quota::new(rate))
        .build();
    assert!(windows.is_ok(), "only token buckets have the limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn clients_racing_never_over_admit() {
    let Some(server) = server() else { return };
    let mut tasks = Vec::new();
    for client in 0..4 {
        // A store per client: separate connections, as separate instances.
        let redis = store(&server, Propagate).await;
        let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "race")
            .quota(quota!("100/hour"))
            .build()
            .unwrap();
        for _ in 0..10 {
            let limiter = limiter.clone();
            tasks.push(tokio::spawn(async move {
                let mut allowed = 0_u32;
                for _ in 0..10 + client {
                    if limiter.hit(&1).await.unwrap().is_allowed() {
                        allowed += 1;
                    }
                }
                allowed
            }));
        }
    }
    let mut allowed = 0;
    for task in tasks {
        allowed += task.await.unwrap();
    }
    assert_eq!(allowed, 100, "560 hits racing for 100 units");
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn until_ready_waits_for_the_next_unit() {
    let Some(server) = server() else { return };
    let redis = store(&server, Propagate).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "until_ready")
        .quota(quota!("20/second"))
        .build()
        .unwrap();
    let start = Instant::now();
    for _ in 0..22 {
        assert!(limiter.until_ready(&1).await.unwrap().is_allowed());
    }
    assert!(
        start.elapsed() >= Duration::from_millis(90),
        "{:?}",
        start.elapsed()
    );
}

/// Hits on a stopped server, answered by each policy, then the recovery.
#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn outages_answer_by_policy_and_recover() {
    let Some(mut server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let open = store(&server, FailOpen).await;
    let closed = store(&server, FailClosed).await;
    let propagate = store(&server, Propagate).await;
    let quota = quota!("1/hour");
    let open: Limiter<u64, _, _> = Limiter::redis_builder(&open, "o")
        .quota(quota)
        .build()
        .unwrap();
    let closed: Limiter<u64, _, _> = Limiter::redis_builder(&closed, "c")
        .quota(quota)
        .build()
        .unwrap();
    let propagate: Limiter<u64, _, _> = Limiter::redis_builder(&propagate, "p")
        .quota(quota)
        .build()
        .unwrap();
    for limiter_hit in [
        open.hit(&1).await,
        closed.hit(&1).await,
        propagate.hit(&1).await,
    ] {
        assert!(limiter_hit.unwrap().is_allowed());
    }

    server.stop();
    for _ in 0..5 {
        let decision = open.hit(&1).await.unwrap();
        assert!(decision.is_allowed(), "fail open allows");
        assert!(!decision.stats().is_known());

        let crate::Decision::Denied(denial) = closed.hit(&1).await.unwrap() else {
            panic!("fail closed denies");
        };
        assert!(denial.is_store_unavailable());
        assert!(denial.retry_after().unwrap() > Duration::ZERO);

        assert!(matches!(
            propagate.hit(&1).await,
            Err(crate::Error::Store(_))
        ));
    }
    // The breaker is open now: answers come at once, not after a timeout.
    let start = Instant::now();
    for _ in 0..100 {
        assert!(open.hit(&1).await.unwrap().is_allowed());
    }
    assert!(
        start.elapsed() < Duration::from_millis(40),
        "{:?}",
        start.elapsed()
    );

    server.start();
    // The restarted server is empty, so each limiter allows its key again
    // once the breaker lets calls through.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let decision = closed.hit(&2).await.unwrap();
        if decision.stats().is_known() {
            assert!(decision.is_allowed());
            break;
        }
        assert!(Instant::now() < deadline, "the store came back");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn connect_fails_when_redis_refuses_the_credentials() {
    let Some(server) = spawn_server(free_port(), &["--requirepass", "s3cret"]) else {
        return;
    };
    let port = server.port().unwrap();
    // Under fail open, a store that connected anyway would allow every hit.
    for url in [
        format!("redis://127.0.0.1:{port}/"),
        format!("redis://:wrong@127.0.0.1:{port}/"),
    ] {
        let connected = RedisStore::builder(url)
            .on_failure(FailOpen)
            .connect()
            .await;
        assert!(matches!(connected, Err(Error::Store(_))));
    }
    let connected = RedisStore::builder(format!("redis://:s3cret@127.0.0.1:{port}/"))
        .on_failure(FailOpen)
        .connect()
        .await;
    assert!(connected.is_ok());
}

/// Errors Redis answers with that retrying won't fix are errors whatever
/// the policy: answering them by fail open would admit everything.
#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn permanent_errors_are_errors_under_every_policy() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let open = store(&server, FailOpen).await;
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&open, "permanent")
        .quota(quota!("1/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());

    let mut admin = sync_connection(&server.url);
    // A key of the wrong type where the limiter's state should be.
    let names = limiter_keys(&mut admin);
    let [name] = names.as_slice() else {
        panic!("one key: {names:?}");
    };
    ::redis::cmd("DEL").arg(name).exec(&mut admin).unwrap();
    ::redis::cmd("LPUSH")
        .arg(name)
        .arg("x")
        .exec(&mut admin)
        .unwrap();
    for _ in 0..5 {
        assert!(matches!(limiter.hit(&1).await, Err(Error::Store(_))));
    }

    // State the script can't parse: it fails at run time.
    let windows: Limiter<u64, FixedWindow, _> = Limiter::redis_builder(&open, "garbage")
        .algorithm(FixedWindow)
        .quota(quota!("1/hour"))
        .build()
        .unwrap();
    let quota = quota!("1/hour");
    let [garbage] = super::super::key_names(
        &super::super::prefix("garbage"),
        &1_u64.key_bytes(),
        &super::super::suffix::<FixedWindow>(&quota),
        1,
    )
    .try_into()
    .unwrap();
    ::redis::cmd("SET")
        .arg(&garbage)
        .arg("garbage")
        .exec(&mut admin)
        .unwrap();
    for _ in 0..5 {
        assert!(matches!(windows.hit(&1).await, Err(Error::Store(_))));
    }

    // Permission to run scripts taken away.
    ::redis::cmd("ACL")
        .arg(&["SETUSER", "default", "-evalsha", "-eval"])
        .exec(&mut admin)
        .unwrap();
    for _ in 0..5 {
        assert!(matches!(limiter.hit(&2).await, Err(Error::Store(_))));
    }
    // Redis answered every time, so the breaker never opened: once
    // permissions are back, the next call goes through.
    ::redis::cmd("ACL")
        .arg(&["SETUSER", "default", "+@all"])
        .exec(&mut admin)
        .unwrap();
    assert!(limiter.hit(&2).await.unwrap().stats().is_known());
}

/// A Redis out of memory is an outage: every policy answers as it should,
/// and the store works again once memory frees up.
#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn an_out_of_memory_redis_is_answered_by_the_policy() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let open = store(&server, FailOpen).await;
    let closed = store(&server, FailClosed).await;
    let propagate = store(&server, Propagate).await;
    let quota = quota!("1/hour");
    let open: Limiter<u64, _, _> = Limiter::redis_builder(&open, "o")
        .quota(quota)
        .build()
        .unwrap();
    let closed: Limiter<u64, _, _> = Limiter::redis_builder(&closed, "c")
        .quota(quota)
        .build()
        .unwrap();
    let propagate: Limiter<u64, _, _> = Limiter::redis_builder(&propagate, "p")
        .quota(quota)
        .build()
        .unwrap();

    let mut admin = sync_connection(&server.url);
    ::redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory-policy", "noeviction"])
        .exec(&mut admin)
        .unwrap();
    ::redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", "1"])
        .exec(&mut admin)
        .unwrap();
    let decision = open.hit(&1).await.unwrap();
    assert!(decision.is_allowed() && !decision.stats().is_known());
    let Decision::Denied(denial) = closed.hit(&1).await.unwrap() else {
        panic!("fail closed denies");
    };
    assert!(denial.is_store_unavailable());
    assert!(matches!(propagate.hit(&1).await, Err(Error::Store(_))));

    ::redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", "0"])
        .exec(&mut admin)
        .unwrap();
    // One failure each, below the breaker's threshold: the next call goes
    // to Redis.
    assert!(closed.hit(&2).await.unwrap().stats().is_known());
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn connect_fails_on_a_replica() {
    let Some(primary) = spawn_server(free_port(), &[]) else {
        return;
    };
    let port = primary.port().unwrap().to_string();
    let Some(replica) = spawn_server(free_port(), &["--replicaof", "127.0.0.1", &port]) else {
        return;
    };
    // Under fail open, a store on a replica would allow every hit.
    let connected = RedisStore::builder(replica.url.as_str())
        .on_failure(FailOpen)
        .connect()
        .await;
    assert!(matches!(connected, Err(Error::Store(_))));
}

/// Forwards new connections to whichever port `target` holds, as a DNS
/// name or virtual address that moves to the new primary on failover.
/// Connections already open stay with the old server.
fn moving_address(target: std::sync::Arc<std::sync::atomic::AtomicU16>) -> u16 {
    use std::net::{TcpListener, TcpStream};

    fn pipe(mut from: TcpStream, mut to: TcpStream) {
        drop(std::io::copy(&mut from, &mut to));
        drop(to.shutdown(std::net::Shutdown::Write));
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for inbound in listener.incoming() {
            let Ok(inbound) = inbound else { return };
            let to = target.load(std::sync::atomic::Ordering::SeqCst);
            let Ok(outbound) = TcpStream::connect(("127.0.0.1", to)) else {
                continue;
            };
            let (Ok(inbound_copy), Ok(outbound_copy)) = (inbound.try_clone(), outbound.try_clone())
            else {
                continue;
            };
            std::thread::spawn(move || pipe(inbound, outbound_copy));
            std::thread::spawn(move || pipe(outbound, inbound_copy));
        }
    });
    port
}

/// A store reaching Redis by an address that moves on failover: the old
/// primary turns replica with the store's connection still open, so writes
/// fail with READONLY while reads succeed. The store must connect again,
/// and so reach the new primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn a_direct_store_follows_an_address_to_the_new_primary() {
    let Some(old) = spawn_server(free_port(), &[]) else {
        return;
    };
    let Some(new) = spawn_server(free_port(), &[]) else {
        return;
    };
    let target = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(old.port().unwrap()));
    let address = moving_address(std::sync::Arc::clone(&target));
    let redis = RedisStore::builder(format!("redis://127.0.0.1:{address}/"))
        .on_failure(FailClosed)
        .connect()
        .await
        .unwrap();
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "moving")
        .quota(quota!("1000/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());

    let new_port = new.port().unwrap();
    target.store(new_port, std::sync::atomic::Ordering::SeqCst);
    ::redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg(new_port)
        .exec(&mut sync_connection(&old.url))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut key = 0_u64;
    loop {
        // Mostly reads, which a replica answers, between the writes.
        for _ in 0..3 {
            let _stats: crate::Stats = limiter.stats(&key).await.unwrap();
        }
        let decision = limiter.hit(&key).await.unwrap();
        if decision.is_allowed() && decision.stats().is_known() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the store reached the new primary"
        );
        key += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !limiter_keys(&mut sync_connection(&new.url)).is_empty(),
        "the hit landed on the new primary"
    );
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn connect_fails_without_a_server() {
    let port = free_port();
    let connected = RedisStore::builder(format!("redis://127.0.0.1:{port}/"))
        .on_failure(FailClosed)
        .connect()
        .await;
    assert!(matches!(connected, Err(crate::Error::Store(_))));
}

/// Three primaries splitting the slots, assembled by hand.
fn cluster() -> Option<Vec<Server>> {
    let dir = std::env::temp_dir().join(format!("porin-cluster-{}", free_port()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.to_str().unwrap().to_owned();
    let mut nodes = Vec::new();
    for _ in 0..3 {
        let port = free_port();
        let config = format!("nodes-{port}.conf");
        nodes.push(spawn_server(
            port,
            &[
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                &config,
                "--dir",
                &dir,
            ],
        )?);
    }
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    for (node, (first, last)) in nodes.iter().zip(ranges) {
        let mut connection = sync_connection(&node.url);
        ::redis::cmd("CLUSTER")
            .arg("ADDSLOTSRANGE")
            .arg(first)
            .arg(last)
            .exec(&mut connection)
            .unwrap();
    }
    let mut first = sync_connection(&nodes[0].url);
    for node in &nodes[1..] {
        ::redis::cmd("CLUSTER")
            .arg("MEET")
            .arg("127.0.0.1")
            .arg(node.port().unwrap())
            .exec(&mut first)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    for node in &nodes {
        let mut connection = sync_connection(&node.url);
        loop {
            let info: String = ::redis::cmd("CLUSTER")
                .arg("INFO")
                .query(&mut connection)
                .unwrap();
            if info.contains("cluster_state:ok") {
                break;
            }
            assert!(Instant::now() < deadline, "the cluster formed");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    Some(nodes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn cluster_runs_each_key_on_its_slot() {
    if std::env::var_os("PORIN_REDIS_URL").is_some() {
        return;
    }
    let Some(nodes) = cluster() else { return };
    let redis = RedisStore::cluster(nodes.iter().map(|node| node.url.clone()))
        .on_failure(Propagate)
        .connect()
        .await
        .expect("connects to the cluster");
    let limiter: Limiter<u32, SlidingLog, _> = Limiter::redis_builder(&redis, "cluster")
        .algorithm(SlidingLog)
        .quota(quota!("3/minute; 10/hour"))
        .build()
        .unwrap();
    // 64 keys spread over all three nodes' slots.
    for key in 0..64 {
        for _ in 0..3 {
            assert!(limiter.hit(&key).await.unwrap().is_allowed());
        }
        assert!(!limiter.hit(&key).await.unwrap().is_allowed(), "key {key}");
    }
    let mut used = 0;
    for node in &nodes {
        let size: u64 = ::redis::cmd("DBSIZE")
            .query(&mut sync_connection(&node.url))
            .unwrap();
        assert!(size > 0, "every node holds some keys");
        used += size;
    }
    assert_eq!(used, 128, "two rates per key");
}

/// A primary, its replica and one Sentinel watching them.
struct SentinelSetup {
    primary: Server,
    _replica: Server,
    watcher: Server,
}

/// The URL of a server this test started, as the default user with
/// `password`.
fn url_with(server: &Server, password: Option<&str>, db: u8) -> String {
    let port = server.port().unwrap();
    match password {
        Some(password) => format!("redis://:{password}@127.0.0.1:{port}/{db}"),
        None => format!("redis://127.0.0.1:{port}/{db}"),
    }
}

/// With a `password`, both servers require it from the default user and
/// also know the user `porin` with the same password.
fn sentinel(password: Option<&str>) -> Option<SentinelSetup> {
    // A replica's full sync writes a file to the server's directory.
    let dir = std::env::temp_dir().join(format!("porin-sentinel-{}", free_port()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.to_str().unwrap().to_owned();
    let mut auth = Vec::new();
    if let Some(password) = password {
        auth = vec![
            "--requirepass".to_owned(),
            password.to_owned(),
            "--masterauth".to_owned(),
            password.to_owned(),
            "--user".to_owned(),
            "porin".to_owned(),
            "on".to_owned(),
            format!(">{password}"),
            "~*".to_owned(),
            "&*".to_owned(),
            "+@all".to_owned(),
        ];
    }
    let auth: Vec<&str> = auth.iter().map(String::as_str).collect();
    let primary = spawn_server(free_port(), &[&["--dir", &dir], &auth[..]].concat())?;
    let primary_port = primary.port().unwrap();
    let replica = spawn_server(
        free_port(),
        &[
            &[
                "--replicaof",
                "127.0.0.1",
                &primary_port.to_string(),
                "--dir",
                &dir,
            ],
            &auth[..],
        ]
        .concat(),
    )?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut connection = sync_connection(&url_with(&replica, password, 0));
    loop {
        let info: String = ::redis::cmd("INFO")
            .arg("replication")
            .query(&mut connection)
            .unwrap();
        if info.contains("master_link_status:up") {
            break;
        }
        assert!(Instant::now() < deadline, "the replica synced");
        std::thread::sleep(Duration::from_millis(50));
    }
    let port = free_port();
    let config = std::env::temp_dir().join(format!("porin-sentinel-{port}.conf"));
    let auth_pass = password.map_or_else(String::new, |password| {
        format!("sentinel auth-pass porin {password}\n")
    });
    std::fs::write(
        &config,
        format!(
            "port {port}\nsentinel monitor porin 127.0.0.1 {primary_port} 1\n{auth_pass}\
             sentinel down-after-milliseconds porin 300\nsentinel failover-timeout porin 2000\n"
        ),
    )
    .unwrap();
    let child = Command::new(binary())
        .arg(&config)
        .arg("--sentinel")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    super::wait_for(port);
    let sentinel = Server {
        child: Some(child),
        url: format!("redis://127.0.0.1:{port}/"),
        launch: None,
    };
    let mut connection = sync_connection(&sentinel.url);
    loop {
        let replicas: Vec<::redis::Value> = ::redis::cmd("SENTINEL")
            .arg("REPLICAS")
            .arg("porin")
            .query(&mut connection)
            .unwrap();
        if !replicas.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "the sentinel found the replica");
        std::thread::sleep(Duration::from_millis(50));
    }
    Some(SentinelSetup {
        primary,
        _replica: replica,
        watcher: sentinel,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn sentinel_follows_a_failover() {
    if std::env::var_os("PORIN_REDIS_URL").is_some() {
        return;
    }
    let Some(mut setup) = sentinel(None) else {
        return;
    };
    let redis = RedisStore::sentinel([setup.watcher.url.clone()], "porin")
        .on_failure(FailClosed)
        .connect()
        .await
        .expect("finds the primary");
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "sentinel")
        .quota(quota!("1000/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());

    setup.primary.stop();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_outage = false;
    loop {
        let decision = limiter.hit(&1).await.unwrap();
        if decision.stats().is_known() && saw_outage {
            assert!(decision.is_allowed());
            break;
        }
        saw_outage |= !decision.stats().is_known();
        assert!(Instant::now() < deadline, "the store found the new primary");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// After a failover that leaves the old primary up as a replica, with the
/// store's connection to it open, writes fail with READONLY while reads
/// still succeed. Mixed traffic then never fails 3 times in a row, so the
/// store must look for the new primary on READONLY itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn sentinel_finds_the_primary_when_the_old_one_turns_replica() {
    if std::env::var_os("PORIN_REDIS_URL").is_some() {
        return;
    }
    let Some(setup) = sentinel(None) else {
        return;
    };
    let redis = RedisStore::sentinel([setup.watcher.url.clone()], "porin")
        .on_failure(FailClosed)
        .connect()
        .await
        .expect("finds the primary");
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "demoted")
        .quota(quota!("1000/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());

    // The Sentinel may not judge the replica ready to promote at once.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut watcher = sync_connection(&setup.watcher.url);
    while let Err(error) = ::redis::cmd("SENTINEL")
        .arg(&["FAILOVER", "porin"])
        .exec(&mut watcher)
    {
        assert!(
            Instant::now() < deadline,
            "the Sentinel fails over: {error}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut key = 0_u64;
    loop {
        // Mostly reads, which a replica answers, between the writes.
        for _ in 0..3 {
            let stats = limiter.stats(&key).await.unwrap();
            assert!(!stats.is_known() || stats.remaining() > 0);
        }
        let decision = limiter.hit(&key).await.unwrap();
        let primary_role: Vec<::redis::Value> = ::redis::cmd("ROLE")
            .query(&mut sync_connection(&setup.primary.url))
            .unwrap();
        let demoted = matches!(primary_role.first(), Some(::redis::Value::BulkString(role)) if role == b"slave");
        if demoted && decision.is_allowed() && decision.stats().is_known() {
            break;
        }
        assert!(Instant::now() < deadline, "the store found the new primary");
        key += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn sentinel_reaches_a_primary_that_needs_credentials() {
    if std::env::var_os("PORIN_REDIS_URL").is_some() {
        return;
    }
    let password = "s3cret";
    let Some(mut setup) = sentinel(Some(password)) else {
        return;
    };
    let anonymous = RedisStore::sentinel([setup.watcher.url.clone()], "porin")
        .on_failure(FailClosed)
        .connect()
        .await;
    assert!(
        matches!(anonymous, Err(Error::Store(_))),
        "without credentials the primary refuses, so connecting fails"
    );

    // The host is a placeholder: the Sentinel names the primary.
    let redis = RedisStore::sentinel([setup.watcher.url.clone()], "porin")
        .primary_url(format!("redis://porin:{password}@primary.invalid/2"))
        .on_failure(FailClosed)
        .connect()
        .await
        .expect("finds the primary and signs in");
    let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "sentinel")
        .quota(quota!("1000/hour"))
        .build()
        .unwrap();
    assert!(limiter.hit(&1).await.unwrap().stats().is_known());
    let size_of = |db| {
        limiter_keys(&mut sync_connection(&url_with(
            &setup.primary,
            Some(password),
            db,
        )))
        .len()
    };
    assert_eq!(size_of(2), 1, "the key is in database 2");
    assert_eq!(size_of(0), 0, "and not in database 0");

    setup.primary.stop();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_outage = false;
    loop {
        let decision = limiter.hit(&1).await.unwrap();
        if decision.stats().is_known() && saw_outage {
            assert!(decision.is_allowed());
            break;
        }
        saw_outage |= !decision.stats().is_known();
        assert!(
            Instant::now() < deadline,
            "the store signed in to the new primary"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_bad_primary_url_fails_to_connect() {
    for url in [
        "not a url",
        "redis://primary/not-a-db",
        "unix:///tmp/redis.sock",
    ] {
        let connected = RedisStore::sentinel(["redis://127.0.0.1:1/"], "porin")
            .primary_url(url)
            .on_failure(FailClosed)
            .connect()
            .await;
        assert!(matches!(connected, Err(crate::Error::Store(_))), "{url}");
    }
}

#[test]
fn primary_tls_comes_from_the_scheme() {
    let tls = |insecure| ConnectionAddr::TcpTls {
        host: "primary".to_owned(),
        port: 6379,
        insecure,
        tls_params: None,
    };
    assert!(matches!(
        tls_mode(&ConnectionAddr::Tcp("primary".to_owned(), 6379)),
        Ok(None)
    ));
    assert!(matches!(tls_mode(&tls(false)), Ok(Some(TlsMode::Secure))));
    assert!(matches!(tls_mode(&tls(true)), Ok(Some(TlsMode::Insecure))));
    assert!(matches!(
        tls_mode(&ConnectionAddr::Unix("/tmp/redis.sock".into())),
        Err(crate::Error::Store(_))
    ));
}

#[test]
fn builders_never_show_a_password() {
    let sentinel = RedisStore::sentinel(["redis://:sentinel-pw@127.0.0.1:26379/"], "porin")
        .primary_url("rediss://porin:primary-pw@primary/2")
        .on_failure(FailClosed);
    let node = RedisStore::builder("redis://:node-pw@127.0.0.1/");
    let shown = format!("{sentinel:?} {node:?}");
    assert!(!shown.contains("-pw"), "{shown}");
    assert!(shown.contains("porin"), "names the primary: {shown}");
}

/// State the script can't read is an error under every algorithm and
/// policy, never read as a fresh key, which would admit a full allowance.
async fn unreadable_state_is_an_error<A: crate::Algorithm + Default>() {
    let Some(server) = spawn_server(free_port(), &[]) else {
        return;
    };
    let open = store(&server, FailOpen).await;
    let quota = quota!("1/hour");
    let limiter: Limiter<u64, A, _> = Limiter::redis_builder(&open, "corrupt")
        .algorithm(A::default())
        .quota(quota)
        .build()
        .unwrap();
    let [name] = super::super::key_names(
        &super::super::prefix("corrupt"),
        &1_u64.key_bytes(),
        &super::super::suffix::<A>(&quota),
        1,
    )
    .try_into()
    .unwrap();
    let mut admin = sync_connection(&server.url);
    for garbage in ["garbage", "-5", "1e3", ""] {
        ::redis::cmd("DEL").arg(&name).exec(&mut admin).unwrap();
        ::redis::cmd("SET")
            .arg(&name)
            .arg(garbage)
            .exec(&mut admin)
            .unwrap();
        assert!(
            matches!(limiter.hit(&1).await, Err(Error::Store(_))),
            "{garbage:?} under {A:?}",
            A = A::default()
        );
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn unreadable_token_bucket_state_is_an_error() {
    unreadable_state_is_an_error::<crate::TokenBucket>().await;
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn unreadable_fixed_window_state_is_an_error() {
    unreadable_state_is_an_error::<FixedWindow>().await;
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn unreadable_sliding_window_state_is_an_error() {
    unreadable_state_is_an_error::<SlidingWindow>().await;
}

#[tokio::test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
async fn unreadable_sliding_log_state_is_an_error() {
    unreadable_state_is_an_error::<SlidingLog>().await;
}
