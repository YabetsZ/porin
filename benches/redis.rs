//! Latency of the Redis store, to choose its per-call timeout.
//!
//! Times every `hit` and reports the distribution, from 1 to 256 tasks
//! sharing one store, next to a plain `PING` through the same kind of
//! connection, so the script's share is visible. Each case first warms up,
//! then records every call. Each case starts on keys no earlier case used,
//! so sliding logs left by one case don't grow the next one's. Most cases
//! spread calls over 100,000 keys, about one hit each; the "hot keys" case
//! uses 100, so each key's sliding log grows past 1,000 entries.
//!
//! Needs a Redis: `PORIN_REDIS_URL` to use a running one, or
//! `PORIN_REDIS_SERVER` (default `redis-server`) to start one on a free
//! port. Without either it says so and exits.
//!
//! `PORIN_REDIS_SERVER=/path/to/redis-server cargo bench --bench redis --features redis-tokio [filter...]`

#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::indexing_slicing,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::panic,
    reason = "benchmark arithmetic and output; a failed setup aborts the run"
)]

use std::future::Future;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use porin::store::{Propagate, RedisStore};
use porin::{Algorithm, Limiter, Quota, SlidingLog, TokenBucket, quota};

/// High enough that every hit is allowed: the allow path is the common one.
const ONE_RATE: Quota = quota!("1000000/second");
/// High enough for every hit, and long enough that no entry expires.
const HOUR_RATE: Quota = quota!("1000000/hour");
const FOUR_RATES: Quota = quota!("1000000/second; 10000000/minute; 100000000/hour; 1000000000/day");
/// Keys per case, so each key gets about one hit per run.
const KEYS: u64 = 100_000;
/// Keys in the hot keys case, so each key gets about 1,100 hits per run.
const HOT_KEYS: u64 = 100;
/// Calls per case, split between its tasks.
const CALLS: usize = 102_400;
const TASKS: [usize; 4] = [1, 16, 64, 256];

/// One call to time: a `hit` on some limiter, or a `PING`.
type Call = Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

fn main() {
    let filter: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| !arg.starts_with("--"))
        .collect();
    let selected =
        |name: &str| filter.is_empty() || filter.iter().any(|f| name.contains(f.as_str()));

    let Some(server) = Server::start() else {
        eprintln!("skipping: no Redis (set PORIN_REDIS_SERVER or PORIN_REDIS_URL)");
        return;
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("can't start the runtime: {error}"),
    };
    let calls = runtime.block_on(calls(&server.url));

    println!("loopback Redis at {}; latency in microseconds", server.url);
    println!(
        "{:<40} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "case", "calls/s", "p50", "p90", "p99", "p99.9", "max"
    );
    let mut first_key = 0;
    for tasks in TASKS {
        for (name, call, keys) in &calls {
            let case = format!("tasks {tasks}/{name}");
            if selected(&case) {
                let (throughput, latencies) = runtime.block_on(run(call, tasks, first_key, *keys));
                report(&case, throughput, latencies);
                first_key += keys;
            }
        }
    }
}

async fn calls(url: &str) -> Vec<(&'static str, Call, u64)> {
    let store = match RedisStore::builder(url)
        .on_failure(Propagate)
        .timeout(Duration::from_secs(5))
        .connect()
        .await
    {
        Ok(store) => store,
        Err(error) => panic!("can't connect to {url}: {error}"),
    };
    let client = match redis::Client::open(url) {
        Ok(client) => client,
        Err(error) => panic!("invalid URL {url}: {error}"),
    };
    let ping = match redis::aio::ConnectionManager::new(client).await {
        Ok(connection) => connection,
        Err(error) => panic!("can't connect to {url}: {error}"),
    };
    vec![
        ("ping", ping_call(ping), KEYS),
        (
            "token bucket 1 rate",
            hit_call(&store, TokenBucket, ONE_RATE, "bench-tb1"),
            KEYS,
        ),
        (
            "token bucket 4 rates",
            hit_call(&store, TokenBucket, FOUR_RATES, "bench-tb4"),
            KEYS,
        ),
        (
            "sliding log 1 rate",
            hit_call(&store, SlidingLog, ONE_RATE, "bench-sl1"),
            KEYS,
        ),
        (
            "sliding log 4 rates",
            hit_call(&store, SlidingLog, FOUR_RATES, "bench-sl4"),
            KEYS,
        ),
        (
            "sliding log 1 rate, hot keys",
            hit_call(&store, SlidingLog, HOUR_RATE, "bench-slh"),
            HOT_KEYS,
        ),
    ]
}

fn ping_call(connection: redis::aio::ConnectionManager) -> Call {
    Arc::new(move |_key| {
        let mut connection = connection.clone();
        Box::pin(async move {
            let reply: Result<String, _> = redis::cmd("PING").query_async(&mut connection).await;
            if let Err(error) = reply {
                panic!("PING failed: {error}");
            }
        })
    })
}

fn hit_call<A: Algorithm + Send + Sync + 'static>(
    store: &RedisStore<Propagate>,
    algorithm: A,
    quota: Quota,
    id: &str,
) -> Call {
    let limiter: Limiter<u64, A, RedisStore<Propagate>> = match Limiter::redis_builder(store, id)
        .algorithm(algorithm)
        .quota(quota)
        .build()
    {
        Ok(limiter) => limiter,
        Err(error) => panic!("can't build the limiter: {error}"),
    };
    let limiter = Arc::new(limiter);
    Arc::new(move |key| {
        let limiter = Arc::clone(&limiter);
        Box::pin(async move {
            match limiter.hit(&key).await {
                Ok(decision) => assert!(decision.is_allowed(), "denied under a huge quota"),
                Err(error) => panic!("hit failed: {error}"),
            }
        })
    })
}

/// Calls per second and every call's latency, with `tasks` tasks calling
/// in a loop at once on `keys` keys from `first_key` on.
async fn run(call: &Call, tasks: usize, first_key: u64, keys: u64) -> (f64, Vec<Duration>) {
    let per_task = CALLS / tasks;
    let pass = |calls: usize| {
        let started = Instant::now();
        let handles: Vec<_> = (0..tasks as u64)
            .map(|task| {
                let call = Arc::clone(call);
                tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(calls);
                    for index in 0..calls as u64 {
                        let key = first_key + (task * 7_919 + index) % keys;
                        let start = Instant::now();
                        call(key).await;
                        latencies.push(start.elapsed());
                    }
                    latencies
                })
            })
            .collect();
        async move {
            let mut latencies = Vec::with_capacity(tasks * calls);
            for handle in handles {
                match handle.await {
                    Ok(task) => latencies.extend(task),
                    Err(error) => panic!("a benchmark task failed: {error}"),
                }
            }
            (started.elapsed(), latencies)
        }
    };
    pass(per_task / 10).await;
    let (elapsed, latencies) = pass(per_task).await;
    (latencies.len() as f64 / elapsed.as_secs_f64(), latencies)
}

fn report(case: &str, throughput: f64, mut latencies: Vec<Duration>) {
    latencies.sort_unstable();
    let at = |per_mille: usize| {
        let index = (latencies.len() - 1) * per_mille / 1_000;
        latencies[index].as_secs_f64() * 1e6
    };
    println!(
        "{case:<40} {throughput:>10.0} {:>8.0} {:>8.0} {:>8.0} {:>8.0} {:>8.0}",
        at(500),
        at(900),
        at(990),
        at(999),
        at(1_000),
    );
}

/// The Redis to measure: one this bench started, or one it was given.
struct Server {
    url: String,
    started: Option<(Child, PathBuf)>,
}

impl Server {
    fn start() -> Option<Self> {
        if let Ok(url) = std::env::var("PORIN_REDIS_URL") {
            return Some(Self { url, started: None });
        }
        let binary =
            std::env::var("PORIN_REDIS_SERVER").unwrap_or_else(|_| "redis-server".to_owned());
        let port = match TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()) {
            Ok(address) => address.port(),
            Err(error) => panic!("no free port: {error}"),
        };
        let dir = std::env::temp_dir().join(format!("porin-bench-redis-{port}"));
        if let Err(error) = std::fs::create_dir_all(&dir) {
            panic!("can't create {}: {error}", dir.display());
        }
        let child = Command::new(&binary)
            .arg("--port")
            .arg(port.to_string())
            .args(["--save", "", "--appendonly", "no", "--dir"])
            .arg(&dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "redis-server didn't start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Some(Self {
            url: format!("redis://127.0.0.1:{port}/"),
            started: Some((child, dir)),
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some((child, dir)) = &mut self.started {
            // Best effort: the server dies with the bench either way, and a
            // leftover empty directory in the temp dir is harmless.
            drop(child.kill());
            drop(child.wait());
            drop(std::fs::remove_dir_all(dir));
        }
    }
}
