//! The scripts against the Rust algorithms, run in microseconds, on a real
//! Redis: every answer and every expiry time must match.
//!
//! Needs a Redis: `PORIN_REDIS_URL` to use a running one, or
//! `PORIN_REDIS_SERVER` (default `redis-server`) to start one per test.
//! Without either the tests skip, unless `PORIN_REQUIRE_REDIS` is set.
#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_wrap,
    reason = "test arithmetic on small generated values"
)]

use std::collections::HashMap;
use std::net::TcpListener;
use std::num::NonZeroU32;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::run_script;
use super::scripts::Scripted;
use crate::algorithm::{Outcome, Retry, Spec, Stats, Unit, check_quota_in};
use crate::quota::MAX_RATES;
use crate::{FixedWindow, Quota, Rate, SlidingLog, SlidingWindow, TokenBucket};

mod deny_cache;
mod fallback;
mod global;
mod limiter;

/// A Redis to test against: one this test started, or one it was given.
pub(crate) struct Server {
    child: Option<Child>,
    pub(crate) url: String,
    /// How to start it again, if this test started it.
    launch: Option<(String, u16, Vec<String>)>,
}

impl Server {
    pub(crate) fn port(&self) -> Option<u16> {
        self.launch.as_ref().map(|(_, port, _)| *port)
    }

    pub(crate) fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
    }

    /// Starts a server this test stopped again, on the same port.
    pub(crate) fn start(&mut self) {
        let (binary, port, args) = self.launch.clone().expect("a server this test started");
        self.child = Some(spawn(&binary, port, &args).expect("redis-server restarts"));
        wait_for(port);
    }
}

fn spawn(binary: &str, port: u16, args: &[String]) -> std::io::Result<Child> {
    // A directory of its own, so no server reads or writes a dump.rdb in
    // the crate, where another test's server would load it. Later `--dir`
    // arguments override it.
    let dir = std::env::temp_dir().join(format!("porin-redis-{port}"));
    std::fs::create_dir_all(&dir)?;
    Command::new(binary)
        .args([
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
        ])
        .arg(&dir)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

fn wait_for(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "redis-server didn't start");
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            drop(child.kill());
            drop(child.wait());
        }
    }
}

pub(crate) fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().expect("a bound address").port()
}

/// Starts `redis-server` with `args` on a free port, or returns `None` (and
/// says so) if there is no Redis to test against.
pub(crate) fn server_with(args: &[&str]) -> Option<Server> {
    if let Ok(url) = std::env::var("PORIN_REDIS_URL") {
        return Some(Server {
            child: None,
            url,
            launch: None,
        });
    }
    spawn_server(free_port(), args)
}

/// The `redis-server` to start, from `PORIN_REDIS_SERVER`.
pub(crate) fn binary() -> String {
    std::env::var("PORIN_REDIS_SERVER").unwrap_or_else(|_| "redis-server".to_owned())
}

/// Starts `redis-server` with `args` on `port`, whatever
/// `PORIN_REDIS_URL` says, or returns `None` if it can't start.
pub(crate) fn spawn_server(port: u16, args: &[&str]) -> Option<Server> {
    let binary = binary();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    let spawned = spawn(&binary, port, &args);
    let Ok(child) = spawned else {
        assert!(
            std::env::var_os("PORIN_REQUIRE_REDIS").is_none(),
            "PORIN_REQUIRE_REDIS is set but `{binary}` can't start"
        );
        #[expect(
            clippy::print_stderr,
            reason = "says why a test passed without running"
        )]
        {
            eprintln!("skipping: no Redis (set PORIN_REDIS_SERVER or PORIN_REDIS_URL)");
        }
        return None;
    };
    let server = Server {
        child: Some(child),
        url: format!("redis://127.0.0.1:{port}/"),
        launch: Some((binary, port, args)),
    };
    wait_for(port);
    Some(server)
}

pub(crate) fn server() -> Option<Server> {
    server_with(&[])
}

pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// Microseconds a day from now, so expiry times set from these fake clocks
/// are in Redis's future and nothing expires during a test.
fn start_us() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970");
    u64::try_from(now.as_micros()).expect("fits") + 86_400_000_000
}

fn rate(count: u32, period_ms: u64, burst: u32) -> Rate {
    Rate::new(
        NonZeroU32::new(count).unwrap(),
        Duration::from_millis(period_ms),
    )
    .unwrap()
    .with_burst(NonZeroU32::new(burst).unwrap())
}

/// 1–4 rates. Small counts and short periods exercise boundaries; large
/// counts over long periods exercise the scripts' exact multiplication.
fn quota(token_bucket: bool) -> impl Strategy<Value = Quota> {
    let small = (
        1..=6_u32,
        prop::sample::select(&[1_u64, 7, 1_000, 60_000][..]),
        1..=8_u32,
    );
    let large = (
        1_000..=3_000_000_u32,
        prop::sample::select(&[3_600_000_u64, 86_400_000][..]),
        1..=8_u32,
    );
    // The extremes: counts up to u32::MAX, periods up to the longest
    // allowed (10,000 days), where the scripts' exact arithmetic is tested
    // hardest.
    let huge = (
        prop_oneof![
            Just(1_u32),
            Just(u32::MAX),
            Just(u32::MAX - 1),
            any::<u32>()
        ],
        prop::sample::select(&[86_400_000_u64, 8_640_000_000, 864_000_000_000][..]),
        prop_oneof![Just(1_u32), Just(u32::MAX), 1..=8_u32],
    );
    let one = prop_oneof![8 => small, 2 => large, 1 => huge];
    prop::collection::vec(one, 1..=MAX_RATES).prop_map(move |rates| {
        let make = |&(count, period_ms, burst): &(u32, u64, u32)| {
            let count = count.max(1);
            let full = rate(count, period_ms, if token_bucket { burst } else { count });
            // Token buckets too slow for Redis to count are refused at
            // build; test the largest burst that fits instead.
            if token_bucket && !super::token_bucket_fits(full) {
                return rate(count, period_ms, 1);
            }
            full
        };
        let mut quota = Quota::new(make(&rates[0]));
        for spec in &rates[1..] {
            if let Ok(with) = quota.with(make(spec)) {
                quota = with;
            }
        }
        quota
    })
}

/// 1–4 sliding log rates: besides small and large ones, logs of hundreds of
/// entries, and counts up to `u32::MAX` over periods up to 10,000 days.
fn log_quota() -> impl Strategy<Value = Quota> {
    let small = (
        1..=6_u32,
        prop::sample::select(&[1_u64, 7, 1_000, 60_000][..]),
    );
    let hundreds = (
        20..=400_u32,
        prop::sample::select(&[1_000_u64, 60_000, 3_600_000][..]),
    );
    let large = (
        prop_oneof![
            1_000..=3_000_000_u32,
            Just(u32::MAX),
            (u32::MAX - 600)..=u32::MAX
        ],
        prop::sample::select(&[3_600_000_u64, 86_400_000, 864_000_000_000][..]),
    );
    let one = prop_oneof![2 => small, 2 => hundreds, 1 => large];
    prop::collection::vec(one, 1..=MAX_RATES).prop_map(|rates| {
        let make = |&(count, period_ms): &(u32, u64)| rate(count, period_ms, count);
        let mut quota = Quota::new(make(&rates[0]));
        for spec in &rates[1..] {
            if let Ok(with) = quota.with(make(spec)) {
                quota = with;
            }
        }
        quota
    })
}

/// (key, time step in us, cost, record), stepping mostly forwards and
/// sometimes back, as Redis `TIME` can after a clock adjustment.
fn ops() -> impl Strategy<Value = Vec<(u8, i64, u32, bool)>> {
    let step = prop_oneof![
        3 => 0..=2_000_i64,
        3 => 0..=400_000_i64,
        2 => 0..=90_000_000_i64,
        1 => -400_000..=0_i64,
        // Up to about three years, so the longest periods roll over, while
        // 50 steps stay far below 2^53 microseconds.
        1 => 0..=100_000_000_000_000_i64,
    ];
    let cost = prop_oneof![
        8 => 1..=2_u32,
        4 => 0..=9_u32,
        1 => any::<u32>(),
        1 => Just(u32::MAX),
    ];
    prop::collection::vec((0..3_u8, step, cost, prop::bool::weighted(0.85)), 1..50)
}

/// Like [`ops`], but sometimes hundreds of them, so sliding logs fill, with
/// costs that free many entries at once or come near `u32::MAX`.
fn log_ops() -> impl Strategy<Value = Vec<(u8, i64, u32, bool)>> {
    // Steps of exactly a period (or one off it) land on the instant an
    // entry expires.
    let period = prop::sample::select(&[1_000_i64, 7_000, 60_000_000, 3_600_000_000][..]);
    let step = prop_oneof![
        3 => 0..=2_000_i64,
        3 => 0..=400_000_i64,
        1 => 0..=90_000_000_i64,
        1 => -400_000..=0_i64,
        2 => (period, -1..=1_i64).prop_map(|(period, off)| period + off),
    ];
    let cost = prop_oneof![
        8 => 1..=2_u32,
        4 => 0..=9_u32,
        1 => 0..=600_u32,
        1 => (u32::MAX - 600)..=u32::MAX,
        1 => any::<u32>(),
    ];
    let op = (0..3_u8, step, cost, prop::bool::weighted(0.9));
    prop_oneof![
        prop::collection::vec(op.clone(), 1..50),
        prop::collection::vec(op, 200..600),
    ]
}

/// Moves every end and base in the sliding logs at `KEYS` so the newest
/// end is `2^52 - ARGV[1]`, keeping times, costs and expiry: any key that
/// has admitted that many units looks like this. A base below where the set
/// starts means the same as that start, so it moves from there.
const SHIFT_ENDS: &str = r#"
local cap = 4503599627370496
for _, key in ipairs(KEYS) do
  local reply = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
  if #reply > 0 then
    local pattern = "^(%d+):(%d+):(%d+)$"
    local first_end, first_cost = string.match(reply[1], pattern)
    local first = tonumber(first_end) - tonumber(first_cost)
    local offset = cap - tonumber(ARGV[1]) - tonumber((string.match(reply[#reply - 1], pattern)))
    local expires = redis.call("PEXPIRETIME", key)
    redis.call("DEL", key)
    for at = 1, #reply, 2 do
      local finish, cost, base = string.match(reply[at], pattern)
      local moved = string.format("%016.0f:%s:%.0f", tonumber(finish) + offset, cost,
        math.max(tonumber(base), first) + offset)
      redis.call("ZADD", key, reply[at + 1], moved)
    end
    redis.call("PEXPIREAT", key, expires)
  end
end
"#;

fn agrees<A: Scripted>(
    quotas: impl Strategy<Value = Quota>,
    ops: impl Strategy<Value = Vec<(u8, i64, u32, bool)>>,
    shift_ends_from: Option<usize>,
) {
    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let start = start_us();
    // 96 cases unless PROPTEST_CASES says otherwise: each case is dozens
    // of round trips.
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(96);
    let mut runner = TestRunner::new(Config::with_cases(cases));
    let shift = ::redis::Script::new(SHIFT_ENDS);
    let result = runner.run(&(quotas, ops), |(quota, ops)| {
        let mut connection = connection.clone();
        runtime
            .block_on(::redis::cmd("FLUSHALL").query_async::<()>(&mut connection))
            .unwrap();
        let mut model: HashMap<u8, [A::State; MAX_RATES]> = HashMap::new();
        let mut now = start;
        for (index, (key, step, cost, record)) in ops.into_iter().enumerate() {
            now = now.saturating_add_signed(step);
            let keys: Vec<String> = (0..quota.rates().len())
                .map(|rate| format!("test:{{{key}}}:{rate}"))
                .collect();
            if shift_ends_from.is_some_and(|from| index >= from && index % 3 == 0) {
                let mut invocation = shift.prepare_invoke();
                for key in &keys {
                    invocation.key(key);
                }
                runtime
                    .block_on(
                        invocation
                            .arg(index % 4)
                            .invoke_async::<()>(&mut connection),
                    )
                    .unwrap();
            }
            let reply = runtime
                .block_on(run_script::<A, _>(
                    &mut connection,
                    &quota,
                    &keys,
                    record,
                    cost,
                    Some(now),
                ))
                .unwrap();
            let states = model
                .entry(key)
                .or_insert_with(|| std::array::from_fn(|_| A::State::default()));
            let (expected, commits) =
                check_quota_in::<A, MAX_RATES>(&quota, Unit::Micros, states, now, cost);
            prop_assert_eq!(
                reply.outcome(),
                expected,
                "key {} at {} cost {} record {}",
                key,
                now,
                cost,
                record
            );
            if !(record && cost > 0 && matches!(expected, Outcome::Allowed(_))) {
                continue;
            }
            for (state, commit) in states.iter_mut().zip(commits) {
                if let Some(commit) = commit {
                    A::commit(state, commit);
                }
            }
            for ((rate, state), key) in quota.rates().iter().zip(states.iter()).zip(&keys) {
                let idle = A::idle_at_ticks(Spec::new(*rate, Unit::Micros), state);
                let expires: i64 = runtime
                    .block_on(
                        ::redis::cmd("PEXPIRETIME")
                            .arg(key)
                            .query_async(&mut connection),
                    )
                    .unwrap();
                prop_assert_eq!(expires, idle.div_ceil(1_000) as i64, "expiry of {}", key);
            }
        }
        Ok::<(), TestCaseError>(())
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn token_bucket_script_agrees_with_the_rust_algorithm() {
    agrees::<TokenBucket>(quota(true), ops(), None);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn fixed_window_script_agrees_with_the_rust_algorithm() {
    agrees::<FixedWindow>(quota(false), ops(), None);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_window_script_agrees_with_the_rust_algorithm() {
    agrees::<SlidingWindow>(quota(false), ops(), None);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_log_script_agrees_with_the_rust_algorithm() {
    agrees::<SlidingLog>(log_quota(), log_ops(), None);
}

/// A key that has admitted about 2^52 units must renumber its log before
/// the running totals stop being exact, and answer as before.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_log_script_agrees_after_renumbering_its_log() {
    agrees::<SlidingLog>(log_quota(), log_ops(), Some(0));
}

/// A hit removes at most 1,000 expired entries, so a log that sees more
/// expire at once keeps pruned entries for a while. They must never count
/// again, including when the clock goes back to when they were live.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_log_script_agrees_with_pruned_entries_left_in_its_log() {
    agrees::<SlidingLog>(
        Just(log_quota_of_3000()),
        log_ops_past_the_prune_limit(1_100..=1_600),
        None,
    );
}

/// Renumbering must keep the ends of pruned entries still in the set
/// positive and keep them from counting.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_log_script_renumbers_with_pruned_entries_left_in_its_log() {
    // Shifting rewrites the whole set, so it starts after the fill.
    agrees::<SlidingLog>(
        Just(log_quota_of_3000()),
        log_ops_past_the_prune_limit(1_100..=1_100),
        Some(1_100),
    );
}

fn log_quota_of_3000() -> Quota {
    Quota::new(rate(3_000, 1_000, 3_000))
}

/// One key fills with `fills` hits, more than a hit prunes, then hits
/// around the instant most of them expire, stepping back as far as the fill
/// and forward.
fn log_ops_past_the_prune_limit(
    fills: std::ops::RangeInclusive<usize>,
) -> impl Strategy<Value = Vec<(u8, i64, u32, bool)>> {
    const PERIOD_US: i64 = 1_000_000;
    (fills, 1..=300_i64).prop_flat_map(|(entries, spacing)| {
        let span = spacing * entries as i64;
        let step = prop_oneof![
            2 => 0..=2_000_i64,
            2 => -span..=0,
            3 => (PERIOD_US - span)..=(PERIOD_US + span),
        ];
        let cost = prop_oneof![4 => 1..=2_u32, 1 => 0..=2_000_u32];
        let tail =
            prop::collection::vec((Just(0_u8), step, cost, prop::bool::weighted(0.8)), 1..40);
        tail.prop_map(move |tail| {
            let mut ops = vec![(0_u8, spacing, 1_u32, true); entries];
            ops.extend(tail);
            ops
        })
    })
}

/// Server-side time of the last command, in microseconds, from `SLOWLOG`
/// set to log every command.
async fn last_command_us(connection: &mut ::redis::aio::MultiplexedConnection) -> i64 {
    let entries: Vec<Vec<::redis::Value>> = ::redis::cmd("SLOWLOG")
        .arg("GET")
        .arg(1)
        .query_async(connection)
        .await
        .unwrap();
    let duration = &entries[0][2];
    ::redis::from_redis_value_ref(duration).unwrap()
}

/// A hit's cost must not grow with the log: with 100,000 live entries, or
/// as many expired ones, a script call takes microseconds, where reading or
/// removing the whole log takes tens to hundreds of milliseconds.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn sliding_log_script_is_fast_with_100000_entries() {
    const ENTRIES: u64 = 100_000;
    const TASKS: u64 = 100;
    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let mut connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let quota = Quota::new(rate(100_000, 3_600_000, 100_000));
    let period = 3_600_000_000;
    let keys = vec!["test:{fast}:0".to_owned()];
    let start = start_us();
    // Tasks interleave their hits on one connection, so the times arrive
    // slightly out of order, as from many instances.
    let filled = runtime.block_on(async {
        let tasks: Vec<_> = (0..TASKS)
            .map(|task| {
                let mut connection = connection.clone();
                let keys = keys.clone();
                tokio::spawn(async move {
                    let mut allowed = 0;
                    for index in 0..ENTRIES / TASKS {
                        let now = start + task * (ENTRIES / TASKS) + index;
                        let reply = run_script::<SlidingLog, _>(
                            &mut connection,
                            &quota,
                            &keys,
                            true,
                            1,
                            Some(now),
                        )
                        .await
                        .unwrap();
                        allowed += u64::from(matches!(reply.outcome(), Outcome::Allowed(_)));
                    }
                    allowed
                })
            })
            .collect();
        let mut allowed = 0;
        for task in tasks {
            allowed += task.await.unwrap();
        }
        allowed
    });
    assert_eq!(filled, ENTRIES, "every hit fits the count");
    runtime
        .block_on(
            ::redis::cmd("CONFIG")
                .arg("SET")
                .arg("slowlog-log-slower-than")
                .arg(0)
                .query_async::<()>(&mut connection),
        )
        .unwrap();
    // (time, mode, cost, allowed): a full log, a cost that frees half of
    // it, half of it expired, a hit that prunes a few entries, a hit that
    // finds all but that last entry expired, and one that finds everything
    // expired.
    let calls = [
        (start + ENTRIES, true, 1, false),
        (start + ENTRIES, false, 50_000, false),
        (start + period + ENTRIES / 2, false, 1, true),
        (start + period + 10, true, 1, true),
        (start + period + ENTRIES, true, 1, true),
        (start + 3 * period, true, 1, true),
    ];
    for (now, record, cost, allowed) in calls {
        let reply = runtime
            .block_on(run_script::<SlidingLog, _>(
                &mut connection,
                &quota,
                &keys,
                record,
                cost,
                Some(now),
            ))
            .unwrap();
        let took = runtime.block_on(last_command_us(&mut connection));
        assert_eq!(
            matches!(reply.outcome(), Outcome::Allowed(_)),
            allowed,
            "cost {cost} at {now}"
        );
        assert!(took < 5_000, "cost {cost} at {now} took {took} us");
    }
}

/// The scripts' exact `a * b / d`, against 128-bit integers. Random
/// differential cases almost never land where doubles round wrongly, so
/// these aim there: results within a hair of an integer. Inputs stay in
/// the domain helpers.lua states: a < 2^32, b and d < 2^50.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn mul_div_is_exact_where_doubles_round_wrongly() {
    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let script = ::redis::Script::new(concat!(
        include_str!("lua/helpers.lua"),
        "local a, b, d = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])\n",
        "return {string.format('%.0f', mul_div_floor(a, b, d)), \
         string.format('%.0f', mul_div_ceil(a, b, d))}\n",
    ));
    let check = |a: u64, b: u64, d: u64| -> Result<(), TestCaseError> {
        let mut connection = connection.clone();
        let got: Vec<String> = runtime
            .block_on(
                script
                    .prepare_invoke()
                    .arg(a)
                    .arg(b)
                    .arg(d)
                    .invoke_async(&mut connection),
            )
            .unwrap();
        let product = u128::from(a) * u128::from(b);
        let floor = product / u128::from(d);
        let ceil = product.div_ceil(u128::from(d));
        prop_assert_eq!(
            &got,
            &vec![floor.to_string(), ceil.to_string()],
            "{} * {} / {}",
            a,
            b,
            d
        );
        Ok(())
    };
    // Found by searching for inputs where floor(a * b / d) in doubles is
    // wrong.
    for (a, b, d) in [
        (3_306_160_984, 379_590_656_564_205, 520_660_380_998_072),
        (3_924_629_816, 584_581_144_575_284, 1_076_363_814_769_630),
        (2_251_871_611, 972_060_842_678_255, 666_370_131_341_638),
        (3_393_578_533, 426_570_715_632_949, 690_086_639_726_826),
    ] {
        check(a, b, d).unwrap();
    }
    // The same search, run each time: d chosen so a * b / d lands just
    // above an integer q.
    let near = (
        2_u64.pow(31)..2_u64.pow(32),
        2_u64.pow(48)..2_u64.pow(50),
        2_u64.pow(30)..2_u64.pow(32),
        0..=1_u64,
    )
        .prop_filter_map("d out of range", |(a, b, q, bump)| {
            let d = u64::try_from(u128::from(a) * u128::from(b) / u128::from(q)).ok()? + bump;
            (d < 2_u64.pow(50)).then_some((a, b, d))
        });
    let any_inputs = (0..2_u64.pow(32), 0..2_u64.pow(50), 1..2_u64.pow(50))
        .prop_filter("result past the scripts' cap", |(a, b, d)| {
            u128::from(*a) * u128::from(*b) / u128::from(*d) < 1 << 52
        });
    let mut runner = TestRunner::new(Config::with_cases(512));
    let result = runner.run(&prop_oneof![near, any_inputs], |(a, b, d)| check(a, b, d));
    if let Err(error) = result {
        panic!("{error}");
    }
}

/// Quotas whose rates refill in whole microseconds, where nanoseconds and
/// microseconds give the same answers.
fn even_quota(token_bucket: bool) -> impl Strategy<Value = Quota> {
    let one = (
        prop::sample::select(&[1_u32, 2, 4, 5, 8, 10, 20, 25, 40, 50, 100, 1_000][..]),
        prop::sample::select(&[1_u64, 1_000, 60_000, 3_600_000][..]),
        1..=8_u32,
    );
    prop::collection::vec(one, 1..=MAX_RATES).prop_map(move |rates| {
        let make = |&(count, period_ms, burst): &(u32, u64, u32)| {
            rate(count, period_ms, if token_bucket { burst } else { count })
        };
        let mut quota = Quota::new(make(&rates[0]));
        for spec in &rates[1..] {
            if let Ok(with) = quota.with(make(spec)) {
                quota = with;
            }
        }
        quota
    })
}

/// A memory limiter in nanoseconds and the Redis script in microseconds,
/// fed the same hits at whole microseconds: they allow and deny alike and
/// report the same units left. Redis rounds times up to whole
/// microseconds, so its reset and retry times may be later, by less than
/// one, and never earlier.
fn memory_and_redis_agree<A: crate::Algorithm + Default>(token_bucket: bool) {
    use crate::{Decision, Limiter, ManualClock, Timestamp};

    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let start = start_us();
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(96);
    let mut runner = TestRunner::new(Config::with_cases(cases));
    let result = runner.run(&(even_quota(token_bucket), ops()), |(quota, ops)| {
        let mut connection = connection.clone();
        runtime
            .block_on(::redis::cmd("FLUSHALL").query_async::<()>(&mut connection))
            .unwrap();
        let clock = ManualClock::new();
        let memory: Limiter<u8, A> = Limiter::builder()
            .algorithm(A::default())
            .quota(quota)
            .clock(clock.clone())
            .build();
        let mut now = start;
        for (key, step, cost, record) in ops {
            now = now.saturating_add_signed(step);
            clock.set(Timestamp::from_nanos(now * 1_000));
            let keys: Vec<String> = (0..quota.rates().len())
                .map(|rate| format!("test:{{{key}}}:{rate}"))
                .collect();
            let redis = runtime
                .block_on(run_script::<A, _>(
                    &mut connection,
                    &quota,
                    &keys,
                    record,
                    cost,
                    Some(now),
                ))
                .unwrap()
                .outcome();
            let local = if record {
                runtime.block_on(memory.hit_n(&key, cost))
            } else {
                runtime.block_on(memory.test_n(&key, cost))
            }
            .unwrap();
            let context = format!("key {key} at {now} cost {cost} record {record}");
            let (redis_stats, redis_retry) = match redis {
                Outcome::Allowed(stats) => (stats, None),
                Outcome::Denied { stats, retry } => (stats, Some(retry)),
            };
            prop_assert_eq!(local.is_allowed(), redis_retry.is_none(), "{}", context);
            prop_assert_eq!(
                local.stats().remaining(),
                redis_stats.remaining,
                "{}",
                context
            );
            let close = |memory: Duration, redis: Duration| {
                redis
                    .checked_sub(memory)
                    .is_some_and(|late| late < Duration::from_micros(1))
            };
            prop_assert!(
                close(local.stats().reset_after(), redis_stats.reset_after),
                "reset {:?} vs {:?}: {}",
                local.stats().reset_after(),
                redis_stats.reset_after,
                context
            );
            if let (Decision::Denied(denial), Some(retry)) = (local, redis_retry) {
                match (denial.retry_after(), retry) {
                    (None, crate::algorithm::Retry::Never) => {}
                    (Some(memory), crate::algorithm::Retry::After(redis)) => {
                        prop_assert!(
                            close(memory, redis),
                            "retry {:?} vs {:?}: {}",
                            memory,
                            redis,
                            context
                        );
                    }
                    (memory, redis) => {
                        prop_assert!(false, "retry {:?} vs {:?}: {}", memory, redis, context);
                    }
                }
            }
        }
        Ok::<(), TestCaseError>(())
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn memory_and_redis_token_buckets_agree() {
    memory_and_redis_agree::<TokenBucket>(true);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn memory_and_redis_fixed_windows_agree() {
    memory_and_redis_agree::<FixedWindow>(false);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn memory_and_redis_sliding_windows_agree() {
    memory_and_redis_agree::<SlidingWindow>(false);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn memory_and_redis_sliding_logs_agree() {
    memory_and_redis_agree::<SlidingLog>(false);
}

/// Stats as a one-rate script reports them: that rate is the limiting one.
fn one_rate_stats(remaining: u32, reset_us: u64) -> Stats {
    Stats {
        remaining,
        reset_after: Duration::from_micros(reset_us),
        limiting: 0,
        limiting_reset: Duration::from_micros(reset_us),
    }
}

fn allowed(remaining: u32, reset_us: u64) -> Outcome {
    Outcome::Allowed(one_rate_stats(remaining, reset_us))
}

fn denied(remaining: u32, reset_us: u64, retry: Retry) -> Outcome {
    Outcome::Denied {
        stats: one_rate_stats(remaining, reset_us),
        retry,
    }
}

fn retry_us(micros: u64) -> Retry {
    Retry::After(Duration::from_micros(micros))
}

/// Runs `A`'s script on `keys` at `now`, which replaces Redis `TIME`.
fn script_at<A: Scripted>(
    runtime: &tokio::runtime::Runtime,
    connection: &mut ::redis::aio::MultiplexedConnection,
    quota: &Quota,
    keys: &[String],
    (record, cost, now): (bool, u32, u64),
) -> Outcome {
    runtime
        .block_on(run_script::<A, _>(
            connection,
            quota,
            keys,
            record,
            cost,
            Some(now),
        ))
        .unwrap()
        .outcome()
}

fn expire_time_ms(
    runtime: &tokio::runtime::Runtime,
    connection: &mut ::redis::aio::MultiplexedConnection,
    key: &str,
) -> i64 {
    runtime
        .block_on(::redis::cmd("PEXPIRETIME").arg(key).query_async(connection))
        .unwrap()
}

/// The GCRA golden table for a token bucket of 1 per second with a burst
/// of 5, every value derived by hand from porin's rules: the emission
/// interval is 1 s, a full bucket is 5 intervals, a hit moves the TAT
/// `cost` intervals past `max(TAT, now)`, `remaining` is
/// `floor((now + 5 s - TAT) / 1 s)` and `reset` is `TAT - now`.
///
/// redis-cell's `CL.THROTTLE key 4 1 1` meters the same bucket: its
/// `max_burst` counts the tokens past the first, so its 4 is porin's burst
/// of 5. It reports times in whole seconds and a `retry_after` of -1 for an
/// allowed hit; porin reports microseconds and no retry time for one.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn token_bucket_script_matches_the_gcra_golden_table() {
    const SECOND: u64 = 1_000_000;
    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let mut connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let quota = Quota::new(rate(1, 1_000, 5));
    let keys = vec!["test:{golden}:0".to_owned()];
    let start = start_us();
    // (time after the start in us, cost, answer)
    let rows = [
        // More than the burst can ever hold.
        (0, 6, denied(5, 0, Retry::Never)),
        (0, 1, allowed(4, SECOND)),
        (0, 1, allowed(3, 2 * SECOND)),
        (0, 1, allowed(2, 3 * SECOND)),
        (0, 1, allowed(1, 4 * SECOND)),
        (0, 1, allowed(0, 5 * SECOND)),
        // TAT 5 s: the next token is free at 6 s - 5 s.
        (0, 1, denied(0, 5 * SECOND, retry_us(SECOND))),
        // TAT 5 s -> 6 s.
        (3 * SECOND, 1, allowed(2, 3 * SECOND)),
        // TAT 6 s -> 7 s: 1.1 tokens spare, rounded down.
        (3_100_000, 1, allowed(1, 3_900_000)),
        // TAT 7 s -> 8 s.
        (4 * SECOND, 1, allowed(1, 4 * SECOND)),
        // TAT 8 s -> 9 s.
        (8 * SECOND, 1, allowed(4, SECOND)),
        // The reference table lists remaining 4 and reset 1 s for this row,
        // then remaining 2, reset 3 s and retry 3 s for the next two: one
        // token fewer than porin, as if one were still spent. The 8 s hit
        // left the TAT at 9 s, so by TAT arithmetic the bucket is full at
        // 9.5 s, and porin reports all 5 with nothing to reset.
        (9_500_000, 0, allowed(5, 0)),
        // TAT max(9 s, 9.5 s) -> 11.5 s.
        (9_500_000, 2, allowed(3, 2 * SECOND)),
        // A TAT of 16.5 s would need now >= 11.5 s.
        (9_500_000, 5, denied(3, 2 * SECOND, retry_us(2 * SECOND))),
    ];
    for (at, cost, expected) in rows {
        let outcome = script_at::<TokenBucket>(
            &runtime,
            &mut connection,
            &quota,
            &keys,
            (true, cost, start + at),
        );
        assert_eq!(outcome, expected, "cost {cost} at {at} us");
    }
}

/// One key of a `10 per 100 ms` quota, run at fake times so nothing
/// depends on the test's speed. Its fixed window starts at `window`, and
/// every first hit lands 30 ms in, at `first`.
struct MillisecondKey {
    _server: Server,
    runtime: tokio::runtime::Runtime,
    connection: ::redis::aio::MultiplexedConnection,
    quota: Quota,
    key: Vec<String>,
    window: u64,
    first: u64,
}

const MS_PERIOD: u64 = 100_000;

impl MillisecondKey {
    fn new(name: &str) -> Option<Self> {
        let server = server()?;
        let runtime = runtime();
        let client = ::redis::Client::open(server.url.as_str()).unwrap();
        let connection = runtime
            .block_on(client.get_multiplexed_async_connection())
            .unwrap();
        let rate = Rate::new(NonZeroU32::new(10).unwrap(), Duration::from_millis(100)).unwrap();
        let window = start_us() / MS_PERIOD * MS_PERIOD;
        Some(Self {
            _server: server,
            runtime,
            connection,
            quota: Quota::new(rate),
            key: vec![format!("test:{{ms}}:{name}")],
            window,
            first: window + 30_000,
        })
    }

    /// A hit of 1 at `now`.
    fn hit<A: Scripted>(&mut self, now: u64) -> Outcome {
        script_at::<A>(
            &self.runtime,
            &mut self.connection,
            &self.quota,
            &self.key,
            (true, 1, now),
        )
    }

    /// When Redis expires the key, in microseconds.
    fn expires_us(&mut self) -> u64 {
        let ms = expire_time_ms(&self.runtime, &mut self.connection, &self.key[0]);
        u64::try_from(ms).expect("the key expires") * 1_000
    }
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn millisecond_token_buckets_refill_and_expire_on_time() {
    let Some(mut key) = MillisecondKey::new("token_bucket") else {
        return;
    };
    let first = key.first;
    // One token every 10 ms.
    for spent in 1..=10 {
        let reset = u64::from(spent) * 10_000;
        assert_eq!(key.hit::<TokenBucket>(first), allowed(10 - spent, reset));
    }
    assert_eq!(key.expires_us(), first + MS_PERIOD, "full 100 ms later");
    assert_eq!(
        key.hit::<TokenBucket>(first),
        denied(0, MS_PERIOD, retry_us(10_000))
    );
    assert_eq!(
        key.hit::<TokenBucket>(first + 9_999),
        denied(0, MS_PERIOD - 9_999, retry_us(1))
    );
    assert_eq!(
        key.hit::<TokenBucket>(first + 10_000),
        allowed(0, MS_PERIOD),
        "one token back after 10 ms"
    );
    assert_eq!(key.expires_us(), first + 110_000);
    let full = script_at::<TokenBucket>(
        &key.runtime,
        &mut key.connection,
        &key.quota,
        &key.key,
        (false, 0, first + 110_000),
    );
    assert_eq!(full, allowed(10, 0), "full at its expiry time");
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn millisecond_fixed_windows_refill_and_expire_on_time() {
    let Some(mut key) = MillisecondKey::new("fixed_window") else {
        return;
    };
    let (window, first) = (key.window, key.first);
    // The window ends 70 ms after the first hit.
    for spent in 1..=10 {
        assert_eq!(key.hit::<FixedWindow>(first), allowed(10 - spent, 70_000));
    }
    assert_eq!(key.expires_us(), window + MS_PERIOD);
    assert_eq!(
        key.hit::<FixedWindow>(window + MS_PERIOD - 1),
        denied(0, 1, retry_us(1))
    );
    assert_eq!(
        key.hit::<FixedWindow>(window + MS_PERIOD),
        allowed(9, MS_PERIOD),
        "the next window starts empty"
    );
    assert_eq!(key.expires_us(), window + 2 * MS_PERIOD);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn millisecond_sliding_logs_refill_and_expire_on_time() {
    let Some(mut key) = MillisecondKey::new("sliding_log") else {
        return;
    };
    let first = key.first;
    // Each hit expires 100 ms after it was made.
    for spent in 1..=10 {
        assert_eq!(key.hit::<SlidingLog>(first), allowed(10 - spent, MS_PERIOD));
    }
    assert_eq!(key.expires_us(), first + MS_PERIOD);
    assert_eq!(
        key.hit::<SlidingLog>(first + MS_PERIOD - 1),
        denied(0, 1, retry_us(1))
    );
    assert_eq!(
        key.hit::<SlidingLog>(first + MS_PERIOD),
        allowed(9, MS_PERIOD),
        "all ten hits have expired"
    );
    assert_eq!(key.expires_us(), first + 2 * MS_PERIOD);
}

/// A hit of cost 0 records nothing, so it must answer exactly as the
/// check `stats` makes, at the same time, on every algorithm.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn cost_zero_hits_answer_exactly_like_stats() {
    fn check<A: Scripted>(
        runtime: &tokio::runtime::Runtime,
        connection: &mut ::redis::aio::MultiplexedConnection,
    ) {
        let quota = Quota::new(rate(3, 1_000, 3))
            .with(rate(5, 60_000, 5))
            .unwrap();
        let keys = vec![
            format!("test:{{zero:{}}}:0", A::NAME),
            format!("test:{{zero:{}}}:1", A::NAME),
        ];
        let start = start_us();
        // Fresh, part spent, exhausted, refilling and idle again.
        for (step, cost) in [(0, 0), (0, 2), (1, 1), (400_000, 0), (1_700_000, 1)] {
            let now = start + step;
            if cost > 0 {
                script_at::<A>(runtime, connection, &quota, &keys, (true, cost, now));
            }
            let hit = script_at::<A>(runtime, connection, &quota, &keys, (true, 0, now));
            let stats = script_at::<A>(runtime, connection, &quota, &keys, (false, 0, now));
            assert!(matches!(hit, Outcome::Allowed(_)), "{} at {step}", A::NAME);
            assert_eq!(hit, stats, "{} at {step}", A::NAME);
        }
    }

    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let mut connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    check::<TokenBucket>(&runtime, &mut connection);
    check::<FixedWindow>(&runtime, &mut connection);
    check::<SlidingWindow>(&runtime, &mut connection);
    check::<SlidingLog>(&runtime, &mut connection);
}

/// Rates past 1,000,000 a second refill a token in under a microsecond.
/// Redis rounds that interval up to 1 us, so it admits at most one unit a
/// microsecond plus the burst, where the nanosecond memory store admits
/// more: 10,000,000 a second with a burst of 10 refills 10 units a
/// microsecond in memory and 1 in Redis.
#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn rates_past_a_million_a_second_refill_one_unit_a_microsecond() {
    use crate::{Limiter, ManualClock, Timestamp};

    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let mut connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let quota = Quota::new(rate(10_000_000, 1_000, 10));
    let keys = vec!["test:{fast}:0".to_owned()];
    let clock = ManualClock::new();
    let memory: Limiter<u8> = Limiter::builder().quota(quota).clock(clock.clone()).build();
    let start = start_us();
    let mut hit = |at: u64| {
        clock.set(Timestamp::from_nanos((start + at) * 1_000));
        let local = runtime.block_on(memory.hit_n(&0, 10)).unwrap();
        let redis = script_at::<TokenBucket>(
            &runtime,
            &mut connection,
            &quota,
            &keys,
            (true, 10, start + at),
        );
        (local.is_allowed(), redis)
    };
    // Both start with a full burst.
    assert_eq!(hit(0), (true, allowed(0, 10)));
    // Memory's TAT is 1,000 ns, so 1 us later it is full again. Redis's is
    // 10 us; 10 more units need it 10 us in the past.
    assert_eq!(hit(1), (true, denied(1, 9, retry_us(9))));
    assert_eq!(hit(9), (true, denied(9, 1, retry_us(1))));
    assert_eq!(hit(10), (true, allowed(0, 10)));
}

/// Quotas where rounding to microseconds changes the arithmetic: rates
/// past 1,000,000 a second, whose interval is below 1 us, and intervals
/// that aren't whole microseconds.
fn rounding_quota(token_bucket: bool) -> impl Strategy<Value = Quota> {
    let fast = (
        prop_oneof![
            1_000_001..=10_000_000_u32,
            Just(u32::MAX),
            1_000_001..=u32::MAX
        ],
        prop::sample::select(&[1_u64, 7, 1_000][..]),
        1..=12_u32,
    );
    let uneven = (
        prop::sample::select(&[3_u32, 6, 7, 9, 11, 13, 999_999, 1_000_001][..]),
        prop::sample::select(&[1_u64, 7, 1_000, 60_000][..]),
        1..=8_u32,
    );
    let one = prop_oneof![fast, uneven];
    prop::collection::vec(one, 1..=MAX_RATES).prop_map(move |rates| {
        let make = |&(count, period_ms, burst): &(u32, u64, u32)| {
            rate(count, period_ms, if token_bucket { burst } else { count })
        };
        let mut quota = Quota::new(make(&rates[0]));
        for spec in &rates[1..] {
            if let Ok(with) = quota.with(make(spec)) {
                quota = with;
            }
        }
        quota
    })
}

/// Like [`ops`], but mostly a few microseconds apart, where a rate past
/// 1,000,000 a second runs dry in Redis and not in memory.
///
/// Time never steps back here. A step back of `d` leaves both stores
/// `d / interval` tokens further behind, which is more tokens for memory's
/// finer interval: at 1,000,001 a millisecond with a burst of 11, a hit and
/// then a check 1 us earlier is denied in memory (1,000 tokens behind) and
/// allowed in Redis (1 token behind). Neither admits more than the burst
/// that way, so that is how each handles a clock going back, not rounding.
fn rounding_ops() -> impl Strategy<Value = Vec<(u8, i64, u32, bool)>> {
    let step = prop_oneof![
        4 => 0..=3_i64,
        2 => 0..=2_000_i64,
        1 => 0..=400_000_i64,
    ];
    let cost = prop_oneof![8 => 1..=12_u32, 1 => 0..=40_u32, 1 => any::<u32>()];
    prop::collection::vec((0..2_u8, step, cost, prop::bool::weighted(0.85)), 1..80)
}

/// Rounding to microseconds may only under-admit: given the same admitted
/// hits, Redis never allows what the nanosecond memory store denies. Redis
/// may deny what memory allows, so memory records only what Redis
/// admitted, or the two histories would part and the comparison mean
/// nothing.
fn redis_never_admits_what_memory_denies<A: crate::Algorithm + Default>(token_bucket: bool) {
    use crate::{Limiter, ManualClock, Timestamp};

    let Some(server) = server() else { return };
    let runtime = runtime();
    let client = ::redis::Client::open(server.url.as_str()).unwrap();
    let connection = runtime
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let start = start_us();
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(96);
    let mut runner = TestRunner::new(Config::with_cases(cases));
    // (both allowed, only memory allowed), to show the runs aren't vacuous.
    let tally = std::cell::Cell::new((0_u32, 0_u32));
    let result = runner.run(
        &(rounding_quota(token_bucket), rounding_ops()),
        |(quota, ops)| {
            let mut connection = connection.clone();
            runtime
                .block_on(::redis::cmd("FLUSHALL").query_async::<()>(&mut connection))
                .unwrap();
            let clock = ManualClock::new();
            let memory: Limiter<u8, A> = Limiter::builder()
                .algorithm(A::default())
                .quota(quota)
                .clock(clock.clone())
                .build();
            let mut now = start;
            for (key, step, cost, record) in ops {
                now = now.saturating_add_signed(step);
                clock.set(Timestamp::from_nanos(now * 1_000));
                let keys: Vec<String> = (0..quota.rates().len())
                    .map(|rate| format!("test:{{{key}}}:{rate}"))
                    .collect();
                let redis = script_at::<A>(
                    &runtime,
                    &mut connection,
                    &quota,
                    &keys,
                    (record, cost, now),
                );
                let redis_allowed = matches!(redis, Outcome::Allowed(_));
                let local = runtime.block_on(memory.test_n(&key, cost)).unwrap();
                let context = format!("{quota} key {key} at {now} cost {cost} record {record}");
                prop_assert!(
                    !redis_allowed || local.is_allowed(),
                    "Redis allowed what memory denies: {}",
                    context
                );
                let (both, only_memory) = tally.get();
                tally.set(match (redis_allowed, local.is_allowed()) {
                    (true, _) => (both + 1, only_memory),
                    (false, true) => (both, only_memory + 1),
                    (false, false) => (both, only_memory),
                });
                if record && redis_allowed {
                    let recorded = runtime.block_on(memory.hit_n(&key, cost)).unwrap();
                    prop_assert!(recorded.is_allowed(), "{}", context);
                }
            }
            Ok::<(), TestCaseError>(())
        },
    );
    if let Err(error) = result {
        panic!("{error}");
    }
    let (both, only_memory) = tally.get();
    assert!(both > 0, "some hits were allowed");
    if token_bucket {
        assert!(only_memory > 0, "some runs reached the rounding");
    }
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn redis_token_buckets_never_admit_what_memory_denies() {
    redis_never_admits_what_memory_denies::<TokenBucket>(true);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn redis_fixed_windows_never_admit_what_memory_denies() {
    redis_never_admits_what_memory_denies::<FixedWindow>(false);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn redis_sliding_windows_never_admit_what_memory_denies() {
    redis_never_admits_what_memory_denies::<SlidingWindow>(false);
}

#[test]
#[cfg_attr(miri, ignore = "needs a Redis server")]
fn redis_sliding_logs_never_admit_what_memory_denies() {
    redis_never_admits_what_memory_denies::<SlidingLog>(false);
}
