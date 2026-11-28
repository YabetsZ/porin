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
use crate::algorithm::{Outcome, Spec, Unit, check_quota_in};
use crate::quota::MAX_RATES;
use crate::{FixedWindow, Quota, Rate, SlidingLog, SlidingWindow, TokenBucket};

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
