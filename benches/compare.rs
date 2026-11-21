//! porin's public `Limiter` against governor's keyed limiter, per clock.
//!
//! A fixed-loop harness instead of divan: each case runs the same number of
//! hits seven times and reports the median, which repeats within about half
//! a nanosecond. Run pinned to one performance core, or to the performance
//! cores for the threaded cases:
//!
//! `taskset -c 4 cargo bench --bench compare --features quanta [filter...]`
//! `taskset -c 0-11 cargo bench --bench compare --features quanta threads`

#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::print_stdout,
    clippy::panic,
    clippy::match_wild_err_arm,
    reason = "benchmark arithmetic and output; a failed setup aborts the run"
)]

use std::hint::black_box;
use std::num::NonZeroU32;
use std::pin::pin;
use std::sync::Barrier;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use porin::{Clock, CoarseClock, Limiter, ManualClock, MonotonicClock, Quota, quota};

/// High enough that every hit is allowed: the allow path is the common one.
const UNLIMITED: Quota = quota!("4000000000/second");
const HITS: u32 = 4_000_000;
const THREADED_HITS: u32 = 1_000_000;
const THREADS: usize = 16;

fn main() {
    let filter: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| !arg.starts_with("--"))
        .collect();
    let selected =
        |name: &str| filter.is_empty() || filter.iter().any(|f| name.contains(f.as_str()));

    for keys in [1_000, 100_000] {
        for case in cases() {
            let name = format!("{keys} keys/{}", case.name);
            if selected(&name) {
                let mut hit = (case.make)(keys);
                report(&name, single(&mut *hit, keys));
            }
        }
    }
    for case in cases() {
        let name = format!("threads {THREADS}/{}", case.name);
        if selected(&name) {
            let hit = (case.make)(100_000);
            report(&name, threaded(&*hit, 100_000));
        }
    }
}

/// One way of hitting a key: governor or porin with some clock.
type Hit = Box<dyn Fn(u64) -> bool + Send + Sync>;

struct Case {
    name: &'static str,
    make: fn(u64) -> Hit,
}

fn cases() -> Vec<Case> {
    let mut cases = vec![Case {
        name: "governor",
        make: |keys| {
            let limiter =
                governor::RateLimiter::keyed(governor::Quota::per_second(NonZeroU32::MAX));
            for key in 0..keys {
                assert!(limiter.check_key(&key).is_ok());
            }
            Box::new(move |key| limiter.check_key(&key).is_ok())
        },
    }];
    cases.push(Case {
        name: "governor upkeep 1ms",
        make: |keys| {
            let clock =
                match governor::clock::QuantaUpkeepClock::from_interval(Duration::from_millis(1)) {
                    Ok(clock) => clock,
                    Err(error) => panic!("can't start governor's upkeep clock: {error}"),
                };
            let limiter = governor::RateLimiter::dashmap_with_clock(
                governor::Quota::per_second(NonZeroU32::MAX),
                clock,
            );
            for key in 0..keys {
                assert!(limiter.check_key(&key).is_ok());
            }
            Box::new(move |key| limiter.check_key(&key).is_ok())
        },
    });
    cases.push(Case {
        name: "porin monotonic",
        make: |keys| porin(MonotonicClock::new(), keys),
    });
    #[cfg(feature = "quanta")]
    cases.push(Case {
        name: "porin quanta",
        make: |keys| porin(porin::QuantaClock::new(), keys),
    });
    cases.push(Case {
        name: "porin coarse 1ms",
        make: |keys| porin(coarse(), keys),
    });
    cases.push(Case {
        name: "porin manual (no clock cost)",
        make: |keys| porin(ManualClock::new(), keys),
    });
    cases
}

fn coarse() -> CoarseClock {
    match CoarseClock::new(Duration::from_millis(1)) {
        Ok(clock) => clock,
        Err(error) => panic!("can't start the coarse clock: {error}"),
    }
}

fn porin(clock: impl Clock + 'static, keys: u64) -> Hit {
    let limiter: Limiter<u64> = Limiter::builder().quota(UNLIMITED).clock(clock).build();
    for key in 0..keys {
        assert!(now_or_never(limiter.hit(&key)).is_ok_and(|decision| decision.is_allowed()));
    }
    Box::new(move |key| now_or_never(limiter.hit(&key)).is_ok_and(|decision| decision.is_allowed()))
}

/// The memory store answers on the first poll.
fn now_or_never<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    match pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => unreachable!("the memory store answers on the first poll"),
    }
}

/// Walks the keys with a large prime stride, so consecutive hits land far
/// apart in memory, as real traffic does.
fn stride(key: u64, keys: u64) -> u64 {
    (key + 7_919) % keys
}

/// Median nanoseconds per hit over seven runs on this thread.
fn single(hit: &mut (dyn Fn(u64) -> bool + Send + Sync), keys: u64) -> f64 {
    let mut key = 0;
    let mut run = |hits: u32| {
        let start = Instant::now();
        for _ in 0..hits {
            black_box(hit(key));
            key = stride(key, keys);
        }
        start.elapsed().as_nanos() as f64 / f64::from(hits)
    };
    run(HITS / 4);
    median((0..7).map(|_| run(HITS)).collect())
}

/// Median over five runs of the mean nanoseconds per hit across threads.
fn threaded(hit: &(dyn Fn(u64) -> bool + Send + Sync), keys: u64) -> f64 {
    let run = || {
        let barrier = Barrier::new(THREADS);
        let per_thread: Vec<f64> = thread::scope(|scope| {
            let workers: Vec<_> = (0..THREADS as u64)
                .map(|worker| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let mut key = worker * 6_151 % keys;
                        for _ in 0..THREADED_HITS / 4 {
                            black_box(hit(key));
                            key = stride(key, keys);
                        }
                        barrier.wait();
                        let start = Instant::now();
                        for _ in 0..THREADED_HITS {
                            black_box(hit(key));
                            key = stride(key, keys);
                        }
                        start.elapsed().as_nanos() as f64 / f64::from(THREADED_HITS)
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| match worker.join() {
                    Ok(nanos) => nanos,
                    Err(_) => panic!("a benchmark thread panicked"),
                })
                .collect()
        });
        per_thread.iter().sum::<f64>() / per_thread.len() as f64
    };
    median((0..5).map(|_| run()).collect())
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples.get(samples.len() / 2).copied().unwrap_or(f64::NAN)
}

fn report(name: &str, nanos: f64) {
    println!("{name:<44} {nanos:7.1} ns");
}
