//! Benchmarks for the in-memory store, with governor's keyed limiter as a
//! reference point.
//!
//! Run inside a memory cap:
//! `RUSTFLAGS="--cfg porin_bench" systemd-run --user --scope -p MemoryMax=8G cargo bench --bench memory`

#[cfg(porin_bench)]
#[expect(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::wildcard_imports,
    clippy::cast_possible_truncation,
    reason = "benchmark code: small bounded counters and fixture lookups"
)]
mod benches {
    use std::cell::Cell;
    use std::num::NonZeroU32;
    use std::time::Duration;

    use divan::Bencher;
    use porin::bench::{AlgorithmState, Store};
    use porin::{Clock, MonotonicClock, Quota, Timestamp, quota};

    /// High enough that every hit is allowed: the allow path is the common one.
    const UNLIMITED: Quota = quota!("4000000000/second");
    const THREADS: &[usize] = &[1, 2, 4, 8, 16];
    const KEYS: u64 = 100_000;

    thread_local! {
        static NEXT_KEY: Cell<u64> = const { Cell::new(0) };
    }

    /// Walks all keys with a large prime stride, so consecutive hits land
    /// far apart in memory, as real traffic does.
    fn next_key() -> u64 {
        NEXT_KEY.with(|next| {
            let key = next.get();
            next.set((key + 7_919) % KEYS);
            key
        })
    }

    fn governor_unlimited() -> governor::DefaultKeyedRateLimiter<u64> {
        governor::RateLimiter::keyed(governor::Quota::per_second(NonZeroU32::MAX))
    }

    /// The pure math alone: check and commit one unit, no map, no lock.
    #[divan::bench(args = ["gcra", "fixed_window", "sliding_window", "sliding_log"])]
    fn algorithm(bencher: Bencher<'_, '_>, name: &str) {
        let rate = quota!("500000/second").rates()[0];
        let mut state = AlgorithmState::new(name);
        let mut now = 0_u64;
        bencher.bench_local(|| {
            now += 2_000;
            state.hit(rate, Timestamp::from_nanos(now))
        });
    }

    /// The fixed costs every hit pays besides the map: reading the clock and
    /// hashing the key.
    mod parts {
        use std::hash::BuildHasher;

        use super::*;

        #[divan::bench]
        fn clock_now(bencher: Bencher<'_, '_>) {
            let clock = MonotonicClock::new();
            bencher.bench_local(|| clock.now());
        }

        #[divan::bench]
        fn hash_u64(bencher: Bencher<'_, '_>) {
            let hasher = std::collections::hash_map::RandomState::new();
            bencher.bench_local(|| hasher.hash_one(divan::black_box(next_key())));
        }
    }

    mod one_hot_key {
        use super::*;

        /// Every thread hits the same key, so they all queue on one shard lock.
        #[divan::bench(threads = THREADS)]
        fn porin(bencher: Bencher<'_, '_>) {
            let store = Store::<u64>::new(1_000_000);
            let clock = MonotonicClock::new();
            bencher.bench(|| store.hit(&UNLIMITED, &7, clock.now()));
        }

        #[divan::bench(threads = THREADS)]
        fn governor(bencher: Bencher<'_, '_>) {
            let limiter = governor_unlimited();
            bencher.bench(|| limiter.check_key(&7).is_ok());
        }
    }

    mod many_keys {
        use super::*;

        /// 100k known keys spread over all shards.
        #[divan::bench(threads = [1, 4, 16])]
        fn porin(bencher: Bencher<'_, '_>) {
            let store = Store::<u64>::new(1_000_000);
            let clock = MonotonicClock::new();
            for key in 0..KEYS {
                store.hit(&UNLIMITED, &key, clock.now());
            }
            bencher.bench(|| store.hit(&UNLIMITED, &next_key(), clock.now()));
        }

        /// The same, with a precomputed time, to separate the store from the clock read.
        #[divan::bench(threads = [1, 16])]
        fn porin_without_clock(bencher: Bencher<'_, '_>) {
            let store = Store::<u64>::new(1_000_000);
            for key in 0..KEYS {
                store.hit(&UNLIMITED, &key, Timestamp::ZERO);
            }
            bencher.bench(|| store.hit(&UNLIMITED, &next_key(), Timestamp::ZERO));
        }

        #[divan::bench(threads = [1, 4, 16])]
        fn governor(bencher: Bencher<'_, '_>) {
            let limiter = governor_unlimited();
            for key in 0..KEYS {
                let _ = limiter.check_key(&key);
            }
            bencher.bench(|| limiter.check_key(&next_key()).is_ok());
        }

        /// String keys such as `user-1234`: longer to hash and compare.
        #[divan::bench]
        fn porin_string_keys(bencher: Bencher<'_, '_>) {
            let keys: Vec<String> = (0..KEYS).map(|key| format!("user-{key}")).collect();
            let store = Store::<String>::new(1_000_000);
            let clock = MonotonicClock::new();
            for key in &keys {
                store.hit(&UNLIMITED, key, clock.now());
            }
            bencher.bench_local(|| {
                let key = &keys[next_key() as usize];
                store.hit(&UNLIMITED, key, clock.now())
            });
        }
    }

    mod full_store {
        use super::*;

        /// Every key stays active for an hour, so the store refuses each new
        /// key. The earliest-idle check skips the sweep.
        #[divan::bench]
        fn refuse_new_keys(bencher: Bencher<'_, '_>) {
            let quota = quota!("1/hour");
            let store = Store::<u64>::new(65_536);
            for key in 0..200_000 {
                store.hit(&quota, &key, Timestamp::ZERO);
            }
            let mut key = 1_000_000_u64;
            bencher.bench_local(|| {
                key += 1;
                store.hit(&quota, &key, Timestamp::from_nanos(1))
            });
        }

        /// A new key every 10 µs, each active for 1 s: about 100k keys are
        /// active at once. With room for 65k, shards stay full and keys keep
        /// expiring, so every new key sweeps a whole shard. With room for 1M,
        /// sweeps are rare and each frees many keys.
        #[divan::bench(args = [65_536, 1_048_576])]
        fn steady_churn(bencher: Bencher<'_, '_>, max_keys: usize) {
            let quota = quota!("1/second");
            let store = Store::<u64>::new(max_keys);
            let mut now = 0_u64;
            let mut key = 0_u64;
            let mut arrive = move || {
                now += 10_000;
                key += 1;
                store.hit(&quota, &key, Timestamp::from_nanos(now))
            };
            for _ in 0..300_000 {
                arrive();
            }
            bencher.bench_local(arrive);
        }

        /// The same churn with room for 65k, under each full-store policy.
        #[divan::bench(args = ["refuse", "evict_nearest_idle", "overflow"])]
        fn steady_churn_policy(bencher: Bencher<'_, '_>, policy: &str) {
            let quota = quota!("1/second");
            let store = match policy {
                "evict_nearest_idle" => Store::<u64>::evicting(65_536, Duration::from_millis(100)),
                "overflow" => Store::<u64>::overflowing(65_536, quota!("1000/second")),
                _ => Store::<u64>::new(65_536),
            };
            let mut now = 0_u64;
            let mut key = 0_u64;
            let mut arrive = move || {
                now += 10_000;
                key += 1;
                store.hit(&quota, &key, Timestamp::from_nanos(now))
            };
            for _ in 0..300_000 {
                arrive();
            }
            bencher.bench_local(arrive);
        }
    }
}

fn main() {
    #[cfg(porin_bench)]
    divan::main();

    #[cfg(not(porin_bench))]
    #[expect(clippy::print_stderr, reason = "the benchmark binary's only output")]
    {
        eprintln!("porin's benchmarks need RUSTFLAGS=\"--cfg porin_bench\"; see benches/memory.rs");
    }
}
