//! [`FailOver`]: while Redis is unavailable, each instance limits keys on
//! its own, to its share of their quota.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::OnceLock;

use super::scripts::Scripted;
use crate::limiter::Decision;
use crate::quota::MAX_RATES;
use crate::store::memory::MemoryStore;
use crate::{FixedWindow, Quota, Rate, SlidingLog, SlidingWindow, Timestamp, TokenBucket};

/// Keys each algorithm's fallback tracks unless [`FailOver::max_keys`] says
/// otherwise.
const DEFAULT_MAX_KEYS: NonZeroUsize = match NonZeroUsize::new(100_000) {
    Some(max_keys) => max_keys,
    None => NonZeroUsize::MIN,
};

/// A failure policy for a [`RedisStore`](super::RedisStore): while Redis
/// fails or the circuit breaker is open, each instance limits keys itself,
/// in memory, to a share of their quota.
///
/// Every limiter on the store falls back to its own algorithm and quota,
/// with each rate's count and burst divided by `instances`, rounding down.
/// A rate whose count is below `instances` gets 1 unit per a period
/// stretched by the same factor, rounded up: `10/day` over 50 instances
/// becomes `1/5 days`. So the shares never add up to more than the quota,
/// except that each instance can always spend one unit at once: `instances`
/// units in a burst across the fleet, however small the quota.
///
/// # Over-admission
///
/// The instances don't share the fallback's state. If more instances run
/// than `instances` says, together they admit more than the quota; with
/// `instances` set to 1, up to as many times the quota as there are
/// instances. Set it to the number of instances that share the Redis, and
/// raise it when you scale out.
///
/// # What it does not do
///
/// - Redis denials are final: the fallback answers only when Redis didn't.
/// - Errors that retrying won't fix, such as refused credentials, stay
///   [`Error::Store`](crate::Error::Store).
/// - [`reset`](crate::Limiter#method.reset-1) is still an error while Redis
///   is unavailable.
/// - When Redis is back, its own state decides again: what the fallback
///   admitted isn't added to it.
///
/// Answers from the fallback have [`Stats::is_fallback`](crate::Stats::is_fallback)
/// set and [`Stats::is_known`](crate::Stats::is_known) cleared, since their
/// numbers are for the share. Each algorithm's fallback is created on first
/// use and tracks at most [`max_keys`](Self::max_keys) keys, refusing new
/// ones when full, as [`WhenFull::Refuse`](crate::WhenFull::Refuse) does.
/// A rate whose share needs a period over 10,000 days denies every hit
/// while the fallback answers, as [`FailClosed`](super::super::FailClosed)
/// would.
///
/// # Examples
///
/// ```no_run
/// use std::num::NonZeroU32;
///
/// use porin::store::{FailOver, RedisStore};
/// use porin::{Limiter, quota};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// // Four instances share this Redis. While it's down, each one allows a
/// // key a quarter of its quota on its own: 25 a minute here.
/// let instances = NonZeroU32::new(4).ok_or("zero")?;
/// let redis = RedisStore::builder("redis://127.0.0.1/")
///     .on_failure(FailOver::to_memory(instances))
///     .connect()
///     .await?;
/// let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "api")
///     .quota(quota!("100/minute"))
///     .build()?;
/// let decision = limiter.hit(&42).await?;
/// if decision.stats().is_fallback() {
///     // Redis didn't answer; this instance decided alone.
/// }
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FailOver {
    instances: NonZeroU32,
    max_keys: NonZeroUsize,
}

impl FailOver {
    /// Falls back to an in-memory limiter per instance, allowing each key
    /// its quota divided by `instances`.
    #[must_use]
    pub const fn to_memory(instances: NonZeroU32) -> Self {
        Self {
            instances,
            max_keys: DEFAULT_MAX_KEYS,
        }
    }

    /// The most keys each algorithm's fallback tracks at once, across every
    /// limiter on the store that uses that algorithm. The default is
    /// 100,000.
    #[must_use]
    pub const fn max_keys(mut self, max_keys: NonZeroUsize) -> Self {
        self.max_keys = max_keys;
        self
    }
}

#[expect(private_interfaces, reason = "sealed: outside crates can't name it")]
impl crate::store::sealed::FailurePolicy for FailOver {
    fn policy(self) -> crate::store::Policy {
        crate::store::Policy::FailOver(self)
    }
}
impl crate::store::FailurePolicy for FailOver {}

/// The fallback limiters of one store, one per algorithm, keyed by each
/// limiter key's first Redis key name, which names the limiter, the key,
/// the algorithm and the quota.
pub(crate) struct Fallback {
    settings: FailOver,
    token_bucket: OnceLock<MemoryStore<String, TokenBucket>>,
    fixed_window: OnceLock<MemoryStore<String, FixedWindow>>,
    sliding_window: OnceLock<MemoryStore<String, SlidingWindow>>,
    sliding_log: OnceLock<MemoryStore<String, SlidingLog>>,
}

impl Fallback {
    pub(super) const fn new(settings: FailOver) -> Self {
        Self {
            settings,
            token_bucket: OnceLock::new(),
            fixed_window: OnceLock::new(),
            sliding_window: OnceLock::new(),
            sliding_log: OnceLock::new(),
        }
    }

    /// The fallback's answer for `key` under `quota`, or `None` if `quota`
    /// has a rate whose share can't be expressed.
    pub(super) fn decide<A: Scripted>(
        &self,
        quota: &Quota,
        key: &str,
        record: bool,
        cost: u32,
        now: Timestamp,
    ) -> Option<Decision> {
        let share = share(quota, self.settings.instances)?;
        let store = A::fallback(self).get_or_init(|| MemoryStore::new(self.settings.max_keys));
        let hit = if record {
            store.hit(&share, key, now, cost)
        } else {
            store.test(&share, key, now, cost)
        };
        Some(Decision::from_fallback(hit))
    }

    /// Forgets `key` in every fallback created so far.
    pub(super) fn reset(&self, key: &str) {
        if let Some(store) = self.token_bucket.get() {
            store.reset(key);
        }
        if let Some(store) = self.fixed_window.get() {
            store.reset(key);
        }
        if let Some(store) = self.sliding_window.get() {
            store.reset(key);
        }
        if let Some(store) = self.sliding_log.get() {
            store.reset(key);
        }
    }

    pub(crate) const fn token_bucket(&self) -> &OnceLock<MemoryStore<String, TokenBucket>> {
        &self.token_bucket
    }

    pub(crate) const fn fixed_window(&self) -> &OnceLock<MemoryStore<String, FixedWindow>> {
        &self.fixed_window
    }

    pub(crate) const fn sliding_window(&self) -> &OnceLock<MemoryStore<String, SlidingWindow>> {
        &self.sliding_window
    }

    pub(crate) const fn sliding_log(&self) -> &OnceLock<MemoryStore<String, SlidingLog>> {
        &self.sliding_log
    }
}

/// One instance's share of `quota` among `instances`: no rate allows more
/// than its count divided by `instances` per period, except that every rate
/// allows at least one unit at once. `None` if a share needs a period
/// longer than a rate can have.
pub(super) fn share(quota: &Quota, instances: NonZeroU32) -> Option<Quota> {
    let mut rates = [None::<Rate>; MAX_RATES];
    for (rate, slot) in quota.rates().iter().zip(&mut rates) {
        *slot = Some(share_of(*rate, instances)?);
    }
    // Two rates whose shares have the same period become one, as strict as
    // both.
    for index in 1..rates.len() {
        let (kept, rest) = rates.split_at_mut(index);
        let Some(slot) = rest.first_mut() else {
            continue;
        };
        let Some(rate) = *slot else { continue };
        if let Some(kept) = kept
            .iter_mut()
            .flatten()
            .find(|kept| kept.period() == rate.period())
        {
            *kept = Rate::new(kept.count().min(rate.count()), rate.period())
                .ok()?
                .with_burst(kept.burst().min(rate.burst()));
            *slot = None;
        }
    }
    let mut rates = rates.into_iter().flatten();
    let first = rates.next()?;
    rates.try_fold(Quota::new(first), |quota, rate| quota.with(rate).ok())
}

fn share_of(rate: Rate, instances: NonZeroU32) -> Option<Rate> {
    let divide = |units: NonZeroU32| {
        NonZeroU32::new(units.get().checked_div(instances.get()).unwrap_or(0))
            .unwrap_or(NonZeroU32::MIN)
    };
    // Below one unit per period, stretch the period instead: 1 unit per
    // `ceil(instances / count)` periods is at most the share.
    let stretch = if rate.count() < instances {
        instances.get().div_ceil(rate.count().get())
    } else {
        1
    };
    let period = rate.period().checked_mul(stretch)?;
    Some(
        Rate::new(divide(rate.count()), period)
            .ok()?
            .with_burst(divide(rate.burst())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota;

    fn instances(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap()
    }

    #[test]
    fn counts_and_bursts_are_divided_rounding_down() {
        assert_eq!(
            share(&quota!("100/minute"), instances(4)),
            Some(quota!("25/minute"))
        );
        assert_eq!(
            share(&quota!("10/second; 1000/day"), instances(3)),
            Some(quota!("3/second; 333/day"))
        );
        let bursty = Quota::new(Rate::per_second(instances(10)).with_burst(instances(100)));
        let expected = Quota::new(Rate::per_second(instances(2)).with_burst(instances(25)));
        assert_eq!(share(&bursty, instances(4)), Some(expected));
        assert_eq!(
            share(&quota!("7/hour"), instances(1)),
            Some(quota!("7/hour"))
        );
    }

    #[test]
    fn a_count_below_the_instances_stretches_the_period() {
        assert_eq!(
            share(&quota!("10/day"), instances(50)),
            Some(quota!("1/5 days"))
        );
        // ceil(4 / 3) = 2: 1 per 2 s each, 2 a second across 4 instances.
        assert_eq!(
            share(&quota!("3/second"), instances(4)),
            Some(quota!("1/2 seconds"))
        );
    }

    #[test]
    fn rates_whose_shares_meet_merge_into_the_stricter() {
        // 1/s and 2/2s both become 1 per 2 s over 2 instances.
        assert_eq!(
            share(&quota!("1/second; 2/2 seconds"), instances(2)),
            Some(quota!("1/2 seconds"))
        );
    }

    #[test]
    fn a_share_past_the_longest_period_is_none() {
        assert_eq!(share(&quota!("1/10000 days"), instances(2)), None);
    }

    /// Over any number of periods, the shares of every instance together
    /// never pass the quota, apart from the one unit each always gets.
    #[test]
    fn shares_never_add_up_to_more_than_the_quota() {
        for count in 1..=40_u32 {
            for fleet in 1..=40_u32 {
                let rate = Rate::new(instances(count), std::time::Duration::from_secs(60)).unwrap();
                let quota = Quota::new(rate);
                let shared = share(&quota, instances(fleet)).unwrap();
                let [part] = shared.rates() else {
                    panic!("one rate");
                };
                // Units per minute across the fleet, as a fraction.
                let fleet_units = u128::from(part.count().get()) * u128::from(fleet) * 60_000;
                let quota_units = u128::from(count) * part.period().as_millis();
                assert!(
                    fleet_units <= quota_units,
                    "{count}/minute over {fleet}: {part}"
                );
            }
        }
    }
}
