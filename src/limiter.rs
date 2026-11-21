//! The [`Limiter`]: the type applications hold and call.

mod decision;

use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub use self::decision::{Decision, Denial, Stats};
use crate::store::memory::{MemoryStore, Stored};
use crate::store::{Hit, WhenFull};
use crate::{Clock, MonotonicClock, Quota, TokenBucket};

/// Keys tracked at once unless [`Builder::max_keys`] says otherwise.
const DEFAULT_MAX_KEYS: NonZeroUsize = match NonZeroUsize::new(1_000_000) {
    Some(max_keys) => max_keys,
    None => NonZeroUsize::MIN,
};

/// A rate limiting algorithm a [`Limiter`] can run: [`TokenBucket`] (the
/// default), [`FixedWindow`](crate::FixedWindow),
/// [`SlidingWindow`](crate::SlidingWindow) or
/// [`SlidingLog`](crate::SlidingLog).
///
/// Sealed: porin's stores are built for exactly these algorithms.
pub trait Algorithm: sealed::Sealed + Copy + fmt::Debug + Send + Sync + 'static {}

mod sealed {
    /// Ties the public algorithm types to the store machinery without
    /// making that machinery public.
    #[expect(
        private_bounds,
        reason = "the sealing: outside crates can't name or implement it"
    )]
    pub trait Sealed: crate::store::memory::Stored {}
}

impl<A> sealed::Sealed for A where A: Stored {}
impl<A> Algorithm for A where A: Stored + Copy + fmt::Debug + Send + Sync + 'static {}

/// The store failed, so the limiter couldn't decide.
///
/// The memory store can't fail, so a memory limiter never returns this.
/// Stores that talk to a server add variants. A denied hit is a
/// [`Decision`], not an error.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {}

impl fmt::Display for Error {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl std::error::Error for Error {}

/// A rate limiter for keys of type `K`, running algorithm `A`.
///
/// Cloning is cheap and clones share state, so build one limiter and clone
/// it into every handler or task.
///
/// # Examples
///
/// ```
/// use porin::{Limiter, quota};
///
/// # pollster::block_on(async {
/// let limiter = Limiter::memory(quota!("2/second"));
/// let user_id = 42_u64;
/// assert!(limiter.hit(&user_id).await?.is_allowed());
/// assert!(limiter.hit(&user_id).await?.is_allowed());
/// assert!(!limiter.hit(&user_id).await?.is_allowed());
/// # Ok::<(), porin::Error>(())
/// # }).unwrap();
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "memory")))]
pub struct Limiter<K, A: Algorithm = TokenBucket> {
    inner: Arc<Inner<K, A>>,
}

struct Inner<K, A: Stored> {
    quota: Quota,
    algorithm: A,
    clock: Box<dyn Clock>,
    store: SizedStore<K, A>,
}

/// The memory store with entries sized to the quota's rate count: a 1-rate
/// quota stores one state per key, not four.
enum SizedStore<K, A: Stored> {
    One(MemoryStore<K, A, 1>),
    Two(MemoryStore<K, A, 2>),
    Three(MemoryStore<K, A, 3>),
    Four(MemoryStore<K, A, 4>),
}

/// Runs `$body` with `$store` bound to whichever size the store has.
macro_rules! with_store {
    ($sized:expr, $store:ident => $body:expr) => {
        match $sized {
            SizedStore::One($store) => $body,
            SizedStore::Two($store) => $body,
            SizedStore::Three($store) => $body,
            SizedStore::Four($store) => $body,
        }
    };
}

impl<K: Hash + Eq> Limiter<K> {
    /// A token bucket limiter that keeps its state in memory, with the
    /// default settings.
    ///
    /// # Examples
    ///
    /// ```
    /// use porin::{Limiter, quota};
    ///
    /// let limiter: Limiter<String> = Limiter::memory(quota!("10/second; 1000/day"));
    /// ```
    #[must_use]
    pub fn memory(quota: Quota) -> Self {
        Self::builder().quota(quota).build()
    }

    /// Starts configuring a limiter. Only the quota is required.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use std::time::Duration;
    ///
    /// use porin::{Limiter, SlidingWindow, WhenFull, quota};
    ///
    /// let max_keys = NonZeroUsize::new(100_000).ok_or("zero")?;
    /// let limiter: Limiter<u64, SlidingWindow> = Limiter::builder()
    ///     .algorithm(SlidingWindow)
    ///     .quota(quota!("100/minute"))
    ///     .max_keys(max_keys)
    ///     .when_full(WhenFull::EvictNearestIdle {
    ///         within: Duration::from_millis(100),
    ///     })
    ///     .build();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn builder() -> Builder<K, TokenBucket, NoQuota> {
        Builder {
            algorithm: TokenBucket,
            quota: NoQuota,
            max_keys: DEFAULT_MAX_KEYS,
            when_full: WhenFull::Refuse,
            clock: Box::new(MonotonicClock::new()),
            keys: PhantomData,
        }
    }
}

#[expect(
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    reason = "the memory store answers at once; stores that talk to a server will await"
)]
impl<K: Hash + Eq, A: Algorithm> Limiter<K, A> {
    /// Spends one unit for `key`, if its quota allows.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn hit<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.hit_n(key, 1).await
    }

    /// Spends `cost` units for `key` at once, if its quota allows all of
    /// them. Nothing is spent on a denial. A cost of 0 spends nothing and
    /// reports the key's stats.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn hit_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        let inner = &*self.inner;
        let now = inner.clock.now();
        let hit = with_store!(&inner.store, store => store.hit(&inner.quota, key, now, cost));
        Ok(Decision::from_hit(hit))
    }

    /// Whether one unit for `key` would be allowed now, without spending it.
    ///
    /// A key the limiter isn't tracking yet is judged as never hit, even if
    /// the store is full and [`hit`](Self::hit) would refuse it.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn test<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.test_n(key, 1).await
    }

    /// Whether `cost` units for `key` would be allowed now, without
    /// spending them. See [`test`](Self::test).
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn test_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(Decision::from_hit(self.peek(key, cost)))
    }

    /// What `key` has left, without spending anything.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn stats<Q>(&self, key: &Q) -> Result<Stats, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Ok(Decision::from_hit(self.peek(key, 0)).stats())
    }

    /// Forgets `key`, giving it its full allowance back.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn reset<Q>(&self, key: &Q) -> Result<(), Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        with_store!(&self.inner.store, store => store.reset(key));
        Ok(())
    }

    /// Spends one unit for `key`, waiting until the quota allows it.
    ///
    /// For clients of an outside API with a known limit: instead of sending a
    /// request that would be refused, wait for the exact moment it fits. Each
    /// wait lasts the denial's [`retry_after`](Denial::retry_after); if other
    /// callers spend the room first, it waits again. Dropping the future
    /// stops waiting and spends nothing.
    ///
    /// Waits use real time, whatever [`Clock`] the limiter was built with.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    ///
    /// # Examples
    ///
    /// ```
    /// use porin::{Limiter, quota};
    ///
    /// # pollster::block_on(async {
    /// let limiter: Limiter<String> = Limiter::memory(quota!("100/second"));
    /// for _ in 0..101 {
    ///     // The 101st call waits about 10 ms for the bucket to refill.
    ///     limiter.until_ready("api.example.com").await?;
    /// }
    /// # Ok::<(), porin::Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn until_ready<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.until_ready_n(key, 1).await
    }

    /// Spends `cost` units for `key`, waiting until the quota allows them
    /// all. Returns a denial without waiting if `cost` can never fit, because
    /// it is larger than the quota allows at once. See
    /// [`until_ready`](Self::until_ready).
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub async fn until_ready_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        loop {
            match self.hit_n(key, cost).await? {
                Decision::Denied(denial) => match denial.retry_after() {
                    Some(wait) => futures_timer::Delay::new(wait).await,
                    None => return Ok(Decision::Denied(denial)),
                },
                allowed @ Decision::Allowed(_) => return Ok(allowed),
            }
        }
    }

    /// The quota every key is limited to.
    #[must_use]
    pub fn quota(&self) -> &Quota {
        &self.inner.quota
    }

    /// How many hits so far came from keys the store had no room to track,
    /// whatever the [`WhenFull`] policy then did with them. A key keeps
    /// adding one per hit until it gets a slot. Worth alerting on: it means
    /// [`Builder::max_keys`] is too low or a flood of keys is arriving.
    #[must_use]
    pub fn full_hits(&self) -> u64 {
        with_store!(&self.inner.store, store => store.full_hits())
    }

    fn peek<Q>(&self, key: &Q, cost: u32) -> Hit
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let inner = &*self.inner;
        let now = inner.clock.now();
        with_store!(&inner.store, store => store.test(&inner.quota, key, now, cost))
    }
}

impl<K, A: Algorithm> Clone for Limiter<K, A> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, A: Algorithm> fmt::Debug for Limiter<K, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = &*self.inner;
        let mut debug = f.debug_struct("Limiter");
        debug
            .field("algorithm", &inner.algorithm)
            .field("quota", &inner.quota);
        with_store!(&inner.store, store => debug.field("store", store));
        debug.finish_non_exhaustive()
    }
}

/// The builder's state before a quota is set. [`Builder::build`] needs one.
#[derive(Clone, Copy, Debug)]
pub struct NoQuota;

/// Configures a [`Limiter`]. Start with [`Limiter::builder`].
#[must_use = "a builder does nothing until `build` is called"]
pub struct Builder<K, A, Q> {
    algorithm: A,
    quota: Q,
    max_keys: NonZeroUsize,
    when_full: WhenFull,
    clock: Box<dyn Clock>,
    keys: PhantomData<fn() -> K>,
}

impl<K, A, Q> Builder<K, A, Q> {
    /// The algorithm to run. The default is [`TokenBucket`].
    pub fn algorithm<B: Algorithm>(self, algorithm: B) -> Builder<K, B, Q> {
        Builder {
            algorithm,
            quota: self.quota,
            max_keys: self.max_keys,
            when_full: self.when_full,
            clock: self.clock,
            keys: PhantomData,
        }
    }

    /// The quota every key is limited to. Required.
    pub fn quota(self, quota: Quota) -> Builder<K, A, Quota> {
        Builder {
            algorithm: self.algorithm,
            quota,
            max_keys: self.max_keys,
            when_full: self.when_full,
            clock: self.clock,
            keys: PhantomData,
        }
    }

    /// The most keys tracked at once. The default is 1,000,000. Keys back
    /// to their full allowance are forgotten to make room; what happens when
    /// none are is set by [`when_full`](Self::when_full).
    pub fn max_keys(mut self, max_keys: NonZeroUsize) -> Self {
        self.max_keys = max_keys;
        self
    }

    /// What to do with a new key when every tracked key is still active.
    /// The default, [`WhenFull::Refuse`], never lets a key over its limit.
    pub fn when_full(mut self, policy: WhenFull) -> Self {
        self.when_full = policy;
        self
    }

    /// Where time comes from. The default is a [`MonotonicClock`]; tests can
    /// pass a [`ManualClock`](crate::ManualClock) and move time by hand.
    pub fn clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }
}

impl<K: Hash + Eq, A: Algorithm> Builder<K, A, Quota> {
    /// Builds the limiter.
    #[must_use]
    pub fn build(self) -> Limiter<K, A> {
        let overflow_rates = match self.when_full {
            WhenFull::Overflow(overflow) => overflow.rates().len(),
            WhenFull::Refuse | WhenFull::EvictNearestIdle { .. } => 0,
        };
        let rates = self.quota.rates().len().max(overflow_rates);
        let store = match rates {
            0 | 1 => SizedStore::One(sized(self.max_keys, self.when_full)),
            2 => SizedStore::Two(sized(self.max_keys, self.when_full)),
            3 => SizedStore::Three(sized(self.max_keys, self.when_full)),
            _ => SizedStore::Four(sized(self.max_keys, self.when_full)),
        };
        Limiter {
            inner: Arc::new(Inner {
                quota: self.quota,
                algorithm: self.algorithm,
                clock: self.clock,
                store,
            }),
        }
    }
}

fn sized<K: Hash + Eq, A: Stored, const N: usize>(
    max_keys: NonZeroUsize,
    when_full: WhenFull,
) -> MemoryStore<K, A, N> {
    MemoryStore::new(max_keys).when_full(when_full)
}

impl<K, A: fmt::Debug, Q: fmt::Debug> fmt::Debug for Builder<K, A, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("algorithm", &self.algorithm)
            .field("quota", &self.quota)
            .field("max_keys", &self.max_keys)
            .field("when_full", &self.when_full)
            .finish_non_exhaustive()
    }
}
