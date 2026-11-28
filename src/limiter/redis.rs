//! A [`Limiter`] whose state lives in Redis.

use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use super::{Builder, Decision, Error, Limiter, NoQuota, Stats};
use crate::key::KeyEncode;
use crate::quota::{QuotaError, QuotaErrorKind};
use crate::store::FailurePolicy;
use crate::store::redis::scripts::Scripted;
use crate::store::redis::{MAX_KEY_BYTES, RedisStore, key_names, prefix, suffix};
use crate::{Algorithm, Quota, TokenBucket};

/// What a Redis limiter holds.
pub struct RedisInner<K, A: Algorithm, P> {
    quota: Quota,
    algorithm: A,
    store: RedisStore<P>,
    /// Every Redis key of this limiter starts with this...
    prefix: String,
    /// ...and ends with this, then the rate index.
    suffix: String,
    keys: PhantomData<fn(&K)>,
}

/// What a builder holds for a Redis store.
pub struct RedisConfig<P> {
    store: RedisStore<P>,
    prefix: String,
}

impl<K: KeyEncode> Limiter<K> {
    /// Starts configuring a limiter that keeps its state in `store`, under
    /// the name `id`. Only the quota is required.
    ///
    /// Limiters sharing a Redis need different ids, or their keys would
    /// share limits. Keys are also told apart by the algorithm and the
    /// quota, so changing either starts every key fresh.
    ///
    /// Redis decides by its own clock, so this builder has no clock or
    /// key-count settings: Redis forgets idle keys by itself.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use porin::store::{FailClosed, RedisStore};
    /// use porin::{Limiter, SlidingWindow, quota};
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis = RedisStore::builder("redis://127.0.0.1/")
    ///     .on_failure(FailClosed)
    ///     .connect()
    ///     .await?;
    /// let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "search")
    ///     .algorithm(SlidingWindow)
    ///     .quota(quota!("100/minute"))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn redis_builder<P: FailurePolicy>(
        store: &RedisStore<P>,
        id: &str,
    ) -> Builder<K, TokenBucket, NoQuota, RedisStore<P>> {
        Builder {
            algorithm: TokenBucket,
            quota: NoQuota,
            config: RedisConfig {
                store: store.clone(),
                prefix: prefix(id),
            },
            keys: PhantomData,
        }
    }
}

impl<K: KeyEncode, A: Algorithm, P: FailurePolicy> Builder<K, A, Quota, RedisStore<P>> {
    /// Builds the limiter.
    ///
    /// # Errors
    ///
    /// [`QuotaErrorKind::RefillTooLong`] if `A` is a token bucket that
    /// takes longer than about 71 years to refill completely, past what the
    /// Redis scripts count exactly.
    pub fn build(self) -> Result<Limiter<K, A, RedisStore<P>>, QuotaError> {
        if !self
            .quota
            .rates()
            .iter()
            .all(|rate| <A as Scripted>::fits(*rate))
        {
            return Err(QuotaError::new(QuotaErrorKind::RefillTooLong, None));
        }
        let RedisConfig { store, prefix } = self.config;
        Ok(Limiter {
            inner: Arc::new(RedisInner {
                suffix: suffix::<A>(&self.quota),
                quota: self.quota,
                algorithm: self.algorithm,
                store,
                prefix,
                keys: PhantomData,
            }),
        })
    }
}

impl<K: KeyEncode, A: Algorithm, P: FailurePolicy> Limiter<K, A, RedisStore<P>> {
    /// Spends one unit for `key`, if its quota allows.
    ///
    /// `Q` must encode like `K`, as `str` does for `String`.
    ///
    /// # Errors
    ///
    /// [`Error::Store`] if Redis fails or doesn't answer in time, under the
    /// [`Propagate`](crate::store::Propagate) policy only; or, under every
    /// policy, if Redis answers with an error that retrying won't fix, such
    /// as refused credentials or permissions, which a failure policy would
    /// otherwise hide.
    /// [`Error::KeyTooLong`] if the key encodes to more than 512 bytes,
    /// whatever the policy.
    pub async fn hit<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.hit_n(key, 1).await
    }

    /// Spends `cost` units for `key`, if its quota allows them all. A
    /// `cost` of 0 records nothing. See [`hit`](Self::hit).
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn hit_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.decide(key, true, cost).await
    }

    /// Whether one unit for `key` would be allowed now, without spending it.
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn test<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.test_n(key, 1).await
    }

    /// Whether `cost` units for `key` would be allowed now, without
    /// spending them.
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn test_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.decide(key, false, cost).await
    }

    /// What `key` has left, without spending anything. Not
    /// [known](Stats::is_known) if Redis didn't answer and the policy is
    /// [`FailOpen`](crate::store::FailOpen) or
    /// [`FailClosed`](crate::store::FailClosed).
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn stats<Q>(&self, key: &Q) -> Result<Stats, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        Ok(self.decide(key, false, 0).await?.stats())
    }

    /// Forgets `key`, giving it its full allowance back.
    ///
    /// # Errors
    ///
    /// [`Error::Store`] if Redis fails or doesn't answer in time, whatever
    /// the failure policy. [`Error::KeyTooLong`] as for [`hit`](Self::hit).
    pub async fn reset<Q>(&self, key: &Q) -> Result<(), Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.inner.store.delete(&self.keys(key)?).await
    }

    /// Spends one unit for `key`, waiting until the quota allows it. See
    /// [`Limiter::until_ready`](Limiter#method.until_ready) on the memory
    /// store.
    ///
    /// While Redis is unavailable, [`FailClosed`](crate::store::FailClosed)
    /// waits until the store is tried again.
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn until_ready<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        self.until_ready_n(key, 1).await
    }

    /// Spends `cost` units for `key`, waiting until the quota allows them
    /// all. Returns a denial without waiting if `cost` can never fit.
    ///
    /// # Errors
    ///
    /// As [`hit`](Self::hit).
    pub async fn until_ready_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: KeyEncode + ?Sized,
    {
        let keys = self.keys(key)?;
        loop {
            let decision = self
                .inner
                .store
                .decide::<A>(&self.inner.quota, &keys, true, cost)
                .await?;
            match decision {
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

    async fn decide<Q>(&self, key: &Q, record: bool, cost: u32) -> Result<Decision, Error>
    where
        Q: KeyEncode + ?Sized,
    {
        let keys = self.keys(key)?;
        self.inner
            .store
            .decide::<A>(&self.inner.quota, &keys, record, cost)
            .await
    }

    fn keys<Q: KeyEncode + ?Sized>(&self, key: &Q) -> Result<Vec<String>, Error> {
        let inner = &*self.inner;
        let bytes = key.key_bytes();
        if bytes.len() > MAX_KEY_BYTES {
            return Err(Error::KeyTooLong);
        }
        Ok(key_names(
            &inner.prefix,
            &bytes,
            &inner.suffix,
            inner.quota.rates().len(),
        ))
    }
}

impl<K, A: Algorithm, P> fmt::Debug for RedisInner<K, A, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisInner")
            .field("algorithm", &self.algorithm)
            .field("quota", &self.quota)
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl<K, A: Algorithm, P: FailurePolicy> fmt::Debug for Limiter<K, A, RedisStore<P>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = &*self.inner;
        f.debug_struct("Limiter")
            .field("algorithm", &inner.algorithm)
            .field("quota", &inner.quota)
            .field("store", &inner.store)
            .finish_non_exhaustive()
    }
}

impl<P> fmt::Debug for RedisConfig<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Redis")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}
