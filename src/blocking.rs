//! A [`Limiter`] for synchronous code: threads, CLI tools, anything without
//! an async runtime.
//!
//! Each method blocks the calling thread until it has an answer. With the
//! memory store that is immediate, except in [`Limiter::until_ready`], which
//! sleeps until the quota allows the hit.
//!
//! # Examples
//!
//! ```
//! use porin::blocking::Limiter;
//! use porin::quota;
//!
//! let limiter = Limiter::memory(quota!("2/second"));
//! assert!(limiter.hit(&7_u64)?.is_allowed());
//! assert!(limiter.hit(&7_u64)?.is_allowed());
//! assert!(!limiter.hit(&7_u64)?.is_allowed());
//! # Ok::<(), porin::Error>(())
//! ```

use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use crate::{Algorithm, Decision, Error, Quota, Stats, TokenBucket};

/// The blocking face of [`crate::Limiter`]: the same limiter, with methods
/// that block instead of returning futures.
///
/// Converts to and from the async limiter for free, and both share state,
/// so sync and async code can limit the same keys.
pub struct Limiter<K, A: Algorithm = TokenBucket> {
    inner: crate::Limiter<K, A>,
}

impl<K: Hash + Eq> Limiter<K> {
    /// A token bucket limiter that keeps its state in memory, with the
    /// default settings. For other settings, configure a
    /// [`crate::Limiter::builder`] and convert the result with `into()`.
    #[must_use]
    pub fn memory(quota: Quota) -> Self {
        crate::Limiter::memory(quota).into()
    }
}

impl<K: Hash + Eq, A: Algorithm> Limiter<K, A> {
    /// Spends one unit for `key`, if its quota allows. See
    /// [`crate::Limiter::hit`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn hit<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        block_on(self.inner.hit(key))
    }

    /// Spends `cost` units for `key` at once, if its quota allows all of
    /// them. See [`crate::Limiter::hit_n`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn hit_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        block_on(self.inner.hit_n(key, cost))
    }

    /// Whether one unit for `key` would be allowed now, without spending it.
    /// See [`crate::Limiter::test`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn test<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        block_on(self.inner.test(key))
    }

    /// Whether `cost` units for `key` would be allowed now, without
    /// spending them. See [`crate::Limiter::test_n`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn test_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        block_on(self.inner.test_n(key, cost))
    }

    /// What `key` has left, without spending anything.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn stats<Q>(&self, key: &Q) -> Result<Stats, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        block_on(self.inner.stats(key))
    }

    /// Forgets `key`, giving it its full allowance back.
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn reset<Q>(&self, key: &Q) -> Result<(), Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        block_on(self.inner.reset(key))
    }

    /// Spends one unit for `key`, sleeping the thread until the quota allows
    /// it. See [`crate::Limiter::until_ready`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn until_ready<Q>(&self, key: &Q) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.until_ready_n(key, 1)
    }

    /// Spends `cost` units for `key`, sleeping the thread until the quota
    /// allows them all, or returning a denial at once if they never can.
    /// See [`crate::Limiter::until_ready_n`].
    ///
    /// # Errors
    ///
    /// Only if the store fails, which the memory store never does.
    pub fn until_ready_n<Q>(&self, key: &Q, cost: u32) -> Result<Decision, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        loop {
            match self.hit_n(key, cost)? {
                Decision::Denied(denial) => match denial.retry_after() {
                    Some(wait) => thread::sleep(wait),
                    None => return Ok(Decision::Denied(denial)),
                },
                allowed @ Decision::Allowed(_) => return Ok(allowed),
            }
        }
    }

    /// The quota every key is limited to.
    #[must_use]
    pub fn quota(&self) -> &Quota {
        self.inner.quota()
    }

    /// How many hits so far came from keys the store had no room to track.
    /// See [`crate::Limiter::full_hits`].
    #[must_use]
    pub fn full_hits(&self) -> u64 {
        self.inner.full_hits()
    }

    /// The async limiter underneath. It shares this limiter's state.
    #[must_use]
    pub fn as_async(&self) -> &crate::Limiter<K, A> {
        &self.inner
    }
}

impl<K, A: Algorithm> From<crate::Limiter<K, A>> for Limiter<K, A> {
    fn from(inner: crate::Limiter<K, A>) -> Self {
        Self { inner }
    }
}

impl<K, A: Algorithm> From<Limiter<K, A>> for crate::Limiter<K, A> {
    fn from(limiter: Limiter<K, A>) -> Self {
        limiter.inner
    }
}

impl<K, A: Algorithm> Clone for Limiter<K, A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, A: Algorithm> fmt::Debug for Limiter<K, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("blocking::Limiter")
            .field(&self.inner)
            .finish()
    }
}

/// Runs `future` on the calling thread, parking it while the future waits.
/// The memory store's futures finish on the first poll; this also serves
/// stores that wait on IO driven by another thread.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            // A wake that arrives before `park` makes `park` return at once,
            // so none is lost. Spurious returns just poll again.
            Poll::Pending => thread::park(),
        }
    }
}

struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
