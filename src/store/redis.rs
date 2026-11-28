//! The Redis store: every decision is one Lua script run atomically by
//! Redis, so any number of instances sharing a Redis enforce one limit.

pub(crate) mod breaker;
pub(crate) mod scripts;
#[cfg(test)]
pub(crate) mod tests;

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use ::redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use ::redis::cluster::ClusterClient;
use ::redis::cluster_async::ClusterConnection;
use ::redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use ::redis::{Cmd, ConnectionAddr, IntoConnectionInfo, Pipeline, RedisFuture, TlsMode, Value};

use self::breaker::{Breaker, CircuitBreaker};
use self::scripts::Scripted;
use super::{FailurePolicy, Policy, sealed};
use crate::algorithm::{Outcome, Retry, Spec, Stats, Unit};
use crate::limiter::{Decision, Error};
use crate::{Quota, Rate};

/// How long a call waits for Redis unless [`RedisStoreBuilder::timeout`]
/// says otherwise.
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(100);

/// How long connecting, at startup or after a failover, may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// A Redis server, Sentinel-managed primary or Cluster that limiters share,
/// so every instance of a service enforces one limit. Features
/// `redis-tokio` and `redis-smol`.
///
/// Each decision is one Lua script that Redis runs atomically, reading the
/// time from Redis itself, so instances can't both take the last unit and
/// their clocks don't matter. Keys expire on their own once they're back to
/// a full allowance.
///
/// `P` is what limiters answer when Redis fails or doesn't answer within
/// the [timeout](RedisStoreBuilder::timeout): [`FailOpen`](super::FailOpen),
/// [`FailClosed`](super::FailClosed) or [`Propagate`](super::Propagate).
/// After 3 failures in a row the store stops asking Redis for a while,
/// growing from 100 ms to 5 s, and answers by `P` at once; see
/// [`CircuitBreaker`].
///
/// Cloning is cheap; clones share the connection.
///
/// # Cost
///
/// Redis runs one script at a time. On one core it served about 80,000
/// decisions a second with a one-rate quota and 33,000 with four rates in
/// porin's benchmarks. A [`SlidingLog`](crate::SlidingLog) keeps a sorted
/// set entry per allowed hit, so its memory grows with the count, but a
/// call only looks up a few entries and removes at most 1,000 expired ones,
/// leaving the rest to later hits: with 100,000 entries in a key's window,
/// live or expired, it still takes microseconds.
///
/// # Eviction
///
/// Run Redis with `maxmemory-policy` set to `noeviction` or a `volatile-*`
/// policy, and memory to spare. Under an `allkeys-*` policy Redis can evict
/// a key that is still limited, which resets its limit.
///
/// # Examples
///
/// ```no_run
/// use porin::store::{FailClosed, RedisStore};
/// use porin::{Limiter, quota};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let redis = RedisStore::builder("redis://127.0.0.1/")
///     .on_failure(FailClosed)
///     .connect()
///     .await?;
/// let limiter: Limiter<u64, _, _> = Limiter::redis_builder(&redis, "api")
///     .quota(quota!("100/minute"))
///     .build()?;
/// if limiter.hit(&42).await?.is_allowed() {
///     // serve the request
/// }
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
pub struct RedisStore<P> {
    shared: Arc<Shared>,
    policy: PhantomData<P>,
}

struct Shared {
    connection: RwLock<Connection>,
    /// Where to connect again when the server stops being a primary.
    source: Source,
    breaker: Breaker,
    timeout: Duration,
}

enum Source {
    /// A server at a fixed address, which may be a name or virtual address
    /// that moves to the new primary on failover.
    Node(::redis::Client),
    /// The cluster client follows the primaries itself.
    Cluster,
    /// Ask the Sentinels for the primary.
    Sentinel(Sentinel),
}

struct Sentinel {
    urls: Vec<String>,
    primary: String,
    /// Credentials, database and TLS for the primary.
    node: Option<SentinelNodeConnectionInfo>,
}

#[derive(Clone)]
enum Connection {
    Node(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::Node(connection) => connection.req_packed_command(cmd),
            Self::Cluster(connection) => connection.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Self::Node(connection) => connection.req_packed_commands(cmd, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Node(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

/// What the store's builder still needs: a failure policy.
#[derive(Clone, Copy, Debug)]
pub struct NoPolicy;

/// What [`RedisStore::builder`] and [`RedisStore::cluster`] configure:
/// Redis reached at the URLs given.
pub struct Direct(Target);

enum Target {
    Node(String),
    Cluster(Vec<String>),
}

/// What [`RedisStore::sentinel`] configures: a primary found through
/// Sentinels, which [`RedisStoreBuilder::primary_url`] can describe.
pub struct ViaSentinel {
    urls: Vec<String>,
    primary: String,
    primary_url: Option<String>,
}

/// Configures a [`RedisStore`]. Start with [`RedisStore::builder`],
/// [`RedisStore::cluster`] or [`RedisStore::sentinel`]; a failure policy is
/// required before `connect`.
///
/// `T` is [`Direct`], or [`ViaSentinel`] for a Sentinel store, which alone
/// takes a [`primary_url`](Self::primary_url).
#[must_use = "a builder does nothing until `connect` is called"]
pub struct RedisStoreBuilder<P, T = Direct> {
    target: T,
    timeout: Duration,
    breaker: CircuitBreaker,
    policy: PhantomData<P>,
}

impl RedisStore<NoPolicy> {
    /// A single Redis server at `url`, such as `redis://127.0.0.1/` or
    /// `rediss://user:password@host:6380/0` (TLS needs one of the `redis`
    /// crate's TLS features).
    pub fn builder(url: impl Into<String>) -> RedisStoreBuilder<NoPolicy> {
        RedisStoreBuilder::new(Direct(Target::Node(url.into())))
    }

    /// A Redis Cluster, found through any of the nodes at `urls`. All the
    /// rates of one key live on one slot, so every script runs on one node.
    pub fn cluster<U: Into<String>>(
        urls: impl IntoIterator<Item = U>,
    ) -> RedisStoreBuilder<NoPolicy> {
        RedisStoreBuilder::new(Direct(Target::Cluster(
            urls.into_iter().map(Into::into).collect(),
        )))
    }

    /// The primary that the Sentinels at `urls` know as `primary`. After a
    /// failover the store asks the Sentinels again for the new primary.
    ///
    /// The URLs' credentials and TLS are for the Sentinels. The primary is
    /// reached with those of [`primary_url`](RedisStoreBuilder::primary_url),
    /// or with none. While the store looks for a new primary, the one call
    /// that triggered the search may wait up to 3 s.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use porin::store::{FailClosed, RedisStore};
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let sentinels = ["redis://10.0.0.1:26379/", "redis://10.0.0.2:26379/"];
    /// let redis = RedisStore::sentinel(sentinels, "mymaster")
    ///     .primary_url("redis://porin:secret@primary/2")
    ///     .on_failure(FailClosed)
    ///     .connect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sentinel<U: Into<String>>(
        urls: impl IntoIterator<Item = U>,
        primary: impl Into<String>,
    ) -> RedisStoreBuilder<NoPolicy, ViaSentinel> {
        RedisStoreBuilder::new(ViaSentinel {
            urls: urls.into_iter().map(Into::into).collect(),
            primary: primary.into(),
            primary_url: None,
        })
    }
}

impl<P, T> RedisStoreBuilder<P, T> {
    const fn new(target: T) -> Self {
        Self {
            target,
            timeout: DEFAULT_TIMEOUT,
            breaker: CircuitBreaker::new(),
            policy: PhantomData,
        }
    }

    /// What limiters answer when Redis fails or doesn't answer in time.
    /// Required, because each choice is wrong for some service.
    pub fn on_failure<F: FailurePolicy>(self, _policy: F) -> RedisStoreBuilder<F, T> {
        RedisStoreBuilder {
            target: self.target,
            timeout: self.timeout,
            breaker: self.breaker,
            policy: PhantomData,
        }
    }

    /// How long a decision waits for Redis before the failure policy
    /// answers. The default is 100 ms: a healthy Redis answers in well
    /// under a millisecond plus the network round trip, and one running
    /// flat out queued calls for up to about 50 ms in porin's benchmarks.
    ///
    /// For a Sentinel store, the one call that looks for a new primary
    /// after Redis fails may wait up to 3 s more.
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// When to stop asking a Redis that keeps failing, and for how long.
    /// The default is [`CircuitBreaker::new`].
    pub const fn circuit_breaker(mut self, breaker: CircuitBreaker) -> Self {
        self.breaker = breaker;
        self
    }
}

impl<P> RedisStoreBuilder<P, ViaSentinel> {
    /// Credentials, database and TLS for the primary, from a URL whose host
    /// and port are ignored, since the Sentinels name the primary:
    /// `redis://user:password@primary/2`, or `rediss://` for TLS (which
    /// needs one of the `redis` crate's TLS features; add `#insecure` to
    /// skip certificate checks).
    ///
    /// Only a Sentinel store has a primary to describe:
    ///
    /// ```compile_fail,E0599
    /// use porin::store::RedisStore;
    ///
    /// let builder = RedisStore::builder("redis://127.0.0.1/").primary_url("redis://primary/2");
    /// ```
    pub fn primary_url(mut self, url: impl Into<String>) -> Self {
        self.target.primary_url = Some(url.into());
        self
    }
}

impl<P: FailurePolicy> RedisStoreBuilder<P> {
    /// Connects, and checks that Redis takes porin's writes: it loads the
    /// scripts and, on a single server, records a hit on the key
    /// `porin:v1:{connect}:0`, which expires within a second. A Cluster
    /// loads the scripts on every primary, so all of them must answer.
    ///
    /// # Errors
    ///
    /// [`Error::Store`] if a URL is invalid, Redis can't be reached or
    /// checked within a second each, refuses the credentials or a command,
    /// or is a replica.
    pub async fn connect(self) -> Result<RedisStore<P>, Error> {
        let (connection, source) = match &self.target.0 {
            Target::Node(url) => {
                let client = ::redis::Client::open(url.as_str()).map_err(store_error)?;
                (node_client(client.clone()).await?, Source::Node(client))
            }
            Target::Cluster(urls) => (cluster(urls).await?, Source::Cluster),
        };
        Ok(RedisStore::new(
            connection,
            source,
            self.timeout,
            self.breaker,
        ))
    }
}

impl<P: FailurePolicy> RedisStoreBuilder<P, ViaSentinel> {
    /// Asks the Sentinels for the primary, connects to it, and checks that
    /// it answers.
    ///
    /// # Errors
    ///
    /// [`Error::Store`] if a URL is invalid, the Sentinels or the primary
    /// can't be reached within a second each, or the primary refuses the
    /// credentials or a command.
    pub async fn connect(self) -> Result<RedisStore<P>, Error> {
        let ViaSentinel {
            urls,
            primary,
            primary_url,
        } = self.target;
        let node = primary_url.as_deref().map(primary_node).transpose()?;
        let sentinel = Sentinel {
            urls,
            primary,
            node,
        };
        let connection = sentinel.primary().await?;
        Ok(RedisStore::new(
            connection,
            Source::Sentinel(sentinel),
            self.timeout,
            self.breaker,
        ))
    }
}

/// How to reach a Sentinel-managed primary, from `url`.
fn primary_node(url: &str) -> Result<SentinelNodeConnectionInfo, Error> {
    let info = url.into_connection_info().map_err(store_error)?;
    let node = SentinelNodeConnectionInfo::default()
        .set_redis_connection_info(info.redis_settings().clone());
    Ok(match tls_mode(info.addr())? {
        Some(mode) => node.set_tls_mode(mode),
        None => node,
    })
}

fn tls_mode(addr: &ConnectionAddr) -> Result<Option<TlsMode>, Error> {
    match addr {
        ConnectionAddr::Tcp(..) => Ok(None),
        ConnectionAddr::TcpTls {
            insecure: false, ..
        } => Ok(Some(TlsMode::Secure)),
        ConnectionAddr::TcpTls { insecure: true, .. } => Ok(Some(TlsMode::Insecure)),
        _ => Err(Error::Store(
            "a primary URL must start with redis:// or rediss://".into(),
        )),
    }
}

fn store_error(error: ::redis::RedisError) -> Error {
    Error::Store(Box::new(error))
}

fn timed_out() -> Error {
    Error::Store("Redis didn't answer in time".into())
}

async fn node_client(client: ::redis::Client) -> Result<Connection, Error> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(CONNECT_TIMEOUT))
        .set_number_of_retries(1);
    let connecting = ConnectionManager::new_with_config(client, config);
    let mut connection = match within(CONNECT_TIMEOUT, connecting).await {
        Some(connected) => Connection::Node(connected.map_err(store_error)?),
        None => return Err(timed_out()),
    };
    // Scripts load on a replica too, so try a write: a hit on a key of its
    // own that expires within a second. A replica answers READONLY.
    let probe = Quota::new(Rate::per_second(std::num::NonZeroU32::MAX));
    let keys = ["porin:v1:{connect}:0".to_owned()];
    let writing =
        run_script::<crate::TokenBucket, _>(&mut connection, &probe, &keys, true, 1, None);
    match within(CONNECT_TIMEOUT, writing).await {
        Some(written) => written.map_err(store_error)?,
        None => return Err(timed_out()),
    };
    verified(connection).await
}

/// `connection`, once Redis has loaded every script. That proves Redis
/// answers and accepts the credentials and the commands porin needs, so a
/// misconfigured store fails at connect instead of answering every call
/// by its failure policy.
async fn verified(mut connection: Connection) -> Result<Connection, Error> {
    let loading = async {
        for script in scripts::all() {
            script.load_async(&mut connection).await?;
        }
        Ok::<(), ::redis::RedisError>(())
    };
    match within(CONNECT_TIMEOUT, loading).await {
        Some(loaded) => loaded.map_err(store_error)?,
        None => return Err(timed_out()),
    }
    Ok(connection)
}

async fn cluster(urls: &[String]) -> Result<Connection, Error> {
    let client = ClusterClient::builder(urls.iter().map(String::as_str))
        .connection_timeout(CONNECT_TIMEOUT)
        .retries(1)
        .build()
        .map_err(store_error)?;
    let connection = match within(CONNECT_TIMEOUT, client.get_async_connection()).await {
        Some(connected) => Connection::Cluster(connected.map_err(store_error)?),
        None => return Err(timed_out()),
    };
    verified(connection).await
}

impl Sentinel {
    /// Asks the Sentinels for the current primary and connects to it.
    async fn primary(&self) -> Result<Connection, Error> {
        let mut sentinel = SentinelClient::build(
            self.urls.iter().map(String::as_str).collect(),
            self.primary.clone(),
            self.node.clone(),
            SentinelServerType::Master,
        )
        .map_err(store_error)?;
        let client = match within(CONNECT_TIMEOUT, sentinel.async_get_client()).await {
            Some(found) => found.map_err(store_error)?,
            None => return Err(timed_out()),
        };
        node_client(client).await
    }
}

/// Runs `future` for at most `limit`.
async fn within<T>(limit: Duration, future: impl Future<Output = T>) -> Option<T> {
    use std::pin::pin;
    use std::task::Poll;

    let mut future = pin!(future);
    let mut delay = pin!(futures_timer::Delay::new(limit));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Some(output));
        }
        delay.as_mut().poll(cx).map(|()| None)
    })
    .await
}

impl<P> RedisStore<P> {
    fn new(
        connection: Connection,
        source: Source,
        timeout: Duration,
        breaker: CircuitBreaker,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                connection: RwLock::new(connection),
                source,
                // A probe may reconnect (up to three connect steps: find
                // the primary, connect, check it) and then call.
                breaker: Breaker::new(
                    breaker,
                    timeout.saturating_add(CONNECT_TIMEOUT.saturating_mul(3)),
                ),
                timeout,
            }),
            policy: PhantomData,
        }
    }
}

impl<P: FailurePolicy> RedisStore<P> {
    /// Runs `A`'s script for the key whose rates live at `keys`, through
    /// the circuit breaker and the timeout, answering by `P` on failure.
    pub(crate) async fn decide<A: Scripted>(
        &self,
        quota: &Quota,
        keys: &[String],
        record: bool,
        cost: u32,
    ) -> Result<Decision, Error> {
        let shared = &*self.shared;
        if let Err(retry_after) = shared.breaker.admit() {
            return fail::<P>(retry_after, None);
        }
        if shared.breaker.should_reconnect() {
            self.reconnect().await;
        }
        let mut connection = shared.connection();
        let answer = within(
            shared.timeout,
            run_script::<A, _>(&mut connection, quota, keys, record, cost, None),
        )
        .await;
        match answer {
            Some(Ok(reply)) => {
                shared.breaker.succeeded();
                Ok(decision(reply.outcome()))
            }
            Some(Err(error)) => match shared.failure(&error) {
                Failure::Permanent => Err(store_error(error)),
                Failure::Outage(retry_after) => fail::<P>(retry_after, Some(store_error(error))),
            },
            None => fail::<P>(shared.breaker.failed(), Some(timed_out())),
        }
    }

    /// Deletes `keys`, through the breaker and the timeout. There is no
    /// decision to fall back on, so failures are errors whatever `P` is.
    pub(crate) async fn delete(&self, keys: &[String]) -> Result<(), Error> {
        let shared = &*self.shared;
        if shared.breaker.admit().is_err() {
            return Err(Error::Store("Redis is unavailable".into()));
        }
        let mut connection = shared.connection();
        let mut delete = ::redis::cmd("DEL");
        delete.arg(keys);
        let deleting = delete.query_async::<()>(&mut connection);
        match within(shared.timeout, deleting).await {
            Some(Ok(())) => {
                shared.breaker.succeeded();
                Ok(())
            }
            Some(Err(error)) => {
                shared.failure(&error);
                Err(store_error(error))
            }
            None => {
                shared.breaker.failed();
                Err(timed_out())
            }
        }
    }

    /// After a failover a Sentinel store's old primary is gone or a
    /// replica, so the store asks the Sentinels where the primary is now.
    async fn reconnect(&self) {
        let shared = &*self.shared;
        // If this call is dropped before it reconnects, the next one must.
        let mut pending = Reconnecting {
            breaker: &shared.breaker,
            done: false,
        };
        let connecting = match &shared.source {
            Source::Cluster => Err(Error::Store("the cluster client reconnects itself".into())),
            Source::Node(client) => node_client(client.clone()).await,
            Source::Sentinel(sentinel) => sentinel.primary().await,
        };
        pending.done = true;
        if let Ok(connection) = connecting {
            *shared
                .connection
                .write()
                .unwrap_or_else(PoisonError::into_inner) = connection;
        }
    }
}

/// Requests a reconnect again if dropped before it is done: a call
/// cancelled while reconnecting must not lose the request.
struct Reconnecting<'a> {
    breaker: &'a Breaker,
    done: bool,
}

impl Drop for Reconnecting<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.breaker.request_reconnect();
        }
    }
}

/// What a Redis error means for the call that got it.
enum Failure {
    /// Redis answered, and retrying won't help: wrong credentials, missing
    /// permissions, a key of the wrong type, a script error. Answering by
    /// [`FailOpen`](super::FailOpen) would admit everything until someone
    /// noticed, so these are errors under every policy.
    Permanent,
    /// Redis is down, busy, or moving: answer by the policy, and try again
    /// after this.
    Outage(Duration),
}

impl Shared {
    fn failure(&self, error: &::redis::RedisError) -> Failure {
        use ::redis::{ErrorKind, ServerErrorKind};

        let permanent = match error.kind() {
            ErrorKind::Server(
                ServerErrorKind::NoPerm | ServerErrorKind::CrossSlot | ServerErrorKind::ExecAbort,
            )
            | ErrorKind::AuthenticationFailed
            | ErrorKind::UnexpectedReturnType => true,
            // A plain ERR is permanent only when the script itself failed:
            // proxies also report upstream outages as ERR.
            ErrorKind::Server(ServerErrorKind::ResponseError) => {
                error.detail().is_some_and(|detail| {
                    detail.starts_with("user_script:") || detail.starts_with("Error running script")
                })
            }
            ErrorKind::Extension => {
                matches!(error.code(), Some("NOAUTH" | "WRONGPASS" | "WRONGTYPE"))
            }
            _ => false,
        };
        if permanent {
            // Redis is reachable; the breaker is for outages.
            self.breaker.succeeded();
            return Failure::Permanent;
        }
        if matches!(error.kind(), ErrorKind::Server(ServerErrorKind::ReadOnly)) {
            // The node is a replica now, as after a failover that kept the
            // old primary's connections open. Connect again without waiting
            // for the breaker to open.
            self.breaker.request_reconnect();
        }
        Failure::Outage(self.breaker.failed())
    }

    fn connection(&self) -> Connection {
        self.connection
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The policy's answer to a failure. `error` is `None` when the breaker
/// answered without asking Redis.
fn fail<P: FailurePolicy>(retry_after: Duration, error: Option<Error>) -> Result<Decision, Error> {
    match P::POLICY {
        Policy::Open => Ok(Decision::allowed_unconfirmed()),
        Policy::Closed => Ok(Decision::denied_unavailable(retry_after)),
        Policy::Propagate => {
            Err(error.unwrap_or_else(|| Error::Store("Redis is unavailable".into())))
        }
    }
}

fn decision(outcome: Outcome) -> Decision {
    Decision::from_hit(match outcome {
        Outcome::Allowed(stats) => super::Hit::Allowed(stats),
        Outcome::Denied { stats, retry } => super::Hit::Denied { stats, retry },
    })
}

impl<P> Clone for RedisStore<P> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            policy: PhantomData,
        }
    }
}

impl<P> fmt::Debug for RedisStore<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match &*self
            .shared
            .connection
            .read()
            .unwrap_or_else(PoisonError::into_inner)
        {
            Connection::Node(_) if matches!(self.shared.source, Source::Sentinel(_)) => "sentinel",
            Connection::Node(_) => "node",
            Connection::Cluster(_) => "cluster",
        };
        f.debug_struct("RedisStore")
            .field("mode", &mode)
            .field("timeout", &self.shared.timeout)
            .field("policy", &std::any::type_name::<P>())
            .finish_non_exhaustive()
    }
}

// URLs can hold passwords, so no `Debug` here shows one.
impl<P, T: fmt::Debug> fmt::Debug for RedisStoreBuilder<P, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisStoreBuilder")
            .field("target", &self.target)
            .field("timeout", &self.timeout)
            .field("circuit_breaker", &self.breaker)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Direct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self.0 {
            Target::Node(_) => "node",
            Target::Cluster(_) => "cluster",
        };
        f.debug_tuple("Direct").field(&mode).finish()
    }
}

impl fmt::Debug for ViaSentinel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViaSentinel")
            .field("primary", &self.primary)
            .finish_non_exhaustive()
    }
}

impl<P: FailurePolicy> sealed::Store for RedisStore<P> {
    type Inner<K, A: crate::Algorithm> = crate::limiter::redis::RedisInner<K, A, P>;
    type Config = crate::limiter::redis::RedisConfig<P>;
}

impl<P: FailurePolicy> super::Store for RedisStore<P> {}

/// What a script answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Reply {
    allowed: bool,
    remaining: u32,
    reset_after_us: u64,
    /// Microseconds; -1 for never.
    retry_after_us: i64,
    limiting: u8,
    limiting_reset_us: u64,
}

impl Reply {
    fn from_redis(values: &[i64]) -> Option<Self> {
        let [allowed, remaining, reset, retry, limiting, limiting_reset] = *values else {
            return None;
        };
        Some(Self {
            allowed: allowed == 1,
            remaining: u32::try_from(remaining).ok()?,
            reset_after_us: u64::try_from(reset).ok()?,
            retry_after_us: retry,
            limiting: u8::try_from(limiting).ok()?,
            limiting_reset_us: u64::try_from(limiting_reset).ok()?,
        })
    }

    pub(crate) fn outcome(self) -> Outcome {
        let stats = Stats {
            remaining: self.remaining,
            reset_after: Duration::from_micros(self.reset_after_us),
            limiting: self.limiting,
            limiting_reset: Duration::from_micros(self.limiting_reset_us),
        };
        if self.allowed {
            return Outcome::Allowed(stats);
        }
        let retry = u64::try_from(self.retry_after_us).map_or(Retry::Never, |micros| {
            Retry::After(Duration::from_micros(micros))
        });
        Outcome::Denied { stats, retry }
    }
}

/// Runs `A`'s script for one key whose rates live at `keys` (one per rate
/// of `quota`, in order). `record` applies an allowed hit; otherwise the
/// script only checks. `now_us` replaces Redis `TIME`, for tests only.
pub(crate) async fn run_script<A, C>(
    connection: &mut C,
    quota: &Quota,
    keys: &[String],
    record: bool,
    cost: u32,
    now_us: Option<u64>,
) -> ::redis::RedisResult<Reply>
where
    A: Scripted,
    C: ConnectionLike + Send,
{
    let script = A::script();
    let mut invocation = script.prepare_invoke();
    for key in keys {
        invocation.key(key);
    }
    invocation
        .arg(if record { "hit" } else { "test" })
        .arg(cost)
        .arg(now_us.map_or_else(String::new, |now| now.to_string()));
    for rate in quota.rates() {
        let spec = Spec::new(*rate, Unit::Micros);
        invocation.arg(spec.period).arg(spec.count).arg(spec.burst);
    }
    let values: Vec<i64> = invocation.invoke_async(connection).await?;
    Reply::from_redis(&values).ok_or_else(|| {
        ::redis::RedisError::from((
            ::redis::ErrorKind::UnexpectedReturnType,
            "a porin script returned an unexpected reply",
        ))
    })
}

/// The longest key [encoding](crate::KeyEncode) a Redis limiter accepts.
pub(crate) const MAX_KEY_BYTES: usize = 512;

/// The Redis keys of one limiter key's rates:
/// `porin:v1:{<limiter id>:<key>}:<algorithm>:<quota>:<rate index>`. Ids and
/// keys are in hex, and `<quota>` is a [fingerprint](fingerprint) of the
/// rates, so a changed quota or algorithm never reads old state. The braces
/// are a Cluster hash tag: all of a key's rates share a slot.
pub(crate) fn key_names(prefix: &str, key: &[u8], suffix: &str, rates: usize) -> Vec<String> {
    let mut base = String::with_capacity(
        prefix
            .len()
            .saturating_add(key.len().saturating_mul(2))
            .saturating_add(suffix.len()),
    );
    base.push_str(prefix);
    push_hex(&mut base, key);
    base.push_str(suffix);
    (0..rates).map(|rate| format!("{base}{rate}")).collect()
}

/// The start of every key of the limiter `id`, up to its key bytes.
pub(crate) fn prefix(id: &str) -> String {
    let mut prefix = String::from("porin:v1:{");
    push_hex(&mut prefix, id.as_bytes());
    prefix.push(':');
    prefix
}

/// What follows the key bytes, up to the rate index.
pub(crate) fn suffix<A: Scripted>(quota: &Quota) -> String {
    format!("}}:{}:{:08x}:", A::NAME, fingerprint(quota))
}

/// A stable 32-bit FNV-1a hash of the quota's rates as the scripts see
/// them. It only has to tell one limiter id's quotas apart over time.
pub(crate) fn fingerprint(quota: &Quota) -> u32 {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut hash = OFFSET;
    for rate in quota.rates() {
        let spec = Spec::new(*rate, Unit::Micros);
        let bytes = spec
            .period
            .to_be_bytes()
            .into_iter()
            .chain(spec.count.to_be_bytes())
            .chain(spec.burst.to_be_bytes());
        for byte in bytes {
            hash = (hash ^ u32::from(byte)).wrapping_mul(PRIME);
        }
    }
    hash
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        for nibble in [byte >> 4, byte & 0x0f] {
            // A nibble is below 16, so it is always a hex digit.
            out.push(char::from_digit(u32::from(nibble), 16).unwrap_or('0'));
        }
    }
}

/// The longest full refill a token bucket may have in Redis: past it, the
/// script's arithmetic would leave the integers doubles hold exactly.
pub(crate) const MAX_TOKEN_BUCKET_REFILL_US: u64 = 1 << 51;

/// Whether a token bucket rate's full refill fits [`MAX_TOKEN_BUCKET_REFILL_US`].
pub(crate) fn token_bucket_fits(rate: Rate) -> bool {
    let spec = Spec::new(rate, Unit::Micros);
    let interval = spec.period.div_ceil(u64::from(spec.count)).max(1);
    // Both factors fit in 64 bits, so the product fits in 128.
    u128::from(interval).saturating_mul(u128::from(spec.burst))
        < u128::from(MAX_TOKEN_BUCKET_REFILL_US)
}
