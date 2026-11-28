//! A tower [`Layer`](tower_layer::Layer) that rate limits HTTP requests, for
//! axum, tonic, hyper and anything else built on tower. Feature `tower`.
//!
//! Each request is keyed by a [`KeyExtractor`], usually [`PeerIp`] or
//! [`Header`], and spends one unit of that key's quota. An allowed request
//! goes on to the service, and its response gets rate limit headers. The
//! layer answers the rest itself:
//!
//! | Situation | Answer |
//! |---|---|
//! | Over the limit | 429, with `Retry-After` |
//! | The store is full of other keys ([`WhenFull`](crate::WhenFull)) | 429, or 503 with [`FullStore::ServiceUnavailable`] |
//! | A remote store didn't answer ([`FailClosed`](crate::store::FailClosed)) or failed ([`Propagate`](crate::store::Propagate)) | 503, with `Retry-After` |
//! | No key (no peer address, missing header) | 400, or a [fallback key](RateLimitLayer::fallback_key) |
//! | A key too long for a remote store | 400 |
//!
//! Headers follow the IETF draft by default, one policy per rate of the
//! quota and the state of the one closest to running out:
//!
//! ```text
//! RateLimit-Policy: "1s";q=10;w=1, "1d";q=1000;w=86400
//! RateLimit: "1s";r=7;t=1
//! ```
//!
//! [`Headers::Legacy`] sends `X-RateLimit-Limit`, `X-RateLimit-Remaining`
//! and `X-RateLimit-Reset` (seconds from now) instead, and [`Headers::Both`]
//! sends both. Times round up to whole seconds. A token bucket's policy is
//! its burst and the time to refill it, since that is what it allows at
//! once.
//!
//! For gRPC services, [`Protocol::Grpc`] answers with gRPC statuses
//! instead: `RESOURCE_EXHAUSTED` for a limited request, `UNAVAILABLE` when
//! the store can't decide. Plain HTTP statuses would reach gRPC clients as
//! `UNAVAILABLE`, which many retry at once.
//!
//! # Examples
//!
//! ```
//! use porin::tower::{PeerIp, RateLimitLayer};
//! use porin::{Limiter, quota};
//!
//! let limiter: Limiter<std::net::IpAddr> = Limiter::memory(quota!("10/second"));
//! let layer = RateLimitLayer::new(limiter, PeerIp::new());
//! // axum: Router::new().route(..).layer(layer), served with
//! // `into_make_service_with_connect_info::<SocketAddr>()` so the peer
//! // address is known.
//! # drop(layer);
//! ```

use std::borrow::Borrow;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::header::{HeaderName, HeaderValue, RETRY_AFTER};
use http::{Extensions, HeaderMap, Method, Request, Response, StatusCode, Uri};

use crate::store::{Memory, Store};
use crate::{Algorithm, Decision, Denial, Error, Limiter, Quota, Stats};

/// A limiter the layer can use: a [`Limiter`] on any store, keyed by `K`.
/// Sealed.
pub trait Limits<K>: sealed::Sealed + Clone + Send + Sync + 'static {
    #[doc(hidden)]
    fn hit_key(&self, key: &K) -> impl Future<Output = Result<Decision, Error>> + Send;
    #[doc(hidden)]
    fn limits(&self) -> &Quota;
    #[doc(hidden)]
    fn bursts(&self) -> bool;
}

mod sealed {
    pub trait Sealed {}
}

impl<K, A: Algorithm, S: Store> sealed::Sealed for Limiter<K, A, S> {}

impl<K, A> Limits<K> for Limiter<K, A, Memory>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    A: Algorithm,
{
    fn hit_key(&self, key: &K) -> impl Future<Output = Result<Decision, Error>> + Send {
        self.hit(key)
    }

    fn limits(&self) -> &Quota {
        self.quota()
    }

    fn bursts(&self) -> bool {
        <A as crate::algorithm::Algorithm>::BURSTS
    }
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl<K, A, P> Limits<K> for Limiter<K, A, crate::store::RedisStore<P>>
where
    K: crate::KeyEncode + Send + Sync + 'static,
    A: Algorithm,
    P: crate::store::FailurePolicy,
{
    fn hit_key(&self, key: &K) -> impl Future<Output = Result<Decision, Error>> + Send {
        self.hit(key)
    }

    fn limits(&self) -> &Quota {
        self.quota()
    }

    fn bursts(&self) -> bool {
        <A as crate::algorithm::Algorithm>::BURSTS
    }
}

/// What a request is limited by: the key it spends from.
pub trait KeyExtractor: Clone + Send + Sync + 'static {
    /// The key type; the limiter must be keyed by it.
    type Key: Send + Sync + 'static;

    /// The request's key, or `None` if it has none.
    fn extract(&self, request: Head<'_>) -> Option<Self::Key>;
}

/// The parts of a request a [`KeyExtractor`] reads.
#[derive(Clone, Copy, Debug)]
pub struct Head<'a> {
    method: &'a Method,
    uri: &'a Uri,
    headers: &'a HeaderMap,
    extensions: &'a Extensions,
}

impl<'a> Head<'a> {
    /// The parts of `request`, as the layer passes them to its extractor;
    /// for testing a [`KeyExtractor`].
    #[must_use]
    pub fn new<B>(request: &'a Request<B>) -> Self {
        Self {
            method: request.method(),
            uri: request.uri(),
            headers: request.headers(),
            extensions: request.extensions(),
        }
    }

    /// The request method.
    #[must_use]
    pub const fn method(&self) -> &'a Method {
        self.method
    }

    /// The request URI.
    #[must_use]
    pub const fn uri(&self) -> &'a Uri {
        self.uri
    }

    /// The request headers.
    #[must_use]
    pub const fn headers(&self) -> &'a HeaderMap {
        self.headers
    }

    /// The request extensions, where servers put the peer address.
    #[must_use]
    pub const fn extensions(&self) -> &'a Extensions {
        self.extensions
    }
}

/// Keys requests by client IP address. IPv6 addresses are grouped by /64,
/// since one client usually holds a whole /64 and could otherwise rotate
/// through it.
///
/// By default the IP is the TCP peer's, read from the request extensions:
/// a [`SocketAddr`]; with feature `axum_0_8`, axum's
/// `ConnectInfo<SocketAddr>` (or `MockConnectInfo`, as in axum's tests);
/// with feature `tonic_0_14`, tonic's `TcpConnectInfo`. A server that puts
/// none of these there leaves every request without a key.
///
/// Behind proxies or a load balancer, the peer is the last proxy;
/// [`trusting_proxies`](Self::trusting_proxies) reads the client from the
/// header the proxies write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerIp {
    trusted_proxies: u8,
    header: ProxyHeader,
}

/// The header trusted proxies record the client's address in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProxyHeader {
    /// `X-Forwarded-For`, which most proxies and cloud load balancers
    /// append to.
    #[default]
    XForwardedFor,
    /// `Forwarded` (RFC 7239), its `for=` addresses.
    Forwarded,
}

impl PeerIp {
    /// Keys by the TCP peer's address.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            trusted_proxies: 0,
            header: ProxyHeader::XForwardedFor,
        }
    }

    /// Keys by the address `proxies` hops back in `header`, where each
    /// proxy appends the address it got the request from. With one load
    /// balancer in front, `proxies` is 1: the client is the last address
    /// listed. A request that lists fewer addresses, or an unreadable one
    /// at that hop, has no key. The other forwarding header is ignored, so
    /// a client can't send its own.
    ///
    /// Only for a service every request reaches through those proxies: a
    /// client that can reach it directly can write any address there.
    #[must_use]
    pub const fn trusting_proxies(self, proxies: std::num::NonZeroU8, header: ProxyHeader) -> Self {
        Self {
            trusted_proxies: proxies.get(),
            header,
        }
    }
}

impl KeyExtractor for PeerIp {
    type Key = IpAddr;

    fn extract(&self, request: Head<'_>) -> Option<IpAddr> {
        let ip = match self.trusted_proxies {
            0 => peer_address(request.extensions)?.ip(),
            hops => forwarded_client(request.headers, self.header, usize::from(hops))?,
        };
        Some(grouped(ip))
    }
}

fn peer_address(extensions: &Extensions) -> Option<SocketAddr> {
    #[cfg(feature = "axum_0_8")]
    {
        use axum::extract::ConnectInfo;
        use axum::extract::connect_info::MockConnectInfo;

        // As axum's own `ConnectInfo` extractor reads it, test mocks too.
        if let Some(ConnectInfo(address)) = extensions.get::<ConnectInfo<SocketAddr>>() {
            return Some(*address);
        }
        if let Some(MockConnectInfo(address)) = extensions.get::<MockConnectInfo<SocketAddr>>() {
            return Some(*address);
        }
    }
    #[cfg(feature = "tonic_0_14")]
    if let Some(info) = extensions.get::<tonic::transport::server::TcpConnectInfo>() {
        if let Some(address) = info.remote_addr() {
            return Some(address);
        }
    }
    extensions.get::<SocketAddr>().copied()
}

/// IPv4-mapped IPv6 addresses as IPv4, and other IPv6 addresses cut to
/// their /64.
fn grouped(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => {
            let prefix = u128::from(v6) & !u128::from(u64::MAX);
            IpAddr::V6(Ipv6Addr::from(prefix))
        }
        v4 @ IpAddr::V4(_) => v4,
    }
}

/// The address `hops` back from the end of the forwarding chain in
/// `header`, across all its lines. It walks from the end and stops at that
/// hop, so a huge header costs no more than a short one, and an element
/// it can't read counts as one address, not a line to skip.
fn forwarded_client(headers: &HeaderMap, header: ProxyHeader, hops: usize) -> Option<IpAddr> {
    let name = match header {
        ProxyHeader::XForwardedFor => HeaderName::from_static("x-forwarded-for"),
        ProxyHeader::Forwarded => http::header::FORWARDED,
    };
    let mut seen = 0_usize;
    for line in headers.get_all(name).iter().rev() {
        for element in line.as_bytes().rsplit(|byte| *byte == b',') {
            seen = seen.saturating_add(1);
            if seen == hops {
                let text = std::str::from_utf8(element).ok()?;
                return match header {
                    ProxyHeader::XForwardedFor => address(text),
                    ProxyHeader::Forwarded => forwarded_for(text),
                };
            }
        }
    }
    None
}

/// The `for=` address of one `Forwarded` element.
fn forwarded_for(element: &str) -> Option<IpAddr> {
    element.split(';').find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        name.eq_ignore_ascii_case("for")
            .then(|| address(value.trim().trim_matches('"')))
            .flatten()
    })
}

/// An address as forwarding headers write it: bare, with a port, or
/// bracketed IPv6 with or without one.
fn address(text: &str) -> Option<IpAddr> {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix('[') {
        return rest.split_once(']')?.0.parse().ok();
    }
    text.parse()
        .ok()
        .or_else(|| text.parse::<SocketAddr>().ok().map(|socket| socket.ip()))
}

/// The longest header value [`Header`] keys by, as remote stores cap keys.
const MAX_HEADER_KEY: usize = 512;

/// Keys requests by the bytes of one header, such as an API key.
///
/// A request has no key if the header is missing, appears more than once,
/// or is longer than 512 bytes. Clients choose this value, so key by a
/// header an earlier layer has authenticated, or each client can spread
/// its requests over keys of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    name: HeaderName,
}

impl Header {
    /// Keys by the header `name`.
    #[must_use]
    pub const fn new(name: HeaderName) -> Self {
        Self { name }
    }
}

impl KeyExtractor for Header {
    type Key = Vec<u8>;

    fn extract(&self, request: Head<'_>) -> Option<Vec<u8>> {
        let mut values = request.headers.get_all(&self.name).iter();
        let value = values.next()?;
        if values.next().is_some() || value.len() > MAX_HEADER_KEY {
            return None;
        }
        Some(value.as_bytes().to_vec())
    }
}

/// Keys requests with a function of the request. Made by [`from_fn`].
pub struct FromFn<F, K> {
    function: F,
    key: PhantomData<fn() -> K>,
}

/// A [`KeyExtractor`] from a function, for keys the provided extractors
/// don't cover: a tenant from the path, tonic's `TcpConnectInfo`, a user
/// id an earlier layer put in the extensions.
///
/// ```
/// use porin::tower::{RateLimitLayer, from_fn};
/// use porin::{Limiter, quota};
///
/// let limiter: Limiter<String> = Limiter::memory(quota!("5/second"));
/// // Limit each first path segment, such as a tenant.
/// let layer = RateLimitLayer::new(
///     limiter,
///     from_fn(|request| {
///         let tenant = request.uri().path().trim_start_matches('/').split('/').next()?;
///         (!tenant.is_empty()).then(|| tenant.to_owned())
///     }),
/// );
/// # drop(layer);
/// ```
pub fn from_fn<F, K>(function: F) -> FromFn<F, K>
where
    F: Fn(Head<'_>) -> Option<K> + Clone + Send + Sync + 'static,
    K: Send + Sync + 'static,
{
    FromFn {
        function,
        key: PhantomData,
    }
}

impl<F, K> KeyExtractor for FromFn<F, K>
where
    F: Fn(Head<'_>) -> Option<K> + Clone + Send + Sync + 'static,
    K: Send + Sync + 'static,
{
    type Key = K;

    fn extract(&self, request: Head<'_>) -> Option<K> {
        (self.function)(request)
    }
}

impl<F: Clone, K> Clone for FromFn<F, K> {
    fn clone(&self) -> Self {
        Self {
            function: self.function.clone(),
            key: PhantomData,
        }
    }
}

impl<F, K> fmt::Debug for FromFn<F, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFn").finish_non_exhaustive()
    }
}

/// Which rate limit headers responses carry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Headers {
    /// `RateLimit-Policy` and `RateLimit`, from the IETF draft.
    #[default]
    Draft,
    /// `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`,
    /// which many clients read today.
    Legacy,
    /// Both sets.
    Both,
}

/// How the layer answers the requests it refuses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Protocol {
    /// HTTP statuses: 429, 503 and 400, with `Retry-After`.
    #[default]
    Http,
    /// gRPC statuses in a trailers-only response: `RESOURCE_EXHAUSTED` (8)
    /// for a limited request, `UNAVAILABLE` (14) when the store can't
    /// decide, `INVALID_ARGUMENT` (3) without a key, with
    /// `grpc-retry-pushback-ms`, which gRPC retry policies honour, in
    /// place of `Retry-After`.
    Grpc,
}

/// What a request refused because the store is full of other keys gets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FullStore {
    /// 429 Too Many Requests, like any other refusal.
    #[default]
    TooManyRequests,
    /// 503 Service Unavailable: the service is overloaded, not the client.
    ServiceUnavailable,
}

/// Rate limits requests with a limiter. See the [module docs](self).
pub struct RateLimitLayer<L, X: KeyExtractor> {
    limiter: L,
    extractor: X,
    config: Arc<Config<X::Key>>,
}

/// A predicate picking requests to let through unlimited.
type Exempt = dyn Fn(Head<'_>) -> bool + Send + Sync;

#[derive(Clone)]
struct Config<K> {
    exempt: Option<Arc<Exempt>>,
    protocol: Protocol,
    headers: Headers,
    full_store: FullStore,
    fallback: Option<K>,
    rates: Vec<RateInfo>,
    policy: HeaderValue,
}

#[derive(Clone)]
struct RateInfo {
    name: String,
    /// Units the rate allows over `window`.
    quota: u32,
    /// Seconds, rounded up.
    window: u64,
}

impl RateInfo {
    /// A token bucket allows its burst at once, then refills it over the
    /// time it takes; a window allows its count per period.
    fn of(rate: crate::Rate, bursts: bool) -> Self {
        let period = rate.period();
        let (quota, window) = if bursts {
            let burst = rate.burst().get();
            let refill = u128::from(burst)
                .saturating_mul(period.as_nanos())
                .div_ceil(u128::from(rate.count().get()));
            let refill = Duration::from_nanos(u64::try_from(refill).unwrap_or(u64::MAX));
            (burst, seconds(refill))
        } else {
            (rate.count().get(), seconds(period))
        };
        Self {
            name: rate_name(period),
            quota,
            window,
        }
    }
}

impl<L, X> RateLimitLayer<L, X>
where
    X: KeyExtractor,
    L: Limits<X::Key>,
{
    /// Limits each request's key, as `extractor` finds it, with `limiter`.
    pub fn new(limiter: L, extractor: X) -> Self {
        let bursts = limiter.bursts();
        let rates: Vec<RateInfo> = limiter
            .limits()
            .rates()
            .iter()
            .map(|rate| RateInfo::of(*rate, bursts))
            .collect();
        let policy = rates
            .iter()
            .map(|info| format!("\"{}\";q={};w={}", info.name, info.quota, info.window))
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            limiter,
            extractor,
            config: Arc::new(Config {
                exempt: None,
                protocol: Protocol::Http,
                headers: Headers::Draft,
                full_store: FullStore::TooManyRequests,
                fallback: None,
                rates,
                // Rate names and numbers are ASCII.
                policy: HeaderValue::from_str(&policy).unwrap_or(HeaderValue::from_static("")),
            }),
        }
    }
}

impl<L, X> RateLimitLayer<L, X>
where
    X: KeyExtractor,
    X::Key: Clone,
{
    /// Which rate limit headers to send. The default is [`Headers::Draft`].
    #[must_use]
    pub fn headers(mut self, headers: Headers) -> Self {
        self.config_mut().headers = headers;
        self
    }

    /// Lets requests `predicate` picks through without limiting them or
    /// adding headers: health checks, internal callers, an allow list.
    /// Checked before the key, so an exempt request needs none.
    ///
    /// ```
    /// use porin::tower::{PeerIp, RateLimitLayer};
    /// use porin::{Limiter, quota};
    ///
    /// let limiter: Limiter<std::net::IpAddr> = Limiter::memory(quota!("10/second"));
    /// let layer = RateLimitLayer::new(limiter, PeerIp::new())
    ///     .exempt(|request| request.uri().path() == "/health");
    /// # drop(layer);
    /// ```
    #[must_use]
    pub fn exempt<F>(mut self, predicate: F) -> Self
    where
        F: Fn(Head<'_>) -> bool + Send + Sync + 'static,
    {
        self.config_mut().exempt = Some(Arc::new(predicate));
        self
    }

    /// How refusals are answered. The default is [`Protocol::Http`]; for
    /// gRPC services, [`Protocol::Grpc`].
    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.config_mut().protocol = protocol;
        self
    }

    /// What to answer when the store is full of other keys. The default
    /// is [`FullStore::TooManyRequests`].
    #[must_use]
    pub fn when_store_full(mut self, answer: FullStore) -> Self {
        self.config_mut().full_store = answer;
        self
    }

    /// A key for requests that have none, instead of answering 400. They
    /// all share its limit, so one of them can use it up for the rest.
    #[must_use]
    pub fn fallback_key(mut self, key: X::Key) -> Self {
        self.config_mut().fallback = Some(key);
        self
    }

    /// The config to change, copied first if a clone of this layer or a
    /// service it made shares it, so they keep theirs.
    fn config_mut(&mut self) -> &mut Config<X::Key> {
        Arc::make_mut(&mut self.config)
    }
}

/// A rate's name in headers: its period, as `1s`, `60s` or `1500ms`.
fn rate_name(period: Duration) -> String {
    let millis = period.as_millis();
    if millis % 1_000 == 0 {
        format!("{}s", millis / 1_000)
    } else {
        format!("{millis}ms")
    }
}

/// Whole seconds, rounded up, for headers.
fn seconds(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
}

impl<L: Clone, X: KeyExtractor> Clone for RateLimitLayer<L, X> {
    fn clone(&self) -> Self {
        Self {
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            config: Arc::clone(&self.config),
        }
    }
}

impl<L: fmt::Debug, X: KeyExtractor + fmt::Debug> fmt::Debug for RateLimitLayer<L, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimitLayer")
            .field("limiter", &self.limiter)
            .field("extractor", &self.extractor)
            .field("headers", &self.config.headers)
            .field("full_store", &self.config.full_store)
            .finish_non_exhaustive()
    }
}

impl<S, L: Clone, X: KeyExtractor> tower_layer::Layer<S> for RateLimitLayer<L, X> {
    type Service = RateLimit<S, L, X>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimit {
            inner,
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            config: Arc::clone(&self.config),
        }
    }
}

/// The service [`RateLimitLayer`] wraps around another.
pub struct RateLimit<S, L, X: KeyExtractor> {
    inner: S,
    limiter: L,
    extractor: X,
    config: Arc<Config<X::Key>>,
}

impl<S: Clone, L: Clone, X: KeyExtractor> Clone for RateLimit<S, L, X> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            config: Arc::clone(&self.config),
        }
    }
}

impl<S: fmt::Debug, L: fmt::Debug, X: KeyExtractor + fmt::Debug> fmt::Debug for RateLimit<S, L, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimit")
            .field("inner", &self.inner)
            .field("limiter", &self.limiter)
            .field("extractor", &self.extractor)
            .finish_non_exhaustive()
    }
}

impl<S, L, X, ReqBody, ResBody> tower_service::Service<Request<ReqBody>> for RateLimit<S, L, X>
where
    S: tower_service::Service<Request<ReqBody>, Response = Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Default,
    X: KeyExtractor,
    X::Key: Clone,
    L: Limits<X::Key>,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response<ResBody>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let exempt = self
            .config
            .exempt
            .as_ref()
            .is_some_and(|exempt| exempt(Head::new(&request)));
        if exempt {
            let fresh = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, fresh);
            return Box::pin(async move { inner.call(request).await });
        }
        let key = self
            .extractor
            .extract(Head::new(&request))
            .or_else(|| self.config.fallback.clone());
        // The inner service was readied for this call; keep it, and leave
        // a fresh clone for the next.
        let fresh = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, fresh);
        let limiter = self.limiter.clone();
        let config = Arc::clone(&self.config);
        Box::pin(async move {
            let Some(key) = key else {
                return Ok(config.answer(Refusal::BadKey, None));
            };
            match limiter.hit_key(key.borrow()).await {
                Ok(Decision::Allowed(stats)) => {
                    let mut response = inner.call(request).await?;
                    config.describe(response.headers_mut(), stats);
                    Ok(response)
                }
                Ok(Decision::Denied(denial)) => Ok(config.refuse(denial)),
                Err(Error::KeyTooLong) => Ok(config.answer(Refusal::BadKey, None)),
                // Nothing says when the store will be back: try in a second.
                Err(Error::Store(_)) => {
                    Ok(config.answer(Refusal::Unavailable, Some(Duration::from_secs(1))))
                }
            }
        })
    }
}

/// Why the layer answered a request itself.
#[derive(Clone, Copy)]
enum Refusal {
    /// Over the limit, or the store is full and answers as if.
    Limited,
    /// The store can't decide.
    Unavailable,
    /// No key, or one too long.
    BadKey,
}

impl<K> Config<K> {
    /// An empty response for `refusal`, telling the client when to retry.
    fn answer<B: Default>(&self, refusal: Refusal, retry_after: Option<Duration>) -> Response<B> {
        let mut response = Response::new(B::default());
        match self.protocol {
            Protocol::Http => {
                *response.status_mut() = match refusal {
                    Refusal::Limited => StatusCode::TOO_MANY_REQUESTS,
                    Refusal::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                    Refusal::BadKey => StatusCode::BAD_REQUEST,
                };
                if let Some(wait) = retry_after {
                    response
                        .headers_mut()
                        .insert(RETRY_AFTER, HeaderValue::from(seconds(wait).max(1)));
                }
            }
            Protocol::Grpc => {
                let headers = response.headers_mut();
                // A trailers-only response: HTTP 200, the status in the
                // headers, no messages.
                let (code, message) = match refusal {
                    Refusal::Limited => ("8", "rate limited"),
                    Refusal::Unavailable => ("14", "rate limit store unavailable"),
                    Refusal::BadKey => ("3", "no rate limit key"),
                };
                headers.insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/grpc"),
                );
                headers.insert(
                    HeaderName::from_static("grpc-status"),
                    HeaderValue::from_static(code),
                );
                headers.insert(
                    HeaderName::from_static("grpc-message"),
                    HeaderValue::from_static(message),
                );
                if let Some(wait) = retry_after {
                    // gRPC retry policies wait this long before retrying,
                    // so round up: earlier would be refused again.
                    let millis =
                        u64::try_from(wait.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX);
                    headers.insert(
                        HeaderName::from_static("grpc-retry-pushback-ms"),
                        HeaderValue::from(millis),
                    );
                }
            }
        }
        response
    }

    fn refuse<B: Default>(&self, denial: Denial) -> Response<B> {
        let refusal = if denial.is_store_unavailable() {
            Refusal::Unavailable
        } else if denial.is_store_full() {
            match self.full_store {
                FullStore::TooManyRequests => Refusal::Limited,
                FullStore::ServiceUnavailable => Refusal::Unavailable,
            }
        } else {
            Refusal::Limited
        };
        let mut response = self.answer(refusal, denial.retry_after());
        if !denial.is_store_full() {
            self.describe(response.headers_mut(), denial.stats());
        }
        response
    }

    /// Rate limit headers for `stats`, if the store reported them.
    fn describe(&self, headers: &mut HeaderMap, stats: Stats) {
        if !stats.is_known() {
            return;
        }
        let Some(rate) = self.rates.get(usize::from(stats.limiting)) else {
            return;
        };
        let reset = seconds(stats.limiting_reset);
        if matches!(self.headers, Headers::Draft | Headers::Both) {
            headers.insert(
                HeaderName::from_static("ratelimit-policy"),
                self.policy.clone(),
            );
            let current = format!("\"{}\";r={};t={}", rate.name, stats.remaining(), reset);
            if let Ok(value) = HeaderValue::from_str(&current) {
                headers.insert(HeaderName::from_static("ratelimit"), value);
            }
        }
        if matches!(self.headers, Headers::Legacy | Headers::Both) {
            headers.insert(
                HeaderName::from_static("x-ratelimit-limit"),
                HeaderValue::from(rate.quota),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-remaining"),
                HeaderValue::from(stats.remaining()),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-reset"),
                HeaderValue::from(reset),
            );
        }
    }
}

#[cfg(test)]
mod tests;
