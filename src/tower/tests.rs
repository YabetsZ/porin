//! The layer around a plain service: answers, headers and keys.

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU8, NonZeroUsize};

use http::{HeaderValue, Request, Response, StatusCode};
use tower::{Layer, Service, ServiceExt, service_fn};

use super::{FullStore, Header, Headers, PeerIp, ProxyHeader, RateLimitLayer, from_fn};
use crate::{Limiter, quota};

type Ready = std::future::Ready<Result<Response<String>, Infallible>>;

fn ok(_request: Request<()>) -> Ready {
    std::future::ready(Ok(Response::new("ok".to_owned())))
}

/// Sends `request` through `layer` around [`ok`].
async fn send<L>(layer: &L, request: Request<()>) -> Response<String>
where
    L: Layer<tower::util::ServiceFn<fn(Request<()>) -> Ready>>,
    L::Service: Service<Request<()>, Response = Response<String>, Error = Infallible>,
{
    let inner = service_fn(ok as fn(_) -> _);
    layer.layer(inner).oneshot(request).await.unwrap()
}

fn from(peer: &str) -> Request<()> {
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert(peer.parse::<SocketAddr>().unwrap());
    request
}

fn header<'a>(response: &'a Response<String>, name: &str) -> Option<&'a str> {
    response
        .headers()
        .get(name)
        .map(|value| value.to_str().unwrap())
}

#[tokio::test]
async fn allows_then_refuses_with_draft_headers() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("2/second; 100/day"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new());
    let first = send(&layer, from("10.0.0.1:5000")).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.body(), "ok");
    assert_eq!(
        header(&first, "ratelimit-policy"),
        Some("\"1s\";q=2;w=1, \"86400s\";q=100;w=86400")
    );
    assert_eq!(header(&first, "ratelimit"), Some("\"1s\";r=1;t=1"));
    assert_eq!(header(&first, "x-ratelimit-limit"), None);

    assert_eq!(
        send(&layer, from("10.0.0.1:5001")).await.status(),
        StatusCode::OK
    );
    let refused = send(&layer, from("10.0.0.1:5002")).await;
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(refused.body(), "");
    assert_eq!(header(&refused, "retry-after"), Some("1"));
    assert_eq!(header(&refused, "ratelimit"), Some("\"1s\";r=0;t=1"));

    let other = send(&layer, from("10.0.0.2:5000")).await;
    assert_eq!(
        other.status(),
        StatusCode::OK,
        "each address has its own limit"
    );
}

#[tokio::test]
async fn the_rate_closest_to_running_out_names_the_headers() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("100/second; 3/day"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new()).headers(Headers::Both);
    let response = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(
        header(&response, "ratelimit"),
        Some("\"86400s\";r=2;t=28800")
    );
    assert_eq!(header(&response, "x-ratelimit-limit"), Some("3"));
    assert_eq!(header(&response, "x-ratelimit-remaining"), Some("2"));
    assert_eq!(header(&response, "x-ratelimit-reset"), Some("28800"));
}

#[tokio::test]
async fn legacy_headers_replace_the_draft_ones() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1500 per 1500 milliseconds"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new()).headers(Headers::Legacy);
    let response = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(header(&response, "ratelimit"), None);
    assert_eq!(header(&response, "ratelimit-policy"), None);
    assert_eq!(header(&response, "x-ratelimit-remaining"), Some("1499"));
}

#[tokio::test]
async fn a_request_without_a_key_is_refused_or_uses_the_fallback() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/second"));
    let layer = RateLimitLayer::new(limiter.clone(), PeerIp::new());
    let keyless = send(&layer, Request::new(())).await;
    assert_eq!(keyless.status(), StatusCode::BAD_REQUEST);

    let shared = IpAddr::from([0, 0, 0, 0]);
    let layer = RateLimitLayer::new(limiter, PeerIp::new()).fallback_key(shared);
    assert_eq!(
        send(&layer, Request::new(())).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&layer, Request::new(())).await.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "keyless requests share the fallback key's limit"
    );
}

#[tokio::test]
async fn ipv6_clients_are_limited_by_their_64() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/second"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new());
    assert_eq!(
        send(&layer, from("[2001:db8:1:2::1]:1")).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&layer, from("[2001:db8:1:2:ffff::9]:1"))
            .await
            .status(),
        StatusCode::TOO_MANY_REQUESTS,
        "the same /64"
    );
    assert_eq!(
        send(&layer, from("[2001:db8:1:3::1]:1")).await.status(),
        StatusCode::OK
    );

    // An IPv4 client reached over IPv6 is its IPv4 address.
    assert_eq!(
        send(&layer, from("[::ffff:192.0.2.7]:1")).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&layer, from("192.0.2.7:1")).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

fn behind_proxy(header: &str, value: &str) -> Request<()> {
    let mut request = from("10.9.9.9:1");
    request.headers_mut().append(
        http::HeaderName::from_bytes(header.as_bytes()).unwrap(),
        HeaderValue::from_str(value).unwrap(),
    );
    request
}

#[tokio::test]
async fn forwarded_addresses_count_only_when_trusted() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/second"));
    // Not trusted: a forged header changes nothing, the peer is the key.
    let direct = RateLimitLayer::new(limiter.clone(), PeerIp::new());
    assert_eq!(
        send(&direct, behind_proxy("x-forwarded-for", "1.1.1.1"))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(&direct, behind_proxy("x-forwarded-for", "2.2.2.2"))
            .await
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    // Two trusted proxies: the client is second from the end, whatever a
    // client put before it.
    let two =
        PeerIp::new().trusting_proxies(NonZeroU8::new(2).unwrap(), ProxyHeader::XForwardedFor);
    let proxied = RateLimitLayer::new(limiter, two);
    let request = behind_proxy("x-forwarded-for", "6.6.6.6, 3.3.3.3, 10.0.0.1");
    assert_eq!(send(&proxied, request).await.status(), StatusCode::OK);
    let request = behind_proxy("x-forwarded-for", "7.7.7.7, 3.3.3.3, 10.0.0.1");
    assert_eq!(
        send(&proxied, request).await.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a forged first address doesn't make a new key"
    );
    let short = behind_proxy("x-forwarded-for", "10.0.0.1");
    assert_eq!(
        send(&proxied, short).await.status(),
        StatusCode::BAD_REQUEST,
        "too few addresses: no key"
    );
}

/// The key `PeerIp` trusting one proxy that writes `header` finds in a
/// request carrying `lines` of `name`.
fn client(header: ProxyHeader, name: &str, lines: &[&[u8]]) -> Option<IpAddr> {
    use super::KeyExtractor as _;
    let mut request = from("10.9.9.9:1");
    for line in lines {
        request.headers_mut().append(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_bytes(line).unwrap(),
        );
    }
    PeerIp::new()
        .trusting_proxies(NonZeroU8::MIN, header)
        .extract(super::Head::new(&request))
}

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

#[test]
fn forwarded_headers_parse() {
    let forwarded = |line: &str| client(ProxyHeader::Forwarded, "forwarded", &[line.as_bytes()]);
    assert_eq!(
        forwarded("for=192.0.2.60;proto=http"),
        Some(ip("192.0.2.60"))
    );
    assert_eq!(
        forwarded("for=1.1.1.1, For=\"[2001:db8:cafe::17]:4711\""),
        Some(ip("2001:db8:cafe::"))
    );
    assert_eq!(forwarded("for=unknown"), None);
    let xff = |line: &str| {
        client(
            ProxyHeader::XForwardedFor,
            "x-forwarded-for",
            &[line.as_bytes()],
        )
    };
    assert_eq!(xff("203.0.113.9:443"), Some(ip("203.0.113.9")));
    assert_eq!(
        xff("1.1.1.1, 2001:db8:1:2:3::4"),
        Some(ip("2001:db8:1:2::"))
    );
    assert_eq!(xff("1.1.1.1, ::ffff:192.0.2.1"), Some(ip("192.0.2.1")));
    assert_eq!(xff("nonsense"), None);
}

#[test]
fn a_client_cant_pick_its_address_behind_a_trusted_proxy() {
    use super::KeyExtractor as _;
    // The proxy writes X-Forwarded-For; a Forwarded header can only be the
    // client's, so it is never read.
    let mut request = from("10.9.9.9:1");
    request
        .headers_mut()
        .insert("x-forwarded-for", HeaderValue::from_static("203.0.113.5"));
    request
        .headers_mut()
        .insert("forwarded", HeaderValue::from_static("for=6.6.6.6"));
    let trusting = PeerIp::new().trusting_proxies(NonZeroU8::MIN, ProxyHeader::XForwardedFor);
    assert_eq!(
        trusting.extract(super::Head::new(&request)),
        Some(ip("203.0.113.5"))
    );

    let xff = |lines: &[&[u8]]| client(ProxyHeader::XForwardedFor, "x-forwarded-for", lines);
    // A byte the client sent that isn't ASCII doesn't hide the address the
    // proxy appended to the same line...
    assert_eq!(xff(&[b"x\xff, 203.0.113.5"]), Some(ip("203.0.113.5")));
    // ...or on a line of its own.
    assert_eq!(
        xff(&[b"6.6.6.6", b"x\xff", b"203.0.113.5"]),
        Some(ip("203.0.113.5"))
    );
    // Unreadable where the client should be: no key, not the next address.
    assert_eq!(xff(&[b"6.6.6.6", b"x\xff"]), None);
}

#[tokio::test]
async fn a_header_can_be_the_key() {
    let limiter: Limiter<Vec<u8>> = Limiter::memory(quota!("1/second"));
    let layer = RateLimitLayer::new(limiter, Header::new(http::header::AUTHORIZATION));
    let with = |token: &'static str| {
        let mut request = Request::new(());
        request
            .headers_mut()
            .insert(http::header::AUTHORIZATION, HeaderValue::from_static(token));
        request
    };
    assert_eq!(send(&layer, with("a")).await.status(), StatusCode::OK);
    assert_eq!(
        send(&layer, with("a")).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(send(&layer, with("b")).await.status(), StatusCode::OK);
    assert_eq!(
        send(&layer, Request::new(())).await.status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn a_function_can_be_the_key() {
    let limiter: Limiter<String> = Limiter::memory(quota!("1/second"));
    let layer = RateLimitLayer::new(
        limiter,
        from_fn(|request| Some(request.uri().path().to_owned())),
    );
    let to = |path: &str| Request::get(path).body(()).unwrap();
    assert_eq!(send(&layer, to("/a")).await.status(), StatusCode::OK);
    assert_eq!(
        send(&layer, to("/a")).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(send(&layer, to("/b")).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_full_store_answers_429_or_503() {
    let limiter: Limiter<IpAddr> = Limiter::builder()
        .quota(quota!("1/hour"))
        .max_keys(NonZeroUsize::MIN)
        .build();
    let layer = RateLimitLayer::new(limiter.clone(), PeerIp::new());
    assert_eq!(
        send(&layer, from("10.0.0.1:1")).await.status(),
        StatusCode::OK
    );
    let full = send(&layer, from("10.0.0.2:1")).await;
    assert_eq!(full.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(header(&full, "retry-after").is_some());
    assert_eq!(
        header(&full, "ratelimit"),
        None,
        "a full store says nothing of the key"
    );

    let layer =
        RateLimitLayer::new(limiter, PeerIp::new()).when_store_full(FullStore::ServiceUnavailable);
    let full = send(&layer, from("10.0.0.3:1")).await;
    assert_eq!(full.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn only_allowed_requests_reach_the_service() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let calls = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&calls);
    let inner = service_fn(move |request: Request<()>| {
        counted.fetch_add(1, Ordering::SeqCst);
        ok(request)
    });
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("3/hour"));
    let mut service = RateLimitLayer::new(limiter, PeerIp::new()).layer(inner);
    for _ in 0..10 {
        let response = service
            .ready()
            .await
            .unwrap()
            .call(from("10.0.0.1:1"))
            .await
            .unwrap();
        assert!(matches!(
            response.status(),
            StatusCode::OK | StatusCode::TOO_MANY_REQUESTS
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn layers_and_services_can_cross_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RateLimitLayer<Limiter<IpAddr>, PeerIp>>();
    assert_send_sync::<
        super::RateLimit<tower::util::ServiceFn<fn(Request<()>) -> Ready>, Limiter<IpAddr>, PeerIp>,
    >();
}

#[tokio::test]
async fn a_cloned_layer_can_be_configured_on_its_own() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("10/second"));
    let base = RateLimitLayer::new(limiter, PeerIp::new());
    let legacy = base.clone().headers(Headers::Legacy);
    let draft = send(&base, from("10.0.0.1:1")).await;
    assert!(
        header(&draft, "ratelimit").is_some(),
        "the original keeps its headers"
    );
    let response = send(&legacy, from("10.0.0.2:1")).await;
    assert!(header(&response, "x-ratelimit-limit").is_some());
    assert_eq!(header(&response, "ratelimit"), None);
}

#[tokio::test]
async fn a_token_bucket_describes_its_burst() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("10/second burst 50"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new()).headers(Headers::Both);
    let response = send(&layer, from("10.0.0.1:1")).await;
    // 50 at once, refilled over 5 seconds.
    assert_eq!(
        header(&response, "ratelimit-policy"),
        Some("\"1s\";q=50;w=5")
    );
    assert_eq!(header(&response, "ratelimit"), Some("\"1s\";r=49;t=1"));
    assert_eq!(header(&response, "x-ratelimit-limit"), Some("50"));

    // A window allows its count, whatever burst the rate was given.
    let windows: Limiter<IpAddr, crate::FixedWindow> = Limiter::builder()
        .algorithm(crate::FixedWindow)
        .quota(quota!("10/second burst 50"))
        .build();
    let layer = RateLimitLayer::new(windows, PeerIp::new());
    let response = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(
        header(&response, "ratelimit-policy"),
        Some("\"1s\";q=10;w=1")
    );
}

#[tokio::test]
async fn a_single_rate_reports_its_own_reset() {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("3/minute"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new());
    let response = send(&layer, from("10.0.0.1:1")).await;
    // One of three units used refills in 20 seconds.
    assert_eq!(header(&response, "ratelimit"), Some("\"60s\";r=2;t=20"));
    // A known key takes the store's lock-free path, which must report the
    // same way.
    let response = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(header(&response, "ratelimit"), Some("\"60s\";r=1;t=40"));
}

#[tokio::test]
async fn a_long_or_repeated_header_is_no_key() {
    let limiter: Limiter<Vec<u8>> = Limiter::memory(quota!("10/second"));
    let layer = RateLimitLayer::new(limiter, Header::new(http::header::AUTHORIZATION));
    let mut long = Request::new(());
    long.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&"x".repeat(513)).unwrap(),
    );
    assert_eq!(send(&layer, long).await.status(), StatusCode::BAD_REQUEST);
    let mut fits = Request::new(());
    fits.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&"x".repeat(512)).unwrap(),
    );
    assert_eq!(send(&layer, fits).await.status(), StatusCode::OK);
    let mut twice = Request::new(());
    twice
        .headers_mut()
        .append(http::header::AUTHORIZATION, HeaderValue::from_static("a"));
    twice
        .headers_mut()
        .append(http::header::AUTHORIZATION, HeaderValue::from_static("b"));
    assert_eq!(send(&layer, twice).await.status(), StatusCode::BAD_REQUEST);
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod redis {
    use std::net::IpAddr;

    use http::{HeaderValue, Request, StatusCode};

    use super::{from, header, send};
    use crate::store::redis::tests::{free_port, spawn_server};
    use crate::store::{FailClosed, FailOpen, FailurePolicy, Propagate, RedisStore};
    use crate::tower::{Header, PeerIp, RateLimitLayer};
    use crate::{Limiter, quota};

    async fn store<P: FailurePolicy>(url: &str, policy: P) -> RedisStore<P> {
        RedisStore::builder(url)
            .on_failure(policy)
            .connect()
            .await
            .unwrap()
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore = "needs a Redis server")]
    async fn a_redis_outage_answers_503_unless_failing_open() {
        let Some(mut server) = spawn_server(free_port(), &[]) else {
            return;
        };
        let quota = quota!("10/second");
        let closed: Limiter<IpAddr, _, _> =
            Limiter::redis_builder(&store(&server.url, FailClosed).await, "c")
                .quota(quota)
                .build()
                .unwrap();
        let propagate: Limiter<IpAddr, _, _> =
            Limiter::redis_builder(&store(&server.url, Propagate).await, "p")
                .quota(quota)
                .build()
                .unwrap();
        let open: Limiter<IpAddr, _, _> =
            Limiter::redis_builder(&store(&server.url, FailOpen).await, "o")
                .quota(quota)
                .build()
                .unwrap();
        let closed = RateLimitLayer::new(closed, PeerIp::new());
        let propagate = RateLimitLayer::new(propagate, PeerIp::new());
        let open = RateLimitLayer::new(open, PeerIp::new());
        let healthy = send(&closed, from("10.0.0.1:1")).await;
        assert_eq!(healthy.status(), StatusCode::OK);
        assert_eq!(header(&healthy, "ratelimit"), Some("\"1s\";r=9;t=1"));

        let grpc = RateLimitLayer::new(
            Limiter::redis_builder(&store(&server.url, FailClosed).await, "g")
                .quota(quota)
                .build()
                .unwrap(),
            PeerIp::new(),
        )
        .protocol(crate::tower::Protocol::Grpc);
        server.stop();
        for layer_answer in [
            send(&closed, from("10.0.0.1:1")).await,
            send(&propagate, from("10.0.0.1:1")).await,
        ] {
            assert_eq!(layer_answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(header(&layer_answer, "retry-after").is_some());
            assert_eq!(header(&layer_answer, "ratelimit"), None);
        }
        let grpc_answer = send(&grpc, from("10.0.0.1:1")).await;
        assert_eq!(
            header(&grpc_answer, "grpc-status"),
            Some("14"),
            "UNAVAILABLE"
        );
        assert!(header(&grpc_answer, "grpc-retry-pushback-ms").is_some());
        let through = send(&open, from("10.0.0.1:1")).await;
        assert_eq!(
            through.status(),
            StatusCode::OK,
            "fail open lets it through"
        );
        assert_eq!(
            header(&through, "ratelimit"),
            None,
            "with nothing known to report"
        );
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore = "needs a Redis server")]
    async fn a_key_too_long_for_redis_answers_400() {
        let Some(server) = spawn_server(free_port(), &[]) else {
            return;
        };
        let limiter: Limiter<Vec<u8>, _, _> =
            Limiter::redis_builder(&store(&server.url, FailOpen).await, "long")
                .quota(quota!("10/second"))
                .build()
                .unwrap();
        let layer = RateLimitLayer::new(limiter, Header::new(http::header::AUTHORIZATION));
        let mut request = Request::new(());
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&"x".repeat(600)).unwrap(),
        );
        assert_eq!(
            send(&layer, request).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
}

#[cfg(feature = "axum_0_8")]
#[tokio::test]
async fn an_axum_router_keys_by_connect_info() {
    use axum::Router;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::routing::get;

    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/second"));
    let app = Router::new()
        .route("/", get(|| async { "hello" }))
        .layer(RateLimitLayer::new(limiter, PeerIp::new()))
        .layer(MockConnectInfo(SocketAddr::from(([10, 1, 1, 1], 4000))));
    let get = || Request::get("/").body(Body::empty()).unwrap();
    let first = app.clone().oneshot(get()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert!(first.headers().contains_key("ratelimit"));
    let second = app.oneshot(get()).await.unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn grpc_services_get_grpc_statuses() {
    use super::Protocol;

    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/minute"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new()).protocol(Protocol::Grpc);
    assert_eq!(
        send(&layer, from("10.0.0.1:1")).await.status(),
        StatusCode::OK
    );
    let refused = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(
        refused.status(),
        StatusCode::OK,
        "gRPC carries its status in headers"
    );
    assert_eq!(header(&refused, "content-type"), Some("application/grpc"));
    assert_eq!(header(&refused, "grpc-status"), Some("8"));
    assert_eq!(header(&refused, "grpc-retry-pushback-ms"), Some("60000"));
    assert_eq!(header(&refused, "retry-after"), None);
    assert!(header(&refused, "ratelimit").is_some());

    let keyless = send(&layer, Request::new(())).await;
    assert_eq!(header(&keyless, "grpc-status"), Some("3"));
}

#[cfg(feature = "tonic_0_14")]
#[tokio::test]
async fn tonic_reads_the_answers_as_grpc_statuses() {
    use super::Protocol;

    let limiter: Limiter<IpAddr> = Limiter::builder()
        .quota(quota!("1/minute"))
        .max_keys(NonZeroUsize::MIN)
        .build();
    let layer = RateLimitLayer::new(limiter, PeerIp::new())
        .protocol(Protocol::Grpc)
        .when_store_full(FullStore::ServiceUnavailable);
    let status_of = |response: &Response<String>| {
        tonic::Status::from_header_map(response.headers()).expect("a gRPC status")
    };
    assert_eq!(
        send(&layer, from("10.0.0.1:1")).await.status(),
        StatusCode::OK
    );
    let refused = send(&layer, from("10.0.0.1:1")).await;
    assert_eq!(status_of(&refused).code(), tonic::Code::ResourceExhausted);
    assert_eq!(status_of(&refused).message(), "rate limited");
    let full = send(&layer, from("10.0.0.2:1")).await;
    assert_eq!(status_of(&full).code(), tonic::Code::Unavailable);
    let keyless = send(&layer, Request::new(())).await;
    assert_eq!(status_of(&keyless).code(), tonic::Code::InvalidArgument);
}

#[cfg(feature = "tonic_0_14")]
#[tokio::test]
async fn peer_ip_reads_tonics_connect_info() {
    use tonic::transport::server::TcpConnectInfo;

    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("1/minute"));
    let layer = RateLimitLayer::new(limiter, PeerIp::new());
    let with_info = || {
        let mut request = Request::new(());
        request.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(SocketAddr::from(([10, 2, 2, 2], 5000))),
        });
        request
    };
    assert_eq!(send(&layer, with_info()).await.status(), StatusCode::OK);
    assert_eq!(
        send(&layer, with_info()).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}
