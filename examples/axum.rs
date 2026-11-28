//! An axum server that limits each client IP to 5 requests a second and
//! 100 a day.
//!
//! ```sh
//! cargo run --example axum --features axum
//! for i in $(seq 7); do curl -si localhost:3000 | head -1; done
//! ```

use std::net::{IpAddr, SocketAddr};

use axum::Router;
use axum::routing::get;
use porin::tower::{PeerIp, RateLimitLayer};
use porin::{Limiter, quota};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let limiter: Limiter<IpAddr> = Limiter::memory(quota!("5/second; 100/day"));
    let app = Router::new()
        .route("/", get(|| async { "hello\n" }))
        .layer(RateLimitLayer::new(limiter, PeerIp::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    // The peer address reaches the layer through ConnectInfo.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
