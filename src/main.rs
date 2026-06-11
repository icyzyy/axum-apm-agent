//! # Rust Reverse Proxy Binary Entry Point
//!
//! This binary runs a simple HTTP reverse proxy using `axum` and `hyper-util`
//! by consuming the shared handlers and telemetry middleware defined in `lib.rs`.

use axum::Router;
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use rust_reverse_proxy::{apm_trace_layer, init_apm, proxy_handler, ProxyClient};
use std::net::SocketAddr;

/// Entry point of the reverse proxy binary application.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize OpenTelemetry APM provider
    let _tracer_provider = init_apm()?;

    // 2. Instantiate the shared Hyper Client for HTTP request forwarding
    let client: ProxyClient = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());

    // 3. Set up the Axum router with the library's fallback handler and telemetry middleware
    let app = Router::new()
        .fallback(proxy_handler)
        .with_state(client)
        .layer(apm_trace_layer());

    // 4. Retrieve listening port and start the server
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);

    let upstream_url = std::env::var("UPSTREAM_URL")
        .unwrap_or_else(|_| "https://httpbin.org".to_string());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Reverse proxy listening on {} -> proxying to {}", addr, upstream_url);

    axum::serve(listener, app).await?;

    // Ensure all remaining telemetry spans are flushed before server exits
    if let Err(err) = _tracer_provider.shutdown() {
        tracing::error!("Error shutting down TracerProvider: {:?}", err);
    }

    Ok(())
}
