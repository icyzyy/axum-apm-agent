//! # Axum APM Reverse Proxy Binary Entry Point
//!
//! This binary runs a simple HTTP reverse proxy using `axum` and `hyper-util`
//! by consuming the shared handlers and telemetry middleware defined in the
//! `axum-apm` library.

use axum::Router;
use axum_apm::{
    AppState, ProxyClient, apm_otel_layer, apm_trace_layer, init_apm, proxy_handler, resolve_port,
    resolve_upstream_url,
};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Entry point of the reverse proxy binary application.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the OpenTelemetry APM provider (global tracer + W3C propagator)
    let (tracer_provider, config) = init_apm()?;

    // 2. Install the tracing subscriber: env filter + fmt + OTel layer
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(apm_otel_layer(&tracer_provider))
        .init();

    // Surface configuration warnings now that the subscriber can receive them
    // (init_apm runs before the subscriber exists and cannot log them itself).
    for warning in &config.warnings {
        tracing::warn!("{warning}");
    }
    tracing::info!("APM tracing initialized");

    // 3. Instantiate the shared Hyper Client for HTTP request forwarding
    let client: ProxyClient =
        hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
            .build(HttpConnector::new());

    // 4. Read the upstream URL once at startup (required, validated)
    let upstream_url = resolve_upstream_url()?;

    // 5. Create application state
    let state = AppState {
        client,
        upstream_url: Arc::new(upstream_url.clone()),
    };

    // 6. Set up the Axum router with the library's fallback handler and telemetry middleware
    let app = Router::new()
        .fallback(proxy_handler)
        .with_state(state)
        .layer(apm_trace_layer());

    // 7. Retrieve the listening port (defaults to 3000; fails on an invalid value)
    let port = resolve_port()?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        "Reverse proxy listening on {} -> proxying to {}",
        addr,
        upstream_url
    );

    // Run server with graceful shutdown on SIGTERM/SIGINT
    let server = axum::serve(listener, app);
    let shutdown_signal = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal, starting graceful shutdown...");
    };

    server.with_graceful_shutdown(shutdown_signal).await?;

    // Ensure all remaining telemetry spans are flushed before server exits
    if let Err(err) = tracer_provider.shutdown() {
        tracing::error!("Error shutting down TracerProvider: {:?}", err);
    }

    Ok(())
}
