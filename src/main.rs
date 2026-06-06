//! # Rust Reverse Proxy with OpenTelemetry / Elastic APM Tracing Middleware
//!
//! This binary runs a simple HTTP reverse proxy using `axum` and `hyper-util`,
//! instrumented via the telemetry library defined in `lib.rs`.

use axum::{
    extract::{Request, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    Router,
};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use rust_reverse_proxy::{apm_trace_layer, init_apm};
use std::net::SocketAddr;
use tracing::Span;

/// Type alias for the Hyper legacy client used to forward requests.
type Client = hyper_util::client::legacy::Client<HttpConnector, axum::body::Body>;

/// Entry point of the reverse proxy binary application.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize OpenTelemetry tracing system via library
    let _tracer_provider = init_apm()?;

    // 2. Instantiate the shared Hyper Client for HTTP request forwarding
    let client: Client = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());

    // 3. Set up the Axum router with the proxy fallback and telemetry middleware
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

/// Handlers for proxying incoming HTTP requests to the upstream target.
#[tracing::instrument(skip(client, req), fields(upstream_uri))]
async fn proxy_handler(
    State(client): State<Client>,
    mut req: Request,
) -> Result<Response, StatusCode> {
    // Retrieve upstream server URL
    let upstream_url = std::env::var("UPSTREAM_URL")
        .unwrap_or_else(|_| "https://httpbin.org".to_string());

    // Retrieve path and query from the incoming request URI
    let path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or("/");

    // Construct the destination URI pointing to the upstream server
    let uri_str = format!("{}{}", upstream_url.trim_end_matches('/'), path_query);
    let target_uri = uri_str.parse::<Uri>().map_err(|e| {
        tracing::error!("Failed to parse target URI: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    // Record target URI and details in the current tracing span
    let span = Span::current();
    span.record("upstream_uri", &uri_str);
    
    // Update the request URI to target the upstream server
    *req.uri_mut() = target_uri;

    // Rewrite the Host header to match the new destination
    if let Some(host) = req.uri().host() {
        let host_val = if let Some(port) = req.uri().port_u16() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        };

        if let Ok(header_val) = header::HeaderValue::from_str(&host_val) {
            req.headers_mut().insert(header::HOST, header_val);
        }
    }

    // Forward the request to the upstream using the hyper client
    let response = client.request(req).await.map_err(|e| {
        tracing::error!("Failed to proxy request: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    Ok(response.into_response())
}
