//! # Rust Reverse Proxy with OpenTelemetry / Elastic APM Tracing Middleware
//!
//! This application implements a simple HTTP reverse proxy using `axum` and `hyper-util`.
//! It integrates with OpenTelemetry for request tracing using `tracing-opentelemetry`
//! and exports tracing events to the Elastic APM server's OTLP HTTP endpoint.
//!
//! ## Configuration
//! The application can be configured using the following environment variables:
//! - `UPSTREAM_URL`: The target upstream server URL to proxy requests to (default: `https://httpbin.org`).
//! - `PORT`: The local port to run the reverse proxy server on (default: `3000`).
//! - `ELASTIC_APM_SERVER_URL`: The URL of the Elastic APM intake server (default: `http://localhost:8200`).
//! - `ELASTIC_APM_SERVICE_NAME`: The service name registered in Elastic APM (default: `rust-reverse-proxy`).
//! - `ELASTIC_APM_SERVICE_VERSION`: The service version registered in Elastic APM (default: `1.0.0`).
//! - `ELASTIC_APM_ENVIRONMENT`: The environment tag registered in Elastic APM (default: `production`).

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    Router,
};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Type alias for the Hyper legacy client that is used to forward requests.
type Client = hyper_util::client::legacy::Client<HttpConnector, Body>;

/// Initializes the OpenTelemetry tracing provider and registers it globally
/// alongside standard terminal logging layer.
fn init_tracing() -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
    let service_name = std::env::var("ELASTIC_APM_SERVICE_NAME")
        .unwrap_or_else(|_| "rust-reverse-proxy".to_string());
    let service_version = std::env::var("ELASTIC_APM_SERVICE_VERSION")
        .unwrap_or_else(|_| "1.0.0".to_string());
    let apm_environment = std::env::var("ELASTIC_APM_ENVIRONMENT")
        .unwrap_or_else(|_| "production".to_string());
    let apm_server_url = std::env::var("ELASTIC_APM_SERVER_URL")
        .unwrap_or_else(|_| "http://localhost:8200".to_string());
    let apm_node_name = std::env::var("ELASTIC_APM_NODE_NAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok().map(|s| s.trim().to_string()));

    // Construct the OTLP HTTP traces target endpoint (Elastic APM listens on /v1/traces)
    let traces_endpoint = format!("{}/v1/traces", apm_server_url.trim_end_matches('/'));

    // Extract authorization credentials if provided in ELASTIC_APM_SECRET_TOKEN or ELASTIC_APM_API_KEY
    let mut headers = std::collections::HashMap::new();
    if let Ok(token) = std::env::var("ELASTIC_APM_SECRET_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        }
    } else if let Ok(api_key) = std::env::var("ELASTIC_APM_API_KEY") {
        let api_key = api_key.trim();
        if !api_key.is_empty() {
            headers.insert("Authorization".to_string(), format!("ApiKey {}", api_key));
        }
    }

    info!(
        "Initializing OpenTelemetry OTLP Exporter targeting: {} (node: {:?}, authenticated: {})",
        traces_endpoint, apm_node_name, !headers.is_empty()
    );

    // 1. Build the OTLP Span Exporter using HTTP
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(traces_endpoint)
        .with_headers(headers)
        .build()?;

    // Build resources attributes
    let mut attributes = vec![
        KeyValue::new("service.version", service_version),
        KeyValue::new("deployment.environment", apm_environment),
    ];
    if let Some(node_name) = apm_node_name {
        attributes.push(KeyValue::new("service.node.name", node_name));
    }

    // 2. Build the TracerProvider using a batch processor, defining service name, version, and environment
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name)
                .with_attributes(attributes)
                .build(),
        )
        .build();

    // 3. Set global tracer provider
    global::set_tracer_provider(provider.clone());

    // 4. Configure local logging filter
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    // 5. Connect tracing subscriber with OpenTelemetry registry layer
    use opentelemetry::trace::TracerProvider;
    let tracer = provider.tracer("rust-reverse-proxy");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(provider)
}

/// The entry point of the reverse proxy application.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize OpenTelemetry tracing system
    let _tracer_provider = init_tracing()?;

    // 2. Instantiate the shared Hyper Client for HTTP request forwarding
    let client: Client = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());

    // 3. Set up the Axum router with a fallback route that proxies all requests
    let app = Router::new()
        .fallback(proxy_handler)
        .with_state(client)
        // Add Tower HTTP TraceLayer to automatically create tracing spans for each request
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let path = request.uri().path();
                    let query = request.uri().query().unwrap_or("");
                    let method = request.method().as_str();
                    let scheme = request.uri().scheme_str().unwrap_or("http");
                    
                    // Standard http version mapping
                    let http_version = match request.version() {
                        axum::http::Version::HTTP_09 => "0.9",
                        axum::http::Version::HTTP_10 => "1.0",
                        axum::http::Version::HTTP_11 => "1.1",
                        axum::http::Version::HTTP_2 => "2.0",
                        axum::http::Version::HTTP_3 => "3.0",
                        _ => "1.1",
                    };
                    
                    // Extract domain and port from the request Host header or URI
                    let mut domain = request.uri().host().unwrap_or("").to_string();
                    let mut port = request.uri().port_u16().unwrap_or(0);
                    
                    let host_header = request.headers()
                        .get(header::HOST)
                        .and_then(|h| h.to_str().ok());

                    let host = match host_header {
                        Some(h) => h.to_string(),
                        None => {
                            if domain.is_empty() {
                                "localhost".to_string()
                            } else {
                                domain.clone()
                            }
                        }
                    };

                    if domain.is_empty() {
                        let parts: Vec<&str> = host.split(':').collect();
                        domain = parts[0].to_string();
                        if parts.len() > 1 {
                            port = parts[1].parse::<u16>().unwrap_or(0);
                        }
                    }
                    if port == 0 {
                        port = if scheme == "https" { 443 } else { 80 };
                    }
                    
                    let path_query = request.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
                    let url_full = format!("{}://{}{}", scheme, host, path_query);

                    // Extract client IP and port from ConnectInfo (socket info from axum::serve)
                    let (client_ip, client_port) = request.extensions()
                        .get::<axum::extract::ConnectInfo<SocketAddr>>()
                        .map(|axum::extract::ConnectInfo(addr)| (addr.ip().to_string(), addr.port()))
                        .unwrap_or_else(|| {
                            // Fallback to X-Forwarded-For if socket connection info is not available
                            let ip = request.headers()
                                .get("x-forwarded-for")
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.split(',').next())
                                .map(|s| s.trim().to_string())
                                .unwrap_or_else(|| "".to_string());
                            (ip, 0)
                        });

                    // Extract User Agent
                    let user_agent = request.headers()
                        .get(header::USER_AGENT)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    // Extract Referrer header
                    let referrer = request.headers()
                        .get(header::REFERER)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    // Extract Content-Type
                    let content_type = request.headers()
                        .get(header::CONTENT_TYPE)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    // Extract Accept header
                    let accept = request.headers()
                        .get(header::ACCEPT)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    // Format transaction name dynamically as "GET /path"
                    let otel_name = format!("{} {}", method, path);

                    // Declare and record standard OTel/ECS HTTP semantic fields
                    info_span!(
                        "http_request",
                        "otel.name" = otel_name,
                        "http.request.method" = method,
                        "url.full" = url_full,
                        "url.path" = path,
                        "url.query" = query,
                        "url.scheme" = scheme,
                        "url.domain" = domain.clone(),
                        "url.port" = port as i64,
                        "server.address" = domain,
                        "server.port" = port as i64,
                        "client.ip" = client_ip.clone(),
                        "client.address" = client_ip.clone(),
                        "client.port" = client_port as i64,
                        "network.peer.address" = client_ip,
                        "network.peer.port" = client_port as i64,
                        "user_agent.original" = user_agent,
                        "http.request.header.referer" = referrer,
                        "http.request.header.content-type" = content_type,
                        "http.request.header.accept" = accept,
                        "http.version" = http_version,
                        "network.protocol.name" = "http",
                        "network.protocol.version" = http_version,
                        "http.response.status_code" = tracing::field::Empty,
                        "http.response.body.bytes" = tracing::field::Empty,
                        "http.response.header.content-type" = tracing::field::Empty,
                    )
                })
                .on_response(|response: &Response<_>, _latency: std::time::Duration, span: &Span| {
                    // Record the status code dynamically when the response completes
                    span.record("http.response.status_code", response.status().as_u16());
                    
                    // Record Content-Length if present in the response headers
                    if let Some(content_length) = response.headers().get(header::CONTENT_LENGTH)
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok()) {
                        span.record("http.response.body.bytes", content_length);
                    }

                    // Record Content-Type response header
                    if let Some(content_type) = response.headers().get(header::CONTENT_TYPE)
                        .and_then(|h| h.to_str().ok()) {
                        span.record("http.response.header.content-type", content_type);
                    }
                })
        );

    // 4. Retrieve listening port and start the server
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .unwrap_or(3000);

    let upstream_url = std::env::var("UPSTREAM_URL")
        .unwrap_or_else(|_| "https://httpbin.org".to_string());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Reverse proxy listening on {} -> proxying to {}", addr, upstream_url);

    axum::serve(listener, app).await?;

    // Ensure all remaining telemetry spans are flushed before server exits
    if let Err(err) = _tracer_provider.shutdown() {
        tracing::error!("Error shutting down TracerProvider: {:?}", err);
    }

    Ok(())
}

/// Handlers for proxying incoming HTTP requests to the upstream target.
///
/// This function extracts the incoming request, rewrites the destination URI
/// to match the configured target (upstream server), adjusts headers like
/// the `Host` header to match the upstream, and then forwards the request
/// using the shared Hyper HTTP client.
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
