use crate::propagation::{extract_header, extract_parent_context};
use axum::{
    extract::{ConnectInfo, Request},
    http::{StatusCode, Version, header},
    response::Response,
};
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::{Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The concrete type of the layer returned by [`apm_trace_layer`].
///
/// Generic over the request/response body type, defaulting to
/// [`axum::body::Body`] so `apm_trace_layer()` works unchanged on a standard
/// axum `Router`.
pub type ApmTraceLayer<B = axum::body::Body> = TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    fn(&Request<B>) -> Span,
    tower_http::trace::DefaultOnRequest,
    fn(&Response<B>, std::time::Duration, &Span),
    tower_http::trace::DefaultOnBodyChunk,
    tower_http::trace::DefaultOnEos,
    tower_http::trace::DefaultOnFailure,
>;

/// Builds the `http_request` span for an incoming request, declaring and
/// recording standard OTel/ECS HTTP semantic fields.
fn make_http_span<B>(request: &Request<B>) -> Span {
    // Zero-overhead short-circuit: when recording is disabled
    // (`ELASTIC_APM_ACTIVE`/`ELASTIC_APM_RECORDING`), return a no-op span so
    // no OTel span is created or exported. Mirrors the Go agent's
    // `Recording()` check in the middleware.
    if !crate::RECORDING.load(std::sync::atomic::Ordering::Relaxed) {
        return Span::none();
    }

    let path = request.uri().path();
    let query = request.uri().query().unwrap_or("");
    let method = request.method().as_str();
    let scheme = request.uri().scheme_str().unwrap_or("http");

    // Map standard HTTP version
    let http_version = match request.version() {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2.0",
        Version::HTTP_3 => "3.0",
        _ => "1.1",
    };

    // Extract domain and port from the request Host header or URI
    let mut domain = request.uri().host().unwrap_or("").to_string();
    let mut port = request.uri().port_u16().unwrap_or(0);

    let host_header = request
        .headers()
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

    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let url_full = format!("{}://{}{}", scheme, host, path_query);

    // Extract client IP and port from ConnectInfo (injected by axum::serve)
    let (client_ip, client_port) = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| (addr.ip().to_string(), addr.port()))
        .unwrap_or_else(|| {
            // Fallback to X-Forwarded-For if socket connection info is not available
            let ip = request
                .headers()
                .get("x-forwarded-for")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.split(',').next())
                .map(|s| s.trim().to_string())
                .unwrap_or_default();
            (ip, 0)
        });

    // Extract User Agent
    let user_agent = extract_header(request.headers(), header::USER_AGENT);

    // Extract Referrer header
    let referrer = extract_header(request.headers(), header::REFERER);

    // Extract Content-Type
    let content_type = extract_header(request.headers(), header::CONTENT_TYPE);

    // Extract Accept header
    let accept = extract_header(request.headers(), header::ACCEPT);

    // Use the matched route pattern (axum's `MatchedPath`) for the span name
    // when available, so the APM transaction view stays bounded (e.g.
    // `GET /users/{id}` instead of `GET /users/123`). Fall back to the
    // concrete path when no route matched (e.g. a reverse-proxy fallback).
    let matched_path = request.extensions().get::<axum::extract::MatchedPath>();
    let otel_name = match matched_path {
        Some(mp) => format!("{} {}", method, mp.as_str()),
        None => format!("{} {}", method, path),
    };

    // Declare and record standard OTel/ECS HTTP semantic fields.
    // `otel.kind` marks this as a server span so APM categorizes it
    // correctly in the service map (instead of defaulting to Internal).
    let span = info_span!(
        "http_request",
        "otel.kind" = "server",
        "otel.name" = otel_name,
        "http.request.method" = method,
        "http.route" = tracing::field::Empty,
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
        // Declared here (and recorded later) so `Span::record` forwards them
        // to the OTel span: tracing drops values for fields that were not
        // declared when the span was created. Unrecorded `Empty` fields are
        // omitted from the exported span, so these add no attributes to
        // spans that never get an error.
        "error.type" = tracing::field::Empty,
        "error.message" = tracing::field::Empty,
    );

    // Record the matched route pattern so the APM UI can display and group by
    // it. Only set when a route actually matched; unmatched requests get
    // `http.route = "unknown route"` at response time (see
    // `record_http_response`).
    if let Some(mp) = matched_path {
        span.record("http.route", mp.as_str());
    }

    // Continue an existing trace when the client sent W3C trace context.
    if let Some(parent) = extract_parent_context(request.headers())
        && let Err(e) = span.set_parent(parent)
    {
        tracing::debug!("Failed to set parent span context: {e}");
    }
    span
}

/// Records response attributes (status code, body size, content type) on the
/// request span.
fn record_http_response<B>(response: &Response<B>, _latency: std::time::Duration, span: &Span) {
    // Record status code
    span.record("http.response.status_code", response.status().as_u16());

    // For 404/405 responses, record `http.route` as "unknown route", mirroring
    // the Go agent's `UnknownRouteRequestName`. This is correct for requests
    // that matched no route. Note: a matched route whose handler returns
    // 404/405 (e.g. "resource not found") has its `http.route` overwritten
    // here; the span name (the APM transaction name) remains the route
    // pattern, so the transaction view is unaffected.
    if matches!(
        response.status(),
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
    ) {
        span.record("http.route", "unknown route");
    }

    // Record response size (only available when the upstream sends Content-Length)
    if let Some(content_length) = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        span.record("http.response.body.bytes", content_length);
    }

    // Record response Content-Type
    if let Some(content_type) = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
    {
        span.record("http.response.header.content-type", content_type);
    }
}

/// Creates a `TraceLayer` pre-configured to record standard Elastic APM /
/// OpenTelemetry semantic conventions for incoming HTTP requests and
/// responses.
///
/// Uses [`axum::body::Body`]; use [`apm_trace_layer_with_body`] for other
/// body types.
#[must_use]
pub fn apm_trace_layer() -> ApmTraceLayer {
    apm_trace_layer_with_body::<axum::body::Body>()
}

/// Like [`apm_trace_layer`], but for a custom request/response body type.
#[must_use]
pub fn apm_trace_layer_with_body<B>() -> ApmTraceLayer<B> {
    TraceLayer::new_for_http()
        .make_span_with(make_http_span as fn(&Request<B>) -> Span)
        .on_response(record_http_response as fn(&Response<B>, std::time::Duration, &Span))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::SUBSCRIBER_LOCK;
    use opentelemetry::global;
    use opentelemetry::trace::{SpanKind, TraceId};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;

    /// RAII guard that sets [`crate::RECORDING`] to a given value and
    /// restores the previous value on drop, so a failing assertion cannot
    /// leak state into later tests in the binary.
    struct RecordingGuard {
        previous: bool,
    }

    impl RecordingGuard {
        fn set(value: bool) -> Self {
            let previous = crate::RECORDING.swap(value, std::sync::atomic::Ordering::SeqCst);
            Self { previous }
        }
    }

    impl Drop for RecordingGuard {
        fn drop(&mut self) {
            crate::RECORDING.store(self.previous, std::sync::atomic::Ordering::SeqCst);
        }
    }

    // --- Trace layer end-to-end ---

    #[tokio::test]
    async fn http_request_span_is_exported_with_server_kind_and_otel_attributes() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        // The tower-http layer only creates `tracing` spans; the
        // tracing-opentelemetry layer of the subscriber converts them to OTel
        // spans (this mirrors what `init_apm` + `apm_otel_layer` do in
        // production).
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        // Scoped default: no permanent global state, restored on drop.
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/api/hello", get(|| async { "hi" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/api/hello")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Consume the body so the request span ends and is exported.
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        // The `otel.name` field renames the exported span to "GET /api/hello".
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /api/hello")
            .expect("http span named via otel.name should be exported");
        assert_eq!(http_span.span_kind, SpanKind::Server);

        let attr = |key: &str| {
            http_span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(attr("url.path").as_deref(), Some("/api/hello"));
        assert_eq!(attr("http.request.method").as_deref(), Some("GET"));
        assert_eq!(attr("http.response.status_code").as_deref(), Some("200"));
    }

    /// A request that matches a parameterized route must produce a span named
    /// by the *route pattern* (e.g. `GET /users/{id}`), not the concrete path
    /// (`GET /users/123`), so the APM transaction view stays bounded. The
    /// matched pattern must also be recorded as `http.route`.
    #[tokio::test]
    async fn span_is_named_by_the_matched_route_pattern() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/users/{id}", get(|| async { "ok" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/users/123")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Consume the body so the request span ends and is exported.
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /users/{id}")
            .unwrap_or_else(|| {
                panic!(
                    "expected a span named by the route pattern, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let attr = |key: &str| {
            http_span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(attr("http.route").as_deref(), Some("/users/{id}"));
        // The concrete path is still recorded for reference.
        assert_eq!(attr("url.path").as_deref(), Some("/users/123"));
    }

    /// A request that matches no route (404) has no `MatchedPath`; the span
    /// falls back to the actual path and records `http.route = "unknown
    /// route"`, mirroring the Go agent's `UnknownRouteRequestName`.
    #[tokio::test]
    async fn unmatched_route_records_unknown_route() {
        use axum::Router;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        // A router with no matching route returns 404.
        let app = Router::new()
            .route("/known", axum::routing::get(|| async { "ok" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/definitely/not/a/route")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /definitely/not/a/route")
            .unwrap_or_else(|| {
                panic!(
                    "expected a span named by the actual path, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let attr = |key: &str| {
            http_span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(attr("http.route").as_deref(), Some("unknown route"));
    }

    /// When `ELASTIC_APM_RECORDING` (or `ELASTIC_APM_ACTIVE`) is disabled, the
    /// trace layer must be a zero-overhead no-op: no span is created or
    /// exported, mirroring the Go agent's `Recording()` short-circuit.
    #[tokio::test]
    async fn trace_layer_is_a_no_op_when_recording_is_disabled() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        // Disable recording for the duration of this test (the guard restores
        // the previous value on drop, even if an assertion below fails).
        let _recording = RecordingGuard::set(false);

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/no-op", get(|| async { "ok" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/no-op")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        // No span should be exported (the layer is a no-op).
        let spans = exporter.get_finished_spans().unwrap();
        assert!(
            spans.is_empty(),
            "no span should be exported when recording is disabled, got: {:?}",
            spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
        );
    }

    /// End-to-end W3C propagation: a request carrying a sampled `traceparent`
    /// must produce an exported span that continues the *remote* trace
    /// (same trace ID, remote parent span ID) instead of starting a new one.
    #[tokio::test]
    async fn incoming_sampled_traceparent_continues_the_remote_trace() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        // extract_parent_context reads the global propagator (init_apm
        // installs it in production).
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/remote", get(|| async { "ok" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/remote")
            .header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Consume the body so the request span ends and is exported.
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /remote")
            .expect("http span should be exported");
        // The span must continue the remote trace, not start a new one.
        assert_eq!(
            http_span.span_context.trace_id(),
            TraceId::from(0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736u128)
        );
        // And the remote span must be recorded as its (remote) parent.
        assert_eq!(
            http_span.parent_span_id,
            opentelemetry::trace::SpanId::from(0x00f0_67aa_0ba9_02b7u64)
        );
        assert!(http_span.parent_span_is_remote);
    }

    /// End-to-end W3C propagation: a request carrying an *unsampled*
    /// `traceparent` must be dropped by the sampler (no exported span).
    #[tokio::test]
    async fn incoming_unsampled_traceparent_drops_the_request_span() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/remote", get(|| async { "ok" }))
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/remote")
            // Same trace as above, but the sampled flag is 0.
            .header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Consume the body so any request span would end and be exported.
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert!(
            !spans.iter().any(|s| s.name == "GET /remote"),
            "an unsampled remote parent must drop the request span, got: {:?}",
            spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
        );
    }
}
