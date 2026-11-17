use crate::config::{
    ApmConfig, ApmError, parse_bool_env, parse_sample_rate, parse_url_patterns, resolve_node_name,
};
use crate::sampling::ApmSampler;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use std::collections::HashMap;
use std::sync::Arc;

/// Initializes the OpenTelemetry tracing provider for Elastic APM.
///
/// Builds the OTLP/HTTP exporter and `SdkTracerProvider` from the
/// `ELASTIC_APM_*` environment variables, registers the provider globally,
/// and installs the W3C TraceContext propagator.
///
/// This function does **not** install a `tracing` subscriber: compose
/// [`apm_otel_layer`] into your application's own subscriber so existing
/// logging setup keeps working. It also does not log anything itself — it
/// runs before your subscriber exists — so log
/// [`ApmConfig::warnings`] after installing the subscriber.
///
/// Returns the `SdkTracerProvider` (keep it alive for the duration of the
/// application and shut it down on exit to flush remaining spans) together
/// with the parsed [`ApmConfig`].
///
/// # Errors
///
/// Returns [`ApmError::ExporterBuild`] when the OTLP exporter cannot be
/// built (e.g. an unparseable `ELASTIC_APM_SERVER_URL`).
pub fn init_apm() -> Result<(SdkTracerProvider, ApmConfig), ApmError> {
    let service_name =
        std::env::var("ELASTIC_APM_SERVICE_NAME").unwrap_or_else(|_| "rust-service".to_string());
    let service_version =
        std::env::var("ELASTIC_APM_SERVICE_VERSION").unwrap_or_else(|_| "1.0.0".to_string());
    let apm_environment =
        std::env::var("ELASTIC_APM_ENVIRONMENT").unwrap_or_else(|_| "production".to_string());
    let apm_server_url = std::env::var("ELASTIC_APM_SERVER_URL")
        .unwrap_or_else(|_| "http://localhost:8200".to_string());

    // Resolve node name: ELASTIC_APM_NODE_NAME if set, else system hostname
    let apm_node_name = resolve_node_name();

    // Resolve the recording switch: both ELASTIC_APM_ACTIVE and
    // ELASTIC_APM_RECORDING default to true; the agent records only when both
    // are enabled. When disabled, the trace layer becomes a zero-overhead
    // no-op (see `make_http_span`).
    let active = parse_bool_env("ELASTIC_APM_ACTIVE", true);
    let recording = parse_bool_env("ELASTIC_APM_RECORDING", true);
    crate::RECORDING.store(active && recording, std::sync::atomic::Ordering::SeqCst);

    // Construct the OTLP HTTP traces target endpoint (Elastic APM listens on /v1/traces)
    let traces_endpoint = format!("{}/v1/traces", apm_server_url.trim_end_matches('/'));

    // Extract authorization credentials if provided in ELASTIC_APM_SECRET_TOKEN or ELASTIC_APM_API_KEY
    let mut headers = HashMap::new();
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

    // 1. Build the OTLP Span Exporter using HTTP
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(traces_endpoint)
        .with_headers(headers)
        .build()?;

    // Build resource attributes
    let mut attributes = vec![
        KeyValue::new("service.version", service_version.clone()),
        KeyValue::new("deployment.environment", apm_environment.clone()),
    ];
    if let Some(node_name) = apm_node_name.clone() {
        attributes.push(KeyValue::new("service.node.name", node_name));
    }

    // 2. Parse transaction sampling configuration
    let ignore_patterns =
        parse_url_patterns(std::env::var("ELASTIC_APM_TRANSACTION_IGNORE_URLS").ok());
    let (sample_rate, warnings) =
        parse_sample_rate(std::env::var("ELASTIC_APM_TRANSACTION_SAMPLE_RATE").ok());

    // 3. Build the TracerProvider using a batch processor with custom sampler
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .with_attributes(attributes)
                .build(),
        )
        .with_sampler(ApmSampler {
            ignore_patterns: Arc::new(ignore_patterns.clone()),
            sample_rate,
        })
        .build();

    // 4. Set global tracer provider
    global::set_tracer_provider(provider.clone());

    // Install the W3C TraceContext propagator so incoming `traceparent`
    // headers are honored and outgoing requests carry the trace context.
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let config = ApmConfig {
        service_name,
        service_version,
        environment: apm_environment,
        node_name: apm_node_name,
        sample_rate,
        ignore_patterns,
        warnings,
    };

    Ok((provider, config))
}

/// Returns the `tracing-opentelemetry` layer for `provider`, converting
/// `tracing` spans into OpenTelemetry spans.
///
/// The tracer is registered under the instrumentation scope name
/// `"axum-apm"`; use [`apm_otel_layer_with_tracer_name`] to choose a
/// different one.
///
/// Compose it into your application's subscriber (alongside your own
/// `EnvFilter` and formatting layers):
///
/// ```ignore
/// let (provider, config) = axum_apm::init_apm()?;
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::EnvFilter::try_from_default_env()
///         .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
///     .with(tracing_subscriber::fmt::layer())
///     .with(axum_apm::apm_otel_layer(&provider))
///     .init();
/// for warning in &config.warnings {
///     tracing::warn!("{warning}");
/// }
/// ```
#[must_use]
pub fn apm_otel_layer<S>(
    provider: &SdkTracerProvider,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    apm_otel_layer_with_tracer_name(provider, "axum-apm")
}

/// Like [`apm_otel_layer`], but registers the tracer under a custom
/// instrumentation scope name (shown as the scope in exported spans).
#[must_use]
pub fn apm_otel_layer_with_tracer_name<S>(
    provider: &SdkTracerProvider,
    tracer_name: impl Into<std::borrow::Cow<'static, str>>,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry::trace::TracerProvider;
    tracing_opentelemetry::layer().with_tracer(provider.tracer(tracer_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apm_trace_layer;
    use crate::test_support::{ENV_LOCK, SUBSCRIBER_LOCK};
    use axum::extract::Request;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn init_apm_reports_config_warnings_in_the_returned_config() {
        // init_apm runs before any subscriber exists, so config problems
        // must surface in the returned ApmConfig — not in dropped log lines.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "0,5") };
        let result = init_apm();
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("ELASTIC_APM_TRANSACTION_SAMPLE_RATE") };

        let (provider, config) = result.expect("init_apm should succeed");
        assert!(
            config
                .warnings
                .iter()
                .any(|w| w.contains("ELASTIC_APM_TRANSACTION_SAMPLE_RATE")),
            "invalid sample rate should produce a warning in the config, got: {:?}",
            config.warnings
        );
        // The effective rate must be the safe fallback, not the typo.
        assert_eq!(config.sample_rate, 1.0);
        // No APM server is reachable in tests; shutdown may time out.
        let _ = provider.shutdown();
    }

    #[tokio::test]
    async fn apm_otel_layer_exports_spans_when_composed_into_a_subscriber() {
        use axum::Router;
        use axum::routing::get;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();

        // The application composes the layer into its own subscriber.
        let subscriber = tracing_subscriber::registry().with(apm_otel_layer(&provider));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(apm_trace_layer());
        let req = Request::builder()
            .uri("/x")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        // Consume the body so the request span ends and is exported.
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert!(
            spans.iter().any(|s| s.name == "GET /x"),
            "expected an exported http span, got: {:?}",
            spans.iter().map(|s| &s.name).collect::<Vec<_>>()
        );
    }
}
