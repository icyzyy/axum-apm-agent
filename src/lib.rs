//! # Elastic APM/OpenTelemetry Tracing Library for Axum
//!
//! This library provides helper functions to configure OpenTelemetry tracing for Elastic APM
//! in Axum applications (both REST APIs and reverse proxies).
//!
//! It supports standard Elastic APM environment variables:
//! - `ELASTIC_APM_SERVER_URL` (default: `http://localhost:8200`)
//! - `ELASTIC_APM_SERVICE_NAME` (default: `rust-service`)
//! - `ELASTIC_APM_SERVICE_VERSION` (default: `1.0.0`)
//! - `ELASTIC_APM_ENVIRONMENT` (default: `production`)
//! - `ELASTIC_APM_NODE_NAME` (node name; defaults to the system hostname)
//! - `ELASTIC_APM_SECRET_TOKEN` (authorization bearer token)
//! - `ELASTIC_APM_API_KEY` (authorization api key)
//! - `ELASTIC_APM_TRANSACTION_IGNORE_URLS` (comma-separated URL patterns to exclude from tracing)
//! - `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` (float 0.0-1.0, sampling rate for transactions)
//! - `ELASTIC_APM_ACTIVE` (bool, default `true`; when `false` the trace layer is a no-op)
//! - `ELASTIC_APM_RECORDING` (bool, default `true`; when `false` the trace layer is a no-op)
//!
//! # W3C trace context propagation
//!
//! `init_apm` installs the W3C `TraceContext` propagator globally, which is
//! what lets [`apm_trace_layer`] continue an incoming `traceparent` and lets
//! [`proxy_handler`] forward it to the upstream. If you build your own
//! `SdkTracerProvider` and skip `init_apm`, install a propagator yourself
//! (e.g. `opentelemetry_sdk::propagation::TraceContextPropagator`) or trace
//! propagation will silently no-op.

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{
        HeaderMap, StatusCode, Uri, Version,
        header::{self, HeaderName, HeaderValue},
    },
    response::{IntoResponse, Response},
};
use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TraceState};
use opentelemetry::{KeyValue, Value, global};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::{SamplingDecision, SamplingResult, ShouldSample};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use regex::Regex;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use tower::Service;
use tower_http::trace::TraceLayer;
use tracing::{Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Application state containing the HTTP client and upstream configuration.
#[derive(Clone)]
pub struct AppState {
    pub client: ProxyClient,
    pub upstream_url: Arc<String>,
}

/// Helper function to extract a header value as a string.
fn extract_header(headers: &HeaderMap, header_name: HeaderName) -> String {
    headers
        .get(header_name)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// `Injector` that writes propagated fields into outgoing request headers.
struct HeaderInjector<'a> {
    headers: &'a mut HeaderMap,
}

impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.headers.insert(name, val);
        }
    }
}

/// `Extractor` that reads propagated fields from incoming request headers.
struct HeaderExtractor<'a> {
    headers: &'a HeaderMap,
}

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|k| k.as_str()).collect()
    }
}

/// Extracts the W3C trace context (`traceparent`/`tracestate`) from incoming
/// request headers so the request can continue an existing trace.
///
/// Returns `None` when no valid remote span context is present.
fn extract_parent_context(headers: &HeaderMap) -> Option<opentelemetry::Context> {
    let cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor { headers })
    });
    cx.has_active_span().then_some(cx)
}

/// Injects the W3C trace context of `cx` into outgoing request headers
/// (`traceparent`/`tracestate`) so the upstream service continues the same trace.
fn inject_trace_context(cx: &opentelemetry::Context, req: &mut Request) {
    global::get_text_map_propagator(|propagator| {
        let mut injector = HeaderInjector {
            headers: req.headers_mut(),
        };
        propagator.inject_context(cx, &mut injector);
    });
}

/// Thread-safe cache for compiled regex patterns used in URL ignore matching.
/// Avoids recompiling the same regex on every sampling decision.
static REGEX_CACHE: OnceLock<
    std::sync::Mutex<HashMap<String, std::result::Result<Regex, regex::Error>>>,
> = OnceLock::new();

/// Whether APM recording is enabled. Set from `ELASTIC_APM_ACTIVE` and
/// `ELASTIC_APM_RECORDING` (both default `true`) by [`init_apm`]. When `false`,
/// [`make_http_span`] returns a no-op span so the trace layer is zero-overhead,
/// mirroring the Go agent's `Recording()` short-circuit.
static RECORDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// The effective APM configuration parsed from the `ELASTIC_APM_*`
/// environment variables, as returned by [`init_apm`].
///
/// `init_apm` runs before your `tracing` subscriber exists, so it cannot
/// log configuration problems itself: log [`warnings`](Self::warnings)
/// after installing the subscriber.
#[derive(Debug, Clone)]
pub struct ApmConfig {
    /// `ELASTIC_APM_SERVICE_NAME` (default `rust-service`).
    pub service_name: String,
    /// `ELASTIC_APM_SERVICE_VERSION` (default `1.0.0`).
    pub service_version: String,
    /// `ELASTIC_APM_ENVIRONMENT` (default `production`).
    pub environment: String,
    /// `ELASTIC_APM_NODE_NAME`, or the system hostname when unset/blank.
    pub node_name: Option<String>,
    /// Effective transaction sample rate (always a finite value in `0.0..=1.0`).
    pub sample_rate: f64,
    /// Effective `ELASTIC_APM_TRANSACTION_IGNORE_URLS` patterns.
    pub ignore_patterns: Vec<String>,
    /// Human-readable warnings from configuration parsing (e.g. an invalid
    /// sample rate that fell back to `1.0`). Empty when the configuration
    /// is valid.
    pub warnings: Vec<String>,
}

/// Parses the `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` value.
///
/// Returns `(rate, warnings)`. The rate defaults to `1.0` when the variable
/// is unset, and also falls back to `1.0` (with a warning in the returned
/// vector) when the value is not a finite number in `0.0..=1.0`, so a typo
/// can never silently change the sampling behavior.
fn parse_sample_rate(raw: Option<String>) -> (f64, Vec<String>) {
    match raw {
        None => (1.0, Vec::new()),
        Some(s) => match s.trim().parse::<f64>() {
            Ok(v) if v.is_finite() && (0.0..=1.0).contains(&v) => (v, Vec::new()),
            Ok(v) => (
                1.0,
                vec![format!(
                    "Invalid ELASTIC_APM_TRANSACTION_SAMPLE_RATE {v} (must be a finite number between 0.0 and 1.0); falling back to 1.0"
                )],
            ),
            Err(e) => (
                1.0,
                vec![format!(
                    "Failed to parse ELASTIC_APM_TRANSACTION_SAMPLE_RATE {s:?}: {e}; falling back to 1.0"
                )],
            ),
        },
    }
}

/// Errors returned by the fallible entry points of this library.
#[derive(Debug, thiserror::Error)]
pub enum ApmError {
    /// `UPSTREAM_URL` is not set.
    #[error("UPSTREAM_URL environment variable is required (e.g. https://api.example.com)")]
    UpstreamUrlMissing,

    /// `UPSTREAM_URL` is not a parseable URL.
    #[error("UPSTREAM_URL {url:?} is not a valid URL: {source}")]
    UpstreamUrlInvalid {
        url: String,
        #[source]
        source: http::uri::InvalidUri,
    },

    /// `UPSTREAM_URL` is not an absolute URL with a scheme and host.
    #[error(
        "UPSTREAM_URL {url:?} must be an absolute URL with a scheme and host (e.g. https://api.example.com)"
    )]
    UpstreamUrlNotAbsolute { url: String },

    /// `PORT` is set but is not a valid port number.
    #[error("PORT {port:?} is not a valid port number: {source}")]
    PortInvalid {
        port: String,
        #[source]
        source: std::num::ParseIntError,
    },

    /// The OTLP exporter could not be built.
    #[error("failed to build the OTLP exporter: {0}")]
    ExporterBuild(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Reads and validates the `UPSTREAM_URL` environment variable.
///
/// Fails when the variable is unset, is not a valid URL, or is not an
/// absolute URL with a scheme and host (e.g. `https://api.example.com`), so a
/// misconfigured proxy never silently forwards traffic to an unintended host.
///
/// # Errors
///
/// Returns an [`ApmError`] naming the problem when `UPSTREAM_URL` is unset,
/// unparseable, or missing a scheme/host.
pub fn resolve_upstream_url() -> Result<String, ApmError> {
    let url = std::env::var("UPSTREAM_URL").map_err(|_| ApmError::UpstreamUrlMissing)?;
    let uri: Uri = url.parse().map_err(|source| ApmError::UpstreamUrlInvalid {
        url: url.clone(),
        source,
    })?;
    if uri.scheme_str().is_none() || uri.host().is_none() {
        return Err(ApmError::UpstreamUrlNotAbsolute { url });
    }
    Ok(url)
}

/// Reads the `PORT` environment variable.
///
/// Defaults to `3000` when unset or blank; fails when set to a value that is
/// not a valid port number, so a typo cannot silently move the listener.
///
/// # Errors
///
/// Returns [`ApmError::PortInvalid`] when `PORT` is set to a non-numeric or
/// out-of-range value.
pub fn resolve_port() -> Result<u16, ApmError> {
    match std::env::var("PORT") {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<u16>()
            .map_err(|source| ApmError::PortInvalid { port: raw, source }),
        _ => Ok(3000),
    }
}

/// Resolves the APM node name: the `ELASTIC_APM_NODE_NAME` environment
/// variable when set to a non-blank value, otherwise the system hostname.
fn resolve_node_name() -> Option<String> {
    if let Ok(name) = std::env::var("ELASTIC_APM_NODE_NAME") {
        let name = name.trim().to_string();
        if !name.is_empty() {
            return Some(name);
        }
    }
    hostname::get()
        .ok()
        .map(|h| h.to_string_lossy().into_owned())
}

/// Parses a boolean environment variable.
///
/// Returns `default` when the variable is unset or blank. Recognizes the
/// common truthy/falsy spellings (`true`/`false`, `1`/`0`, `yes`/`no`,
/// `on`/`off`, case-insensitive); any other non-blank value falls back to
/// `default` (so a typo cannot silently flip the switch).
fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" => default,
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

/// Parses a comma-separated string of URL patterns into a Vec<String>.
/// Patterns can be exact matches, glob patterns (with * and ?), or regex.
fn parse_url_patterns(patterns_env: Option<String>) -> Vec<String> {
    patterns_env
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Retrieves or initializes the global regex compilation cache.
///
/// This runs on the per-span hot path, so a poisoned mutex is recovered from
/// (the critical section only compiles/matches regexes and cannot corrupt the
/// map) instead of panicking every subsequent span creation.
fn get_regex_cache()
-> std::sync::MutexGuard<'static, HashMap<String, std::result::Result<Regex, regex::Error>>> {
    REGEX_CACHE
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Converts a glob-style URL pattern to a regex pattern string.
/// '*' matches any sequence of characters, '?' matches a single character.
/// For patterns starting with '^' or ending with '$', treats as raw regex.
fn glob_to_regex(pattern: &str) -> std::result::Result<Regex, regex::Error> {
    if pattern.starts_with('^') || pattern.ends_with('$') {
        Regex::new(pattern)
    } else {
        let mut re = String::from("^");
        for c in pattern.chars() {
            match c {
                '*' => re.push_str(".*"),
                '?' => re.push('.'),
                c if ".*+{}()|[]\\^$".contains(c) => {
                    re.push('\\');
                    re.push(c);
                }
                c => re.push(c),
            }
        }
        // Anchor the end so patterns without a trailing wildcard match exactly
        // (e.g. `/health` must not match `/healthcheck`).
        re.push('$');
        Regex::new(&re)
    }
}

/// Checks if a given URL path matches any of the configured ignore patterns.
/// Uses a global mutex-protected cache to avoid compiling the same regex repeatedly.
fn url_patterns_match(url_path: &str, patterns: &[String]) -> bool {
    let mut cache = get_regex_cache();
    for pattern in patterns {
        let re = match cache.get(pattern) {
            Some(res) => res.clone(),
            None => {
                let res = glob_to_regex(pattern);
                cache.insert(pattern.clone(), res.clone());
                res
            }
        };
        match re {
            Ok(compiled) => {
                if compiled.is_match(url_path) {
                    return true;
                }
            }
            Err(_) => {
                if pattern == url_path {
                    return true;
                }
            }
        }
    }
    false
}

/// Extracts the URL path from span attributes, checking common OpenTelemetry
/// semantic conventions: `url.path` (set by [`apm_trace_layer`]), plus the
/// legacy `http.target` / `http.route` for spans created elsewhere.
fn extract_url_path_from_attrs(attributes: &[KeyValue]) -> Option<String> {
    for kv in attributes {
        let Value::String(s) = &kv.value else {
            continue;
        };
        if !matches!(kv.key.as_str(), "url.path" | "http.target" | "http.route") {
            continue;
        }
        let path = s.as_ref();
        if let Some(idx) = path.rfind('?') {
            return Some(path[..idx].to_string());
        }
        return Some(path.to_string());
    }
    None
}

/// Custom sampler that ignores specified URL patterns and applies a configurable
/// sampling rate. Respects parent span sampling decisions for distributed tracing.
#[derive(Clone)]
struct ApmSampler {
    ignore_patterns: Arc<Vec<String>>,
    sample_rate: f64,
}

impl std::fmt::Debug for ApmSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApmSampler")
            .field("ignore_patterns", &self.ignore_patterns)
            .field("sample_rate", &self.sample_rate)
            .finish()
    }
}

/// Compares a probability against the trace ID using the same algorithm as
/// the OpenTelemetry SDK's trace_id_ratio_based sampler.
fn sample_based_on_probability(prob: f64, trace_id: TraceId) -> SamplingDecision {
    if prob >= 1.0 {
        SamplingDecision::RecordAndSample
    } else if prob <= 0.0 {
        SamplingDecision::Drop
    } else {
        let prob_upper_bound = (prob * (1u64 << 63) as f64) as u64;
        let bytes = trace_id.to_bytes();
        let (_, low) = bytes.split_at(8);
        // Invariant: a 16-byte trace ID always yields an 8-byte low half.
        let trace_id_low = u64::from_be_bytes(
            low.try_into()
                .expect("low half of a trace ID is always 8 bytes"),
        );
        let rnd = trace_id_low >> 1;
        if rnd < prob_upper_bound {
            SamplingDecision::RecordAndSample
        } else {
            SamplingDecision::Drop
        }
    }
}

impl ShouldSample for ApmSampler {
    fn should_sample(
        &self,
        parent_context: Option<&opentelemetry::Context>,
        trace_id: TraceId,
        _name: &str,
        _span_kind: &SpanKind,
        attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let has_parent = parent_context.is_some_and(|cx| cx.has_active_span());

        if has_parent {
            let parent_sampled = parent_context
                .map(|cx| cx.span().span_context().is_sampled())
                .unwrap_or(false);

            if parent_sampled {
                return SamplingResult {
                    decision: SamplingDecision::RecordAndSample,
                    attributes: Vec::new(),
                    trace_state: parent_context
                        .map(|cx| cx.span().span_context().trace_state().clone())
                        .unwrap_or_default(),
                };
            } else {
                return SamplingResult {
                    decision: SamplingDecision::Drop,
                    attributes: Vec::new(),
                    trace_state: parent_context
                        .map(|cx| cx.span().span_context().trace_state().clone())
                        .unwrap_or_default(),
                };
            }
        }

        if !self.ignore_patterns.is_empty()
            && let Some(url_path) = extract_url_path_from_attrs(attributes)
            && url_patterns_match(&url_path, &self.ignore_patterns)
        {
            return SamplingResult {
                decision: SamplingDecision::Drop,
                attributes: Vec::new(),
                trace_state: TraceState::default(),
            };
        }

        let decision = sample_based_on_probability(self.sample_rate, trace_id);
        SamplingResult {
            decision,
            attributes: Vec::new(),
            trace_state: TraceState::default(),
        }
    }
}

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
    RECORDING.store(active && recording, std::sync::atomic::Ordering::SeqCst);

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
    if !RECORDING.load(std::sync::atomic::Ordering::Relaxed) {
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
        // spans that never get an error or are not proxied requests.
        "error.type" = tracing::field::Empty,
        "error.message" = tracing::field::Empty,
        "upstream_uri" = tracing::field::Empty,
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

/// A tower [`Layer`](tower::Layer) that recovers panics from the wrapped
/// service, records them as APM errors on the current span, and returns a
/// `500 Internal Server Error` response instead of dropping the connection.
///
/// Apply it *inside* [`apm_trace_layer`] so the APM span is active when a
/// panic is caught:
///
/// ```ignore
/// let app = Router::new()
///     .route("/users/{id}", get(handler))
///     .layer(axum_apm::apm_panic_recovery_layer()) // inner: recovers panics
///     .layer(axum_apm::apm_trace_layer());         // outer: creates the span
/// ```
///
/// This mirrors the Go agent's `apmechov4` middleware, which recovers panics
/// and reports them to APM (and can replace the framework's own recover).
#[derive(Clone, Debug, Default)]
pub struct ApmPanicRecoveryLayer;

impl<S> tower::Layer<S> for ApmPanicRecoveryLayer {
    type Service = ApmPanicRecoveryService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ApmPanicRecoveryService { inner }
    }
}

/// The [`Service`] produced by [`ApmPanicRecoveryLayer`].
///
/// Panics raised while polling the request future are recovered; note the
/// limits of that protection:
///
/// - `poll_ready` is forwarded to the inner service *without* panic
///   protection (a panic there propagates to the caller — harmless for
///   axum's `Router`, whose `poll_ready` is infallible).
/// - The `S::Error = Infallible` bound means this layer cannot wrap services
///   that report errors (e.g. `tower::timeout::Timeout` or
///   `tower::limit::ConcurrencyLimit`).
/// - Panics raised while *dropping* the inner future (e.g. on client
///   disconnect) are not caught; that is inherent to `catch_unwind` (the
///   same limitation as Go's `recover`).
#[derive(Clone, Debug)]
pub struct ApmPanicRecoveryService<S> {
    inner: S,
}

impl<S> Service<Request> for ApmPanicRecoveryService<S>
where
    S: Service<Request, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    <S as Service<Request>>::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = ApmPanicRecoveryFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner_future = self.inner.call(req);
        let boxed: BoxedResponseFuture = Box::pin(inner_future);
        ApmPanicRecoveryFuture::new(boxed)
    }
}

/// A boxed, `Send` future producing an HTTP response (the inner service's
/// future, boxed so [`ApmPanicRecoveryFuture`] is always `Unpin`).
type BoxedResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response, std::convert::Infallible>> + Send>>;

/// A future that wraps an inner future and catches any panic it raises,
/// converting the panic into a recorded APM error plus a `500` response.
///
/// Not directly constructible; produced by [`ApmPanicRecoveryService::call`].
///
/// The inner future is boxed so this wrapper is always `Unpin` (the inner
/// future may not be).
pub struct ApmPanicRecoveryFuture {
    inner: Option<BoxedResponseFuture>,
}

impl ApmPanicRecoveryFuture {
    fn new(inner: BoxedResponseFuture) -> Self {
        Self { inner: Some(inner) }
    }
}

impl Future for ApmPanicRecoveryFuture {
    type Output = Result<Response, std::convert::Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let inner = this
            .inner
            .as_mut()
            .expect("ApmPanicRecoveryFuture polled after completion");
        // Catch a panic raised while polling the inner future. `AssertUnwindSafe`
        // is required because `&mut Context` is not `UnwindSafe`; the panic is
        // fully contained (we do not resume the inner future).
        let result = panic::catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx)));
        match result {
            Ok(Poll::Ready(Ok(response))) => {
                this.inner = None;
                Poll::Ready(Ok(response))
            }
            // Unreachable: the inner service's error is `Infallible`.
            Ok(Poll::Ready(Err(e))) => match e {},
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                this.inner = None;
                record_panic_on_current_span(&payload);
                let response =
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
                Poll::Ready(Ok(response))
            }
        }
    }
}

/// Records a caught panic on the current [`Span`] (the APM request span when
/// this layer is composed inside [`apm_trace_layer`]): sets the error fields
/// and emits an `exception` span event, mirroring the Go agent's panic
/// reporting.
fn record_panic_on_current_span(payload: &Box<dyn std::any::Any + Send>) {
    let span = Span::current();
    let message = extract_panic_message(payload);
    span.record("error.type", "#Panic");
    span.record("error.message", message.as_str());
    tracing::error!(
        parent: &span,
        exception.type = "#Panic",
        exception.message = message.as_str(),
        "exception"
    );
}

/// Extracts a human-readable message from a panic payload (the `&str` or
/// `String` message when available, otherwise a generic description).
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "handler panicked".to_string()
    }
}

/// Creates an [`ApmPanicRecoveryLayer`]. See the type's docs for usage.
#[must_use]
pub fn apm_panic_recovery_layer() -> ApmPanicRecoveryLayer {
    ApmPanicRecoveryLayer
}

/// Type alias for the Hyper legacy client used to forward requests.
pub type ProxyClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    axum::body::Body,
>;

/// Default handler for proxying incoming HTTP requests to an upstream target.
///
/// Uses the upstream URL from `AppState` instead of reading environment variables
/// on each request for better performance.
///
/// The resolved `upstream_uri` is recorded on the *current* APM span (the
/// request span created by [`apm_trace_layer`]) rather than on a dedicated
/// span of its own, so that no extra span is exported per proxied request and
/// nothing is exported when recording is disabled.
pub async fn proxy_handler(
    State(state): State<AppState>,
    mut req: Request,
) -> Result<Response, StatusCode> {
    // Retrieve path and query from the incoming request URI
    let path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or("/");

    // Construct the destination URI pointing to the upstream server
    let uri_str = format!("{}{}", state.upstream_url.trim_end_matches('/'), path_query);
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

    // Propagate the trace context to the upstream so it continues the same trace
    let cx = Span::current().context();
    inject_trace_context(&cx, &mut req);

    // Forward the request to the upstream using the hyper client
    let response = state.client.request(req).await.map_err(|e| {
        tracing::error!("Failed to proxy request: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    Ok(response.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    fn patterns(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// RAII guard that sets [`RECORDING`] to a given value and restores the
    /// previous value on drop, so a failing assertion cannot leak state into
    /// later tests in the binary.
    struct RecordingGuard {
        previous: bool,
    }

    impl RecordingGuard {
        fn set(value: bool) -> Self {
            let previous = RECORDING.swap(value, std::sync::atomic::Ordering::SeqCst);
            Self { previous }
        }
    }

    impl Drop for RecordingGuard {
        fn drop(&mut self) {
            RECORDING.store(self.previous, std::sync::atomic::Ordering::SeqCst);
        }
    }

    // --- URL ignore pattern matching ---

    #[test]
    fn exact_pattern_matches_only_the_exact_path() {
        let p = patterns(&["/health"]);
        assert!(url_patterns_match("/health", &p));
        assert!(!url_patterns_match("/healthcheck", &p));
        assert!(!url_patterns_match("/health/live", &p));
        assert!(!url_patterns_match("/x/health", &p));
    }

    #[test]
    fn glob_star_matches_the_whole_subtree() {
        let p = patterns(&["/status/*"]);
        assert!(url_patterns_match("/status/anything", &p));
        assert!(url_patterns_match("/status/a/b", &p));
        assert!(!url_patterns_match("/status", &p));
        assert!(!url_patterns_match("/statusx/anything", &p));
    }

    #[test]
    fn glob_question_mark_matches_exactly_one_character() {
        let p = patterns(&["/status/?"]);
        assert!(url_patterns_match("/status/a", &p));
        assert!(!url_patterns_match("/status/ab", &p));
        assert!(!url_patterns_match("/status/", &p));
    }

    #[test]
    fn regex_pattern_is_used_verbatim() {
        let p = patterns(&["^/api/v1/health.*"]);
        assert!(url_patterns_match("/api/v1/health", &p));
        assert!(url_patterns_match("/api/v1/health/deep", &p));
        assert!(!url_patterns_match("/api/v2/health", &p));
    }

    #[test]
    fn unmatched_path_returns_false() {
        let p = patterns(&["/health"]);
        assert!(!url_patterns_match("/api/users", &p));
    }

    // --- Trace-id-ratio sampling ---

    fn sampler(rate: f64, ignore: Vec<String>) -> ApmSampler {
        ApmSampler {
            ignore_patterns: Arc::new(ignore),
            sample_rate: rate,
        }
    }

    #[test]
    fn ratio_one_samples_every_trace() {
        for i in 0..100u128 {
            let tid = TraceId::from(i << 64);
            assert_eq!(
                sample_based_on_probability(1.0, tid),
                SamplingDecision::RecordAndSample
            );
        }
    }

    #[test]
    fn ratio_zero_drops_every_trace() {
        for i in 0..100u128 {
            let tid = TraceId::from(i << 64);
            assert_eq!(
                sample_based_on_probability(0.0, tid),
                SamplingDecision::Drop
            );
        }
    }

    #[test]
    fn ratio_decision_is_deterministic_per_trace_id() {
        let tid = TraceId::from(0x0123_4567_89ab_cdefu128);
        let a = sample_based_on_probability(0.5, tid);
        let b = sample_based_on_probability(0.5, tid);
        assert_eq!(a, b);
    }

    // --- Parent decision respect ---

    fn parent_context(sampled: bool) -> opentelemetry::Context {
        use opentelemetry::testing::trace::TestSpan;
        let flags = if sampled {
            opentelemetry::trace::TraceFlags::SAMPLED
        } else {
            opentelemetry::trace::TraceFlags::default()
        };
        let sc = opentelemetry::trace::SpanContext::new(
            TraceId::from(0x4bf9_2f35_77b3_4da6u128),
            opentelemetry::trace::SpanId::from(0x00f0_67aa_0ba9_02b7u64),
            flags,
            true,
            TraceState::default(),
        );
        opentelemetry::Context::new().with_span(TestSpan(sc))
    }

    #[test]
    fn sampled_parent_forces_sample_even_at_zero_rate() {
        let s = sampler(0.0, vec![]);
        let res = s.should_sample(
            Some(&parent_context(true)),
            TraceId::from(1u128),
            "GET /x",
            &SpanKind::Server,
            &[],
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::RecordAndSample);
    }

    #[test]
    fn unsampled_parent_forces_drop_even_at_full_rate() {
        let s = sampler(1.0, vec![]);
        let res = s.should_sample(
            Some(&parent_context(false)),
            TraceId::from(1u128),
            "GET /x",
            &SpanKind::Server,
            &[],
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::Drop);
    }

    // --- Ignore patterns at the sampler level ---

    #[test]
    fn ignored_root_url_is_dropped_before_rate_applies() {
        let s = sampler(1.0, vec!["/health".to_string()]);
        let attrs = vec![KeyValue::new("url.path", "/health")];
        let res = s.should_sample(
            None,
            TraceId::from(1u128),
            "GET /health",
            &SpanKind::Server,
            &attrs,
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::Drop);
    }

    // --- Trace layer end-to-end ---

    // Tests that install a default `tracing` subscriber must hold this lock:
    // the default subscriber is process-global and the tests run in parallel.
    // Async-aware because the guard is held across await points.
    static SUBSCRIBER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn http_request_span_is_exported_with_server_kind_and_otel_attributes() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

    /// A handler that panics must not drop the connection: the panic is
    /// recovered into a 500 response, and the panic is recorded on the APM
    /// span (status Error + an `exception` event), mirroring the Go agent's
    /// panic-recovery behavior.
    #[tokio::test]
    async fn panic_in_handler_is_recovered_and_recorded_as_an_error() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::{Status, TracerProvider as _};
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

        async fn panicking_handler() -> &'static str {
            panic!("boom");
        }

        // The panic-recovery layer sits *inside* the trace layer so the APM
        // span is active when the panic is caught.
        let app = Router::new()
            .route("/panic", get(panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        // The panic is recovered into a 500 response (connection not dropped).
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        // The span must be marked as an error.
        assert!(
            matches!(http_span.status, Status::Error { .. }),
            "expected the span status to be Error, got: {:?}",
            http_span.status
        );
        // The panic must be recorded as an `exception` span event.
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(ev_attr("exception.message").as_deref(), Some("boom"));
        // The span itself carries the error fields (the APM error view).
        let span_attr = |key: &str| {
            http_span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(span_attr("error.type").as_deref(), Some("#Panic"));
        assert_eq!(span_attr("error.message").as_deref(), Some("boom"));
    }

    /// A panic raised on a *later* poll (after the handler has already
    /// returned `Pending`) must also be recovered: `catch_unwind` wraps every
    /// poll of the inner future, not just the first.
    #[tokio::test]
    async fn panic_on_a_later_poll_is_recovered() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

        async fn late_panicking_handler() -> &'static str {
            // Suspend first so the panic happens on a later poll.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            panic!("late boom");
        }

        let app = Router::new()
            .route("/late-panic", get(late_panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/late-panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /late-panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(ev_attr("exception.message").as_deref(), Some("late boom"));
    }

    /// A panic with a non-string payload (e.g. `panic!(42)`) still produces a
    /// usable `exception.message` via the fallback text.
    #[tokio::test]
    async fn panic_with_non_string_payload_uses_the_fallback_message() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

        async fn non_string_panicking_handler() -> &'static str {
            // `panic_any` raises a panic whose payload is not a `&str` or
            // `String` (here an `i32`), exercising the fallback message.
            std::panic::panic_any(42i32);
        }

        let app = Router::new()
            .route("/non-string-panic", get(non_string_panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/non-string-panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /non-string-panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(
            ev_attr("exception.message").as_deref(),
            Some("handler panicked")
        );
    }

    /// When `ELASTIC_APM_RECORDING` (or `ELASTIC_APM_ACTIVE`) is disabled, the
    /// trace layer must be a zero-overhead no-op: no span is created or
    /// exported, mirroring the Go agent's `Recording()` short-circuit.
    #[tokio::test]
    async fn trace_layer_is_a_no_op_when_recording_is_disabled() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

    /// The bundled proxy shape (`fallback(proxy_handler)`) must also export
    /// no spans when recording is disabled: `proxy_handler` must not create
    /// its own span, or it would be exported as a root span even with the
    /// http span disabled.
    #[tokio::test]
    async fn proxy_handler_exports_no_span_when_recording_is_disabled() {
        use axum::Router;
        use hyper_util::client::legacy::connect::HttpConnector;
        use hyper_util::rt::TokioExecutor;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
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

        // The same router shape as the bundled proxy binary: a fallback to
        // `proxy_handler`. The upstream URL is deliberately malformed so the
        // handler fails fast at URI parsing (400) without any network I/O —
        // the response status is irrelevant here.
        let state = AppState {
            client: hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
                .build(HttpConnector::new()),
            upstream_url: Arc::new("not a valid upstream url".to_string()),
        };
        let app = Router::new()
            .fallback(proxy_handler)
            .with_state(state)
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/anything")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

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
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

    #[tokio::test]
    async fn apm_otel_layer_exports_spans_when_composed_into_a_subscriber() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry_sdk::trace::InMemorySpanExporter;
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

    // --- W3C trace context propagation ---

    #[test]
    fn extract_parent_context_reads_traceparent_header() {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let cx = extract_parent_context(&headers).expect("should extract parent context");
        let remote_span = cx.span();
        let sc = remote_span.span_context();
        assert_eq!(
            sc.trace_id(),
            TraceId::from(0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736u128)
        );
        assert_eq!(
            sc.span_id(),
            opentelemetry::trace::SpanId::from(0x00f0_67aa_0ba9_02b7u64)
        );
        assert!(sc.is_sampled());
    }

    #[test]
    fn extract_parent_context_is_none_without_traceparent() {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        assert!(extract_parent_context(&axum::http::HeaderMap::new()).is_none());
    }

    #[test]
    fn inject_trace_context_writes_traceparent_for_span_context() {
        use opentelemetry::testing::trace::TestSpan;
        use opentelemetry::trace::Span as OtelSpan;
        use opentelemetry::trace::Tracer as _;
        use opentelemetry::trace::TracerProvider as _;

        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let span = tracer.start("test-span");

        let cx = opentelemetry::Context::current_with_span(TestSpan(span.span_context().clone()));
        let mut req = Request::new(axum::body::Body::empty());
        inject_trace_context(&cx, &mut req);

        let tp = req
            .headers()
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .expect("traceparent header should be injected");
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4, "unexpected traceparent format: {tp}");
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1], span.span_context().trace_id().to_string());
        assert_eq!(parts[3], "01");
    }

    // Tests that mutate process environment variables must hold this lock:
    // tests run in parallel threads of one process, and env vars are global.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn resolve_upstream_url_fails_when_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("UPSTREAM_URL") };
        let err = resolve_upstream_url().unwrap_err();
        assert!(
            err.to_string().contains("UPSTREAM_URL"),
            "error should name the missing variable: {err}"
        );
    }

    #[test]
    fn resolve_upstream_url_fails_when_not_a_valid_url() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("UPSTREAM_URL", "not a url") };
        let err = resolve_upstream_url().unwrap_err();
        assert!(
            err.to_string().contains("not a valid"),
            "error should explain the URL is invalid: {err}"
        );
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("UPSTREAM_URL") };
    }

    #[test]
    fn resolve_upstream_url_returns_the_configured_url() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("UPSTREAM_URL", "https://api.example.com") };
        assert_eq!(resolve_upstream_url().unwrap(), "https://api.example.com");
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("UPSTREAM_URL") };
    }

    #[test]
    fn resolve_upstream_url_rejects_urls_without_a_scheme() {
        // A forgotten scheme (e.g. `api.example.com`) must fail at startup,
        // not start the proxy and 502 every request at runtime.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        for invalid in ["api.example.com", "localhost:3000"] {
            // SAFETY: env mutation serialized by ENV_LOCK.
            unsafe { std::env::set_var("UPSTREAM_URL", invalid) };
            let err = resolve_upstream_url().unwrap_err();
            assert!(
                err.to_string().contains("absolute"),
                "error should explain the URL must be absolute: {err}"
            );
        }
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("UPSTREAM_URL") };
    }

    #[test]
    fn resolve_upstream_url_rejects_relative_paths() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("UPSTREAM_URL", "/path") };
        let err = resolve_upstream_url().unwrap_err();
        assert!(
            err.to_string().contains("absolute"),
            "error should explain the URL must be absolute: {err}"
        );
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("UPSTREAM_URL") };
    }

    #[test]
    fn resolve_port_defaults_to_3000_when_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("PORT") };
        assert_eq!(resolve_port().unwrap(), 3000);
    }

    #[test]
    fn resolve_port_defaults_to_3000_when_blank() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("PORT", "   ") };
        assert_eq!(resolve_port().unwrap(), 3000);
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("PORT") };
    }

    #[test]
    fn resolve_port_parses_a_valid_port() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("PORT", " 8080 ") };
        assert_eq!(resolve_port().unwrap(), 8080);
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("PORT") };
    }

    #[test]
    fn resolve_port_fails_on_an_invalid_port() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        for invalid in ["not-a-port", "70000", "-1"] {
            // SAFETY: env mutation serialized by ENV_LOCK.
            unsafe { std::env::set_var("PORT", invalid) };
            let err = resolve_port().unwrap_err();
            assert!(
                err.to_string().contains("PORT"),
                "error should name the variable: {err}"
            );
        }
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("PORT") };
    }

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

    // --- Configuration parsing ---

    #[test]
    fn parse_bool_env_defaults_when_unset_or_blank() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("TEST_BOOL_ENV", "   ") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn parse_bool_env_recognizes_truthy_and_falsy_spellings() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        for (value, expected) in [
            ("true", true),
            ("TRUE", true),
            ("1", true),
            ("yes", true),
            ("on", true),
            ("false", false),
            ("False", false),
            ("0", false),
            ("no", false),
            ("off", false),
        ] {
            // SAFETY: env mutation serialized by ENV_LOCK.
            unsafe { std::env::set_var("TEST_BOOL_ENV", value) };
            assert_eq!(
                parse_bool_env("TEST_BOOL_ENV", !expected),
                expected,
                "value: {value}"
            );
        }
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn parse_bool_env_falls_back_to_default_on_unrecognized_value() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("TEST_BOOL_ENV", "maybe") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn sample_rate_parses_valid_values_without_warnings() {
        for (input, expected) in [
            (None, 1.0),
            (Some("0.5"), 0.5),
            (Some("0"), 0.0),
            (Some("1"), 1.0),
            (Some(" 0.25 "), 0.25),
        ] {
            let (rate, warnings) = parse_sample_rate(input.map(str::to_string));
            assert_eq!(rate, expected, "input: {input:?}");
            assert!(
                warnings.is_empty(),
                "no warnings expected for {input:?}: {warnings:?}"
            );
        }
    }

    #[test]
    fn sample_rate_falls_back_to_one_and_warns_on_invalid_values() {
        for invalid in ["abc", "0,1", "nan", "inf", "-0.1", "2.0", "1.5"] {
            let (rate, warnings) = parse_sample_rate(Some(invalid.to_string()));
            assert_eq!(rate, 1.0, "{invalid:?} should fall back to 1.0");
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains("ELASTIC_APM_TRANSACTION_SAMPLE_RATE")),
                "{invalid:?} should produce a warning naming the variable: {warnings:?}"
            );
        }
    }

    #[test]
    fn node_name_prefers_env_var_over_hostname() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("ELASTIC_APM_NODE_NAME", "test-node") };
        assert_eq!(resolve_node_name().as_deref(), Some("test-node"));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("ELASTIC_APM_NODE_NAME") };
    }

    #[test]
    fn node_name_ignores_blank_env_var() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("ELASTIC_APM_NODE_NAME", "   ") };
        // Blank value must not be used; falls back to the system hostname.
        assert_ne!(resolve_node_name().as_deref(), Some("   "));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("ELASTIC_APM_NODE_NAME") };
    }

    #[test]
    fn non_ignored_root_url_is_sampled_at_full_rate() {
        let s = sampler(1.0, vec!["/health".to_string()]);
        let attrs = vec![KeyValue::new("url.path", "/api/users")];
        let res = s.should_sample(
            None,
            TraceId::from(1u128),
            "GET /api/users",
            &SpanKind::Server,
            &attrs,
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::RecordAndSample);
    }
}
