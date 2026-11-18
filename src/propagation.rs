use axum::{
    extract::Request,
    http::{
        HeaderMap,
        header::{HeaderName, HeaderValue},
    },
};
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;

/// Helper function to extract a header value as a string.
pub(crate) fn extract_header(headers: &HeaderMap, header_name: HeaderName) -> String {
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
        if key == "traceparent" {
            // Prefer the W3C `traceparent`; fall back to the legacy
            // `Elastic-Apm-Traceparent` header, which uses the same format.
            // This mirrors the Go agent's backwards-compatible parsing of the
            // legacy header sent by older Elastic agents.
            return self
                .headers
                .get("traceparent")
                .or_else(|| self.headers.get("elastic-apm-traceparent"))
                .and_then(|v| v.to_str().ok());
        }
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
pub(crate) fn extract_parent_context(headers: &HeaderMap) -> Option<opentelemetry::Context> {
    let cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor { headers })
    });
    cx.has_active_span().then_some(cx)
}

/// Injects the trace context of `cx` into outgoing request headers so a
/// downstream service (e.g. an upstream you proxy to) continues the same
/// trace.
///
/// Writes the W3C `traceparent`/`tracestate` headers, plus the legacy
/// `Elastic-Apm-Traceparent` header (same format as `traceparent`) so older
/// Elastic agents that only understand the legacy header can also continue
/// the trace — mirroring the Go agent, whose `TraceparentHeader` constant is
/// an alias for the legacy header.
pub fn inject_trace_context<B>(cx: &opentelemetry::Context, req: &mut Request<B>) {
    global::get_text_map_propagator(|propagator| {
        let mut injector = HeaderInjector {
            headers: req.headers_mut(),
        };
        propagator.inject_context(cx, &mut injector);
    });
    // Mirror the W3C traceparent into the legacy Elastic-Apm-Traceparent
    // header for backwards compatibility with older agents.
    if let Some(tp) = req.headers().get("traceparent").cloned() {
        req.headers_mut().insert("elastic-apm-traceparent", tp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceId;

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

    /// A request carrying only the legacy `Elastic-Apm-Traceparent` header
    /// (no W3C `traceparent`) must still continue the remote trace, mirroring
    /// the Go agent's backwards-compatible parsing of the legacy header. The
    /// legacy header uses the same format as the W3C `traceparent`.
    #[test]
    fn extract_parent_context_reads_legacy_elastic_traceparent_header() {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let mut headers = axum::http::HeaderMap::new();
        // Only the legacy header is present (no W3C traceparent).
        headers.insert(
            "Elastic-Apm-Traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let cx = extract_parent_context(&headers)
            .expect("should extract parent context from the legacy header");
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

    /// When both the W3C `traceparent` and the legacy `Elastic-Apm-Traceparent`
    /// headers are present, the W3C header must take precedence.
    #[test]
    fn extract_parent_context_prefers_w3c_over_legacy() {
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
        headers.insert(
            "Elastic-Apm-Traceparent",
            "00-11111111111111111111111111111111-2222222222222222-01"
                .parse()
                .unwrap(),
        );
        let cx = extract_parent_context(&headers).expect("should extract parent context");
        let remote_span = cx.span();
        let sc = remote_span.span_context();
        // The W3C traceparent takes precedence over the legacy header.
        assert_eq!(
            sc.trace_id(),
            TraceId::from(0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736u128)
        );
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

    /// `inject_trace_context` must also write the legacy
    /// `Elastic-Apm-Traceparent` header (same value as the W3C `traceparent`)
    /// so older Elastic agents can continue the trace.
    #[test]
    fn inject_trace_context_writes_legacy_elastic_traceparent_header() {
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
        let legacy = req
            .headers()
            .get("elastic-apm-traceparent")
            .and_then(|v| v.to_str().ok())
            .expect("legacy Elastic-Apm-Traceparent header should be injected");
        assert_eq!(tp, legacy, "legacy header must mirror the W3C traceparent");
    }
}
