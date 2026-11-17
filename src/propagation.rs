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

/// Injects the W3C trace context of `cx` into outgoing request headers
/// (`traceparent`/`tracestate`) so a downstream service (e.g. an upstream
/// you proxy to) continues the same trace.
pub fn inject_trace_context<B>(cx: &opentelemetry::Context, req: &mut Request<B>) {
    global::get_text_map_propagator(|propagator| {
        let mut injector = HeaderInjector {
            headers: req.headers_mut(),
        };
        propagator.inject_context(cx, &mut injector);
    });
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
}
