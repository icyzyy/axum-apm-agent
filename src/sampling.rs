use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TraceState};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::trace::{SamplingDecision, SamplingResult, ShouldSample};
use regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Thread-safe cache for compiled regex patterns used in URL ignore matching.
/// Avoids recompiling the same regex on every sampling decision.
static REGEX_CACHE: OnceLock<
    std::sync::Mutex<HashMap<String, std::result::Result<Regex, regex::Error>>>,
> = OnceLock::new();

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

/// Extracts the request URL (path + query) from span attributes, checking
/// common OpenTelemetry semantic conventions. This mirrors the Go agent, which
/// matches the full request URL (path + query) against ignore patterns — not
/// just the path.
///
/// Checks, in order:
/// - `http.target` (legacy; already path + query).
/// - `url.path` + `url.query` (combined to form path + query).
fn extract_url_from_attrs(attributes: &[KeyValue]) -> Option<String> {
    // Prefer the legacy http.target (already path + query).
    for kv in attributes {
        let Value::String(s) = &kv.value else {
            continue;
        };
        if kv.key.as_str() == "http.target" {
            return Some(s.as_ref().to_string());
        }
    }
    // Otherwise, combine url.path and url.query.
    let mut path: Option<String> = None;
    let mut query: Option<String> = None;
    for kv in attributes {
        let Value::String(s) = &kv.value else {
            continue;
        };
        match kv.key.as_str() {
            "url.path" => path = Some(s.as_ref().to_string()),
            "url.query" => query = Some(s.as_ref().to_string()),
            _ => {}
        }
    }
    match (path, query) {
        (Some(path), Some(query)) if !query.is_empty() => Some(format!("{path}?{query}")),
        (Some(path), _) => Some(path),
        _ => None,
    }
}

/// Custom sampler that ignores specified URL patterns and applies a configurable
/// sampling rate. Respects parent span sampling decisions for distributed tracing.
#[derive(Clone)]
pub(crate) struct ApmSampler {
    pub(crate) ignore_patterns: Arc<Vec<String>>,
    pub(crate) sample_rate: f64,
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
            && let Some(url) = extract_url_from_attrs(attributes)
            && url_patterns_match(&url, &self.ignore_patterns)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
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

    // --- Ignore patterns match the full URL (path + query), like the Go agent ---

    /// A pattern that includes the query string must match a URL with that
    /// query, mirroring the Go agent's full-URL (path + query) matching.
    #[test]
    fn ignore_pattern_matches_full_url_with_query() {
        let s = sampler(1.0, vec!["/health?x=1".to_string()]);
        let attrs = vec![
            KeyValue::new("url.path", "/health"),
            KeyValue::new("url.query", "x=1"),
        ];
        let res = s.should_sample(
            None,
            TraceId::from(1u128),
            "GET /health?x=1",
            &SpanKind::Server,
            &attrs,
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::Drop);
    }

    /// A path-only pattern must NOT match a URL that carries a query string,
    /// because the Go agent matches the full URL (path + query). This is a
    /// deliberate behavior change from the previous path-only matching.
    #[test]
    fn ignore_pattern_path_only_does_not_match_url_with_query() {
        let s = sampler(1.0, vec!["/health".to_string()]);
        let attrs = vec![
            KeyValue::new("url.path", "/health"),
            KeyValue::new("url.query", "x=1"),
        ];
        let res = s.should_sample(
            None,
            TraceId::from(1u128),
            "GET /health?x=1",
            &SpanKind::Server,
            &attrs,
            &[],
        );
        assert_eq!(res.decision, SamplingDecision::RecordAndSample);
    }

    /// A URL with no query string is matched by a path-only pattern (the
    /// common case), so existing ignore configurations keep working.
    #[test]
    fn ignore_pattern_path_only_matches_url_without_query() {
        let s = sampler(1.0, vec!["/health".to_string()]);
        let attrs = vec![
            KeyValue::new("url.path", "/health"),
            KeyValue::new("url.query", ""),
        ];
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
}
