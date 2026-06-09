//! Benchmark trigger detection.
//!
//! Parses PR comment bodies for "run benchmark …" trigger phrases. There is no
//! allowlist: any requested name is accepted and resolved on the runner.

use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;

use crate::models::BenchmarkRequest;

/// Unified trigger regex: matches `run benchmark(s) [name1 name2 ...]`.
static TRIGGER_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*run\s+(benchmarks?)(?:\s+([a-zA-Z0-9_\-\s]+?))?\s*$").unwrap()
});

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CommentConfig {
    env: Option<HashMap<String, String>>,
    baseline: Option<SideConfig>,
    changed: Option<SideConfig>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SideConfig {
    #[serde(rename = "ref")]
    git_ref: Option<String>,
    env: Option<HashMap<String, String>>,
}

/// Result of [`detect_benchmark`].
pub enum DetectResult {
    /// Successfully parsed trigger and config.
    Parsed(BenchmarkRequest),
    /// Trigger matched but YAML config had errors.
    ConfigError(String),
    /// Not a trigger at all.
    None,
}

/// Parse the extra lines (after the trigger line) into structured env vars and refs.
///
/// Supports an optional ` ```yaml ` / ` ``` ` fence around the YAML content.
/// Returns `Err` with a human-readable message if YAML is present but invalid.
#[allow(clippy::type_complexity)]
fn parse_sections(
    lines: &[&str],
) -> Result<
    (
        HashMap<String, String>,
        HashMap<String, String>,
        HashMap<String, String>,
        Option<String>,
        Option<String>,
    ),
    String,
> {
    let yaml: String = lines
        .iter()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with("```")
        })
        .copied()
        .collect::<Vec<&str>>()
        .join("\n");

    if yaml.trim().is_empty() {
        return Ok(Default::default());
    }

    let config: CommentConfig =
        serde_yaml::from_str(&yaml).map_err(|e| format!("invalid configuration: {e}"))?;

    let shared_env = config.env.unwrap_or_default();
    let baseline_env = config
        .baseline
        .as_ref()
        .and_then(|s| s.env.clone())
        .unwrap_or_default();
    let changed_env = config
        .changed
        .as_ref()
        .and_then(|s| s.env.clone())
        .unwrap_or_default();
    let baseline_ref = config.baseline.as_ref().and_then(|s| s.git_ref.clone());
    let changed_ref = config.changed.as_ref().and_then(|s| s.git_ref.clone());

    Ok((
        shared_env,
        baseline_env,
        changed_env,
        baseline_ref,
        changed_ref,
    ))
}

/// Result of parsing the trigger line.
pub enum TriggerKind {
    /// Specific benchmark names were given.
    Named(Vec<String>),
    /// `run benchmarks` (plural) with no names → default suite.
    DefaultSuite,
    /// `run benchmark` (singular) with no names → error.
    SingularNoNames,
}

/// Parse the trigger line. Returns `None` if it doesn't match the trigger pattern at all.
pub fn parse_trigger(trigger: &str) -> Option<TriggerKind> {
    let caps = TRIGGER_RE.captures(trigger)?;
    let word = caps.get(1).unwrap().as_str(); // "benchmark" or "benchmarks"
    let is_plural = word.to_lowercase().ends_with('s');

    match caps.get(2) {
        Some(names_match) => {
            let names: Vec<String> = names_match
                .as_str()
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            if names.is_empty() {
                if is_plural {
                    Some(TriggerKind::DefaultSuite)
                } else {
                    Some(TriggerKind::SingularNoNames)
                }
            } else {
                Some(TriggerKind::Named(names))
            }
        }
        None => {
            if is_plural {
                Some(TriggerKind::DefaultSuite)
            } else {
                Some(TriggerKind::SingularNoNames)
            }
        }
    }
}

/// Parse a PR comment body into a [`BenchmarkRequest`] if it matches a trigger pattern.
///
/// Recognizes `run benchmarks` (default suite), `run benchmarks <names>`, and
/// `run benchmark <names>`. `run benchmark` without names returns `None` (caller
/// should post a help message). Any requested names are accepted; there is no
/// allowlist.
///
/// Supports `baseline:`/`changed:` sections with `env:` and `ref:` sub-entries.
pub fn detect_benchmark(body: &str) -> DetectResult {
    let lines: Vec<&str> = body.trim().lines().collect();
    if lines.is_empty() {
        return DetectResult::None;
    }

    let trigger = lines[0];
    let extra = &lines[1..];

    let trigger_kind = match parse_trigger(trigger) {
        Some(k) => k,
        None => return DetectResult::None,
    };

    let (shared_env, baseline_env, changed_env, baseline_ref, changed_ref) =
        match parse_sections(extra) {
            Ok(sections) => sections,
            Err(e) => return DetectResult::ConfigError(e),
        };

    match trigger_kind {
        TriggerKind::DefaultSuite => DetectResult::Parsed(BenchmarkRequest {
            benchmarks: vec![],
            env_vars: shared_env,
            baseline_env_vars: baseline_env,
            changed_env_vars: changed_env,
            baseline_ref,
            changed_ref,
        }),
        TriggerKind::Named(names) => {
            if names.is_empty() {
                return DetectResult::None;
            }

            // No allowlist: accept any requested names. Names that resolve to
            // neither a Criterion bench target nor a `bench.sh` suite simply
            // fail on the runner.
            DetectResult::Parsed(BenchmarkRequest {
                benchmarks: names,
                env_vars: shared_env,
                baseline_env_vars: baseline_env,
                changed_env_vars: changed_env,
                baseline_ref,
                changed_ref,
            })
        }
        TriggerKind::SingularNoNames => DetectResult::None,
    }
}

/// Returns `true` if the first line starts with "run benchmark" (case-insensitive).
/// Used to detect malformed or unauthorized trigger attempts.
pub fn is_benchmark_trigger(body: &str) -> bool {
    let first_line = body.trim().lines().next().unwrap_or("");
    let lower = first_line.trim().to_lowercase();
    lower.starts_with("run benchmark")
}

/// Returns `true` if `run benchmark` (singular) was used without benchmark names.
pub fn is_singular_no_names(body: &str) -> bool {
    let first_line = body.trim().lines().next().unwrap_or("");
    matches!(
        parse_trigger(first_line),
        Some(TriggerKind::SingularNoNames)
    )
}

/// Returns `true` if the comment body is exactly "show benchmark queue" (case-insensitive).
pub fn is_queue_request(body: &str) -> bool {
    body.trim().eq_ignore_ascii_case("show benchmark queue")
}

/// Build the usage/help message shown for malformed triggers and config errors.
///
/// There is no allowlist, so this no longer enumerates valid benchmarks — any
/// name is accepted and resolved on the runner (an unresolvable name fails
/// there). The benchmark `bench.sh` suites and Criterion benches available are
/// whatever the target repo defines.
pub fn usage_message() -> String {
    "Usage:\n\
         ```\n\
         run benchmark <name>           # run specific benchmark(s)\n\
         run benchmarks                 # run default suite\n\
         run benchmarks <name1> <name2> # run specific benchmarks\n\
         ```\n\
         Any benchmark name is accepted: `bench.sh` suite names (e.g. `tpch`, \
         `clickbench_partitioned`, `wide_schema`) and Criterion bench targets \
         (e.g. `sql_planner`) are resolved automatically. A name that matches \
         neither fails on the runner.\n\n\
         Per-side configuration (`run benchmark tpch` followed by):\n\
         ```yaml\n\
         env:\n\
           # shared env is inherited by BOTH the build and the run, so build\n\
           # flags go here. Builds default to no debuginfo for speed; opt back\n\
           # in for hung-job gdb dumps and cap jobs to stay within memory:\n\
           CARGO_PROFILE_RELEASE_DEBUG: \"1\"\n\
           CARGO_BUILD_JOBS: \"1\"\n\
         baseline:\n\
           ref: v45.0.0\n\
           env:\n\
             # per-side env only reaches the benchmark run, not the build\n\
             DATAFUSION_RUNTIME_MEMORY_LIMIT: 1G\n\
         changed:\n\
           ref: v46.0.0\n\
           env:\n\
             DATAFUSION_RUNTIME_MEMORY_LIMIT: 2G\n\
         ```"
    .to_string()
}

/// Format the allowlist as a comma-separated list of GitHub profile links.
pub fn allowed_users_markdown(allowed_users: &HashSet<String>) -> String {
    let mut users: Vec<&str> = allowed_users.iter().map(|s| s.as_str()).collect();
    users.sort();
    users
        .iter()
        .map(|u| format!("[{u}](https://github.com/{u})"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RepoEntry;
    use crate::models::JobType;

    fn df_entry() -> RepoEntry {
        RepoEntry {
            kind: "datafusion".into(),
            default_standard: vec![
                "clickbench_partitioned".into(),
                "tpcds".into(),
                "tpch".into(),
            ],
        }
    }

    fn arrow_entry() -> RepoEntry {
        RepoEntry {
            kind: "arrow".into(),
            default_standard: vec![],
        }
    }

    // ── detect_benchmark ────────────────────────────────────────────

    /// Helper to unwrap a DetectResult::Parsed or panic.
    fn unwrap_parsed(result: DetectResult) -> BenchmarkRequest {
        match result {
            DetectResult::Parsed(req) => req,
            DetectResult::ConfigError(e) => panic!("expected Parsed, got ConfigError: {e}"),
            DetectResult::None => panic!("expected Parsed, got None"),
        }
    }

    fn is_none(result: &DetectResult) -> bool {
        matches!(result, DetectResult::None)
    }

    fn is_parsed(result: &DetectResult) -> bool {
        matches!(result, DetectResult::Parsed(_))
    }

    #[test]
    fn detect_default_suite() {
        let req = unwrap_parsed(detect_benchmark("run benchmarks"));
        assert!(req.benchmarks.is_empty());
        assert!(req.env_vars.is_empty());
    }

    #[test]
    fn detect_default_suite_with_env_vars() {
        let body = "run benchmarks\nenv:\n  DATAFUSION_RUNTIME_MEMORY_LIMIT: 1G";
        let req = unwrap_parsed(detect_benchmark(body));
        assert!(req.benchmarks.is_empty());
        assert_eq!(
            req.env_vars.get("DATAFUSION_RUNTIME_MEMORY_LIMIT").unwrap(),
            "1G"
        );
    }

    #[test]
    fn detect_single_named() {
        let req = unwrap_parsed(detect_benchmark("run benchmark tpch_mem"));
        assert_eq!(req.benchmarks, vec!["tpch_mem"]);
    }

    #[test]
    fn detect_multiple_named() {
        let req = unwrap_parsed(detect_benchmark("run benchmark tpch_mem tpch10"));
        assert_eq!(req.benchmarks, vec!["tpch_mem", "tpch10"]);
    }

    #[test]
    fn detect_criterion_benchmark() {
        let req = unwrap_parsed(detect_benchmark("run benchmark sql_planner"));
        assert_eq!(req.benchmarks, vec!["sql_planner"]);
    }

    #[test]
    fn detect_any_name_is_accepted() {
        // No allowlist: previously-unknown names now parse and are scheduled.
        let req = unwrap_parsed(detect_benchmark("run benchmark anything_goes"));
        assert_eq!(req.benchmarks, vec!["anything_goes"]);

        let req = unwrap_parsed(detect_benchmark("run benchmark tpch_mem bogus"));
        assert_eq!(req.benchmarks, vec!["tpch_mem", "bogus"]);
    }

    #[test]
    fn detect_not_a_trigger() {
        assert!(is_none(&detect_benchmark("hello world")));
    }

    #[test]
    fn detect_empty_string() {
        assert!(is_none(&detect_benchmark("")));
    }

    #[test]
    fn detect_case_insensitive() {
        assert!(is_parsed(&detect_benchmark("Run Benchmarks")));
        assert!(is_parsed(&detect_benchmark("RUN BENCHMARK tpch")));
    }

    // ── plural trigger with names (new) ─────────────────────────────

    #[test]
    fn detect_plural_with_names() {
        let req = unwrap_parsed(detect_benchmark("run benchmarks tpch clickbench_1"));
        assert_eq!(req.benchmarks, vec!["tpch", "clickbench_1"]);
    }

    // ── singular without names returns None ─────────────────────────

    #[test]
    fn detect_singular_no_names_returns_none() {
        assert!(is_none(&detect_benchmark("run benchmark")));
    }

    #[test]
    fn is_singular_no_names_detects() {
        assert!(is_singular_no_names("run benchmark"));
        assert!(is_singular_no_names("  run benchmark  "));
        assert!(!is_singular_no_names("run benchmarks"));
        assert!(!is_singular_no_names("run benchmark tpch"));
    }

    // ── section parsing ─────────────────────────────────────────────

    #[test]
    fn parse_baseline_changed_env_vars() {
        let body = "run benchmark tpch\nbaseline:\n  env:\n    DATAFUSION_RUNTIME_MEMORY_LIMIT: 1G\nchanged:\n  env:\n    DATAFUSION_RUNTIME_MEMORY_LIMIT: 2G";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(
            req.baseline_env_vars
                .get("DATAFUSION_RUNTIME_MEMORY_LIMIT")
                .unwrap(),
            "1G"
        );
        assert_eq!(
            req.changed_env_vars
                .get("DATAFUSION_RUNTIME_MEMORY_LIMIT")
                .unwrap(),
            "2G"
        );
        assert!(req.env_vars.is_empty());
    }

    #[test]
    fn parse_baseline_ref() {
        let body = "run benchmarks tpch clickbench_1\nbaseline:\n  ref: abc1234def";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(req.baseline_ref.as_deref(), Some("abc1234def"));
        assert!(req.changed_ref.is_none());
    }

    #[test]
    fn parse_both_refs_with_env() {
        let body = "run benchmark tpch\nbaseline:\n  ref: v45.0.0\n  env:\n    FOO: old_value\nchanged:\n  ref: v46.0.0\n  env:\n    FOO: new_value";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(req.baseline_ref.as_deref(), Some("v45.0.0"));
        assert_eq!(req.changed_ref.as_deref(), Some("v46.0.0"));
        assert_eq!(req.baseline_env_vars.get("FOO").unwrap(), "old_value");
        assert_eq!(req.changed_env_vars.get("FOO").unwrap(), "new_value");
    }

    #[test]
    fn parse_shared_plus_per_side() {
        let body = "run benchmark tpch\nenv:\n  SHARED_SETTING: enabled\nbaseline:\n  env:\n    DATAFUSION_RUNTIME_MEMORY_LIMIT: 1G\nchanged:\n  env:\n    DATAFUSION_RUNTIME_MEMORY_LIMIT: 2G";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(req.env_vars.get("SHARED_SETTING").unwrap(), "enabled");
        assert_eq!(
            req.baseline_env_vars
                .get("DATAFUSION_RUNTIME_MEMORY_LIMIT")
                .unwrap(),
            "1G"
        );
        assert_eq!(
            req.changed_env_vars
                .get("DATAFUSION_RUNTIME_MEMORY_LIMIT")
                .unwrap(),
            "2G"
        );
    }

    #[test]
    fn parse_explicit_env_section() {
        let body = "run benchmark tpch\nenv:\n  DATAFUSION_RUNTIME_MEMORY_LIMIT: 1G";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(
            req.env_vars.get("DATAFUSION_RUNTIME_MEMORY_LIMIT").unwrap(),
            "1G"
        );
    }

    #[test]
    fn parse_yaml_fenced_block() {
        let body = "run benchmark tpch\n```yaml\nbaseline:\n  ref: v45.0.0\n  env:\n    FOO: bar\nchanged:\n  ref: v46.0.0\n```";
        let req = unwrap_parsed(detect_benchmark(body));
        assert_eq!(req.baseline_ref.as_deref(), Some("v45.0.0"));
        assert_eq!(req.changed_ref.as_deref(), Some("v46.0.0"));
        assert_eq!(req.baseline_env_vars.get("FOO").unwrap(), "bar");
    }

    #[test]
    fn parse_unknown_field_returns_config_error() {
        let body = "run benchmark tpch\ncurrent:\n  ref: HEAD";
        match detect_benchmark(body) {
            DetectResult::ConfigError(e) => {
                assert!(e.contains("unknown field"), "error was: {e}");
            }
            other => panic!(
                "expected ConfigError, got {}",
                match other {
                    DetectResult::Parsed(_) => "Parsed",
                    DetectResult::None => "None",
                    DetectResult::ConfigError(_) => unreachable!(),
                }
            ),
        }
    }

    // ── RepoEntry::job_type ─────────────────────────────────────────

    #[test]
    fn job_type_datafusion_repo() {
        assert_eq!(df_entry().job_type(), JobType::Datafusion);
    }

    #[test]
    fn job_type_arrow_repo() {
        assert_eq!(arrow_entry().job_type(), JobType::ArrowCriterion);
    }

    // ── usage_message ───────────────────────────────────────────────

    #[test]
    fn usage_message_has_usage_and_no_allowlist() {
        let msg = usage_message();
        assert!(msg.contains("run benchmark"));
        assert!(msg.contains("Any benchmark name is accepted"));
    }

    // ── is_benchmark_trigger ────────────────────────────────────────

    #[test]
    fn trigger_named() {
        assert!(is_benchmark_trigger("run benchmark tpch"));
    }

    #[test]
    fn trigger_default() {
        assert!(is_benchmark_trigger("run benchmarks"));
    }

    #[test]
    fn trigger_case_insensitive() {
        assert!(is_benchmark_trigger("Run Benchmark FOO"));
    }

    #[test]
    fn trigger_not_matching() {
        assert!(!is_benchmark_trigger("hello"));
    }

    #[test]
    fn trigger_leading_whitespace() {
        assert!(is_benchmark_trigger("  run benchmark x  "));
    }

    // ── is_queue_request ────────────────────────────────────────────

    #[test]
    fn queue_request_exact() {
        assert!(is_queue_request("show benchmark queue"));
    }

    #[test]
    fn queue_request_case_insensitive() {
        assert!(is_queue_request("SHOW BENCHMARK QUEUE"));
    }

    #[test]
    fn queue_request_extra_words() {
        assert!(!is_queue_request("show benchmark queue please"));
    }

    #[test]
    fn queue_request_wrong_phrase() {
        assert!(!is_queue_request("run benchmarks"));
    }

    // ── allowed_users_markdown ──────────────────────────────────────

    #[test]
    fn allowed_users_contains_known_user() {
        let users: HashSet<String> = ["alamb", "zhuqi-lucas"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let md = allowed_users_markdown(&users);
        assert!(md.contains("[alamb](https://github.com/alamb)"));
        // Verify sorted (a before z)
        let pos_a = md.find("[alamb]").unwrap();
        let pos_z = md.find("[zhuqi-lucas]").unwrap();
        assert!(pos_a < pos_z);
    }
}
