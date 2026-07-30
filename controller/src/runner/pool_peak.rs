//! Per-query peak `MemoryPool` reservation, read from the benchmark results JSON.
//!
//! DataFusion's `bench.sh compare_detail` (and the `compare.py` behind it)
//! parses only `elapsed` and `row_count` out of each results JSON, so
//! `pool_peak_bytes` — added by
//! [apache/datafusion#23985](https://github.com/apache/datafusion/pull/23985) —
//! never reaches that table no matter what the file contains. This module reads
//! the same JSONs directly and renders the field itself, keeping the
//! presentation here rather than patching DataFusion.
//!
//! Two ways the field can be missing, both normal and both reported as "not
//! available on this side" rather than as zero:
//!
//! * The benchmark ran without a memory limit. DataFusion installs the
//!   recording pool only alongside a pool it has a limit for, so with no
//!   `DATAFUSION_RUNTIME_MEMORY_LIMIT` there is nothing to record and the field
//!   is omitted. That is the default for runs triggered here.
//! * The side predates #23985 — e.g. a `baseline: ref: v45.0.0` comparison.
//!
//! The run-wide counterpart is [`monitor`](super::monitor), which samples peak
//! RSS from the benchmark's process subtree. The two are paired per *benchmark
//! invocation*, not per query: `pool_peak_bytes` is scoped to one query while
//! the RSS sample covers the whole invocation, so the only defensible pairing
//! is the largest per-query peak in a run against that run's peak RSS.

use std::collections::BTreeSet;
use std::path::Path;

use tracing::warn;

use crate::runner::monitor::{format_bytes, ResourceStats};

/// Peak pool reservation for one query, as recorded in a results JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPeak {
    /// Query id as the benchmark labelled it, e.g. `"1"` or `"Q23"`.
    pub query: String,
    /// `None` when the field was absent — see the module docs for when that
    /// happens. Never conflated with a recorded `0`.
    pub pool_peak_bytes: Option<u64>,
}

/// The queries from one results JSON file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchPeaks {
    /// Requested benchmark name, e.g. `tpch`.
    pub bench: String,
    /// Results file stem, e.g. `tpch_sf1`. One benchmark can write several
    /// (`h2o` writes one per run type), so this is what identifies a table.
    pub source: String,
    pub queries: Vec<QueryPeak>,
}

impl BenchPeaks {
    /// Largest per-query peak in this file, or `None` when no query reported
    /// one. A run where every query recorded `0` yields `Some(0)`.
    pub fn max_pool_peak(&self) -> Option<u64> {
        self.queries.iter().filter_map(|q| q.pool_peak_bytes).max()
    }
}

/// Whether any query on this side reported a peak at all.
fn any_data(peaks: &[BenchPeaks]) -> bool {
    peaks.iter().any(|b| b.max_pool_peak().is_some())
}

/// Extract the per-query peaks from a DataFusion benchmark results JSON.
///
/// Returns `None` only when the file is not a results JSON at all (unparseable,
/// or no `queries` array). A well-formed file whose queries all lack
/// `pool_peak_bytes` parses fine, into `None` peaks.
pub fn parse_results_json(contents: &str) -> Option<Vec<QueryPeak>> {
    let value: serde_json::Value = serde_json::from_str(contents).ok()?;
    let queries = value.get("queries")?.as_array()?;
    Some(
        queries
            .iter()
            .map(|q| QueryPeak {
                query: query_id(q),
                pool_peak_bytes: q.get("pool_peak_bytes").and_then(|v| v.as_u64()),
            })
            .collect(),
    )
}

/// The `query` field as a display string. DataFusion writes it as a string,
/// but `compare.py` types it as an int, so accept either rather than depending
/// on which.
fn query_id(query: &serde_json::Value) -> String {
    match query.get("query") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

/// The `*.json` file names currently in `dir`. Empty when `dir` is unreadable.
///
/// Taken before and after a benchmark invocation so the files it wrote can be
/// attributed to it: the results file name does not follow the benchmark name
/// (`tpch` writes `tpch_sf1.json`, `topk_tpch` writes `run_topk_tpch.json`), so
/// there is nothing to match on. Each side writes into its own `results/<name>`
/// directory and the tree is cleaned before the run, so within one side each
/// invocation's files are genuinely new.
pub async fn snapshot_json_files(dir: &Path) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return names;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            names.insert(name.to_string());
        }
    }
    names
}

/// Read the `*.json` files in `dir` that were absent from `before`, attributing
/// them to `bench`.
///
/// A file that cannot be read or parsed is warned about and skipped: a missing
/// pool section is not worth failing a benchmark run over.
pub async fn collect_new(dir: &Path, before: &BTreeSet<String>, bench: &str) -> Vec<BenchPeaks> {
    let after = snapshot_json_files(dir).await;
    let mut out = Vec::new();
    for name in after.difference(before) {
        let path = dir.join(name);
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                warn!("reading results JSON {}: {e}", path.display());
                continue;
            }
        };
        let Some(queries) = parse_results_json(&contents) else {
            warn!("results JSON {} did not parse; skipping", path.display());
            continue;
        };
        out.push(BenchPeaks {
            bench: bench.to_string(),
            source: name.trim_end_matches(".json").to_string(),
            queries,
        });
    }
    out
}

/// Render the peak-pool section for the PR comment.
///
/// Returns an empty string when neither side reported a single peak, so a
/// default run — which sets no memory limit and therefore records nothing —
/// leaves the comment exactly as it was before this section existed.
pub fn format_pool_peak_section(
    base_label: &str,
    changed_label: &str,
    base: &[BenchPeaks],
    changed: &[BenchPeaks],
    base_stats: &[(String, ResourceStats)],
    changed_stats: &[(String, ResourceStats)],
) -> String {
    let base_has = any_data(base);
    let changed_has = any_data(changed);
    if !base_has && !changed_has {
        return String::new();
    }

    let mut out = format!(
        "Peak `MemoryPool` reservation per query — what DataFusion's accounting \
         believes it reserved. Recorded only when the benchmark runs with \
         `DATAFUSION_RUNTIME_MEMORY_LIMIT` set.\n\n\
         Base: `{base_label}` | Changed: `{changed_label}`\n\n"
    );

    if !base_has {
        out.push_str(&missing_side_note("Base", base_label));
    }
    if !changed_has {
        out.push_str(&missing_side_note("Changed", changed_label));
    }

    for source in sources_in_order(base, changed) {
        let b = base.iter().find(|p| p.source == source);
        let c = changed.iter().find(|p| p.source == source);
        out.push_str(&format_query_table(&source, b, c));
    }

    out.push_str(&format_vs_rss_table(
        base_label,
        changed_label,
        base,
        changed,
        base_stats,
        changed_stats,
    ));

    out
}

/// Why a side's column is entirely `n/a`. Both causes are indistinguishable
/// from the results JSON alone — an absent field looks the same either way — so
/// the note names both rather than guessing.
fn missing_side_note(side: &str, label: &str) -> String {
    format!(
        "> {side} (`{label}`) reported no `pool_peak_bytes`: it ran without a memory \
         limit, or predates apache/datafusion#23985.\n\n"
    )
}

/// Every results-file stem seen on either side, base order first.
fn sources_in_order(base: &[BenchPeaks], changed: &[BenchPeaks]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    for peaks in base.iter().chain(changed.iter()) {
        if seen.insert(peaks.source.clone()) {
            order.push(peaks.source.clone());
        }
    }
    order
}

/// One per-query table for a single results file.
fn format_query_table(
    source: &str,
    base: Option<&BenchPeaks>,
    changed: Option<&BenchPeaks>,
) -> String {
    let bench = base.or(changed).map(|p| p.bench.as_str()).unwrap_or(source);

    // Most benchmarks write one file named after themselves; only name both
    // when they differ (`tpch` → `tpch_sf1`, `h2o` → `h2o_groupby`).
    let mut out = if bench == source {
        format!("**`{bench}`**\n\n")
    } else {
        format!("**`{bench}` — `{source}`**\n\n")
    };
    out.push_str("| Query | Base | Changed | Change |\n| --- | --- | --- | --- |\n");

    for query in queries_in_order(base, changed) {
        let b = base.and_then(|p| lookup(p, &query));
        let c = changed.and_then(|p| lookup(p, &query));
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            query,
            render_bytes(b),
            render_bytes(c),
            render_change(b, c),
        ));
    }
    out.push('\n');
    out
}

/// Every query id seen on either side, base order first. A query present in one
/// side's JSON but not the other still gets a row.
fn queries_in_order(base: Option<&BenchPeaks>, changed: Option<&BenchPeaks>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    for query in base
        .into_iter()
        .chain(changed)
        .flat_map(|p| p.queries.iter())
    {
        if seen.insert(query.query.clone()) {
            order.push(query.query.clone());
        }
    }
    order
}

/// The peak recorded for `query`, or `None` when the query is absent from this
/// side or ran without a recorder.
fn lookup(peaks: &BenchPeaks, query: &str) -> Option<u64> {
    peaks
        .queries
        .iter()
        .find(|q| q.query == query)
        .and_then(|q| q.pool_peak_bytes)
}

fn render_bytes(value: Option<u64>) -> String {
    value.map(format_bytes).unwrap_or_else(|| "n/a".to_string())
}

/// Percent change of `changed` against `base`, or `n/a` when either side is
/// missing. A base of zero has no percent change to report, so that reads
/// `n/a` too rather than as an infinite regression.
fn render_change(base: Option<u64>, changed: Option<u64>) -> String {
    match (base, changed) {
        (Some(b), Some(c)) if b > 0 => {
            let pct = (c as f64 - b as f64) / b as f64 * 100.0;
            format!("{pct:+.1}%")
        }
        (Some(0), Some(0)) => "0.0%".to_string(),
        _ => "n/a".to_string(),
    }
}

/// The pool peak against the peak RSS the monitor sampled for the same run.
///
/// Paired per benchmark invocation, since that is the window both numbers cover:
/// the pool figure is the largest reservation any single query in the run
/// reached, and the RSS figure is the high-water mark of the whole invocation.
fn format_vs_rss_table(
    base_label: &str,
    changed_label: &str,
    base: &[BenchPeaks],
    changed: &[BenchPeaks],
    base_stats: &[(String, ResourceStats)],
    changed_stats: &[(String, ResourceStats)],
) -> String {
    let mut rows = String::new();
    for bench in benches_in_order(base, changed) {
        for (label, peaks, stats) in [
            ("base", base, base_stats),
            ("changed", changed, changed_stats),
        ] {
            let Some(pool) = max_for_bench(peaks, &bench) else {
                continue;
            };
            let Some(rss) = stats
                .iter()
                .find(|(name, _)| *name == bench)
                .map(|(_, s)| s.peak_memory_bytes)
            else {
                continue;
            };
            let side = if label == "base" {
                format!("base (`{base_label}`)")
            } else {
                format!("changed (`{changed_label}`)")
            };
            rows.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                bench,
                side,
                format_bytes(pool),
                format_bytes(rss),
                render_gap(pool, rss),
                render_ratio(pool, rss),
            ));
        }
    }

    if rows.is_empty() {
        return String::new();
    }

    let mut out = String::from(
        "**Pool accounting vs. process RSS**\n\n\
         Max pool peak is the largest reservation any single query in the run reached; \
         peak RSS covers the whole invocation, including data loading and allocator \
         retention, and the two high-water marks need not coincide in time. The gap is \
         therefore an upper bound on what the pool did not account for, not a \
         measurement of it.\n\n\
         | Benchmark | Side | Max pool peak | Peak RSS | Gap | RSS / pool |\n\
         | --- | --- | --- | --- | --- | --- |\n",
    );
    out.push_str(&rows);
    out.push('\n');
    out
}

/// Every benchmark name seen on either side, base order first.
fn benches_in_order(base: &[BenchPeaks], changed: &[BenchPeaks]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    for peaks in base.iter().chain(changed.iter()) {
        if seen.insert(peaks.bench.clone()) {
            order.push(peaks.bench.clone());
        }
    }
    order
}

/// Largest peak across every results file `bench` wrote on this side.
fn max_for_bench(peaks: &[BenchPeaks], bench: &str) -> Option<u64> {
    peaks
        .iter()
        .filter(|p| p.bench == bench)
        .filter_map(|p| p.max_pool_peak())
        .max()
}

/// `rss - pool`, or a negative gap when the sampled RSS peak came in under the
/// pool peak — possible, since RSS is sampled once a second and can miss the
/// true high-water mark.
fn render_gap(pool: u64, rss: u64) -> String {
    if rss >= pool {
        format_bytes(rss - pool)
    } else {
        format!("\u{2212}{}", format_bytes(pool - rss))
    }
}

fn render_ratio(pool: u64, rss: u64) -> String {
    if pool == 0 {
        return "n/a".to_string();
    }
    format!("{:.1}\u{d7}", rss as f64 / pool as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peaks(bench: &str, source: &str, queries: &[(&str, Option<u64>)]) -> BenchPeaks {
        BenchPeaks {
            bench: bench.to_string(),
            source: source.to_string(),
            queries: queries
                .iter()
                .map(|(q, p)| QueryPeak {
                    query: q.to_string(),
                    pool_peak_bytes: *p,
                })
                .collect(),
        }
    }

    fn stats(peak_memory_bytes: u64) -> ResourceStats {
        ResourceStats {
            peak_memory_bytes,
            ..Default::default()
        }
    }

    #[test]
    fn parses_pool_peak_when_present() {
        let json = r#"{
            "context": {"datafusion_version": "50.0.0"},
            "queries": [
                {"query": "1", "iterations": [{"elapsed": 1.0, "row_count": 4}],
                 "start_time": 1, "success": true, "pool_peak_bytes": 262528},
                {"query": "2", "iterations": [{"elapsed": 2.0, "row_count": 8}],
                 "start_time": 2, "success": true, "pool_peak_bytes": 0}
            ]
        }"#;
        let parsed = parse_results_json(json).unwrap();
        assert_eq!(parsed[0].pool_peak_bytes, Some(262528));
        // A recorded zero is a real reading, not a missing field.
        assert_eq!(parsed[1].pool_peak_bytes, Some(0));
    }

    #[test]
    fn parses_results_predating_the_field() {
        // Exactly the shape a v45-era baseline writes.
        let json = r#"{
            "context": {"datafusion_version": "45.0.0"},
            "queries": [
                {"query": "1", "iterations": [{"elapsed": 1.0, "row_count": 4}],
                 "start_time": 1, "success": true}
            ]
        }"#;
        let parsed = parse_results_json(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].pool_peak_bytes, None);
    }

    #[test]
    fn parses_numeric_query_ids() {
        let json = r#"{"queries": [{"query": 7, "pool_peak_bytes": 128}]}"#;
        let parsed = parse_results_json(json).unwrap();
        assert_eq!(parsed[0].query, "7");
        assert_eq!(parsed[0].pool_peak_bytes, Some(128));
    }

    #[test]
    fn rejects_non_results_json() {
        assert!(parse_results_json("not json").is_none());
        assert!(parse_results_json("{}").is_none());
        assert!(parse_results_json(r#"{"queries": 3}"#).is_none());
    }

    #[test]
    fn section_is_empty_when_neither_side_recorded() {
        // The default run: no memory limit, so the field is absent everywhere.
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", None), ("2", None)])];
        let changed = vec![peaks("tpch", "tpch_sf1", &[("1", None), ("2", None)])];
        let section = format_pool_peak_section(
            "abc1234",
            "my-branch",
            &base,
            &changed,
            &[("tpch".into(), stats(1 << 30))],
            &[("tpch".into(), stats(1 << 30))],
        );
        assert_eq!(section, "");
    }

    #[test]
    fn section_notes_a_side_that_recorded_nothing() {
        // `baseline: ref: v45.0.0` against a current main.
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", None)])];
        let changed = vec![peaks("tpch", "tpch_sf1", &[("1", Some(1024))])];
        let section = format_pool_peak_section(
            "v45.0.0",
            "main",
            &base,
            &changed,
            &[("tpch".into(), stats(2048))],
            &[("tpch".into(), stats(2048))],
        );
        assert!(section.contains("Base (`v45.0.0`) reported no `pool_peak_bytes`"));
        assert!(!section.contains("Changed (`main`) reported no"));
        // The missing side reads as unavailable, never as zero.
        assert!(section.contains("| 1 | n/a | 1.0 KiB | n/a |"));
        // Only the side that recorded gets an RSS pairing row.
        assert!(section.contains("| tpch | changed (`main`) | 1.0 KiB | 2.0 KiB |"));
        assert!(!section.contains("base (`v45.0.0`) | "));
    }

    #[test]
    fn query_present_on_only_one_side_still_gets_a_row() {
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", Some(1024))])];
        let changed = vec![peaks(
            "tpch",
            "tpch_sf1",
            &[("1", Some(2048)), ("2", Some(512))],
        )];
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &base,
            &changed,
            &[("tpch".into(), stats(4096))],
            &[("tpch".into(), stats(4096))],
        );
        assert!(section.contains("| 1 | 1.0 KiB | 2.0 KiB | +100.0% |"));
        assert!(section.contains("| 2 | n/a | 512 B | n/a |"));
    }

    #[test]
    fn zero_peaks_render_as_zero_not_missing() {
        let base = vec![peaks("nlj", "nlj", &[("1", Some(0)), ("2", Some(0))])];
        let changed = vec![peaks("nlj", "nlj", &[("1", Some(0)), ("2", Some(4096))])];
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &base,
            &changed,
            &[("nlj".into(), stats(8192))],
            &[("nlj".into(), stats(8192))],
        );
        assert!(section.contains("| 1 | 0 B | 0 B | 0.0% |"));
        // Growth from a zero base has no percent to report.
        assert!(section.contains("| 2 | 0 B | 4.0 KiB | n/a |"));
        // A base whose every query peaked at zero is still "recorded", so the
        // section renders rather than being suppressed.
        assert!(!section.contains("Base (`base-sha`) reported no"));
        assert!(section.contains("| nlj | base (`base-sha`) | 0 B | 8.0 KiB | 8.0 KiB | n/a |"));
    }

    #[test]
    fn vs_rss_table_pairs_per_benchmark() {
        let base = vec![peaks(
            "tpch",
            "tpch_sf1",
            &[("1", Some(1 << 20)), ("2", Some(4 << 20))],
        )];
        let changed = vec![peaks(
            "tpch",
            "tpch_sf1",
            &[("1", Some(1 << 20)), ("2", Some(2 << 20))],
        )];
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &base,
            &changed,
            &[("tpch".into(), stats(8 << 20))],
            &[("tpch".into(), stats(8 << 20))],
        );
        // Max across queries, not the last one.
        assert!(section
            .contains("| tpch | base (`base-sha`) | 4.0 MiB | 8.0 MiB | 4.0 MiB | 2.0\u{d7} |"));
        assert!(section
            .contains("| tpch | changed (`branch`) | 2.0 MiB | 8.0 MiB | 6.0 MiB | 4.0\u{d7} |"));
    }

    #[test]
    fn rss_below_pool_peak_renders_a_negative_gap() {
        // The RSS sampler polls once a second and can miss the true peak.
        assert_eq!(render_gap(4096, 1024), "\u{2212}3.0 KiB");
        assert_eq!(render_gap(1024, 4096), "3.0 KiB");
    }

    #[test]
    fn one_benchmark_writing_several_results_files_gets_a_table_each() {
        let base = vec![
            peaks("h2o", "h2o_groupby", &[("1", Some(1024))]),
            peaks("h2o", "h2o_join", &[("1", Some(4096))]),
        ];
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &base,
            &[],
            &[("h2o".into(), stats(8192))],
            &[],
        );
        assert!(section.contains("**`h2o` — `h2o_groupby`**"));
        assert!(section.contains("**`h2o` — `h2o_join`**"));
        // The RSS pairing takes the max across both files, once.
        assert_eq!(section.matches("| h2o | base (`base-sha`)").count(), 1);
        assert!(section.contains("| h2o | base (`base-sha`) | 4.0 KiB | 8.0 KiB |"));
    }

    #[test]
    fn missing_resource_stats_drop_only_the_rss_row() {
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", Some(1024))])];
        let section = format_pool_peak_section("base-sha", "branch", &base, &[], &[], &[]);
        // Per-query table still renders.
        assert!(section.contains("| 1 | 1.0 KiB | n/a | n/a |"));
        // The pairing table is omitted entirely rather than showing empty rows.
        assert!(!section.contains("Pool accounting vs. process RSS"));
    }

    #[tokio::test]
    async fn collect_new_reads_only_files_the_invocation_wrote() {
        let dir = std::env::temp_dir().join("pool_peak_collect_new");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(
            dir.join("old.json"),
            r#"{"queries": [{"query": "1", "pool_peak_bytes": 1}]}"#,
        )
        .unwrap();
        let before = snapshot_json_files(&dir).await;
        assert_eq!(before.len(), 1);

        std::fs::write(
            dir.join("tpch_sf1.json"),
            r#"{"queries": [{"query": "1", "pool_peak_bytes": 4096}]}"#,
        )
        .unwrap();
        // Non-JSON and unparseable files are ignored rather than fatal.
        std::fs::write(dir.join("notes.txt"), "ignored").unwrap();
        std::fs::write(dir.join("broken.json"), "{ not json").unwrap();

        let collected = collect_new(&dir, &before, "tpch").await;
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].source, "tpch_sf1");
        assert_eq!(collected[0].bench, "tpch");
        assert_eq!(collected[0].max_pool_peak(), Some(4096));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_of_a_missing_directory_is_empty() {
        assert!(snapshot_json_files(Path::new("/nonexistent/results"))
            .await
            .is_empty());
    }
}
