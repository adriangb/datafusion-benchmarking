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
//!   is omitted. That is the default for runs triggered here. A suite that sets
//!   its own limit in SQL (`SET datafusion.runtime.memory_limit`, as
//!   `spill_views` does) records under that limit.
//! * The side predates #23985 — e.g. a `baseline: ref: v45.0.0` comparison.
//!
//! The suites that `bench.sh` runs through the Criterion SQL harness
//! (`cargo bench --bench sql`, e.g. `spill_views`, `wide_schema`) write their
//! results JSON to the [`CRITERION_RESULTS_SUBDIR`] of the results directory,
//! with the same format and no timings (`critcmp` reports those). The harness
//! only does this since the DataFusion change in [`HARNESS_PR`]. The runner
//! takes `bench.sh` from `main` but runs each side's own harness, so a side
//! older than that change writes no file for these suites.
//!
//! Some benchmarks therefore write no results JSON at all, so there is no file
//! to read the field from, with or without a memory limit: Criterion
//! `[[bench]]` targets always, and the SQL-harness suites on a side that
//! predates [`HARNESS_PR`]. When the trigger asked for a memory limit and
//! neither side wrote a file, the section names these benchmarks in a note, so
//! that "no data" does not read as "not supported" or the other way around.
//! Without a limit, they are left out silently, like everything else. When only
//! one side wrote a file, that side's column is `n/a` with a note that says so.
//!
//! The runner detects them by what the invocation produced, not by name: a
//! requested benchmark that left no results JSON on a side is one. The missing
//! file is the actual reason there is no data, and `bench.sh` can move a suite
//! between `dfbench` and the SQL harness (`tpch` has both paths), so a list of
//! names would go stale. The only `bench.sh` suite that writes no results JSON
//! and is not Criterion-based is `compile_profile`, which measures build time
//! and has no queries to record peaks for.
//!
//! The run-wide counterpart is [`monitor`](super::monitor), which samples peak
//! RSS from the benchmark's process subtree. The two are paired per *benchmark
//! invocation*, not per query: `pool_peak_bytes` is scoped to one query while
//! the RSS sample covers the whole invocation, so the only defensible pairing
//! is the largest per-query peak in a run against that run's peak RSS.

use std::collections::{BTreeSet, HashMap};
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

/// The env var that makes DataFusion install the pool that records peaks.
const MEMORY_LIMIT_ENV: &str = "DATAFUSION_RUNTIME_MEMORY_LIMIT";

/// Whether the trigger set [`MEMORY_LIMIT_ENV`] in any of the given env blocks
/// (the shared `env:` block or a per-side one). An empty value does not count.
pub fn memory_limit_requested<'a>(
    envs: impl IntoIterator<Item = &'a HashMap<String, String>>,
) -> bool {
    envs.into_iter().any(|env| {
        env.get(MEMORY_LIMIT_ENV)
            .is_some_and(|v| !v.trim().is_empty())
    })
}

/// Requested benchmarks that left no results JSON on either side, in request
/// order. See the module docs for why this identifies the Criterion-based ones.
fn benches_without_results(
    requested: &[String],
    base: &[BenchPeaks],
    changed: &[BenchPeaks],
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    requested
        .iter()
        .filter(|bench| {
            !base
                .iter()
                .chain(changed.iter())
                .any(|p| &p.bench == *bench)
        })
        .filter(|bench| seen.insert(bench.as_str()))
        .cloned()
        .collect()
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

/// Subdirectory of a side's results directory where `bench.sh` has the
/// Criterion SQL harness write its results JSON. It is kept out of the results
/// directory itself because `bench.sh compare` reads every `*.json` there as
/// timing results, and these files hold no timings (Criterion keeps those, and
/// `critcmp` reports them).
pub const CRITERION_RESULTS_SUBDIR: &str = "criterion";

/// The `*.json` files currently in `dir` and in its
/// [`CRITERION_RESULTS_SUBDIR`], as paths relative to `dir` (`tpch_sf1.json`,
/// `criterion/spill_views.json`). Empty when neither is readable.
///
/// Taken before and after a benchmark invocation so the files it wrote can be
/// attributed to it: the results file name does not follow the benchmark name
/// (`tpch` writes `tpch_sf1.json`, `topk_tpch` writes `run_topk_tpch.json`), so
/// there is nothing to match on. Each side writes into its own `results/<name>`
/// directory and the tree is cleaned before the run, so within one side each
/// invocation's files are genuinely new.
pub async fn snapshot_json_files(dir: &Path) -> BTreeSet<String> {
    let mut names = json_files_in(dir, "").await;
    names.extend(
        json_files_in(
            &dir.join(CRITERION_RESULTS_SUBDIR),
            &format!("{CRITERION_RESULTS_SUBDIR}/"),
        )
        .await,
    );
    names
}

/// The `*.json` file names in `dir`, each with `prefix` prepended.
async fn json_files_in(dir: &Path, prefix: &str) -> BTreeSet<String> {
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
            names.insert(format!("{prefix}{name}"));
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
        let source = Path::new(name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(name)
            .to_string();
        out.push(BenchPeaks {
            bench: bench.to_string(),
            source,
            queries,
        });
    }
    out
}

/// Render the peak-pool section for the PR comment.
///
/// `requested` is every benchmark the run was asked for, and
/// `limit_requested` is whether the trigger set a memory limit (see
/// [`memory_limit_requested`]).
///
/// Returns an empty string when neither side reported a single peak and there
/// is nothing to explain, so a default run — which sets no memory limit and
/// therefore records nothing — leaves the comment exactly as it was before this
/// section existed. When a limit was requested, a benchmark that wrote no
/// results JSON gets a note instead of disappearing without a word.
#[allow(clippy::too_many_arguments)]
pub fn format_pool_peak_section(
    base_label: &str,
    changed_label: &str,
    requested: &[String],
    limit_requested: bool,
    base: &[BenchPeaks],
    changed: &[BenchPeaks],
    base_stats: &[(String, ResourceStats)],
    changed_stats: &[(String, ResourceStats)],
) -> String {
    let base_has = any_data(base);
    let changed_has = any_data(changed);
    let unavailable_note = if limit_requested {
        no_results_note(&benches_without_results(requested, base, changed))
    } else {
        String::new()
    };
    if !base_has && !changed_has && unavailable_note.is_empty() {
        return String::new();
    }

    let mut out = format!(
        "Peak `MemoryPool` reservation per query — what DataFusion's accounting \
         believes it reserved. Recorded only for benchmarks that write a results \
         JSON (the `dfbench` suites, and the suites `bench.sh` runs through the \
         Criterion SQL harness since {HARNESS_PR}), and only under a memory limit: \
         `DATAFUSION_RUNTIME_MEMORY_LIMIT`, or one the suite sets itself.\n\n\
         Base: `{base_label}` | Changed: `{changed_label}`\n\n"
    );

    if !base_has && !changed_has {
        // Only the note to show: every benchmark that wrote a results JSON
        // (if any) recorded nothing, so its table would be all `n/a`.
        out.push_str(&unavailable_note);
        return out;
    }

    // A side that wrote no results JSON at all is explained per benchmark by
    // `one_side_note` below; this note is for results JSONs without the field.
    if !base_has && !base.is_empty() {
        out.push_str(&missing_side_note("Base", base_label));
    }
    if !changed_has && !changed.is_empty() {
        out.push_str(&missing_side_note("Changed", changed_label));
    }
    out.push_str(&one_side_note(
        "Base",
        base_label,
        &benches_missing_from(changed, base),
    ));
    out.push_str(&one_side_note(
        "Changed",
        changed_label,
        &benches_missing_from(base, changed),
    ));
    out.push_str(&unavailable_note);

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

/// The DataFusion change that made the Criterion SQL harness write a results
/// JSON. Sides older than it write none for those suites.
const HARNESS_PR: &str = "apache/datafusion#25644";

/// Why `benches` have no rows: they wrote no results JSON on either side.
/// Empty when `benches` is empty.
fn no_results_note(benches: &[String]) -> String {
    if benches.is_empty() {
        return String::new();
    }
    let names = code_list(benches);
    let (it, runs) = if benches.len() == 1 {
        ("it", "runs")
    } else {
        ("they", "run")
    };
    format!(
        "> Pool peaks are not available for {names}: {it} {runs} through Criterion \
         and wrote no results JSON. A Criterion bench target never writes one. The \
         SQL harness `cargo bench --bench sql`, which `bench.sh` uses for some \
         suites, writes one only since {HARNESS_PR}.\n\n"
    )
}

/// Benchmarks that wrote a results JSON on the `present` side but none on the
/// `absent` side, in `present` order.
fn benches_missing_from(present: &[BenchPeaks], absent: &[BenchPeaks]) -> Vec<String> {
    benches_in_order(present, &[])
        .into_iter()
        .filter(|bench| !absent.iter().any(|p| &p.bench == bench))
        .collect()
}

/// Why one side's column is `n/a` for `benches`: that side wrote no results
/// JSON for them while the other side did. Both sides run the same `bench.sh`,
/// so the difference is the DataFusion checkout, and in practice it is a Criterion
/// SQL harness that predates [`HARNESS_PR`] (for example, `baseline: ref:`
/// set to an older release). Empty when `benches` is empty.
fn one_side_note(side: &str, label: &str, benches: &[String]) -> String {
    if benches.is_empty() {
        return String::new();
    }
    format!(
        "> {side} (`{label}`) wrote no results JSON for {}, so its column is `n/a`. \
         For a suite that `bench.sh` runs through the Criterion SQL harness, this \
         means that side predates {HARNESS_PR}.\n\n",
        code_list(benches),
    )
}

/// `a`, `b`, `c`
fn code_list(items: &[String]) -> String {
    items
        .iter()
        .map(|b| format!("`{b}`"))
        .collect::<Vec<_>>()
        .join(", ")
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

    fn names(benches: &[&str]) -> Vec<String> {
        benches.iter().map(|b| b.to_string()).collect()
    }

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn memory_limit_requested_checks_every_env_block() {
        let empty = HashMap::new();
        let limit = env(&[(MEMORY_LIMIT_ENV, "512M")]);
        let other = env(&[("RUST_LOG", "debug")]);
        let blank = env(&[(MEMORY_LIMIT_ENV, " ")]);
        assert!(!memory_limit_requested([&empty, &other, &empty]));
        assert!(!memory_limit_requested([&blank]));
        // Shared block, or either per-side block.
        assert!(memory_limit_requested([&limit, &empty, &empty]));
        assert!(memory_limit_requested([&empty, &empty, &limit]));
    }

    #[test]
    fn criterion_bench_with_limit_gets_a_note() {
        // `spill_views` through the SQL harness: no results JSON on either side.
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &names(&["spill_views"]),
            true,
            &[],
            &[],
            &[("spill_views".into(), stats(1 << 30))],
            &[("spill_views".into(), stats(1 << 30))],
        );
        assert!(section.contains(
            "> Pool peaks are not available for `spill_views`: it runs through Criterion \
             and wrote no results JSON."
        ));
        assert!(section.contains(HARNESS_PR));
        // Neither the per-side notes nor empty tables: those describe a results
        // JSON that lacks the field, or one that only one side wrote.
        assert!(!section.contains("reported no `pool_peak_bytes`"));
        assert!(!section.contains("wrote no results JSON for"));
        assert!(!section.contains("| Query |"));
        assert!(!section.contains("Pool accounting vs. process RSS"));
    }

    #[test]
    fn criterion_bench_without_limit_renders_nothing() {
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &names(&["spill_views", "wide_schema"]),
            false,
            &[],
            &[],
            &[("spill_views".into(), stats(1 << 30))],
            &[("spill_views".into(), stats(1 << 30))],
        );
        assert_eq!(section, "");
    }

    #[test]
    fn data_present_renders_the_table_as_before() {
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", Some(1024))])];
        let changed = vec![peaks("tpch", "tpch_sf1", &[("1", Some(2048))])];
        let base_stats = [("tpch".into(), stats(4096))];
        let changed_stats = [("tpch".into(), stats(4096))];
        let with_limit = format_pool_peak_section(
            "base-sha",
            "branch",
            &names(&["tpch"]),
            true,
            &base,
            &changed,
            &base_stats,
            &changed_stats,
        );
        let without = format_pool_peak_section(
            "base-sha",
            "branch",
            &[],
            false,
            &base,
            &changed,
            &base_stats,
            &changed_stats,
        );
        // A benchmark that wrote its results JSON gets no note, so the section
        // is exactly what it was before the note existed.
        assert_eq!(with_limit, without);
        assert!(!with_limit.contains("not available"));
        assert!(with_limit.contains("| 1 | 1.0 KiB | 2.0 KiB | +100.0% |"));
    }

    #[test]
    fn mixed_run_keeps_the_table_and_notes_the_criterion_benches() {
        let base = vec![peaks("tpch", "tpch_sf1", &[("1", Some(1024))])];
        let changed = vec![peaks("tpch", "tpch_sf1", &[("1", Some(2048))])];
        let section = format_pool_peak_section(
            "base-sha",
            "branch",
            &names(&["tpch", "spill_views", "wide_schema"]),
            true,
            &base,
            &changed,
            &[],
            &[],
        );
        assert!(section.contains("| 1 | 1.0 KiB | 2.0 KiB | +100.0% |"));
        assert!(section.contains(
            "> Pool peaks are not available for `spill_views`, `wide_schema`: they run through Criterion"
        ));
        assert!(!section.contains("`tpch`, "));
    }

    /// `spill_views` with the harness that writes `criterion/spill_views.json`
    /// on both sides. The suite sets its own limit in SQL, so peaks are there
    /// even though the trigger set no `DATAFUSION_RUNTIME_MEMORY_LIMIT`.
    #[test]
    fn criterion_suite_with_results_json_gets_the_table() {
        let base = vec![peaks(
            "spill_views",
            "spill_views",
            &[
                (
                    "spill_views/q01_sort_string_1_distinct_repeated",
                    Some(40 << 20),
                ),
                (
                    "spill_views/q04_sort_string_all_distinct_distinct",
                    Some(96 << 20),
                ),
            ],
        )];
        let changed = vec![peaks(
            "spill_views",
            "spill_views",
            &[
                (
                    "spill_views/q01_sort_string_1_distinct_repeated",
                    Some(20 << 20),
                ),
                (
                    "spill_views/q04_sort_string_all_distinct_distinct",
                    Some(96 << 20),
                ),
            ],
        )];
        for limit_requested in [false, true] {
            let section = format_pool_peak_section(
                "base-sha",
                "branch",
                &names(&["spill_views"]),
                limit_requested,
                &base,
                &changed,
                &[("spill_views".into(), stats(1 << 30))],
                &[("spill_views".into(), stats(1 << 30))],
            );
            assert!(section.contains("**`spill_views`**"));
            assert!(section.contains(
                "| spill_views/q01_sort_string_1_distinct_repeated | 40.0 MiB | 20.0 MiB | -50.0% |"
            ));
            assert!(section.contains(
                "| spill_views/q04_sort_string_all_distinct_distinct | 96.0 MiB | 96.0 MiB | +0.0% |"
            ));
            assert!(section.contains("| spill_views | base (`base-sha`) | 96.0 MiB | 1.0 GiB |"));
            // Data on both sides: no note of any kind.
            assert!(!section.contains("> "), "{section}");
        }
    }

    /// `baseline: ref:` set to a release whose harness predates the change:
    /// only the changed side wrote `criterion/spill_views.json`.
    #[test]
    fn criterion_suite_with_results_json_on_one_side_notes_the_other() {
        let changed = vec![peaks(
            "spill_views",
            "spill_views",
            &[(
                "spill_views/q01_sort_string_1_distinct_repeated",
                Some(40 << 20),
            )],
        )];
        let section = format_pool_peak_section(
            "v55.0.0",
            "branch",
            &names(&["spill_views"]),
            true,
            &[],
            &changed,
            &[("spill_views".into(), stats(1 << 30))],
            &[("spill_views".into(), stats(1 << 30))],
        );
        assert!(section.contains(
            "| spill_views/q01_sort_string_1_distinct_repeated | n/a | 40.0 MiB | n/a |"
        ));
        assert!(section.contains(
            "> Base (`v55.0.0`) wrote no results JSON for `spill_views`, so its column is `n/a`."
        ));
        // Not the notes for a results JSON without the field, for a changed
        // side, or for a benchmark with no results JSON on either side.
        assert!(!section.contains("reported no `pool_peak_bytes`"));
        assert!(!section.contains("> Changed"));
        assert!(!section.contains("Pool peaks are not available"));
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
            &[],
            false,
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
            &[],
            false,
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
            &[],
            false,
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
            &[],
            false,
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
            &[],
            false,
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
            &[],
            false,
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
        let section =
            format_pool_peak_section("base-sha", "branch", &[], false, &base, &[], &[], &[]);
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
    async fn collect_new_reads_the_criterion_results_subdir() {
        let dir = std::env::temp_dir().join("pool_peak_collect_new_criterion");
        let _ = std::fs::remove_dir_all(&dir);
        let criterion = dir.join(CRITERION_RESULTS_SUBDIR);
        std::fs::create_dir_all(&criterion).unwrap();

        std::fs::write(
            dir.join("tpch_sf1.json"),
            r#"{"queries": [{"query": "1", "pool_peak_bytes": 1}]}"#,
        )
        .unwrap();
        let before = snapshot_json_files(&dir).await;
        assert_eq!(before.len(), 1);

        // What the Criterion SQL harness writes: no timings, only peaks.
        std::fs::write(
            criterion.join("spill_views.json"),
            r#"{"queries": [{"query": "spill_views/q01_sort_string_1_distinct_repeated",
                "iterations": [], "start_time": 1, "success": true,
                "pool_peak_bytes": 41943040}]}"#,
        )
        .unwrap();

        let collected = collect_new(&dir, &before, "spill_views").await;
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].source, "spill_views");
        assert_eq!(collected[0].bench, "spill_views");
        assert_eq!(collected[0].max_pool_peak(), Some(40 << 20));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_of_a_missing_directory_is_empty() {
        assert!(snapshot_json_files(Path::new("/nonexistent/results"))
            .await
            .is_empty());
    }
}
