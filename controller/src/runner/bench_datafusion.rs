//! Unified DataFusion benchmark runner.
//!
//! There is no benchmark allowlist and no per-name classification baked into
//! the controller. For each requested benchmark the runner resolves how to run
//! it at runtime:
//!
//! * If the name is a real Criterion `[[bench]]` target in the `benchmarks`
//!   crate (discovered via `cargo metadata`), it is run with
//!   `cargo bench --bench <name> -- --save-baseline <side>`, enabling whatever
//!   `required-features` that target declares.
//! * Otherwise it is run through `bench.sh run <name>` (TPC-H variants take a
//!   direct `dfbench` shortcut). `bench.sh` itself runs some suites through the
//!   Criterion SQL harness (`wide_schema`, …) and others through `dfbench`.
//!
//! Comparison is then driven by the artifacts each run actually produced, not
//! by the benchmark name: `results/<side>/*.json` is diffed with
//! `bench.sh compare_detail`, and `target/criterion` baselines are diffed with
//! `critcmp`. A run mixing both families emits both sections.
//!
//! `compare_detail` reports wall time only, so the same `results/<side>/*.json`
//! are additionally read here for `pool_peak_bytes` — see
//! [`pool_peak`](super::pool_peak). The Criterion SQL harness writes the same
//! JSON, without timings, to `results/<side>/criterion/`, which
//! `compare_detail` does not read, so its suites get pool peaks without a
//! second timing table. Criterion `[[bench]]` targets write no such JSON, so
//! they have no pool peaks.
//!
//! With `rounds: N` (from `BENCH_ROUNDS`), each `bench.sh` suite runs in
//! [`abba_order`]. Each pass writes to `results/<side>.pass<k>/`, and
//! [`merge_pass_dirs`] combines the passes into `results/<side>/` before the
//! comparison. Resource stats and pool peaks come from each side's first pass.
//! Criterion `[[bench]]` targets still run once per side.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::github;
use crate::runner::build_env;
use crate::runner::config::RunnerConfig;
use crate::runner::criterion::{Criterion, Run};
use crate::runner::execution::{execute, partition, CommandHarness, CommandRun};
use crate::sharding::{Shard, ShardSupport};

// Mixed DataFusion harnesses/setup lack uniform discovery and batched subset
// execution. Enable sharding once their adapters implement that shared contract.
pub const SHARD_SUPPORT: ShardSupport = ShardSupport::SingleWorkerOnly(
    "DataFusion setup does not yet support uniform discovery and batched subset execution",
);
use crate::runner::git;
use crate::runner::monitor::{self, ResourceStats};
use crate::runner::pool_peak::{self, BenchPeaks};
use crate::runner::poster::CommentPoster;
use crate::runner::shell;
use crate::runner::trigger;

/// Run DataFusion benchmarks comparing a PR branch to its merge-base.
pub async fn run(config: &RunnerConfig, poster: &CommentPoster) -> Result<()> {
    SHARD_SUPPORT.validate_execution(config.shard)?;
    let repo_url = config.repo_url();
    let benchmarks = &config.benchmarks;

    let branch_dir = PathBuf::from("/workspace/datafusion-branch");
    let base_dir = PathBuf::from("/workspace/datafusion-base");
    let bench_dir = PathBuf::from("/workspace/datafusion-bench");

    // Clone and checkout PR branch
    info!("=== Cloning PR branch ===");
    git::clone_shallow(&repo_url, &branch_dir, 200).await?;
    let branch_name = git::checkout_pr(&config.pr_url, &branch_dir).await?;
    let merge_base = git::merge_base(&branch_dir).await?;

    // If a custom changed ref is specified, checkout that instead of PR head
    if let Some(ref changed_ref) = config.changed_ref {
        info!(changed_ref, "=== Checking out custom changed ref ===");
        git::fetch_pr_ref(&config.pr_url, &branch_dir).await?;
        git::fetch_origin(&branch_dir).await?;
        git::checkout(&branch_dir, changed_ref).await?;
    }

    // Determine baseline: custom ref or merge-base
    let baseline_display: String;
    info!("=== Cloning merge-base ===");
    git::clone_shallow(&repo_url, &base_dir, 200).await?;
    if let Some(ref baseline_ref) = config.baseline_ref {
        info!(baseline_ref, "=== Checking out custom baseline ref ===");
        git::fetch_pr_ref(&config.pr_url, &base_dir).await?;
        git::fetch_origin(&base_dir).await?;
        git::checkout(&base_dir, baseline_ref).await?;
        baseline_display = baseline_ref.clone();
    } else {
        git::checkout(&base_dir, &merge_base).await?;
        baseline_display = merge_base.clone();
    }

    // Pre-install stable toolchain to avoid rustup race in parallel builds
    git::rustup_stable().await?;

    // Resolve which requested names are real Criterion bench targets. Anything
    // else runs through bench.sh. Resolved from the branch checkout so a new
    // bench target added in the PR is recognized.
    let requested: Vec<String> = benchmarks.split_whitespace().map(String::from).collect();
    let bench_targets = criterion_bench_targets(&branch_dir.join("benchmarks")).await;
    let is_criterion = |bench: &str| bench_targets.contains_key(bench);
    let any_shell = requested.iter().any(|b| !is_criterion(b));

    // dfbench is only needed for the bench.sh / TPC-H path. Build it for both
    // sides in parallel; skip entirely for Criterion-only runs.
    let builds = if any_shell {
        info!("=== Compiling dfbench for PR branch and merge-base in parallel ===");
        let branch_build = shell::spawn_command(
            "cargo",
            &["build", "--release", "--bin", "dfbench"],
            &branch_dir.join("benchmarks"),
            "/tmp/branch_build.log",
        );
        let base_build = shell::spawn_command(
            "cargo",
            &["build", "--release", "--bin", "dfbench"],
            &base_dir.join("benchmarks"),
            "/tmp/base_build.log",
        );
        Some((branch_build, base_build))
    } else {
        None
    };

    // Post "running" comment
    let uname = shell::uname().await;
    let instance_type = shell::node_instance_type().await;
    let pod_resources = shell::pod_resources();
    let lscpu = shell::lscpu().await;
    let pr_number = config.pr_number()?;

    // Resolve display names for the comparison
    let changed_display = config.changed_ref.as_deref().unwrap_or(&branch_name);
    let changed_sha = git::rev_parse_head(&branch_dir).await?;
    let base_sha = git::rev_parse_head(&base_dir).await?;
    let baseline_label = if config.baseline_ref.is_some() {
        baseline_display.clone()
    } else {
        format!("{} (merge-base)", &base_sha[..7.min(base_sha.len())])
    };

    let comparison = trigger::Comparison {
        repo: &config.repo,
        changed_display,
        changed_sha: &changed_sha,
        baseline_label: &baseline_label,
        base_sha: &base_sha,
    };
    let config_block = trigger::config_block(config, benchmarks);

    let footer = github::issues_footer(config.runner_repo_url.as_deref());
    let running_body = format!(
        "\u{1f916} Benchmark running (GKE) | [trigger]({})\n\
         **Instance:** `{instance_type}` ({pod_resources}) | `{uname}`\n\
         <details><summary>CPU Details (lscpu)</summary>\n\n\
         ```\n\
         {lscpu}\n\
         ```\n\n\
         </details>\n\n\
         {comparison}\n\n\
         {config_block}\
         Results will be posted here when complete{footer}",
        config.comment_url,
        comparison = comparison.line(),
    );
    poster
        .post_comment(&config.repo, pr_number, &running_body)
        .await?;

    // Wait for builds
    if let Some((branch_build, base_build)) = builds {
        info!("=== Waiting for builds ===");
        branch_build
            .await
            .context("branch build task panicked")?
            .context("branch build failed")?;
        base_build
            .await
            .context("base build task panicked")?
            .context("base build failed")?;
        info!("=== Builds complete ===");
    }

    // Set up bench runner from a third checkout (only used by the bench.sh path)
    let bench_benchmarks = bench_dir.join("benchmarks");
    if any_shell {
        info!("=== Setting up bench runner ===");
        git::clone_shallow(&repo_url, &bench_dir, 200).await?;
        git::checkout(&bench_dir, "origin/main").await?;

        // Clean any prior results
        let results_dir = bench_benchmarks.join("results");
        if results_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&results_dir).await;
        }

        // Copy TPC-H expected answer files so bench.sh skips the docker-based copy
        copy_tpch_answers(&bench_benchmarks).await;
    }

    // Run each benchmark
    let mut base_stats_list: Vec<(String, ResourceStats)> = Vec::new();
    let mut branch_stats_list: Vec<(String, ResourceStats)> = Vec::new();
    // Per-query pool peaks, attributed to the invocation that wrote them.
    let mut base_peaks: Vec<BenchPeaks> = Vec::new();
    let mut branch_peaks: Vec<BenchPeaks> = Vec::new();

    let bench_dir_str = bench_benchmarks.to_string_lossy().to_string();

    let baseline_extra_env = config.baseline_env_args();
    let changed_extra_env = config.changed_env_args();

    // Explicit RESULTS_NAME / baseline name ensures bench.sh and Criterion save
    // to a predictable location per side, regardless of branch vs detached HEAD.
    let base_results_name = "HEAD".to_string();
    let bench_branch_name = git::sanitize_branch_name(&branch_name);

    let base_results_dir = bench_benchmarks.join("results").join(&base_results_name);
    let branch_results_dir = bench_benchmarks.join("results").join(&bench_branch_name);

    // Track whether any Criterion baselines were produced per side, so the
    // comparison can pick `critcmp HEAD <branch>` vs a branch-only `critcmp`.
    let mut criterion_base_ok = false;
    let mut criterion_branch_ok = false;

    for bench in &requested {
        if is_criterion(bench) {
            let required_features = bench_targets
                .get(bench)
                .map(Vec::as_slice)
                .unwrap_or_default();
            info!("** Setting up data for criterion bench {bench} **");
            setup_criterion_data(bench, &branch_dir, &base_dir).await;

            info!("** Running {bench} baseline (criterion) **");
            match run_criterion_side(
                bench,
                &base_dir,
                &base_results_name,
                &config.bench_filter,
                &baseline_extra_env,
                required_features,
                config.shard,
            )
            .await
            {
                Ok(Some(stats)) => {
                    criterion_base_ok = true;
                    base_stats_list.push((bench.clone(), stats));
                }
                Ok(None) => {}
                Err(e) => {
                    // Most likely a new bench target absent on the base — fall
                    // back to a branch-only comparison for it.
                    warn!("Criterion baseline for {bench} failed (new bench?): {e:#}");
                }
            }

            info!("** Running {bench} branch (criterion) **");
            let stats = run_criterion_side(
                bench,
                &branch_dir,
                &bench_branch_name,
                &config.bench_filter,
                &changed_extra_env,
                required_features,
                config.shard,
            )
            .await
            .with_context(|| format!("run {bench} (branch, criterion)"))?;
            if let Some(stats) = stats {
                criterion_branch_ok = true;
                branch_stats_list.push((bench.clone(), stats));
            }
            continue;
        }

        info!("** Creating data if needed for {bench} **");
        cache_data(bench, &bench_dir_str).await;

        let mut base_pass_dirs: Vec<PathBuf> = Vec::new();
        let mut branch_pass_dirs: Vec<PathBuf> = Vec::new();
        for (pass, side) in abba_order(config.rounds).into_iter().enumerate() {
            let (label, side_dir, side_name, side_results_dir, extra_env) = match side {
                Side::Baseline => (
                    "base",
                    &base_dir,
                    &base_results_name,
                    &base_results_dir,
                    &baseline_extra_env,
                ),
                Side::Branch => (
                    "branch",
                    &branch_dir,
                    &bench_branch_name,
                    &branch_results_dir,
                    &changed_extra_env,
                ),
            };
            let (results_name, results_dir) = if config.rounds == 1 {
                (side_name.clone(), side_results_dir.clone())
            } else {
                let name = format!("{side_name}.pass{pass}");
                let dir = bench_benchmarks.join("results").join(&name);
                (name, dir)
            };
            let extra_env = with_split_max_time(extra_env, config, side, config.rounds);

            info!("** Running {bench} {label} (pass {pass}) **");
            let spill_dir = PathBuf::from(format!("/workspace/spill-{label}-{bench}"));
            let _ = tokio::fs::create_dir_all(&spill_dir).await;
            // The results file name doesn't follow the benchmark name, so bracket
            // the run and attribute whatever JSON appears to this invocation.
            let before = pool_peak::snapshot_json_files(&results_dir).await;
            let stats = run_shell_side(
                bench,
                side_dir,
                &bench_benchmarks,
                &results_name,
                side_name,
                &spill_dir,
                &extra_env,
                config.shard,
            )
            .await
            .with_context(|| format!("run {bench} ({label}, pass {pass})"))?;
            let _ = tokio::fs::remove_dir_all(&spill_dir).await;
            let peaks = pool_peak::collect_new(&results_dir, &before, bench).await;

            let (pass_dirs, peaks_list, stats_list) = match side {
                Side::Baseline => (&mut base_pass_dirs, &mut base_peaks, &mut base_stats_list),
                Side::Branch => (
                    &mut branch_pass_dirs,
                    &mut branch_peaks,
                    &mut branch_stats_list,
                ),
            };
            // Pool peaks and resource stats come from each side's first pass
            // only: they describe one run of the suite, and later passes
            // repeat the same work.
            if pass_dirs.is_empty() {
                peaks_list.extend(peaks);
                if let Some(stats) = stats {
                    stats_list.push((bench.clone(), stats));
                }
            }
            pass_dirs.push(results_dir);
        }
        if config.rounds > 1 {
            merge_pass_dirs(&base_pass_dirs, &base_results_dir).await?;
            merge_pass_dirs(&branch_pass_dirs, &branch_results_dir).await?;
        }
    }

    // Compare and post results, driven by the artifacts that were produced:
    //   - results/*.json (dfbench suites) → `bench.sh compare` + `compare_detail`
    //   - target/criterion baselines (Criterion targets + the bench.sh SQL
    //     harness) → `critcmp`
    let mut report = String::new();

    if any_shell && results_have_json(&bench_benchmarks, &base_results_name).await {
        // Two tables over the same results JSON, differing only in the statistic
        // `compare.py` reduces each query's iterations to:
        //
        //   `compare`        → min  (fastest iteration)
        //   `compare_detail` → mean (and renders min/mean±stddev/max)
        //
        // The verdict table is the min-based one. An A/A run — identical SHAs on
        // both sides, so every reported difference is noise — put the mean-based
        // per-query error at 1.29% median / 4.31% worst and the aggregate at
        // -0.26%, against 0.55% / 3.52% and +0.00% for min:
        // https://github.com/adriangb/datafusion/pull/15#issuecomment-5139405485
        //
        // Both sides run sequentially in one pod, so a slow iteration is usually
        // the node interfering rather than the query; the mean folds that in and
        // the min discards it. `compare_detail` still follows, because the spread
        // it shows is what makes an unstable query legible as unstable instead of
        // as a regression.
        let summary = shell::run_command(
            "./bench.sh",
            &["compare", &base_results_name, &bench_branch_name],
            &bench_benchmarks,
        )
        .await
        .context("bench.sh compare")?;
        report.push_str(&summary);

        let detail = shell::run_command(
            "./bench.sh",
            &["compare_detail", &base_results_name, &bench_branch_name],
            &bench_benchmarks,
        )
        .await
        .context("bench.sh compare_detail")?;
        if !report.is_empty() {
            report.push('\n');
        }
        report.push_str("Distribution per query (min / mean ±stddev / max):\n\n");
        report.push_str(&detail);
    }

    // Criterion output (from either path) lands under <side>/target/criterion.
    criterion_branch_ok = criterion_branch_ok || criterion_dir_present(&branch_dir).await;
    criterion_base_ok = criterion_base_ok || criterion_dir_present(&base_dir).await;
    if criterion_branch_ok {
        let critcmp = if criterion_base_ok {
            // Gather both sides' baselines into one tree, then diff them.
            copy_criterion_baselines(&base_dir, &branch_dir).await;
            shell::run_command(
                "critcmp",
                &[base_results_name.as_str(), bench_branch_name.as_str()],
                &branch_dir,
            )
            .await
            .context("critcmp")?
        } else {
            // No baseline available (e.g. a brand-new bench) — branch-only.
            let mut out = String::from("New benchmark — branch-only Criterion results:\n\n");
            out.push_str(
                &shell::run_command("critcmp", &[bench_branch_name.as_str()], &branch_dir)
                    .await
                    .context("critcmp")?,
            );
            out
        };
        if !report.is_empty() {
            report.push('\n');
        }
        report.push_str(&critcmp);
    }

    let resource_section = format_resource_section(&base_stats_list, &branch_stats_list);
    let limit_requested = pool_peak::memory_limit_requested([
        &config.shared_env_vars,
        &config.baseline_env_vars,
        &config.changed_env_vars,
    ]);
    let pool_section = pool_peak::format_pool_peak_section(
        &baseline_label,
        changed_display,
        &requested,
        limit_requested,
        &base_peaks,
        &branch_peaks,
        &base_stats_list,
        &branch_stats_list,
    );
    let result_body = format_result_comment(
        &config.comment_url,
        &comparison.line(),
        &config_block,
        &report,
        &resource_section,
        &pool_section,
        &instance_type,
        &pod_resources,
        &lscpu,
        &footer,
    );
    poster
        .post_comment(&config.repo, pr_number, &result_body)
        .await?;

    Ok(())
}

/// Discover Criterion `[[bench]]` targets in the `benchmarks` crate via
/// `cargo metadata`, keyed by target name and carrying each target's
/// `required-features`. Returns an empty map on any failure — the caller then
/// treats every name as a `bench.sh` suite (which itself reports unknown
/// names), so a metadata hiccup degrades gracefully rather than misrouting.
async fn criterion_bench_targets(crate_dir: &Path) -> BTreeMap<String, Vec<String>> {
    let out = match shell::run_command(
        "cargo",
        &["metadata", "--no-deps", "--format-version", "1"],
        crate_dir,
    )
    .await
    {
        Ok(out) => out,
        Err(e) => {
            warn!("cargo metadata failed; treating all benches as bench.sh suites: {e:#}");
            return BTreeMap::new();
        }
    };
    parse_bench_targets(&out)
}

/// Extract `bench`-kind targets from `cargo metadata` JSON, keyed by target
/// name and carrying the target's `required-features`.
///
/// The features are load-bearing: cargo refuses to run a bench target whose
/// `required-features` are not enabled, and exits non-zero saying so rather
/// than skipping it. `multi_group_by` in `datafusion-physical-plan` needs
/// `test_utils`, for one. Target names are unique across the workspace in
/// practice, but a collision takes the union of the requirements, because the
/// run selects the target by name across every member.
fn parse_bench_targets(metadata_json: &str) -> BTreeMap<String, Vec<String>> {
    let mut targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) else {
        return targets;
    };
    let Some(packages) = value.get("packages").and_then(|p| p.as_array()) else {
        return targets;
    };
    for pkg in packages {
        let Some(pkg_targets) = pkg.get("targets").and_then(|t| t.as_array()) else {
            continue;
        };
        for target in pkg_targets {
            let is_bench = target
                .get("kind")
                .and_then(|k| k.as_array())
                .map(|kinds| kinds.iter().any(|k| k.as_str() == Some("bench")))
                .unwrap_or(false);
            if !is_bench {
                continue;
            }
            let Some(name) = target.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let entry = targets.entry(name.to_string()).or_default();
            let required = target
                .get("required-features")
                .and_then(|f| f.as_array())
                .map(Vec::as_slice)
                .unwrap_or_default();
            for feature in required.iter().filter_map(|f| f.as_str()) {
                if !entry.iter().any(|f| f == feature) {
                    entry.push(feature.to_string());
                }
            }
        }
    }
    targets
}

/// The `--features` flag for a Criterion run: `parquet`, which the DataFusion
/// bench targets have always been run with, plus the target's own
/// `required-features`. The run is invoked from the workspace root, where
/// cargo resolves each bare feature name against the members that declare it,
/// so features owned by different packages can be listed together.
fn criterion_features_arg(required_features: &[String]) -> String {
    let mut features = vec!["parquet".to_string()];
    for feature in required_features {
        if !features.contains(feature) {
            features.push(feature.clone());
        }
    }
    format!("--features={}", features.join(","))
}

/// Run a Criterion bench target on one side, saving a named baseline.
///
/// A non-empty `bench_filter` (from the `BENCH_FILTER` env var) is passed
/// through to Criterion as a test-name filter.
async fn run_criterion_side(
    bench: &str,
    side_dir: &Path,
    baseline_name: &str,
    bench_filter: &str,
    extra_env: &[String],
    required_features: &[String],
    shard: Shard,
) -> Result<Option<ResourceStats>> {
    let harness = Criterion {
        bench_args: vec![
            "bench".into(),
            criterion_features_arg(required_features),
            "--bench".into(),
            bench.into(),
        ],
    };
    let plan = Run::new(baseline_name, bench_filter, side_dir, extra_env);
    let plan = partition(&harness, plan, shard, bench).await?;
    execute(&harness, &plan).await
}

/// Run a single `bench.sh` benchmark on one side (base or branch).
///
/// For TPC-H variants we bypass `bench.sh run` and invoke the prebuilt `dfbench`
/// binary directly. Upstream PR apache/datafusion#21707 ported `bench.sh`'s
/// `run_tpch` to a Criterion-based SQL harness whose data paths are relative
/// to `${DATAFUSION_DIR}/benchmarks` and whose timings live under
/// `target/criterion/`, neither of which fits this controller's layout (data
/// is in `bench_dir/benchmarks/data`, comparisons read JSON from
/// `bench_dir/benchmarks/results/`). The `dfbench tpch` subcommand still
/// exists upstream, so we call it directly with the same args the old
/// `run_tpch` used. Other benchmarks continue through `bench.sh`.
#[allow(clippy::too_many_arguments)]
async fn run_shell_side(
    bench: &str,
    side_dir: &Path,
    bench_benchmarks: &Path,
    results_name: &str,
    criterion_baseline: &str,
    spill_dir: &Path,
    extra_env: &[String],
    shard: Shard,
) -> Result<Option<ResourceStats>> {
    let command = if let Some((tpch_args, results_filename)) = tpch_direct_args(bench) {
        tpch_command(
            side_dir,
            bench_benchmarks,
            results_name,
            spill_dir,
            extra_env,
            tpch_args,
            results_filename,
        )
        .await?
    } else {
        let mut args = shell_side_env_args(
            side_dir,
            results_name,
            criterion_baseline,
            spill_dir,
            extra_env,
        );
        args.extend([
            "./bench.sh".to_string(),
            "run".to_string(),
            bench.to_string(),
        ]);
        CommandRun {
            program: "env".into(),
            args,
            cwd: bench_benchmarks.into(),
            spill_dir: Some(spill_dir.into()),
        }
    };
    let plan = partition(&CommandHarness, command, shard, bench).await?;
    execute(&CommandHarness, &plan).await
}

/// `KEY=VALUE` args passed to `env` ahead of `./bench.sh run`. `env` applies
/// them left to right, so the per-side `extra_env` overrides the runner's
/// defaults, and [`build_env`]'s settings come last so nothing overrides them.
fn shell_side_env_args(
    side_dir: &Path,
    results_name: &str,
    criterion_baseline: &str,
    spill_dir: &Path,
    extra_env: &[String],
) -> Vec<String> {
    let mut args: Vec<String> = vec![
        format!("DATAFUSION_DIR={}", side_dir.display()),
        format!("RESULTS_NAME={results_name}"),
        // Suites that bench.sh runs through the Criterion SQL harness read
        // SQL_CARGO_COMMAND; setting it unconditionally is harmless for the
        // dfbench-based suites (which never read it) and saves a named
        // baseline per side for the ones that do, so we can critcmp them.
        // The baseline is named after the side, not the pass, so `critcmp`
        // finds it. With several rounds the last pass's baseline wins.
        format!(
            "SQL_CARGO_COMMAND=cargo bench --bench sql -- --save-baseline {criterion_baseline}"
        ),
    ];
    args.extend(extra_env.iter().cloned());
    // After `extra_env`, so a trigger cannot move the spill files away from
    // the directory the monitor samples.
    args.push(spill_tmpdir_arg(spill_dir));
    // Also overrides the trigger's shared `env:` block, which the runner
    // inherits from the pod.
    args.extend(build_env::args());
    args
}

/// `TMPDIR=<spill_dir>` for the benchmark process, so its spill files land in
/// the directory the monitor samples for "Peak spill".
///
/// dfbench and the Criterion SQL harness build their runtime with
/// `DiskManagerBuilder::default()`, which creates its spill directory under
/// `std::env::temp_dir()`, that is `$TMPDIR` or `/tmp`. No DataFusion binary
/// reads a `DATAFUSION_RUNTIME_TEMP_DIRECTORY` env var (runtime settings are
/// not part of `ConfigOptions::from_env`), so `TMPDIR` is the only knob that
/// works for every binary `bench.sh` starts.
///
/// Compilers that run inside the monitored command (`cargo bench --bench sql`)
/// also write to `$TMPDIR`. The monitor only counts DataFusion's own spill
/// directories, so that does not inflate the figure (see
/// `monitor::spill_size`).
fn spill_tmpdir_arg(spill_dir: &Path) -> String {
    format!("TMPDIR={}", spill_dir.display())
}

/// Map a TPC-H bench name to (dfbench tpch args, results JSON filename).
/// Returns `None` for non-TPC-H benchmarks.
fn tpch_direct_args(bench: &str) -> Option<(Vec<String>, String)> {
    let (sf, in_mem) = match bench {
        "tpch" => ("1", false),
        "tpch10" => ("10", false),
        "tpch_mem" => ("1", true),
        "tpch_mem10" => ("10", true),
        _ => return None,
    };
    let mut args: Vec<String> = vec![
        "tpch".into(),
        "--iterations".into(),
        "5".into(),
        "--scale-factor".into(),
        sf.into(),
        "--format".into(),
        "parquet".into(),
        "--prefer_hash_join".into(),
        "true".into(),
    ];
    if in_mem {
        args.push("-m".into());
    }
    let results_filename = if in_mem {
        format!("tpch_mem_sf{sf}.json")
    } else {
        format!("tpch_sf{sf}.json")
    };
    Some((args, results_filename))
}

/// Which side of the comparison a pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Baseline,
    Branch,
}

/// Pass order for `rounds` interleaved rounds: A B, B A, A B, ...
///
/// Each pair of rounds is a palindrome (A B B A), so a linear drift in node
/// speed adds the same total delay to both sides.
fn abba_order(rounds: u32) -> Vec<Side> {
    (0..rounds)
        .flat_map(|round| {
            if round % 2 == 0 {
                [Side::Baseline, Side::Branch]
            } else {
                [Side::Branch, Side::Baseline]
            }
        })
        .collect()
}

/// `extra_env` plus a `MAX_TIME` that gives each round an equal share of the
/// requested time budget, so `rounds: N` does not multiply the run time.
///
/// The side's own env wins over the shared `env:` block, as it does for the
/// run itself. Appending works because `env` applies its args left to right,
/// and the shared block reaches the process only through the pod env.
fn with_split_max_time(
    extra_env: &[String],
    config: &RunnerConfig,
    side: Side,
    rounds: u32,
) -> Vec<String> {
    let mut env = extra_env.to_vec();
    if rounds <= 1 {
        return env;
    }
    let side_env = match side {
        Side::Baseline => &config.baseline_env_vars,
        Side::Branch => &config.changed_env_vars,
    };
    let requested = side_env
        .get("MAX_TIME")
        .or_else(|| config.shared_env_vars.get("MAX_TIME"));
    if let Some(requested) = requested {
        match requested.trim().parse::<f64>() {
            Ok(total) if total > 0.0 => {
                env.push(format!("MAX_TIME={}", total / f64::from(rounds)));
            }
            _ => warn!("MAX_TIME={requested} is not a positive number; not splitting it"),
        }
    }
    env
}

/// Merge several dfbench results JSON documents for the same suite into one.
///
/// The first document supplies everything except iterations. Each later
/// document's per-query `iterations` are appended to the query with the same
/// `query` field. A query that only a later document has is appended whole.
fn merge_results_json(docs: &[serde_json::Value]) -> Result<serde_json::Value> {
    let (first, rest) = docs.split_first().context("no results to merge")?;
    let mut merged = first.clone();
    let queries = merged
        .get_mut("queries")
        .and_then(|q| q.as_array_mut())
        .context("results JSON has no `queries` array")?;
    for doc in rest {
        let Some(more) = doc.get("queries").and_then(|q| q.as_array()) else {
            continue;
        };
        for query in more {
            let existing = queries
                .iter_mut()
                .find(|q| q.get("query").is_some() && q.get("query") == query.get("query"));
            let Some(existing) = existing else {
                queries.push(query.clone());
                continue;
            };
            let extra = query
                .get("iterations")
                .and_then(|i| i.as_array())
                .cloned()
                .unwrap_or_default();
            match existing
                .get_mut("iterations")
                .and_then(|i| i.as_array_mut())
            {
                Some(iterations) => iterations.extend(extra),
                None => existing["iterations"] = serde_json::Value::Array(extra),
            }
        }
    }
    Ok(merged)
}

/// Merge each top-level `*.json` file that the first pass wrote into `dest`,
/// combining the same file from every pass with [`merge_results_json`].
///
/// A pass that lacks the file is skipped: a failed pass already aborts the
/// run, so this is a suite that writes its output conditionally.
async fn merge_pass_dirs(pass_dirs: &[PathBuf], dest: &Path) -> Result<()> {
    let Some(first) = pass_dirs.first() else {
        return Ok(());
    };
    let mut entries = match tokio::fs::read_dir(first).await {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    tokio::fs::create_dir_all(dest)
        .await
        .with_context(|| format!("creating {}", dest.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        if Path::new(&name).extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let mut docs = Vec::new();
        for dir in pass_dirs {
            let path = dir.join(&name);
            let Ok(text) = tokio::fs::read_to_string(&path).await else {
                continue;
            };
            docs.push(
                serde_json::from_str(&text)
                    .with_context(|| format!("parsing {}", path.display()))?,
            );
        }
        let merged = merge_results_json(&docs)
            .with_context(|| format!("merging {}", name.to_string_lossy()))?;
        tokio::fs::write(dest.join(&name), serde_json::to_vec_pretty(&merged)?)
            .await
            .with_context(|| format!("writing merged {}", name.to_string_lossy()))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn tpch_command(
    side_dir: &Path,
    bench_benchmarks: &Path,
    results_name: &str,
    spill_dir: &Path,
    extra_env: &[String],
    tpch_args: Vec<String>,
    results_filename: String,
) -> Result<CommandRun> {
    let dfbench = side_dir.join("target/release/dfbench");
    if !dfbench.exists() {
        anyhow::bail!("dfbench binary not found at {}", dfbench.display());
    }

    // bench.sh writes results under SCRIPT_DIR/results/<RESULTS_NAME>; mimic that.
    let results_dir = bench_benchmarks.join("results").join(results_name);
    tokio::fs::create_dir_all(&results_dir)
        .await
        .with_context(|| format!("creating results dir {}", results_dir.display()))?;
    let results_file = results_dir.join(&results_filename);

    // Data lives in bench_benchmarks/data/tpch_sf<SF> (created by `bench.sh data tpch`).
    // Locate the right data dir from the args we built (--scale-factor SF).
    let sf = tpch_args
        .iter()
        .skip_while(|a| a.as_str() != "--scale-factor")
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("missing --scale-factor in tpch args"))?;
    let data_path = bench_benchmarks.join(format!("data/tpch_sf{sf}"));

    let mut env_args: Vec<String> = extra_env.to_vec();
    env_args.push(spill_tmpdir_arg(spill_dir));
    env_args.push(dfbench.to_string_lossy().into_owned());
    env_args.extend(tpch_args);
    env_args.extend([
        "--path".to_string(),
        data_path.to_string_lossy().into_owned(),
        "-o".to_string(),
        results_file.to_string_lossy().into_owned(),
    ]);

    Ok(CommandRun {
        program: "env".into(),
        args: env_args,
        cwd: bench_benchmarks.into(),
        spill_dir: Some(spill_dir.into()),
    })
}

/// Datasets a Criterion bench target needs generated before it can run.
fn required_datasets(bench_name: &str) -> &'static [&'static str] {
    match bench_name {
        "sql_planner" => &["clickbench_partitioned"],
        _ => &[],
    }
}

/// Generate the data a Criterion bench needs, once in the branch checkout, and
/// symlink it into the base checkout so both sides share the same inputs.
async fn setup_criterion_data(bench: &str, branch_dir: &Path, base_dir: &Path) {
    let datasets = required_datasets(bench);
    if datasets.is_empty() {
        return;
    }

    let branch_benchmarks = branch_dir.join("benchmarks");
    let bench_dir_str = branch_benchmarks.to_string_lossy().to_string();
    for dataset in datasets {
        info!("Setting up data for {dataset}");
        cache_data(dataset, &bench_dir_str).await;
    }

    let branch_data = branch_benchmarks.join("data");
    let base_data = base_dir.join("benchmarks/data");
    if branch_data.exists() && !base_data.exists() {
        info!("Symlinking benchmark data into base directory");
        let _ = tokio::fs::symlink(&branch_data, &base_data).await;
    }
}

/// Copy Criterion baselines from base into the branch target tree so a single
/// `critcmp` invocation can see both sides.
async fn copy_criterion_baselines(base_dir: &Path, branch_dir: &Path) {
    let src = base_dir.join("target/criterion");
    let dst = branch_dir.join("target/criterion");
    if src.exists() {
        let _ = shell::run_command(
            "cp",
            &[
                "-r",
                &format!("{}/.", src.to_string_lossy()),
                &dst.to_string_lossy(),
            ],
            Path::new("/"),
        )
        .await;
    }
}

/// Whether `<side>/target/criterion` exists (i.e. a Criterion run wrote there).
async fn criterion_dir_present(side_dir: &Path) -> bool {
    tokio::fs::metadata(side_dir.join("target/criterion"))
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
}

/// Whether the base side produced any `results/<name>/*.json` (dfbench suites).
async fn results_have_json(bench_benchmarks: &Path, results_name: &str) -> bool {
    let dir = bench_benchmarks.join("results").join(results_name);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return false;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("json") {
            return true;
        }
    }
    false
}

/// Copy TPC-H answer files from the baked-in location into the benchmark data dirs.
async fn copy_tpch_answers(bench_dir: &Path) {
    let answers_src = Path::new("/data/tpch-answers");
    if !answers_src.exists() {
        return;
    }
    for sf in &["1", "10"] {
        let dest = bench_dir.join(format!("data/tpch_sf{sf}/answers"));
        let _ = tokio::fs::create_dir_all(&dest).await;
        let _ = shell::run_command(
            "cp",
            &[
                "-r",
                &format!("{}/.", answers_src.to_string_lossy()),
                &dest.to_string_lossy(),
            ],
            Path::new("/"),
        )
        .await;
    }
}

/// Run data generation with cache support via /scripts/cache_data.sh.
async fn cache_data(bench: &str, bench_dir: &str) {
    let cache_script = Path::new("/scripts/cache_data.sh");
    if cache_script.exists() {
        let _ = shell::run_command(
            "/scripts/cache_data.sh",
            &[bench, bench_dir],
            Path::new(bench_dir),
        )
        .await;
    } else {
        // Fallback: run bench.sh data directly
        let _ = shell::run_command("./bench.sh", &["data", bench], Path::new(bench_dir)).await;
    }
}

/// Build the resource usage section from collected stats.
fn format_resource_section(
    base_stats: &[(String, ResourceStats)],
    branch_stats: &[(String, ResourceStats)],
) -> String {
    let mut section = String::new();
    for (bench, stats) in base_stats {
        section.push_str(&monitor::format_resource_comment(
            &format!("{bench} \u{2014} base (merge-base)"),
            stats,
        ));
        section.push('\n');
    }
    for (bench, stats) in branch_stats {
        section.push_str(&monitor::format_resource_comment(
            &format!("{bench} \u{2014} branch"),
            stats,
        ));
        section.push('\n');
    }
    section
}

/// Format the result comment body.
///
/// `comparison` and `config_block` restate what was run — the same two lines
/// the "running" comment opened with — so the result stands on its own instead
/// of only linking back to the trigger.
///
/// `pool_section` is empty whenever no side recorded a `pool_peak_bytes` and
/// there is nothing to explain — the default, since runs set no memory limit
/// unless the trigger comment asks for one. Its `<details>` block is then
/// omitted entirely, leaving the comment as it was before the section existed.
/// With a limit set, a Criterion-based benchmark (which records no peaks) gets
/// a note there instead of silently producing nothing.
#[allow(clippy::too_many_arguments)]
fn format_result_comment(
    comment_url: &str,
    comparison: &str,
    config_block: &str,
    report: &str,
    resource_section: &str,
    pool_section: &str,
    instance_type: &str,
    pod_resources: &str,
    lscpu: &str,
    footer: &str,
) -> String {
    let pool_block = if pool_section.is_empty() {
        String::new()
    } else {
        format!(
            "<details><summary>Memory Pool Peaks</summary>\n\n\
             {pool_section}\
             </details>\n\n"
        )
    };
    format!(
        "\u{1f916} Benchmark completed (GKE) | [trigger]({comment_url})\n\n\
         **Instance:** `{instance_type}` ({pod_resources})\n\n\
         {comparison}\n\n\
         {config_block}\
         <details><summary>CPU Details (lscpu)</summary>\n\n\
         ```\n\
         {lscpu}\n\
         ```\n\n\
         </details>\n\n\
         <details><summary>Details</summary>\n\
         <p>\n\n\
         ```\n\
         {report}\
         ```\n\n\
         </p>\n\
         </details>\n\n\
         {pool_block}\
         <details><summary>Resource Usage</summary>\n\n\
         {resource_section}\
         </details>\n\
         {footer}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Value of the last `KEY=VALUE` assignment to `key` in `env_args`.
    fn last_assignment(env_args: &[String], key: &str) -> Option<String> {
        env_args.iter().rev().find_map(|a| {
            a.split_once('=')
                .filter(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        })
    }

    #[test]
    fn result_comment_format() {
        let comment = format_result_comment(
            "https://example.com/comment",
            "Comparing my-branch (aaa) to bbb (merge-base) [diff](https://example.com/diff)",
            "<details><summary>Run configuration</summary>\n\n```yaml\nrun benchmark tpch\n```\n\n</details>\n\n",
            "test report\n",
            "resources\n",
            "",
            "c4a-standard-48",
            "12 vCPU / 65 GiB",
            "lscpu output",
            "",
        );
        assert!(comment.contains("Benchmark completed"));
        assert!(comment.contains("[trigger](https://example.com/comment)"));
        assert!(comment.contains("test report"));
        assert!(comment.contains("<details>"));
        assert!(comment.contains("Resource Usage"));
        assert!(comment.contains("resources"));
        assert!(comment.contains("c4a-standard-48"));
        assert!(comment.contains("12 vCPU / 65 GiB"));
        assert!(comment.contains("lscpu output"));
        // Nothing recorded a pool peak — the section is absent, not empty.
        assert!(!comment.contains("Memory Pool Peaks"));
    }

    #[test]
    fn result_comment_restates_what_was_run() {
        // A result comment should be readable without opening the trigger.
        let comment = format_result_comment(
            "https://example.com/comment",
            "Comparing my-branch (aaa) to bbb (merge-base) [diff](https://example.com/diff)",
            "<details><summary>Run configuration</summary>\n\n```yaml\nrun benchmark tpch\nbaseline:\n  ref: \"v45.0.0\"\n```\n\n</details>\n\n",
            "test report\n",
            "resources\n",
            "",
            "c4a-standard-48",
            "12 vCPU / 65 GiB",
            "lscpu output",
            "",
        );
        assert!(comment.contains("Comparing my-branch (aaa) to bbb (merge-base)"));
        assert!(comment.contains("<details><summary>Run configuration</summary>"));
        assert!(comment.contains("run benchmark tpch"));
        assert!(comment.contains("ref: \"v45.0.0\""));
        // Stated up front, before the results themselves.
        assert!(comment.find("Run configuration").unwrap() < comment.find("test report").unwrap());
    }

    #[test]
    fn result_comment_includes_pool_section_when_recorded() {
        let comment = format_result_comment(
            "https://example.com/comment",
            "Comparing my-branch (aaa) to bbb (merge-base) [diff](https://example.com/diff)",
            "",
            "test report\n",
            "resources\n",
            "pool peaks table\n",
            "c4a-standard-48",
            "12 vCPU / 65 GiB",
            "lscpu output",
            "",
        );
        assert!(comment.contains("<details><summary>Memory Pool Peaks</summary>"));
        assert!(comment.contains("pool peaks table"));
        // Sits between the wall-time report and the run-wide resource sampling.
        let pool = comment.find("Memory Pool Peaks").unwrap();
        assert!(pool > comment.find("test report").unwrap());
        assert!(pool < comment.find("Resource Usage").unwrap());
    }

    #[test]
    fn parse_bench_targets_picks_bench_kind() {
        let json = r#"{
            "packages": [
                {"targets": [
                    {"name": "dfbench", "kind": ["bin"]},
                    {"name": "sql_planner", "kind": ["bench"]},
                    {"name": "sql", "kind": ["bench"]},
                    {"name": "datafusion-benchmarks", "kind": ["lib"]}
                ]}
            ]
        }"#;
        let targets = parse_bench_targets(json);
        assert!(targets.contains_key("sql_planner"));
        assert!(targets.contains_key("sql"));
        assert!(!targets.contains_key("dfbench"));
        assert!(!targets.contains_key("datafusion-benchmarks"));
    }

    /// A target that declares `required-features` carries them through, so the
    /// run can enable them. `multi_group_by` failed both sides of
    /// apache/datafusion#23648 because `test_utils` was dropped here.
    #[test]
    fn parse_bench_targets_carries_required_features() {
        let json = r#"{
            "packages": [
                {"targets": [
                    {"name": "sql_planner", "kind": ["bench"]},
                    {
                        "name": "multi_group_by",
                        "kind": ["bench"],
                        "required-features": ["test_utils"]
                    }
                ]}
            ]
        }"#;
        let targets = parse_bench_targets(json);
        assert_eq!(targets["multi_group_by"], vec!["test_utils".to_string()]);
        assert!(targets["sql_planner"].is_empty());
    }

    /// Two packages naming the same bench target contribute both requirements,
    /// because `--bench <name>` selects across the whole workspace.
    #[test]
    fn parse_bench_targets_unions_colliding_names() {
        let json = r#"{
            "packages": [
                {"targets": [
                    {"name": "shared", "kind": ["bench"], "required-features": ["a"]}
                ]},
                {"targets": [
                    {"name": "shared", "kind": ["bench"], "required-features": ["a", "b"]}
                ]}
            ]
        }"#;
        assert_eq!(
            parse_bench_targets(json)["shared"],
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn parse_bench_targets_handles_garbage() {
        assert!(parse_bench_targets("not json").is_empty());
        assert!(parse_bench_targets("{}").is_empty());
    }

    #[test]
    fn criterion_features_arg_appends_required_features() {
        assert_eq!(criterion_features_arg(&[]), "--features=parquet");
        assert_eq!(
            criterion_features_arg(&["test_utils".to_string()]),
            "--features=parquet,test_utils"
        );
        // A target that requires `parquet` itself does not repeat it.
        assert_eq!(
            criterion_features_arg(&["parquet".to_string()]),
            "--features=parquet"
        );
    }

    #[test]
    fn required_datasets_sql_planner() {
        assert_eq!(
            required_datasets("sql_planner"),
            &["clickbench_partitioned"]
        );
    }

    #[test]
    fn required_datasets_unknown_is_empty() {
        assert!(required_datasets("wide_schema").is_empty());
    }

    fn side_env(extra_env: &[&str]) -> Vec<String> {
        let extra_env: Vec<String> = extra_env.iter().map(|s| s.to_string()).collect();
        shell_side_env_args(
            Path::new("/workspace/base"),
            "HEAD",
            "HEAD",
            Path::new("/workspace/spill"),
            &extra_env,
        )
    }

    #[test]
    fn abba_order_alternates_within_each_round() {
        use Side::{Baseline as A, Branch as B};
        assert_eq!(abba_order(1), vec![A, B]);
        assert_eq!(abba_order(2), vec![A, B, B, A]);
        assert_eq!(abba_order(3), vec![A, B, B, A, A, B]);
        assert_eq!(abba_order(4), vec![A, B, B, A, A, B, B, A]);
    }

    #[test]
    fn merge_results_appends_iterations_by_query() {
        let pass0 = serde_json::json!({
            "context": {"benchmark_version": "1"},
            "queries": [
                {"query": "Q1", "iterations": [{"elapsed": 1.0, "row_count": 4}],
                 "start_time": 10, "success": true},
                {"query": "Q2", "iterations": [{"elapsed": 2.0, "row_count": 1}],
                 "start_time": 11, "success": true},
            ],
        });
        let pass1 = serde_json::json!({
            "context": {"benchmark_version": "other"},
            "queries": [
                {"query": "Q2", "iterations": [{"elapsed": 2.5, "row_count": 1}],
                 "start_time": 21, "success": true},
                {"query": "Q1", "iterations": [{"elapsed": 1.5, "row_count": 4},
                                               {"elapsed": 1.6, "row_count": 4}],
                 "start_time": 20, "success": true},
                {"query": "Q3", "iterations": [{"elapsed": 3.0, "row_count": 0}],
                 "start_time": 22, "success": true},
            ],
        });
        let merged = merge_results_json(&[pass0, pass1]).unwrap();
        assert_eq!(
            merged,
            serde_json::json!({
                "context": {"benchmark_version": "1"},
                "queries": [
                    {"query": "Q1", "iterations": [{"elapsed": 1.0, "row_count": 4},
                                                   {"elapsed": 1.5, "row_count": 4},
                                                   {"elapsed": 1.6, "row_count": 4}],
                     "start_time": 10, "success": true},
                    {"query": "Q2", "iterations": [{"elapsed": 2.0, "row_count": 1},
                                                   {"elapsed": 2.5, "row_count": 1}],
                     "start_time": 11, "success": true},
                    {"query": "Q3", "iterations": [{"elapsed": 3.0, "row_count": 0}],
                     "start_time": 22, "success": true},
                ],
            })
        );
    }

    #[test]
    fn merge_results_rejects_empty_and_malformed_input() {
        assert!(merge_results_json(&[]).is_err());
        assert!(merge_results_json(&[serde_json::json!({"context": {}})]).is_err());
    }

    #[tokio::test]
    async fn merge_pass_dirs_writes_merged_files_to_dest() {
        let root = std::env::temp_dir().join("bench_datafusion_merge_pass_dirs");
        let _ = std::fs::remove_dir_all(&root);
        let passes: Vec<PathBuf> = (0..2).map(|i| root.join(format!("HEAD.pass{i}"))).collect();
        for (i, dir) in passes.iter().enumerate() {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(
                dir.join("tpch_sf1.json"),
                format!(
                    r#"{{"queries": [{{"query": "Q1", "iterations": [{{"elapsed": {i}}}]}}]}}"#
                ),
            )
            .unwrap();
        }
        std::fs::write(passes[0].join("notes.txt"), "ignored").unwrap();
        let dest = root.join("HEAD");
        merge_pass_dirs(&passes, &dest).await.unwrap();

        let merged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dest.join("tpch_sf1.json")).unwrap())
                .unwrap();
        assert_eq!(
            merged["queries"][0]["iterations"],
            serde_json::json!([{"elapsed": 0}, {"elapsed": 1}])
        );
        assert!(!dest.join("notes.txt").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    fn max_time_config(shared: &[(&str, &str)], changed: &[(&str, &str)]) -> RunnerConfig {
        let map = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        RunnerConfig {
            pr_url: String::new(),
            comment_id: String::new(),
            comment_url: String::new(),
            benchmarks: String::new(),
            bench_type: crate::runner::config::BenchType::Datafusion,
            bench_name: String::new(),
            bench_filter: String::new(),
            repo: String::new(),
            poster_mode: crate::runner::config::PosterMode::Direct {
                github_token: String::new(),
            },
            sccache_gcs_bucket: None,
            data_cache_bucket: None,
            shared_env_vars: map(shared),
            baseline_env_vars: Default::default(),
            changed_env_vars: map(changed),
            baseline_ref: None,
            changed_ref: None,
            runner_repo_url: None,
            resources: Default::default(),
            shard: Shard::default(),
            frozen_sources: None,
            trigger_comment: None,
            rounds: 4,
        }
    }

    #[test]
    fn max_time_is_split_across_rounds() {
        let cfg = max_time_config(&[("MAX_TIME", "20")], &[("MAX_TIME", "10")]);
        let base = with_split_max_time(&["A=1".into()], &cfg, Side::Baseline, 4);
        assert_eq!(base, vec!["A=1".to_string(), "MAX_TIME=5".to_string()]);
        let branch = with_split_max_time(&[], &cfg, Side::Branch, 4);
        assert_eq!(branch, vec!["MAX_TIME=2.5".to_string()]);
        assert!(with_split_max_time(&[], &cfg, Side::Baseline, 1).is_empty());
        let unset = max_time_config(&[], &[]);
        assert!(with_split_max_time(&[], &unset, Side::Baseline, 4).is_empty());
        let bad = max_time_config(&[("MAX_TIME", "soon")], &[]);
        assert!(with_split_max_time(&[], &bad, Side::Baseline, 4).is_empty());
    }

    #[test]
    fn shell_side_env_forces_build_settings() {
        let args = side_env(&["RUST_LOG=debug"]);
        assert_eq!(
            last_assignment(&args, "CARGO_PROFILE_BENCH_LTO").as_deref(),
            Some("thin")
        );
        assert_eq!(
            last_assignment(&args, "CARGO_BUILD_JOBS").as_deref(),
            Some("5")
        );
        assert_eq!(
            last_assignment(&args, "SQL_CARGO_COMMAND").as_deref(),
            Some("cargo bench --bench sql -- --save-baseline HEAD")
        );
        assert_eq!(last_assignment(&args, "RUST_LOG").as_deref(), Some("debug"));
    }

    #[test]
    fn shell_side_env_trigger_cannot_override_build_settings() {
        // Per-side env comes through `extra_env`. The shared env block is
        // inherited, and any explicit arg overrides it, so the forced args
        // win over both.
        let args = side_env(&[
            "CARGO_PROFILE_BENCH_LTO=fat",
            "CARGO_PROFILE_RELEASE_LTO=fat",
            "CARGO_BUILD_JOBS=12",
        ]);
        assert_eq!(
            last_assignment(&args, "CARGO_PROFILE_BENCH_LTO").as_deref(),
            Some("thin")
        );
        assert_eq!(
            last_assignment(&args, "CARGO_BUILD_JOBS").as_deref(),
            Some("5")
        );
        assert_eq!(
            last_assignment(&args, "CARGO_PROFILE_RELEASE_LTO").as_deref(),
            Some("thin")
        );
    }

    #[test]
    fn shell_side_env_per_side_env_still_applies() {
        let args = side_env(&["DATAFUSION_RUNTIME_MEMORY_LIMIT=2G"]);
        assert_eq!(
            last_assignment(&args, "DATAFUSION_RUNTIME_MEMORY_LIMIT").as_deref(),
            Some("2G")
        );
    }

    #[test]
    fn shell_side_env_points_tmpdir_at_spill_dir() {
        // A trigger's TMPDIR must not move spill files away from the
        // directory the monitor samples.
        let args = side_env(&["TMPDIR=/elsewhere"]);
        assert_eq!(
            last_assignment(&args, "TMPDIR").as_deref(),
            Some("/workspace/spill")
        );
        // Nothing reads this var, so the runner no longer sets it.
        assert_eq!(
            last_assignment(&args, "DATAFUSION_RUNTIME_TEMP_DIRECTORY"),
            None
        );
    }

    #[test]
    fn tpch_direct_args_maps_variants() {
        let (args, results) = tpch_direct_args("tpch").unwrap();
        assert!(args.contains(&"--scale-factor".to_string()));
        assert!(args.contains(&"1".to_string()));
        assert!(!args.contains(&"-m".to_string()));
        assert_eq!(results, "tpch_sf1.json");

        let (args, results) = tpch_direct_args("tpch10").unwrap();
        assert!(args.contains(&"10".to_string()));
        assert!(!args.contains(&"-m".to_string()));
        assert_eq!(results, "tpch_sf10.json");

        let (args, results) = tpch_direct_args("tpch_mem").unwrap();
        assert!(args.contains(&"-m".to_string()));
        assert_eq!(results, "tpch_mem_sf1.json");

        let (args, results) = tpch_direct_args("tpch_mem10").unwrap();
        assert!(args.contains(&"-m".to_string()));
        assert!(args.contains(&"10".to_string()));
        assert_eq!(results, "tpch_mem_sf10.json");

        assert!(tpch_direct_args("clickbench_1").is_none());
        assert!(tpch_direct_args("topk_tpch").is_none());
    }
}
