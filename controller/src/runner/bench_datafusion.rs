//! Unified DataFusion benchmark runner.
//!
//! There is no benchmark allowlist and no per-name classification baked into
//! the controller. For each requested benchmark the runner resolves how to run
//! it at runtime:
//!
//! * If the name is a real Criterion `[[bench]]` target in the `benchmarks`
//!   crate (discovered via `cargo metadata`), it is run with
//!   `cargo bench --bench <name> -- --save-baseline <side>`.
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
//! [`pool_peak`](super::pool_peak).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::github;
use crate::runner::config::RunnerConfig;
use crate::runner::git;
use crate::runner::monitor::{self, ResourceStats};
use crate::runner::pool_peak::{self, BenchPeaks};
use crate::runner::poster::CommentPoster;
use crate::runner::shell;
use crate::runner::trigger;

/// Run DataFusion benchmarks comparing a PR branch to its merge-base.
pub async fn run(config: &RunnerConfig, poster: &CommentPoster) -> Result<()> {
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
    let is_criterion = |bench: &str| bench_targets.contains(bench);
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
            info!("** Setting up data for criterion bench {bench} **");
            setup_criterion_data(bench, &branch_dir, &base_dir).await;

            info!("** Running {bench} baseline (criterion) **");
            match run_criterion_side(
                bench,
                &base_dir,
                &base_results_name,
                &config.bench_filter,
                &baseline_extra_env,
            )
            .await
            {
                Ok(stats) => {
                    criterion_base_ok = true;
                    base_stats_list.push((bench.clone(), stats));
                }
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
            )
            .await
            .with_context(|| format!("run {bench} (branch, criterion)"))?;
            criterion_branch_ok = true;
            branch_stats_list.push((bench.clone(), stats));
            continue;
        }

        info!("** Creating data if needed for {bench} **");
        cache_data(bench, &bench_dir_str).await;

        info!("** Running {bench} baseline **");
        let base_spill_dir = PathBuf::from(format!("/workspace/spill-base-{bench}"));
        let _ = tokio::fs::create_dir_all(&base_spill_dir).await;
        // The results file name doesn't follow the benchmark name, so bracket
        // the run and attribute whatever JSON appears to this invocation.
        let base_before = pool_peak::snapshot_json_files(&base_results_dir).await;
        let base_stats = run_shell_side(
            bench,
            &base_dir,
            &bench_benchmarks,
            &base_results_name,
            &base_spill_dir,
            &baseline_extra_env,
        )
        .await
        .with_context(|| format!("run {bench} (base)"))?;
        let _ = tokio::fs::remove_dir_all(&base_spill_dir).await;
        base_peaks.extend(pool_peak::collect_new(&base_results_dir, &base_before, bench).await);
        base_stats_list.push((bench.clone(), base_stats));

        info!("** Running {bench} branch **");
        let branch_spill_dir = PathBuf::from(format!("/workspace/spill-branch-{bench}"));
        let _ = tokio::fs::create_dir_all(&branch_spill_dir).await;
        let branch_before = pool_peak::snapshot_json_files(&branch_results_dir).await;
        let branch_stats = run_shell_side(
            bench,
            &branch_dir,
            &bench_benchmarks,
            &bench_branch_name,
            &branch_spill_dir,
            &changed_extra_env,
        )
        .await
        .with_context(|| format!("run {bench} (branch)"))?;
        let _ = tokio::fs::remove_dir_all(&branch_spill_dir).await;
        branch_peaks
            .extend(pool_peak::collect_new(&branch_results_dir, &branch_before, bench).await);
        branch_stats_list.push((bench.clone(), branch_stats));
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
    let pool_section = pool_peak::format_pool_peak_section(
        &baseline_label,
        changed_display,
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

/// Discover Criterion `[[bench]]` target names in the `benchmarks` crate via
/// `cargo metadata`. Returns an empty set on any failure — the caller then
/// treats every name as a `bench.sh` suite (which itself reports unknown
/// names), so a metadata hiccup degrades gracefully rather than misrouting.
async fn criterion_bench_targets(crate_dir: &Path) -> HashSet<String> {
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
            return HashSet::new();
        }
    };
    parse_bench_targets(&out)
}

/// Extract names of `bench`-kind targets from `cargo metadata` JSON.
fn parse_bench_targets(metadata_json: &str) -> HashSet<String> {
    let mut targets = HashSet::new();
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
            if is_bench {
                if let Some(name) = target.get("name").and_then(|n| n.as_str()) {
                    targets.insert(name.to_string());
                }
            }
        }
    }
    targets
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
) -> Result<ResourceStats> {
    let mut bench_args: Vec<String> = vec![
        "bench".into(),
        "--features=parquet".into(),
        "--bench".into(),
        bench.into(),
        "--".into(),
        "--save-baseline".into(),
        baseline_name.into(),
    ];
    if !bench_filter.is_empty() {
        bench_args.push(bench_filter.to_string());
    }
    let (_, stats) = if extra_env.is_empty() {
        let args_ref: Vec<&str> = bench_args.iter().map(|s| s.as_str()).collect();
        shell::run_command_monitored("cargo", &args_ref, side_dir, None).await?
    } else {
        let mut env_args: Vec<String> = extra_env.to_vec();
        env_args.push("cargo".to_string());
        env_args.extend(bench_args);
        let env_args_ref: Vec<&str> = env_args.iter().map(|s| s.as_str()).collect();
        shell::run_command_monitored("env", &env_args_ref, side_dir, None).await?
    };
    Ok(stats)
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
async fn run_shell_side(
    bench: &str,
    side_dir: &Path,
    bench_benchmarks: &Path,
    results_name: &str,
    spill_dir: &Path,
    extra_env: &[String],
) -> Result<ResourceStats> {
    if let Some((tpch_args, results_filename)) = tpch_direct_args(bench) {
        run_tpch_direct(
            side_dir,
            bench_benchmarks,
            results_name,
            spill_dir,
            extra_env,
            tpch_args,
            results_filename,
        )
        .await
    } else {
        let mut args: Vec<String> = vec![
            format!("DATAFUSION_DIR={}", side_dir.display()),
            format!("RESULTS_NAME={results_name}"),
            format!("DATAFUSION_RUNTIME_TEMP_DIRECTORY={}", spill_dir.display()),
            // Suites that bench.sh runs through the Criterion SQL harness read
            // SQL_CARGO_COMMAND; setting it unconditionally is harmless for the
            // dfbench-based suites (which never read it) and saves a named
            // baseline per side for the ones that do, so we can critcmp them.
            format!("SQL_CARGO_COMMAND=cargo bench --bench sql -- --save-baseline {results_name}"),
        ];
        args.extend(extra_env.iter().cloned());
        args.extend([
            "./bench.sh".to_string(),
            "run".to_string(),
            bench.to_string(),
        ]);
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let (_, stats) = shell::run_command_monitored(
            "env",
            &args_ref,
            bench_benchmarks,
            Some(spill_dir.to_path_buf()),
        )
        .await?;
        Ok(stats)
    }
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

#[allow(clippy::too_many_arguments)]
async fn run_tpch_direct(
    side_dir: &Path,
    bench_benchmarks: &Path,
    results_name: &str,
    spill_dir: &Path,
    extra_env: &[String],
    tpch_args: Vec<String>,
    results_filename: String,
) -> Result<ResourceStats> {
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

    let mut env_args: Vec<String> = vec![format!(
        "DATAFUSION_RUNTIME_TEMP_DIRECTORY={}",
        spill_dir.display()
    )];
    env_args.extend(extra_env.iter().cloned());
    env_args.push(dfbench.to_string_lossy().into_owned());
    env_args.extend(tpch_args);
    env_args.extend([
        "--path".to_string(),
        data_path.to_string_lossy().into_owned(),
        "-o".to_string(),
        results_file.to_string_lossy().into_owned(),
    ]);

    let env_args_ref: Vec<&str> = env_args.iter().map(|s| s.as_str()).collect();
    let (_, stats) = shell::run_command_monitored(
        "env",
        &env_args_ref,
        bench_benchmarks,
        Some(spill_dir.to_path_buf()),
    )
    .await?;
    Ok(stats)
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
/// `pool_section` is empty whenever no side recorded a `pool_peak_bytes` — the
/// default, since runs set no memory limit unless the trigger comment asks for
/// one. Its `<details>` block is then omitted entirely, leaving the comment as
/// it was before the section existed.
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
        assert!(targets.contains("sql_planner"));
        assert!(targets.contains("sql"));
        assert!(!targets.contains("dfbench"));
        assert!(!targets.contains("datafusion-benchmarks"));
    }

    #[test]
    fn parse_bench_targets_handles_garbage() {
        assert!(parse_bench_targets("not json").is_empty());
        assert!(parse_bench_targets("{}").is_empty());
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
