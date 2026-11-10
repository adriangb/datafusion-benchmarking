//! Arrow-rs criterion benchmark runner — ports `run_arrow_criterion.sh`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::criterion_report::{ExecutionContext, RunnerInfo};
use crate::runner::config::RunnerConfig;
use crate::runner::criterion::{Criterion, Run};
use crate::runner::execution::{execute, partition};
use crate::runner::git;
use crate::runner::monitor;
use crate::runner::poster::CommentPoster;
use crate::runner::shell;
use crate::sharding::ShardSupport;

pub const SHARD_SUPPORT: ShardSupport = ShardSupport::Partitioned;

/// Run an arrow-rs criterion benchmark comparing a PR branch to its merge-base.
pub async fn run(config: &RunnerConfig, poster: &CommentPoster) -> Result<()> {
    let mut context = ExecutionContext::from_config(config);
    let mut info = RunnerInfo {
        node_name: std::env::var("NODE_NAME").unwrap_or_else(|_| "unknown".into()),
        instance: shell::node_instance_type().await,
        resources: shell::pod_resources(),
        uname: shell::uname().await,
        cpu_details: shell::lscpu().await,
        bench_command: format!("cargo {}", bench_command_args(&config.bench_name).join(" ")),
        resource_report: String::new(),
    };
    let outcome = async {
        config.shard.validate()?;
        let sources = poster.criterion_sources(config).await?;
        context.sources = Some(sources.clone());
        poster.criterion_started(config, &context).await?;
        poster.post_runner_info(&info).await?;
        let result = run_worker(config, poster, &sources, &mut info).await?;
        poster.criterion_result(config, &context, &result).await
    }
    .await;
    if let Err(error) = &outcome {
        let detail = format!(
            "{error:#}\n\nRunner log (last 20 lines):\n{}",
            shell::tail_log(20).await
        );
        if let Err(post_error) = poster
            .criterion_error(config, &context, &info, &detail)
            .await
        {
            warn!(error = %post_error, "failed to post error comment");
        }
    }
    outcome
}

async fn run_worker(
    config: &RunnerConfig,
    poster: &CommentPoster,
    sources: &crate::sharding::FrozenSources,
    info: &mut RunnerInfo,
) -> Result<crate::shard_reporting::ShardResult> {
    let repo_url = config.repo_url();
    let bench_name = &config.bench_name;

    let branch_dir = PathBuf::from("/workspace/arrow-rs-branch");
    let base_dir = PathBuf::from("/workspace/arrow-rs-base");

    // Clone and checkout PR branch
    info!("=== Cloning PR branch ===");
    git::clone_shallow(&repo_url, &branch_dir, 200).await?;
    git::checkout_frozen(&branch_dir, &sources.changed_sha).await?;
    git::submodule_update(&branch_dir).await?;
    git::cargo_update(&branch_dir).await?;

    info!("=== Cloning merge-base ===");
    git::clone_shallow(&repo_url, &base_dir, 200).await?;
    git::checkout_frozen(&base_dir, &sources.baseline_sha).await?;
    git::submodule_update(&base_dir).await?;
    git::cargo_update(&base_dir).await?;

    // Pre-install stable toolchain to avoid rustup race in parallel builds
    git::rustup_stable().await?;

    // Every worker builds, partitions and measures in the same way.
    // The partition operation alone decides how much of the work it owns.
    info!("=== Compiling PR branch and merge-base in parallel ===");
    let mut build_args = bench_command_args(bench_name);
    build_args.push("--no-run".into());
    let branch_build = shell::spawn_command(
        "cargo",
        &str_slice(&build_args),
        &branch_dir,
        "/tmp/branch_build.log",
    );
    let base_build = shell::spawn_command(
        "cargo",
        &str_slice(&build_args),
        &base_dir,
        "/tmp/base_build.log",
    );
    branch_build
        .await
        .context("branch build task panicked")?
        .context("branch build failed")?;
    let baseline_available = match base_build.await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("Baseline build failed (benchmark may be new): {e:#}");
            false
        }
        Err(e) => {
            warn!("Baseline build task panicked (benchmark may be new): {e:#}");
            false
        }
    };
    info!("=== Compilation complete ===");

    let harness = Criterion {
        bench_args: bench_command_args(bench_name),
    };
    let base_env = config.baseline_env_args();
    let branch_env = config.changed_env_args();
    let base = if baseline_available {
        Some(
            partition(
                &harness,
                Run::new("base", &config.bench_filter, &base_dir, &base_env),
                config.shard,
                bench_name,
            )
            .await?,
        )
    } else {
        None
    };
    let branch = partition(
        &harness,
        Run::new("changed", &config.bench_filter, &branch_dir, &branch_env),
        config.shard,
        bench_name,
    )
    .await?;

    let base_stats = if let Some(base) = &base {
        info!("=== Running benchmark on merge-base ===");
        execute(&harness, base).await?
    } else {
        info!("=== Skipping merge-base benchmark (baseline build failed) ===");
        None
    };
    if let Some(stats) = &base_stats {
        info.resource_report = monitor::format_resource_comment("base (merge-base)", stats);
        poster.post_runner_info(info).await?;
    }
    info!("=== Running benchmark on PR branch ===");
    let branch_stats = execute(&harness, &branch).await?;
    if let Some(stats) = &branch_stats {
        info.resource_report.push_str(&format!(
            "\n{}",
            monitor::format_resource_comment("branch", stats)
        ));
        poster.post_runner_info(info).await?;
    }
    let result = crate::shard_reporting::ShardResult {
        base: match &base {
            Some(base) => Some(base.plan.export(base_stats.is_some()).await?),
            None => None,
        },
        changed: branch.plan.export(branch_stats.is_some()).await?,
        info: info.clone(),
    };
    Ok(result)
}

/// Build cargo bench args for arrow-rs criterion.
fn bench_command_args(bench_name: &str) -> Vec<String> {
    vec![
        "bench".to_string(),
        "--features=arrow,async,test_common,experimental,object_store".to_string(),
        "--bench".to_string(),
        bench_name.to_string(),
    ]
}

/// Convert Vec<String> to a slice of &str for run_command.
fn str_slice(v: &[String]) -> Vec<&str> {
    v.iter().map(|s| s.as_str()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_args_construction() {
        let args = bench_command_args("concatenate_kernel");
        assert_eq!(
            args,
            vec![
                "bench",
                "--features=arrow,async,test_common,experimental,object_store",
                "--bench",
                "concatenate_kernel"
            ]
        );
    }
}
