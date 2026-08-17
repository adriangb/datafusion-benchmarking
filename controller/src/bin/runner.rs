//! Benchmark runner entry point.
//!
//! Parses environment variables, dispatches to the appropriate benchmark
//! workflow, and posts error comments on failure.

use anyhow::Result;
use tracing::{error, info};

use benchmark_controller::github;
use benchmark_controller::runner::config::{BenchType, PosterMode, RunnerConfig};
use benchmark_controller::runner::poster::CommentPoster;
use benchmark_controller::runner::{bench_arrow, bench_datafusion, shell, trigger};

#[tokio::main]
async fn main() {
    // Initialize tracing
    let logfire = logfire::configure()
        .with_service_name("benchmark-runner")
        .send_to_logfire(logfire::config::SendToLogfire::IfTokenPresent)
        .with_console(Some(logfire::config::ConsoleOptions::default()))
        .finish()
        .expect("failed to configure tracing");
    let _logfire_guard = logfire.shutdown_guard();

    // Initialize the output log file
    let _ = tokio::fs::write(shell::OUTPUT_FILE, b"").await;

    // Parse config
    let config = match RunnerConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to parse runner config");
            std::process::exit(1);
        }
    };

    // Set up sccache if configured
    config.setup_sccache();

    info!(
        bench_type = ?config.bench_type,
        pr_url = %config.pr_url,
        benchmarks = %config.benchmarks,
        "starting benchmark runner"
    );

    let poster = config.build_poster();

    if let Err(e) = run_benchmark(&config, &poster).await {
        error!(error = %e, "benchmark failed");
        if runner_posts_failure_comment(&config.poster_mode) {
            post_error_comment(&config, &poster).await;
        }
        std::process::exit(1);
    }

    // Log sccache stats if enabled
    shell::log_sccache_stats().await;
}

/// Proxy-mode runs are reconciled by the controller, which owns their
/// terminal failure notification. Direct runs have no controller job record.
fn runner_posts_failure_comment(poster_mode: &PosterMode) -> bool {
    matches!(poster_mode, PosterMode::Direct { .. })
}

async fn run_benchmark(config: &RunnerConfig, poster: &CommentPoster) -> Result<()> {
    match config.bench_type {
        BenchType::Datafusion | BenchType::MainTracking => {
            bench_datafusion::run(config, poster).await
        }
        BenchType::ArrowCriterion => bench_arrow::run(config, poster).await,
    }
}

async fn post_error_comment(config: &RunnerConfig, poster: &CommentPoster) {
    let tail = shell::tail_log(20).await;

    // A failure can happen before either checkout resolves, so there is no
    // comparison line here — just the requested configuration.
    let bench_names = match config.bench_type {
        BenchType::ArrowCriterion => config.bench_name.as_str(),
        _ => config.benchmarks.as_str(),
    };
    let config_block = trigger::config_block(config, bench_names);

    let footer = github::issues_footer(config.runner_repo_url.as_deref());
    let body = format!(
        "Benchmark for [this request]({}) failed.\n\n\
         {config_block}\
         Last 20 lines of output:\n\
         <details><summary>Click to expand</summary>\n\n\
         ```\n\
         {tail}\n\
         ```\n\n\
         </details>{footer}",
        config.comment_url,
    );

    let pr_number = match config.pr_number() {
        Ok(n) => n,
        Err(e) => {
            error!(error = %e, "cannot post error comment: failed to parse PR number");
            return;
        }
    };

    if let Err(e) = poster.post_comment(&config.repo, pr_number, &body).await {
        error!(error = %e, "failed to post error comment");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_runners_leave_failure_comments_to_the_controller() {
        let mode = PosterMode::Proxy {
            controller_url: "http://controller".to_string(),
            job_id: "1".to_string(),
            token: "token".to_string(),
        };

        assert!(!runner_posts_failure_comment(&mode));
    }

    #[test]
    fn direct_runners_post_their_own_failure_comments() {
        let mode = PosterMode::Direct {
            github_token: "token".to_string(),
        };

        assert!(runner_posts_failure_comment(&mode));
    }
}
