//! `CommentPoster` chooses between posting PR comments directly to GitHub
//! (with a `GITHUB_TOKEN` — used by standalone runs and scheduled main tracking)
//! and proxying through the controller (used by PR-triggered runs, which
//! have no GitHub creds in the pod).

use anyhow::{Context, Result};
use backon::{ExponentialBuilder, Retryable};
use sha2::{Digest, Sha256};

use crate::criterion_report::{self, ExecutionContext, RunnerInfo, WorkerReport};
use crate::runner::config::RunnerConfig;
use crate::shard_reporting::{Baseline, ShardResult};
use crate::sharding::{FrozenSources, ShardSupport};

use crate::github::GitHubClient;
use crate::runner::controller_client::ControllerClient;

#[derive(Clone)]
pub enum CommentPoster {
    /// Post directly to GitHub using a `GITHUB_TOKEN`.
    Direct(GitHubClient),
    /// Proxy through the controller's `POST /jobs/{id}/comment` endpoint.
    Proxy(ControllerClient),
}

impl CommentPoster {
    pub async fn criterion_sources(&self, config: &RunnerConfig) -> Result<FrozenSources> {
        if matches!(self, Self::Direct(_)) {
            ShardSupport::SingleWorkerOnly("direct runs have no multi-host result collector")
                .validate_execution(config.shard)?;
        }
        let sources = match self {
            Self::Proxy(_) => config
                .frozen_sources
                .clone()
                .context("missing controller-provided frozen commits")?,
            Self::Direct(gh) => match &config.frozen_sources {
                Some(sources) => sources.clone(),
                None => {
                    gh.freeze_sources(
                        &config.repo,
                        config.pr_number()?,
                        config.baseline_ref.as_deref(),
                        config.changed_ref.as_deref(),
                    )
                    .await?
                }
            },
        };
        sources.validate()?;
        Ok(sources)
    }

    pub async fn criterion_started(
        &self,
        config: &RunnerConfig,
        context: &ExecutionContext,
    ) -> Result<()> {
        if let Self::Direct(gh) = self {
            let body = criterion_report::start_body(context, config.runner_repo_url.as_deref())?;
            gh.post_comment(
                &config.repo,
                config.pr_number()?,
                &criterion_report::github_body(context, body, 0),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn post_runner_info(&self, info: &RunnerInfo) -> Result<()> {
        if let Self::Proxy(client) = self {
            client.post_runner_info(info).await?;
        }
        Ok(())
    }

    pub async fn criterion_result(
        &self,
        config: &RunnerConfig,
        context: &ExecutionContext,
        result: &ShardResult,
    ) -> Result<()> {
        match self {
            Self::Proxy(client) => client.post_result(result).await,
            Self::Direct(gh) => {
                Self::direct_report(gh, config, context, &result.info, Some(result), None).await
            }
        }
    }

    pub async fn criterion_error(
        &self,
        config: &RunnerConfig,
        context: &ExecutionContext,
        info: &RunnerInfo,
        error: &str,
    ) -> Result<()> {
        if let Self::Direct(gh) = self {
            Self::direct_report(gh, config, context, info, None, Some(error)).await?;
        }
        Ok(())
    }

    async fn direct_report(
        gh: &GitHubClient,
        config: &RunnerConfig,
        context: &ExecutionContext,
        info: &RunnerInfo,
        result: Option<&ShardResult>,
        error: Option<&str>,
    ) -> Result<()> {
        let merged = (|| {
            let mut base = Baseline::empty("base");
            let mut changed = Baseline::empty("changed");
            if let Some(result) = result {
                if let Some(export) = &result.base {
                    base.extend(export)?;
                }
                changed.extend(&result.changed)?;
            }
            Ok((base, changed))
        })();
        let workers = [WorkerReport {
            index: config.shard.index,
            info: Some(info),
            result,
            error,
        }];
        let report = criterion_report::report_body(
            context,
            merged,
            &workers,
            config.runner_repo_url.as_deref(),
        )
        .await?;
        // Standalone runs have no persisted execution key. Include the trigger,
        // commits and report contents so recovery cannot reuse another run's data.
        let key = format!("{:x}", Sha256::digest(report.inline_body().as_bytes()));
        let (body, _) = (|| report.github_body(gh, &key, 0, None))
            .retry(ExponentialBuilder::default().with_max_times(3))
            .when(crate::github::is_retryable)
            .sleep(tokio::time::sleep)
            .await?;
        gh.post_comment(&config.repo, config.pr_number()?, &body)
            .await
    }

    pub async fn post_comment(&self, repo: &str, pr_number: i64, body: &str) -> Result<()> {
        match self {
            Self::Direct(c) => c.post_comment(repo, pr_number, body).await,
            Self::Proxy(c) => c.post_comment(repo, pr_number, body).await,
        }
    }
}
