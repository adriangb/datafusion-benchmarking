//! Per-target execution and reporting for Criterion benchmarks.
//!
//! Combining exports is a disjoint union of case records, not a statistical
//! aggregation. Controller-managed workers submit exports and diagnostics to
//! SQLite; the reconciler publishes their final report.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;

use crate::{
    criterion_report::{self, ExecutionContext, RunnerInfo, WorkerReport},
    github::GitHubClient,
    models::BenchmarkJob,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Baseline {
    pub name: String,
    // Keep Criterion's estimates and metadata intact, including unknown fields.
    pub benchmarks: BTreeMap<String, Value>,
}

impl Baseline {
    pub fn empty(name: &str) -> Self {
        Self {
            name: name.into(),
            benchmarks: BTreeMap::new(),
        }
    }

    pub fn validate(&self, name: &str) -> Result<()> {
        ensure!(self.name == name, "unexpected baseline name: {}", self.name);
        for (id, record) in &self.benchmarks {
            ensure!(
                !id.is_empty() && !id.chars().any(char::is_control),
                "invalid case ID"
            );
            ensure!(
                record["baseline"].as_str() == Some(name),
                "incorrect case baseline: {id}"
            );
            ensure!(
                record["fullname"].as_str() == Some(&format!("{name}/{id}")),
                "incorrect case fullname: {id}"
            );
            ensure!(
                record["criterion_benchmark_v1"]["full_id"].as_str() == Some(id),
                "incorrect case identity: {id}"
            );
            ensure!(
                record["criterion_estimates_v1"].is_object(),
                "missing case estimates: {id}"
            );
        }
        Ok(())
    }

    pub(crate) fn extend(&mut self, other: &Self) -> Result<()> {
        other.validate(&self.name)?;
        for (id, record) in &other.benchmarks {
            ensure!(
                !self.benchmarks.contains_key(id),
                "duplicate {} case: {id}",
                self.name
            );
            self.benchmarks.insert(id.clone(), record.clone());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardResult {
    /// None means the baseline build failed. Some(empty) means this shard had
    /// no matching baseline cases. These are deliberately different states.
    pub base: Option<Baseline>,
    pub changed: Baseline,
    #[serde(flatten)]
    pub info: RunnerInfo,
}

impl ShardResult {
    pub fn validate(&self, job: &BenchmarkJob) -> Result<()> {
        let shard = job.shard()?;
        ensure!(
            job.uses_collected_results(),
            "not a collected Criterion job"
        );
        let target = target(job)?;
        for (name, export) in [
            ("base", self.base.as_ref()),
            ("changed", Some(&self.changed)),
        ] {
            if let Some(export) = export {
                export.validate(name)?;
                for id in export.benchmarks.keys() {
                    ensure!(
                        shard.owns(&target, id),
                        "case {id} does not belong to this shard"
                    );
                }
            }
        }
        Ok(())
    }
}

fn target(job: &BenchmarkJob) -> Result<String> {
    let names: Vec<String> = serde_json::from_str(&job.benchmarks)?;
    ensure!(names.len() == 1, "expected one Criterion target per worker");
    Ok(names[0].clone())
}

/// Idempotent submission, including a retry after the reconciler made the job
/// terminal. Conflicting submissions never overwrite the first accepted export.
/// The INSERT itself checks status to close the HTTP/reconciler race.
pub async fn store_result(
    pool: &SqlitePool,
    job: &BenchmarkJob,
    result: &ShardResult,
) -> Result<bool> {
    result.validate(job)?;
    let json = serde_json::to_string(result)?;
    sqlx::query("INSERT INTO shard_results (job_id, result_json) SELECT id, ? FROM benchmark_jobs WHERE id = ? AND status IN ('pending', 'running') ON CONFLICT(job_id) DO NOTHING")
        .bind(&json).bind(job.id).execute(pool).await?;
    let stored: Option<String> =
        sqlx::query_scalar("SELECT result_json FROM shard_results WHERE job_id = ?")
            .bind(job.id)
            .fetch_optional(pool)
            .await?;
    Ok(stored
        .map(|json| serde_json::from_str::<ShardResult>(&json))
        .transpose()?
        .as_ref()
        == Some(result))
}

pub async fn store_runner_info(pool: &SqlitePool, job_id: i64, info: &RunnerInfo) -> Result<bool> {
    let json = serde_json::to_string(info)?;
    sqlx::query("INSERT INTO runner_metadata (job_id, info_json) SELECT id, ? FROM benchmark_jobs WHERE id = ? AND status IN ('pending', 'running') ON CONFLICT(job_id) DO UPDATE SET info_json = excluded.info_json")
        .bind(&json).bind(job_id).execute(pool).await?;
    let stored: Option<String> =
        sqlx::query_scalar("SELECT info_json FROM runner_metadata WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(pool)
            .await?;
    Ok(stored
        .map(|json| serde_json::from_str::<RunnerInfo>(&json))
        .transpose()?
        .as_ref()
        == Some(info))
}

async fn jobs_for_execution(
    pool: &SqlitePool,
    comment_id: i64,
    benchmarks: &str,
) -> Result<Vec<BenchmarkJob>> {
    Ok(sqlx::query_as(
        "SELECT * FROM benchmark_jobs WHERE comment_id = ? AND benchmarks = ? ORDER BY shard_index",
    )
    .bind(comment_id)
    .bind(benchmarks)
    .fetch_all(pool)
    .await?)
}

/// Called before starting any worker. A persisted notification ID makes all
/// later workers no-ops, even when they are scheduled in different loop passes.
pub async fn ensure_started(
    pool: &SqlitePool,
    gh: &GitHubClient,
    job: &BenchmarkJob,
    runner_repo: Option<&str>,
) -> Result<()> {
    let (posted, key): (Option<i64>, String) = sqlx::query_as(
        "SELECT started_comment_id, start_key FROM sharded_runs WHERE comment_id = ? AND benchmarks = ?")
        .bind(job.comment_id).bind(&job.benchmarks).fetch_one(pool).await?;
    if posted.is_some() {
        return Ok(());
    }
    let context = ExecutionContext::from_job(job)?;
    let marker = marker(job.comment_id, "start", &key);
    let body = criterion_report::github_body(
        &context,
        criterion_report::start_body(&context, runner_repo)?,
        marker.chars().count() + 2,
    );
    let id = gh
        .ensure_run_comment(&job.repo, job.pr_number, &marker, &body)
        .await?;
    sqlx::query(
        "UPDATE sharded_runs SET started_comment_id = ? WHERE comment_id = ? AND benchmarks = ?",
    )
    .bind(id)
    .bind(job.comment_id)
    .bind(&job.benchmarks)
    .execute(pool)
    .await?;
    Ok(())
}

fn marker(comment_id: i64, phase: &str, key: &str) -> String {
    format!("<!-- benchmark-request:{comment_id}:{phase}:{key} -->")
}

/// Publish final reports for execution units whose workers are all terminal.
/// Posting failures are retried on the next reconciliation pass.
pub async fn reconcile(
    pool: &SqlitePool,
    gh: &GitHubClient,
    runner_repo: Option<&str>,
) -> Result<()> {
    let ready: Vec<(i64, String)> = sqlx::query_as("SELECT comment_id, benchmarks FROM sharded_runs r WHERE finished_comment_id IS NULL AND EXISTS (SELECT 1 FROM benchmark_jobs j WHERE j.comment_id = r.comment_id AND j.benchmarks = r.benchmarks) AND NOT EXISTS (SELECT 1 FROM benchmark_jobs j WHERE j.comment_id = r.comment_id AND j.benchmarks = r.benchmarks AND j.status IN ('pending', 'running')) ORDER BY comment_id, benchmarks")
        .fetch_all(pool).await?;
    for (comment_id, benchmarks) in ready {
        if let Err(error) = finish_execution(pool, gh, comment_id, &benchmarks, runner_repo).await {
            tracing::warn!(comment_id, benchmarks, %error, "failed to publish combined shard report");
        }
    }
    Ok(())
}

async fn finish_execution(
    pool: &SqlitePool,
    gh: &GitHubClient,
    comment_id: i64,
    benchmarks: &str,
    runner_repo: Option<&str>,
) -> Result<()> {
    let jobs = jobs_for_execution(pool, comment_id, benchmarks).await?;
    let job = jobs.first().context("no jobs in execution unit")?;
    ensure_started(pool, gh, job, runner_repo).await?;
    let rows: Vec<(i64, String)> = sqlx::query_as("SELECT job_id, result_json FROM shard_results WHERE job_id IN (SELECT id FROM benchmark_jobs WHERE comment_id = ? AND benchmarks = ?)")
        .bind(comment_id).bind(benchmarks).fetch_all(pool).await?;
    let results: BTreeMap<i64, ShardResult> = rows
        .into_iter()
        .map(|(id, json)| Ok((id, serde_json::from_str(&json)?)))
        .collect::<Result<_>>()?;
    let rows: Vec<(i64, String)> = sqlx::query_as("SELECT job_id, info_json FROM runner_metadata WHERE job_id IN (SELECT id FROM benchmark_jobs WHERE comment_id = ? AND benchmarks = ?)")
        .bind(comment_id).bind(benchmarks).fetch_all(pool).await?;
    let info: BTreeMap<i64, RunnerInfo> = rows
        .into_iter()
        .map(|(id, json)| Ok((id, serde_json::from_str(&json)?)))
        .collect::<Result<_>>()?;
    let report = final_body(&jobs, &results, &info, runner_repo).await?;
    let (key, existing_gist): (String, Option<String>) = sqlx::query_as(
        "SELECT finish_key, comparison_gist_url FROM sharded_runs WHERE comment_id = ? AND benchmarks = ?",
    )
    .bind(comment_id)
    .bind(benchmarks)
    .fetch_one(pool)
    .await?;
    let marker = marker(comment_id, "finish", &key);
    let (body, gist_url) = report
        .github_body(gh, &key, marker.chars().count() + 2, existing_gist)
        .await?;
    if let Some(url) = gist_url {
        sqlx::query("UPDATE sharded_runs SET comparison_gist_url = ? WHERE comment_id = ? AND benchmarks = ?")
            .bind(url).bind(comment_id).bind(benchmarks).execute(pool).await?;
    }
    let id = gh
        .ensure_run_comment(&job.repo, job.pr_number, &marker, &body)
        .await?;
    sqlx::query(
        "UPDATE sharded_runs SET finished_comment_id = ? WHERE comment_id = ? AND benchmarks = ?",
    )
    .bind(id)
    .bind(comment_id)
    .bind(benchmarks)
    .execute(pool)
    .await?;
    Ok(())
}

/// Validate the complete fan-out before trusting it as a single comparison.
fn validate_jobs(jobs: &[&BenchmarkJob]) -> Result<()> {
    let first = jobs.first().context("empty shard group")?;
    let mut indices = BTreeSet::new();
    for job in jobs {
        job.shard()?;
        ensure!(
            job.comment_id == first.comment_id
                && job.repo == first.repo
                && job.pr_number == first.pr_number
                && job.benchmarks == first.benchmarks
                && job.effective_shards == first.effective_shards
                && job.resolved_source_json == first.resolved_source_json,
            "inconsistent shard identities"
        );
        ensure!(indices.insert(job.shard_index), "duplicate shard index");
    }
    ensure!(
        indices == (0..first.effective_shards).collect(),
        "missing shard jobs"
    );
    Ok(())
}

fn merge(
    jobs: &[&BenchmarkJob],
    results: &BTreeMap<i64, ShardResult>,
) -> Result<(Baseline, Baseline)> {
    validate_jobs(jobs)?;
    let mut base = Baseline::empty("base");
    let mut changed = Baseline::empty("changed");
    for job in jobs {
        if let Some(result) = results.get(&job.id) {
            result.validate(job)?;
            if let Some(export) = &result.base {
                base.extend(export)?;
            }
            changed.extend(&result.changed)?;
        }
    }
    Ok((base, changed))
}

pub(crate) async fn compare(base: &Baseline, changed: &Baseline) -> Result<String> {
    let dir = tempfile::tempdir()?;
    let mut paths = Vec::new();
    for export in [base, changed] {
        if export.benchmarks.is_empty() {
            continue;
        }
        let path = dir.path().join(format!("{}.json", export.name));
        tokio::fs::write(&path, serde_json::to_vec(export)?).await?;
        paths.push(path);
    }
    if paths.is_empty() {
        return Ok("No matching cases; no measurements run.\n".into());
    }
    // Never load controller-local Criterion artifacts or inherit CARGO_TARGET_DIR.
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        tokio::process::Command::new("critcmp")
            .args(["--color", "never", "--target-dir"])
            .arg(dir.path().join("empty-target"))
            .args(paths)
            .current_dir(dir.path())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("critcmp timed out")??;
    ensure!(
        output.status.success(),
        "critcmp failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

async fn final_body(
    jobs: &[BenchmarkJob],
    results: &BTreeMap<i64, ShardResult>,
    info: &BTreeMap<i64, RunnerInfo>,
    runner_repo: Option<&str>,
) -> Result<criterion_report::Report> {
    let first = jobs.first().context("empty execution unit")?;
    ensure!(
        jobs.iter().all(|j| j.benchmarks == first.benchmarks),
        "mixed targets in execution unit"
    );
    let context = ExecutionContext::from_job(first)?;
    let workers: Vec<_> = jobs
        .iter()
        .map(|job| {
            let result = results.get(&job.id);
            WorkerReport {
                index: job.shard_index,
                info: result.map(|r| &r.info).or_else(|| info.get(&job.id)),
                result,
                error: if job.status != "completed" || result.is_none() {
                    Some(
                        job.error_message
                            .as_deref()
                            .unwrap_or("worker completed without submitting Criterion exports"),
                    )
                } else {
                    None
                },
            }
        })
        .collect();
    criterion_report::report_body(
        &context,
        merge(&jobs.iter().collect::<Vec<_>>(), results),
        &workers,
        runner_repo,
    )
    .await
}

#[cfg(test)]
#[path = "shard_reporting_tests.rs"]
pub(crate) mod tests;
