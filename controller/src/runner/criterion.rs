//! Criterion discovery/execution adapter shared by all repository setups.
//! Cargo arguments, working directory and environment are supplied by callers.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use crate::runner::{
    build_env, criterion_sharding as cases, execution::Harness, monitor::ResourceStats, shell,
};

pub struct Criterion {
    pub bench_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub label: String,
    pub filter: Option<String>,
    pub dir: PathBuf,
    pub env: Vec<String>,
    pub target_override: Option<PathBuf>,
}

impl Run {
    pub fn new(label: &str, filter: &str, dir: &Path, env: &[String]) -> Self {
        Self {
            label: label.into(),
            filter: (!filter.is_empty()).then(|| filter.into()),
            dir: dir.into(),
            env: env.into(),
            target_override: None,
        }
    }

    pub fn target_dir(&self) -> PathBuf {
        self.resolve_target_dir(std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from))
    }

    fn resolve_target_dir(&self, inherited: Option<PathBuf>) -> PathBuf {
        let target =
            self.target_override
                .clone()
                .or_else(|| {
                    self.env.iter().rev().find_map(|value| {
                        value.strip_prefix("CARGO_TARGET_DIR=").map(PathBuf::from)
                    })
                })
                .or(inherited)
                .unwrap_or_else(|| "target".into());
        if target.is_absolute() {
            target
        } else {
            self.dir.join(target)
        }
    }

    /// A successful invocation can still have no matching cases. Preserve that
    /// as an empty baseline, distinct from a baseline that could not be built.
    pub async fn export(&self, measured: bool) -> Result<crate::shard_reporting::Baseline> {
        let empty = crate::shard_reporting::Baseline::empty(&self.label);
        if !measured {
            return Ok(empty);
        }
        let target = self.target_dir();
        match tokio::fs::metadata(target.join("criterion")).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(empty),
            Err(error) => return Err(error.into()),
            Ok(metadata) => ensure!(metadata.is_dir(), "Criterion data path is not a directory"),
        }
        tracing::info!(baseline = %self.label, target_dir = %target.display(), "exporting Criterion results");
        let output = tokio::process::Command::new("critcmp")
            .arg("--target-dir")
            .arg(&target)
            .args(["--export", &self.label])
            .current_dir(&self.dir)
            .kill_on_drop(true)
            .output()
            .await
            .context("run critcmp --export")?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        // critcmp 0.1.8 reports absent baselines as errors, unlike comparisons.
        // Do not turn malformed artifacts or other export failures into empty data.
        if !output.status.success()
            && (stderr.trim() == "could not find any benchmark data"
                || stderr.trim() == format!("failed to find baseline '{}'", self.label))
        {
            return Ok(empty);
        }
        ensure!(
            output.status.success(),
            "critcmp export {} failed ({}): {stderr}",
            self.label,
            output.status
        );
        let export: crate::shard_reporting::Baseline = serde_json::from_slice(&output.stdout)?;
        export.validate(&self.label)?;
        Ok(export)
    }
}

#[derive(Deserialize)]
struct Metadata {
    target_directory: PathBuf,
}

fn cargo_args(env: &[String], args: Vec<String>) -> Vec<String> {
    env.iter()
        .cloned()
        .chain(build_env::args())
        .chain(["cargo".into()])
        .chain(args)
        .collect()
}

fn refs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

impl Criterion {
    async fn list(&self, run: &Run) -> Result<BTreeSet<String>> {
        let mut args = self.bench_args.clone();
        args.extend(["--", "--list", "--format", "terse"].map(String::from));
        args.extend(run.filter.clone());
        cases::parse_listing(
            &shell::run_command("env", &refs(&cargo_args(&run.env, args)), &run.dir).await?,
        )
    }
}

impl Harness for Criterion {
    type Plan = Run;
    type Output = ResourceStats;

    async fn discover(&self, run: &mut Run) -> Result<BTreeSet<String>> {
        let args = cargo_args(
            &run.env,
            ["metadata", "--no-deps", "--format-version=1"]
                .map(String::from)
                .into(),
        );
        let metadata: Metadata =
            serde_json::from_str(&shell::run_command("env", &refs(&args), &run.dir).await?)?;
        run.target_override = Some(metadata.target_directory);
        self.list(run).await
    }

    async fn select(
        &self,
        run: &mut Run,
        inventory: &BTreeSet<String>,
        selected: &BTreeSet<String>,
    ) -> Result<()> {
        run.filter = cases::execution_filter(
            inventory,
            selected,
            run.filter.as_deref().unwrap_or_default(),
        )?;
        if selected != inventory {
            ensure!(
                self.list(run).await? == *selected,
                "unstable case IDs or incompatible Criterion selector"
            );
        }
        Ok(())
    }

    async fn execute(&self, run: &Run) -> Result<ResourceStats> {
        let mut args = self.bench_args.clone();
        args.extend(["--", "--save-baseline", &run.label].map(String::from));
        args.extend(run.filter.clone());
        let (command, args) = if run.env.is_empty() {
            ("cargo", args)
        } else {
            ("env", cargo_args(&run.env, args))
        };
        let (_, stats) =
            shell::run_command_monitored(command, &refs(&args), &run.dir, None).await?;
        Ok(stats)
    }

    async fn measured_cases(&self, run: &Run, _: &ResourceStats) -> Result<BTreeSet<String>> {
        let export = shell::run_command(
            "critcmp",
            &[
                "--target-dir",
                &run.target_dir().to_string_lossy(),
                "--export",
                &run.label,
            ],
            &run.dir,
        )
        .await?;
        cases::export_ids(&export, &run.label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_directory_respects_metadata_side_and_inherited_overrides() {
        let mut run = Run::new("base", "", Path::new("/checkout"), &[]);
        assert_eq!(run.resolve_target_dir(None), Path::new("/checkout/target"));
        assert_eq!(
            run.resolve_target_dir(Some("/shared-target".into())),
            Path::new("/shared-target")
        );
        assert_eq!(
            run.resolve_target_dir(Some("relative-target".into())),
            Path::new("/checkout/relative-target")
        );
        run.env = vec![
            "CARGO_TARGET_DIR=/first".into(),
            "CARGO_TARGET_DIR=side-target".into(),
        ];
        assert_eq!(
            run.resolve_target_dir(Some("/inherited".into())),
            Path::new("/checkout/side-target")
        );
        run.target_override = Some("/metadata-target".into());
        assert_eq!(
            run.resolve_target_dir(Some("/inherited".into())),
            Path::new("/metadata-target")
        );
    }

    #[test]
    fn forced_build_settings_follow_side_overrides() {
        let args = cargo_args(&["CARGO_BUILD_JOBS=99".into()], vec!["bench".into()]);
        assert!(
            args.iter().position(|a| a == "CARGO_BUILD_JOBS=5").unwrap()
                > args
                    .iter()
                    .position(|a| a == "CARGO_BUILD_JOBS=99")
                    .unwrap()
        );
        assert_eq!(&args[args.len() - 2..], &["cargo", "bench"]);
    }
}
