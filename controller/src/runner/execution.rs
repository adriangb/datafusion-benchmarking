//! Harness-neutral partitioning and execution. Adapters never choose owners or
//! receive a shard count: they discover cases, apply a selection and execute it.

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{bail, ensure, Result};

use crate::runner::{monitor::ResourceStats, shell};
use crate::sharding::Shard;

// Static dispatch; these futures are awaited locally, not spawned by the engine.
#[allow(async_fn_in_trait)]
pub trait Harness {
    type Plan;
    type Output;

    async fn discover(&self, _plan: &mut Self::Plan) -> Result<BTreeSet<String>> {
        bail!("this harness does not support case discovery")
    }
    async fn select(
        &self,
        _plan: &mut Self::Plan,
        _inventory: &BTreeSet<String>,
        _selected: &BTreeSet<String>,
    ) -> Result<()> {
        bail!("this harness does not support subset execution")
    }
    async fn execute(&self, plan: &Self::Plan) -> Result<Self::Output>;
    async fn measured_cases(
        &self,
        _plan: &Self::Plan,
        _output: &Self::Output,
    ) -> Result<BTreeSet<String>> {
        bail!("this harness does not support coverage validation")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition<P> {
    pub plan: P,
    /// None is identity; Some(empty) skips execution.
    pub coverage: Option<BTreeSet<String>>,
}

/// Count one is the identity for every harness, including opaque commands. No
/// discovery or selector translation is evaluated in that case.
pub async fn partition<H: Harness>(
    harness: &H,
    mut plan: H::Plan,
    shard: Shard,
    target: &str,
) -> Result<Partition<H::Plan>> {
    if shard.validate()?.count() == 1 {
        return Ok(Partition {
            plan,
            coverage: None,
        });
    }
    let inventory = harness.discover(&mut plan).await?;
    let selected = crate::sharding::select(&inventory, target, shard);
    if !selected.is_empty() {
        harness.select(&mut plan, &inventory, &selected).await?;
    }
    Ok(Partition {
        plan,
        coverage: Some(selected),
    })
}

pub async fn execute<H: Harness>(
    harness: &H,
    partition: &Partition<H::Plan>,
) -> Result<Option<H::Output>> {
    if partition.coverage.as_ref().is_some_and(|c| c.is_empty()) {
        return Ok(None);
    }
    let output = harness.execute(&partition.plan).await?;
    if let Some(coverage) = &partition.coverage {
        let actual = harness.measured_cases(&partition.plan, &output).await?;
        validate_coverage(coverage, &actual)?;
    }
    Ok(Some(output))
}

pub fn validate_coverage(expected: &BTreeSet<String>, actual: &BTreeSet<String>) -> Result<()> {
    ensure!(
        actual == expected,
        "measured cases differ from assigned cases: missing={:?}, unexpected={:?}",
        expected.difference(actual).collect::<Vec<_>>(),
        actual.difference(expected).collect::<Vec<_>>()
    );
    Ok(())
}

/// Adapter for existing scripts/binaries without a discovery/subset protocol.
/// It uses the same engine at count one; unsupported distributed plans fail
/// closed instead of silently executing the full command on every worker.
pub struct CommandHarness;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRun {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub spill_dir: Option<PathBuf>,
}

impl Harness for CommandHarness {
    type Plan = CommandRun;
    type Output = ResourceStats;

    async fn execute(&self, plan: &CommandRun) -> Result<ResourceStats> {
        let args: Vec<_> = plan.args.iter().map(String::as_str).collect();
        let (_, stats) =
            shell::run_command_monitored(&plan.program, &args, &plan.cwd, plan.spill_dir.clone())
                .await?;
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // A deliberately non-Criterion adapter, exercising the complete protocol.
    struct Queries {
        calls: Cell<usize>,
        corrupt: bool,
    }
    impl Harness for Queries {
        type Plan = Vec<String>;
        type Output = Vec<String>;
        async fn discover(&self, plan: &mut Self::Plan) -> Result<BTreeSet<String>> {
            self.calls.set(self.calls.get() + 1);
            Ok(plan.iter().cloned().collect())
        }
        async fn select(
            &self,
            plan: &mut Self::Plan,
            _: &BTreeSet<String>,
            selected: &BTreeSet<String>,
        ) -> Result<()> {
            plan.retain(|id| selected.contains(id)); // Preserve harness order.
            Ok(())
        }
        async fn execute(&self, plan: &Self::Plan) -> Result<Self::Output> {
            self.calls.set(self.calls.get() + 1);
            Ok(if self.corrupt {
                vec!["unexpected".into()]
            } else {
                plan.clone()
            })
        }
        async fn measured_cases(
            &self,
            _: &Self::Plan,
            output: &Self::Output,
        ) -> Result<BTreeSet<String>> {
            Ok(output.iter().cloned().collect())
        }
    }

    #[tokio::test]
    async fn identity_does_not_discover_or_change_any_plan() {
        let harness = Queries {
            calls: Cell::new(0),
            corrupt: false,
        };
        let original = vec!["Q9".into(), "Q1".into()];
        let p = partition(&harness, original.clone(), Shard::default(), "suite")
            .await
            .unwrap();
        assert_eq!(p.plan, original);
        assert_eq!(harness.calls.get(), 0);
        assert_eq!(execute(&harness, &p).await.unwrap(), Some(original));
        let command = CommandRun {
            program: "untouched".into(),
            args: vec![
                "".into(),
                "nested/λ.*".into(),
                "[".into(),
                "x".repeat(70_000),
            ],
            cwd: "/nonexistent".into(),
            spill_dir: None,
        };
        let p = partition(&CommandHarness, command.clone(), Shard::default(), "suite")
            .await
            .unwrap();
        assert_eq!(p.plan, command);
        assert!(p.coverage.is_none());
        assert!(partition(
            &CommandHarness,
            command,
            Shard { count: 2, index: 0 },
            "suite"
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn any_harness_gets_stable_complete_partitions_and_empty_assignments() {
        let base = vec!["Q9".into(), "Q1".into(), "removed".into()];
        let changed = vec!["added".into(), "Q9".into(), "Q1".into()];
        for count in [1, 2, 4, 8] {
            let mut owners = std::collections::BTreeMap::new();
            for input in [&base, &changed] {
                let mut all = BTreeSet::new();
                for index in 0..count {
                    let harness = Queries {
                        calls: Cell::new(0),
                        corrupt: false,
                    };
                    let shard = Shard { count, index };
                    let p = partition(&harness, input.clone(), shard, "suite")
                        .await
                        .unwrap();
                    let before = harness.calls.get();
                    let output = execute(&harness, &p).await.unwrap();
                    if output.is_none() {
                        assert_eq!(before, harness.calls.get());
                    }
                    let output = output.unwrap_or_default();
                    let expected: Vec<_> = input
                        .iter()
                        .filter(|id| shard.owns("suite", id))
                        .cloned()
                        .collect();
                    assert_eq!(output, expected);
                    for id in output {
                        if let Some(previous) = owners.insert(id.clone(), index) {
                            assert_eq!(previous, index); // Common cases stay paired.
                        }
                        assert!(all.insert(id));
                    }
                }
                assert_eq!(all, input.iter().cloned().collect());
            }
        }
    }

    #[tokio::test]
    async fn coverage_validation_is_not_harness_specific() {
        let harness = Queries {
            calls: Cell::new(0),
            corrupt: true,
        };
        let count = 2;
        let index = crate::sharding::owner("suite", "Q1", count);
        let p = partition(&harness, vec!["Q1".into()], Shard { count, index }, "suite")
            .await
            .unwrap();
        assert!(execute(&harness, &p).await.is_err());
    }
}
