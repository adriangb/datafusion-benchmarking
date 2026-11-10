//! Explicit, harness-independent work ownership. This is a versioned hash contract:
//! never change the encoding/hash without introducing a new assignment version.

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Retain the original domain/version so extracting adapters never moves existing cases.
pub const ASSIGNMENT_VERSION: &str = "criterion-shard-v1";
pub const MAX_SHARDS: u32 = 8;
pub const WORKER_FLAG: &str = "--shard-config";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    /// Resolved execution count, never re-derived from current capabilities.
    pub count: u32,
    pub index: u32,
}

impl Default for Shard {
    fn default() -> Self {
        Self { count: 1, index: 0 }
    }
}

/// Declared by the harness/setup, not inferred from case counts or history.
#[derive(Debug, Clone, Copy)]
pub enum ShardSupport {
    Partitioned,
    SingleWorkerOnly(&'static str),
}

impl ShardSupport {
    pub fn resolve(self, requested: Option<u32>) -> Result<u32> {
        let shard = Shard {
            count: requested.unwrap_or(1),
            index: 0,
        };
        self.validate_execution(shard)?;
        Ok(shard.count())
    }

    /// Workers validate the persisted plan, never independently clamp it: doing
    /// so after fan-out would execute the entire workload on every worker.
    pub fn validate_execution(self, shard: Shard) -> Result<()> {
        shard.validate()?;
        if let Self::SingleWorkerOnly(reason) = self {
            ensure!(
                shard.count() == 1,
                "shards greater than 1 are not supported by this setup: {reason}. Omit shards or use shards: 1"
            );
        }
        Ok(())
    }
}

impl Shard {
    pub fn count(self) -> u32 {
        self.count
    }

    pub fn validate(self) -> Result<Self> {
        ensure!(
            (1..=MAX_SHARDS).contains(&self.count()),
            "shards must be between 1 and {MAX_SHARDS}"
        );
        ensure!(
            self.index < self.count(),
            "shard index must be less than shard count"
        );
        Ok(self)
    }

    pub fn label(self) -> String {
        if self.count() == 1 {
            String::new()
        } else {
            format!(" — shard {}/{}", self.index + 1, self.count())
        }
    }

    /// Rendezvous hashing: adding cases never moves existing cases; adding a
    /// shard only moves cases to the new shard. No Rust DefaultHasher seeds.
    pub fn owns(self, target: &str, case: &str) -> bool {
        owner(target, case, self.count()) == self.index
    }
}

pub fn select(
    ids: &std::collections::BTreeSet<String>,
    target: &str,
    shard: Shard,
) -> std::collections::BTreeSet<String> {
    ids.iter()
        .filter(|id| shard.owns(target, id))
        .cloned()
        .collect()
}

pub fn owner(target: &str, case: &str, count: u32) -> u32 {
    assert!(count > 0);
    let mut best = 0;
    let mut best_score = score(target, case, 0);
    for index in 1..count {
        let candidate = score(target, case, index);
        if candidate > best_score {
            best = index;
            best_score = candidate;
        }
    }
    best
}

fn score(target: &str, case: &str, index: u32) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"criterion-shard-v1\0");
    for value in [target, case] {
        hash.update(
            u32::try_from(value.len())
                .expect("benchmark ID too long")
                .to_be_bytes(),
        );
        hash.update(value.as_bytes());
    }
    hash.update(index.to_be_bytes());
    hash.finalize().into()
}

/// Metadata only: the controller never compiles or enumerates benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrozenSources {
    pub baseline_sha: String,
    pub changed_sha: String,
    pub pr_head_ref: String,
}

impl FrozenSources {
    pub fn validate(&self) -> Result<()> {
        for sha in [&self.baseline_sha, &self.changed_sha] {
            ensure!(
                sha.len() == 40 && sha.bytes().all(|c| c.is_ascii_hexdigit()),
                "expected a full resolved Git commit SHA"
            );
        }
        Ok(())
    }
}

/// Internal runner arguments, not benchmark environment variables.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerShard {
    pub version: String,
    pub shard: Shard,
    pub sources: FrozenSources,
}

impl WorkerShard {
    pub fn from_args(mut args: impl Iterator<Item = String>) -> Result<Option<Self>> {
        let mut config = None;
        while let Some(arg) = args.next() {
            if arg != WORKER_FLAG {
                continue;
            }
            ensure!(config.is_none(), "duplicate shard configuration");
            let value: Self =
                serde_json::from_str(&args.next().context("missing shard configuration")?)?;
            ensure!(
                value.version == ASSIGNMENT_VERSION,
                "unsupported shard assignment version"
            );
            value.shard.validate()?;
            value.sources.validate()?;
            config = Some(value);
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_counts_and_indices() {
        assert_eq!(Shard::default().validate().unwrap().count(), 1);
        for count in [0, MAX_SHARDS + 1] {
            assert!(Shard { count, index: 0 }.validate().is_err());
        }
        assert!(Shard { count: 2, index: 2 }.validate().is_err());
        assert_eq!(Shard::default(), Shard { count: 1, index: 0 });
    }

    #[test]
    fn worker_configuration_is_opt_in_and_versioned() {
        assert!(WorkerShard::from_args(std::iter::empty())
            .unwrap()
            .is_none());
        let value = WorkerShard {
            version: ASSIGNMENT_VERSION.into(),
            shard: Shard { count: 4, index: 2 },
            sources: FrozenSources {
                baseline_sha: "a".repeat(40),
                changed_sha: "b".repeat(40),
                pr_head_ref: "branch".into(),
            },
        };
        let json = serde_json::to_string(&value).unwrap();
        let parsed = WorkerShard::from_args([WORKER_FLAG.into(), json.clone()].into_iter())
            .unwrap()
            .unwrap();
        assert_eq!(parsed.shard, value.shard);
        assert_eq!(parsed.sources, value.sources);
        assert!(WorkerShard::from_args([WORKER_FLAG.into()].into_iter()).is_err());
        assert!(WorkerShard::from_args(
            [
                WORKER_FLAG.into(),
                json.replace(ASSIGNMENT_VERSION, "unknown")
            ]
            .into_iter()
        )
        .is_err());
    }

    #[test]
    fn unsupported_shards_are_rejected_at_admission_and_execution() {
        let limited = ShardSupport::SingleWorkerOnly("no batch selection");
        for requested in [None, Some(1)] {
            assert_eq!(limited.resolve(requested).unwrap(), 1);
            assert_eq!(ShardSupport::Partitioned.resolve(requested).unwrap(), 1);
        }
        for count in 2..=MAX_SHARDS {
            assert_eq!(
                ShardSupport::Partitioned.resolve(Some(count)).unwrap(),
                count
            );
            let error = limited.resolve(Some(count)).unwrap_err().to_string();
            assert!(error.contains("no batch selection"));
            assert!(error.contains("shards: 1"));
            assert_eq!(
                limited
                    .validate_execution(Shard { count, index: 0 })
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
        for count in [0, MAX_SHARDS + 1] {
            assert!(limited.resolve(Some(count)).is_err());
            assert!(ShardSupport::Partitioned.resolve(Some(count)).is_err());
        }
        assert!(limited.validate_execution(Shard::default()).is_ok());
    }

    #[test]
    fn golden_encoding() {
        // The expected values are independently computed from the documented
        // SHA-256 byte encoding, not generated by the production implementation.
        assert_eq!(owner("arrow_writer", "group/a", 4), 0);
        assert_eq!(owner("arrow_writer", "group/b", 4), 0);
        assert_eq!(owner("arrow_writer", "nested/λ [10]", 8), 2);
    }

    #[test]
    fn exactly_one_owner_and_minimal_movement() {
        for i in 0..1000 {
            let case = format!("group/case-{i}");
            for count in 1..MAX_SHARDS {
                let old = owner("writer", &case, count);
                let new = owner("writer", &case, count + 1);
                assert!(new == old || new == count);
                let owners = (0..count)
                    .filter(|&index| Shard { count, index }.owns("writer", &case))
                    .count();
                assert_eq!(owners, 1);
            }
        }
    }
}
