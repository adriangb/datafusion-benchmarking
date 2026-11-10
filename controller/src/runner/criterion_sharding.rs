//! Criterion's public listing/filtering protocol and coverage checks.
//! Neither discovery order nor the other side's inventory affects ownership.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

pub fn parse_listing(output: &str) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for line in output.lines() {
        if let Some(id) = line.strip_suffix(": benchmark") {
            ensure!(
                !id.is_empty() && !id.chars().any(char::is_control),
                "invalid Criterion case ID"
            );
            ensure!(
                ids.insert(id.to_string()),
                "duplicate Criterion case ID: {id}"
            );
        }
    }
    // Terse Criterion emits nothing for an empty selection. Do not interpret
    // an incompatible harness's help/test output as successful empty discovery.
    ensure!(!ids.is_empty() || output.trim().is_empty(), "no Criterion terse records in listing output; sharding requires a compatible Criterion harness");
    Ok(ids)
}

/// None MUST mean skip execution, not omit the filter (which would run all).
pub fn selector(ids: &BTreeSet<String>) -> Result<Option<String>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let pattern = format!(
        r"\A(?:{})\z",
        ids.iter()
            .map(|id| regex::escape(id))
            .collect::<Vec<_>>()
            .join("|")
    );
    // Stay well below Linux's per-argument limit. Do not silently split into
    // fresh processes, which would change allocator/process-lifetime behavior.
    ensure!(
        pattern.len() <= 64 * 1024,
        "shard selector exceeds 64 KiB; request more shards or a narrower BENCH_FILTER"
    );
    regex::Regex::new(&pattern).context("compile shard selector")?;
    Ok(Some(pattern))
}

/// Reuse the caller's original filter when this worker owns the whole filtered
/// inventory. In particular, one shard needs no generated alternation (or its
/// argument-size limit). None here means omit the filter; empty selections are
/// rejected and must be skipped by the caller before invoking a benchmark.
pub fn execution_filter(
    inventory: &BTreeSet<String>,
    selected: &BTreeSet<String>,
    user_filter: &str,
) -> Result<Option<String>> {
    ensure!(
        !selected.is_empty(),
        "cannot execute an empty case selection"
    );
    ensure!(
        selected.is_subset(inventory),
        "selected cases must belong to the filtered inventory"
    );
    if selected == inventory {
        Ok((!user_filter.is_empty()).then(|| user_filter.to_string()))
    } else {
        selector(selected)
    }
}

#[derive(Deserialize)]
struct Export {
    name: String,
    benchmarks: BTreeMap<String, serde::de::IgnoredAny>,
}

/// Read measured case IDs, leaving estimates and rendering to critcmp.
pub fn export_ids(json: &str, label: &str) -> Result<BTreeSet<String>> {
    let export: Export = serde_json::from_str(json).context("parse critcmp export")?;
    ensure!(export.name == label, "unexpected exported baseline name");
    Ok(export.benchmarks.into_keys().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_and_literal_selectors() {
        let ids =
            parse_listing("fixture diagnostic\ncommon/a: benchmark\nλ [10]+?(x): benchmark\n")
                .unwrap();
        let re = regex::Regex::new(&selector(&ids).unwrap().unwrap()).unwrap();
        for id in &ids {
            assert!(re.is_match(id));
        }
        assert!(!re.is_match("prefix/common/a"));
        assert!(!re.is_match("common/ab"));
        assert!(selector(&BTreeSet::new()).unwrap().is_none());
        assert!(parse_listing("").unwrap().is_empty());
        assert!(parse_listing("a: benchmark\na: benchmark\n").is_err());
        assert!(parse_listing("a: test\n").is_err());
        assert!(parse_listing(": benchmark\n").is_err());
    }

    #[test]
    fn whole_inventory_keeps_the_original_filter_without_a_selector_size_limit() {
        let inventory: BTreeSet<_> = (0..2000)
            .map(|i| format!("long-benchmark-name-with-parameters/{i}"))
            .collect();
        assert!(selector(&inventory).is_err());
        assert_eq!(execution_filter(&inventory, &inventory, "").unwrap(), None);
        assert_eq!(
            execution_filter(&inventory, &inventory, "long-benchmark")
                .unwrap()
                .as_deref(),
            Some("long-benchmark")
        );
        assert!(execution_filter(&inventory, &BTreeSet::new(), "").is_err());
        assert!(execution_filter(&inventory, &BTreeSet::from(["unknown".into()]), "").is_err());
        let subset = inventory.iter().take(2).cloned().collect();
        assert_eq!(
            execution_filter(&inventory, &subset, "").unwrap(),
            selector(&subset).unwrap()
        );
    }

    #[test]
    fn export_ids_checks_baseline_name_and_format() {
        let json = r#"{"name":"base","benchmarks":{"a":{}}}"#;
        assert_eq!(
            export_ids(json, "base").unwrap(),
            BTreeSet::from(["a".into()])
        );
        assert!(export_ids(json, "changed").is_err());
        assert!(export_ids("{}", "base").is_err());
    }
}
