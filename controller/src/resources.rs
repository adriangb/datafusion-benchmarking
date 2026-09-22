//! Pod resource requests carried by a trigger comment.
//!
//! A trigger can size its own pod and pick a CPU architecture:
//!
//! ```yaml
//! run benchmark tpch
//! resources:
//!   cpu: "16"
//!   memory: "128Gi"
//!   arch: arm64
//! ```
//!
//! Every key is optional; an absent key keeps the controller default
//! (`DEFAULT_CPU`, `DEFAULT_MEMORY`, `DEFAULT_MACHINE_FAMILY`). Values are
//! validated here, before the job is scheduled, so a bad value becomes a reply
//! on the PR instead of a pod that never starts.

use serde::Deserialize;

/// Upper bound on `resources.cpu`, unless `MAX_CPU` overrides it.
///
/// The Performance compute class tops out at the largest shape of the node's
/// machine family, which is 72 vCPU for `c4a`.
pub const DEFAULT_MAX_CPU: &str = "72";

/// Upper bound on `resources.memory`, unless `MAX_MEMORY` overrides it.
///
/// 576Gi is the memory of a `c4a-highmem-72`, the largest `c4a` shape.
pub const DEFAULT_MAX_MEMORY: &str = "576Gi";

/// The CPU architectures a trigger may ask for. Each maps to a machine family
/// in [`crate::job_manager`].
pub const SUPPORTED_ARCHES: [&str; 2] = ["arm64", "amd64"];

/// Caps on what a trigger may request, so a typo cannot ask for a pod that
/// will never schedule.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    max_cpu: String,
    max_cpu_millis: u64,
    max_memory: String,
    max_memory_bytes: u64,
}

impl ResourceLimits {
    /// Build limits from two Kubernetes quantities. Returns `Err` if either
    /// one is not a quantity, which fails controller startup.
    pub fn new(max_cpu: &str, max_memory: &str) -> Result<Self, String> {
        let max_cpu_millis =
            parse_cpu_millis(max_cpu).map_err(|e| format!("invalid max CPU `{max_cpu}`: {e}"))?;
        let max_memory_bytes = parse_memory_bytes(max_memory)
            .map_err(|e| format!("invalid max memory `{max_memory}`: {e}"))?;
        Ok(Self {
            max_cpu: max_cpu.to_string(),
            max_cpu_millis,
            max_memory: max_memory.to_string(),
            max_memory_bytes,
        })
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CPU, DEFAULT_MAX_MEMORY).expect("built-in defaults are valid")
    }
}

/// A validated resource request. `None` on a field means "use the controller
/// default".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodResources {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub arch: Option<String>,
}

impl PodResources {
    /// `true` when the trigger asked for nothing, so every default applies.
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none() && self.arch.is_none()
    }

    /// One-line summary of the non-default fields, e.g. `16 CPU, 128Gi, arm64`.
    /// Returns `None` when nothing was requested.
    pub fn summary(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let mut parts = Vec::new();
        if let Some(cpu) = &self.cpu {
            parts.push(format!("{cpu} CPU"));
        }
        if let Some(memory) = &self.memory {
            parts.push(memory.clone());
        }
        if let Some(arch) = &self.arch {
            parts.push(arch.clone());
        }
        Some(parts.join(", "))
    }
}

/// The `resources:` block of a trigger comment, before validation.
///
/// Fields are [`serde_yaml::Value`] rather than `String` so `cpu: 16` and
/// `cpu: "16"` both work — YAML types the first as a number.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ResourcesConfig {
    #[serde(default)]
    cpu: Option<serde_yaml::Value>,
    #[serde(default)]
    memory: Option<serde_yaml::Value>,
    #[serde(default)]
    arch: Option<serde_yaml::Value>,
}

impl ResourcesConfig {
    /// Check every present value and return the request to schedule.
    pub fn validate(&self, limits: &ResourceLimits) -> Result<PodResources, String> {
        let cpu = match scalar(&self.cpu, "cpu")? {
            Some(cpu) => {
                let millis = parse_cpu_millis(&cpu)
                    .map_err(|e| format!("invalid `resources.cpu` value `{cpu}`: {e}"))?;
                if millis > limits.max_cpu_millis {
                    return Err(format!(
                        "`resources.cpu` of `{cpu}` is above the limit of `{}`",
                        limits.max_cpu
                    ));
                }
                Some(cpu)
            }
            None => None,
        };

        let memory = match scalar(&self.memory, "memory")? {
            Some(memory) => {
                let bytes = parse_memory_bytes(&memory)
                    .map_err(|e| format!("invalid `resources.memory` value `{memory}`: {e}"))?;
                if bytes > limits.max_memory_bytes {
                    return Err(format!(
                        "`resources.memory` of `{memory}` is above the limit of `{}`",
                        limits.max_memory
                    ));
                }
                Some(memory)
            }
            None => None,
        };

        let arch = match scalar(&self.arch, "arch")? {
            Some(arch) => {
                if !SUPPORTED_ARCHES.contains(&arch.as_str()) {
                    return Err(format!(
                        "invalid `resources.arch` value `{arch}`: expected `{}` or `{}`",
                        SUPPORTED_ARCHES[0], SUPPORTED_ARCHES[1]
                    ));
                }
                Some(arch)
            }
            None => None,
        };

        Ok(PodResources { cpu, memory, arch })
    }
}

/// Read one `resources:` value as a string. A number becomes its text (so
/// `cpu: 16` works), and an empty value is the same as an absent key.
fn scalar(value: &Option<serde_yaml::Value>, field: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::String(s)) => {
            let s = s.trim();
            if s.is_empty() {
                Ok(None)
            } else {
                Ok(Some(s.to_string()))
            }
        }
        Some(serde_yaml::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(_) => Err(format!(
            "`resources.{field}` must be a string or a number, e.g. `cpu: \"16\"`"
        )),
    }
}

/// Convert a Kubernetes CPU quantity (`"16"`, `"1.5"`, `"500m"`) to millicores.
fn parse_cpu_millis(value: &str) -> Result<u64, String> {
    let (digits, scale) = match value.strip_suffix('m') {
        Some(rest) => (rest, 1.0),
        None => (value, 1000.0),
    };
    let amount = positive_number(digits).ok_or(
        "expected a Kubernetes CPU quantity, e.g. `16` for 16 cores or `500m` for half a core",
    )?;
    Ok((amount * scale).round() as u64)
}

/// Binary (`Ki`) and decimal (`k`) suffixes Kubernetes accepts on a memory
/// quantity, longest first so `Mi` is matched before `M`.
const MEMORY_SUFFIXES: [(&str, f64); 12] = [
    ("Ki", 1024.0),
    ("Mi", 1048576.0),
    ("Gi", 1073741824.0),
    ("Ti", 1099511627776.0),
    ("Pi", 1125899906842624.0),
    ("Ei", 1152921504606846976.0),
    ("k", 1e3),
    ("M", 1e6),
    ("G", 1e9),
    ("T", 1e12),
    ("P", 1e15),
    ("E", 1e18),
];

/// Convert a Kubernetes memory quantity (`"128Gi"`, `"1000M"`, `"512"`) to bytes.
fn parse_memory_bytes(value: &str) -> Result<u64, String> {
    // Kubernetes reads `m` as milli-bytes, so `128m` is a tenth of a byte
    // rather than 128 MB. Name the mistake instead of scheduling it.
    if let Some(digits) = value.strip_suffix('m') {
        if positive_number(digits).is_some() {
            return Err(format!(
                "`m` means milli-bytes; write `{digits}Mi` for mebibytes or `{digits}M` for megabytes"
            ));
        }
    }

    let (digits, scale) = MEMORY_SUFFIXES
        .iter()
        .find_map(|(suffix, scale)| value.strip_suffix(suffix).map(|rest| (rest, *scale)))
        .unwrap_or((value, 1.0));

    let amount = positive_number(digits)
        .ok_or("expected a Kubernetes memory quantity, e.g. `128Gi` or `1000M`")?;
    Ok((amount * scale) as u64)
}

/// Parse a finite, above-zero decimal number. Rejects `0`, negatives, `inf`
/// and `NaN`, all of which Kubernetes refuses as a request.
fn positive_number(digits: &str) -> Option<f64> {
    let amount: f64 = digits.parse().ok()?;
    (amount.is_finite() && amount > 0.0).then_some(amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<PodResources, String> {
        let config: ResourcesConfig = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        config.validate(&ResourceLimits::default())
    }

    // ── every key present ───────────────────────────────────────────

    #[test]
    fn all_keys_are_kept() {
        let got = parse("cpu: \"16\"\nmemory: \"128Gi\"\narch: arm64").unwrap();
        assert_eq!(
            got,
            PodResources {
                cpu: Some("16".into()),
                memory: Some("128Gi".into()),
                arch: Some("arm64".into()),
            }
        );
        assert_eq!(got.summary().as_deref(), Some("16 CPU, 128Gi, arm64"));
    }

    /// YAML types an unquoted `16` as a number, which a user will write.
    #[test]
    fn an_unquoted_cpu_number_is_accepted() {
        let got = parse("cpu: 16").unwrap();
        assert_eq!(got.cpu.as_deref(), Some("16"));
    }

    #[test]
    fn amd64_is_accepted() {
        assert_eq!(parse("arch: amd64").unwrap().arch.as_deref(), Some("amd64"));
    }

    // ── absent keys ─────────────────────────────────────────────────

    #[test]
    fn absent_keys_stay_none() {
        let got = parse("cpu: \"16\"").unwrap();
        assert_eq!(got.cpu.as_deref(), Some("16"));
        assert!(got.memory.is_none());
        assert!(got.arch.is_none());
        assert!(!got.is_empty());
    }

    #[test]
    fn an_empty_block_requests_nothing() {
        let got = ResourcesConfig::default()
            .validate(&ResourceLimits::default())
            .unwrap();
        assert!(got.is_empty());
        assert!(got.summary().is_none());
    }

    /// `cpu:` with nothing after it is the same as leaving the key out.
    #[test]
    fn a_null_value_is_the_same_as_absent() {
        assert!(parse("cpu:\nmemory:\narch:").unwrap().is_empty());
    }

    // ── invalid values ──────────────────────────────────────────────

    #[test]
    fn a_non_quantity_cpu_is_rejected() {
        let err = parse("cpu: sixteen").unwrap_err();
        assert!(err.contains("`resources.cpu`"), "{err}");
        assert!(err.contains("sixteen"), "{err}");
    }

    #[test]
    fn a_zero_or_negative_request_is_rejected() {
        assert!(parse("cpu: 0").is_err());
        assert!(parse("cpu: -4").is_err());
        assert!(parse("memory: 0").is_err());
        assert!(parse("memory: \"-1Gi\"").is_err());
    }

    /// Kubernetes is case-sensitive about quantity suffixes.
    #[test]
    fn a_lowercase_memory_suffix_is_rejected() {
        assert!(parse("memory: 128gi").is_err());
    }

    /// `128m` is 0.128 bytes to Kubernetes, never 128 MB.
    #[test]
    fn a_milli_byte_memory_is_rejected_by_name() {
        let err = parse("memory: 128m").unwrap_err();
        assert!(err.contains("milli-bytes"), "{err}");
        assert!(err.contains("128Mi"), "{err}");
    }

    #[test]
    fn an_unsupported_arch_is_rejected() {
        let err = parse("arch: riscv64").unwrap_err();
        assert!(err.contains("riscv64"), "{err}");
        assert!(err.contains("arm64"), "{err}");
        // The job manager's legacy alias is not trigger syntax.
        assert!(parse("arch: x86_64").is_err());
    }

    #[test]
    fn a_non_scalar_value_is_rejected() {
        let err = parse("cpu:\n  - 16").unwrap_err();
        assert!(err.contains("string or a number"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let err = parse("gpu: 1").unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
    }

    // ── limits ──────────────────────────────────────────────────────

    #[test]
    fn a_request_above_the_limit_is_rejected() {
        let err = parse("cpu: 720").unwrap_err();
        assert!(err.contains("above the limit of `72`"), "{err}");

        let err = parse("memory: 5760Gi").unwrap_err();
        assert!(err.contains("above the limit of `576Gi`"), "{err}");
    }

    #[test]
    fn a_request_at_the_limit_is_accepted() {
        assert_eq!(parse("cpu: 72").unwrap().cpu.as_deref(), Some("72"));
        assert_eq!(
            parse("memory: 576Gi").unwrap().memory.as_deref(),
            Some("576Gi")
        );
    }

    #[test]
    fn limits_are_configurable() {
        let limits = ResourceLimits::new("192", "1488Gi").unwrap();
        let config: ResourcesConfig = serde_yaml::from_str("cpu: 128").unwrap();
        assert_eq!(
            config.validate(&limits).unwrap().cpu.as_deref(),
            Some("128")
        );
    }

    #[test]
    fn unparseable_limits_are_an_error() {
        assert!(ResourceLimits::new("lots", "576Gi").is_err());
        assert!(ResourceLimits::new("72", "576 gigs").is_err());
    }

    // ── quantity parsing ────────────────────────────────────────────

    #[test]
    fn cpu_quantities_convert_to_millicores() {
        assert_eq!(parse_cpu_millis("16").unwrap(), 16_000);
        assert_eq!(parse_cpu_millis("1.5").unwrap(), 1_500);
        assert_eq!(parse_cpu_millis("500m").unwrap(), 500);
        assert!(parse_cpu_millis("").is_err());
        assert!(parse_cpu_millis("16x").is_err());
    }

    #[test]
    fn memory_quantities_convert_to_bytes() {
        assert_eq!(parse_memory_bytes("1Ki").unwrap(), 1024);
        assert_eq!(
            parse_memory_bytes("128Gi").unwrap(),
            128 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_memory_bytes("1000M").unwrap(), 1_000_000_000);
        assert_eq!(parse_memory_bytes("512").unwrap(), 512);
        assert!(parse_memory_bytes("128 Gi").is_err());
        assert!(parse_memory_bytes("Gi").is_err());
    }
}
