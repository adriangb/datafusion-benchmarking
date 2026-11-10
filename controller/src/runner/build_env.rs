//! Build settings the runner forces on every cargo build it starts.
//!
//! `cargo bench --bench sql` links 7 binaries under the bench profile, which
//! inherits DataFusion's release profile (`lto = true`, `codegen-units = 1`).
//! Each fat-LTO link peaks at 13-15 GB, so parallel links exceeded the 65 GiB
//! pod and it was OOM-killed before any query ran. The `dfbench` builds, which
//! run for both sides at once, link one binary each at the same cost.
//!
//! Thin LTO peaks at ~5 GB per link and measured within ±1-2% of fat LTO at
//! run time (`off` was ~5% slower, so it is not used). Both sides of a
//! comparison build the same way, so the comparison is unaffected.
//!
//! The job cap bounds how many compiles and links run at once. It was measured
//! on a 12-core host, building `cargo bench --bench sql --no-run` at thin LTO:
//! from scratch, 11.9 min uncapped, 14.0 min at 5 jobs (+18%), 17.6 min at
//! 3 jobs (+48%). Two runs on the pod, capped and uncapped, agreed query for
//! query and showed no end-to-end penalty.
//!
//! A trigger cannot override these: a build that is OOM-killed or times out
//! gives no result at all. Note that they are sized for the default build. A
//! run that also asks for debuginfo (`CARGO_PROFILE_RELEASE_DEBUG`) makes each
//! rustc much larger, and may need this cap lowered.

/// The forced settings. The bench profile inherits the release profile, but
/// cargo needs the override on each one it actually builds with: `cargo build
/// --release` and `bench.sh`'s `cargo run --release` read the release keys,
/// `cargo bench` reads the bench keys.
pub const FORCED: [(&str, &str); 3] = [
    ("CARGO_PROFILE_RELEASE_LTO", "thin"),
    ("CARGO_PROFILE_BENCH_LTO", "thin"),
    ("CARGO_BUILD_JOBS", "5"),
];

/// Apply the settings to the runner's own environment, which every build the
/// runner spawns inherits. This also covers the builds that run without an
/// `env` wrapper, and the ones `bench.sh` starts on its own.
pub fn apply() {
    for (key, value) in FORCED {
        std::env::set_var(key, value);
    }
}

/// The settings as `KEY=VALUE` args for an `env` command line. Append them
/// last, after the trigger's env vars, so an explicit assignment from a
/// trigger cannot override them.
pub fn args() -> Vec<String> {
    FORCED.iter().map(|(k, v)| format!("{k}={v}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_render_as_env_assignments() {
        assert_eq!(
            args(),
            vec![
                "CARGO_PROFILE_RELEASE_LTO=thin",
                "CARGO_PROFILE_BENCH_LTO=thin",
                "CARGO_BUILD_JOBS=5",
            ]
        );
    }
}
