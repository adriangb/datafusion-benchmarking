//! Command execution helpers for the benchmark runner.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;
use tracing::{info, warn};

use crate::runner::monitor::{CgroupMonitor, ResourceStats};

/// Path to the shared output log that captures all runner output.
pub const OUTPUT_FILE: &str = "/tmp/benchmark_output.txt";

const COMMAND_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_DIAGNOSTIC_AFTER_SECS: u64 = 600;
const DEFAULT_DIAGNOSTIC_INTERVAL_SECS: u64 = 300;
const PRE_DEADLINE_DIAGNOSTIC_OFFSET_SECS: u64 = 300;

/// Run a command, log it, stream output to the log file, and return stdout as a string.
/// Fails if the command exits with a non-zero status.
pub async fn run_command(cmd: &str, args: &[&str], cwd: &Path) -> Result<String> {
    info!(cmd, ?args, ?cwd, "running command");

    let temp_id = temp_log_id();
    let stdout_path = format!("/tmp/cmd-{temp_id}.stdout");
    let stderr_path = format!("/tmp/cmd-{temp_id}.stderr");

    let stdout_file = std::fs::File::create(&stdout_path)
        .with_context(|| format!("failed to create stdout log: {stdout_path}"))?;
    let stderr_file = std::fs::File::create(&stderr_path)
        .with_context(|| format!("failed to create stderr log: {stderr_path}"))?;

    let mut child = Command::new(cmd)
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .with_context(|| format!("failed to spawn: {cmd} {}", args.join(" ")))?;

    let pid = child.id();
    let status = wait_for_child(cmd, args, cwd, &mut child, pid).await?;

    let stdout = tokio::fs::read_to_string(&stdout_path).await.unwrap_or_default();
    let stderr = tokio::fs::read_to_string(&stderr_path).await.unwrap_or_default();

    // Append to output log
    append_to_log(&stdout).await;
    append_to_log(&stderr).await;

    let _ = tokio::fs::remove_file(&stdout_path).await;
    let _ = tokio::fs::remove_file(&stderr_path).await;

    if !status.success() {
        let code = status.code().unwrap_or(-1);
        anyhow::bail!(
            "{cmd} {} exited with code {code}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            args.join(" ")
        );
    }

    Ok(stdout)
}

/// Run a command with cgroup resource monitoring. Returns both stdout and resource stats.
///
/// If `spill_dir` is provided, the monitor will poll the directory size every
/// second to track peak spill usage.
pub async fn run_command_monitored(
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    spill_dir: Option<PathBuf>,
) -> Result<(String, ResourceStats)> {
    let monitor = CgroupMonitor::start(spill_dir);
    let output = run_command(cmd, args, cwd).await?;
    let stats = monitor.finish().await;
    Ok((output, stats))
}

/// Spawn a command in the background, returning a JoinHandle that resolves to the Result.
/// Output is redirected to a log file at `log_path`.
pub fn spawn_command(
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    log_path: &str,
) -> tokio::task::JoinHandle<Result<()>> {
    let cmd = cmd.to_string();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let cwd = cwd.to_path_buf();
    let log_path = log_path.to_string();

    tokio::spawn(async move {
        let log_file = std::fs::File::create(&log_path)
            .with_context(|| format!("failed to create log file: {log_path}"))?;

        let status = Command::new(&cmd)
            .args(&args)
            .current_dir(&cwd)
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file))
            .status()
            .await
            .with_context(|| format!("failed to spawn: {cmd} {}", args.join(" ")))?;

        if !status.success() {
            let log_content = tokio::fs::read_to_string(&log_path)
                .await
                .unwrap_or_default();
            append_to_log(&log_content).await;
            let code = status.code().unwrap_or(-1);
            anyhow::bail!(
                "{cmd} {} exited with code {code}\n{log_content}",
                args.join(" ")
            );
        }

        Ok(())
    })
}

/// Append text to the shared output log file.
pub async fn append_to_log(text: &str) {
    if text.is_empty() {
        return;
    }
    use tokio::io::AsyncWriteExt;
    if let Ok(mut f) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(OUTPUT_FILE)
        .await
    {
        let _ = f.write_all(text.as_bytes()).await;
    }
}

/// Read the last N lines from the output log file.
pub async fn tail_log(n: usize) -> String {
    match tokio::fs::read_to_string(OUTPUT_FILE).await {
        Ok(content) => {
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(n);
            lines[start..].join("\n")
        }
        Err(_) => String::new(),
    }
}

/// Get the system uname string.
pub async fn uname() -> String {
    match Command::new("uname").arg("-a").output().await {
        Ok(output) => String::from_utf8_lossy(&output.stdout).trim().to_string(),
        Err(_) => "unknown".to_string(),
    }
}

/// Get CPU details via lscpu (architecture, model, cores, flags, etc.).
pub async fn lscpu() -> String {
    match Command::new("lscpu").output().await {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        _ => "lscpu not available".to_string(),
    }
}

/// Query the Kubernetes API for the node's instance type label.
/// Returns a string like "c4a-standard-48" or a fallback message.
pub async fn node_instance_type() -> String {
    use k8s_openapi::api::core::v1::Node;
    use kube::{Api, Client};

    let node_name = match std::env::var("NODE_NAME") {
        Ok(n) => n,
        Err(_) => return "unknown (NODE_NAME not set)".to_string(),
    };

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(_) => return "unknown (k8s client unavailable)".to_string(),
    };

    let nodes: Api<Node> = Api::all(client);
    match nodes.get(&node_name).await {
        Ok(node) => node
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("node.kubernetes.io/instance-type"))
            .cloned()
            .unwrap_or_else(|| "unknown (label not found)".to_string()),
        Err(_) => "unknown (node lookup failed)".to_string(),
    }
}

/// Build a human-readable pod resource summary from Downward API env vars.
/// Returns e.g. "12 vCPU / 65Gi RAM" or an empty string if not available.
pub fn pod_resources() -> String {
    let cpu = std::env::var("POD_CPU_LIMIT").ok();
    let mem = std::env::var("POD_MEM_LIMIT").ok();
    match (cpu, mem) {
        (Some(c), Some(m)) => {
            let mem_display = format_bytes(&m);
            format!("{c} vCPU / {mem_display}")
        }
        _ => String::new(),
    }
}

/// Format a byte string (from the Downward API) into a human-friendly unit.
fn format_bytes(bytes_str: &str) -> String {
    let bytes: u64 = match bytes_str.parse() {
        Ok(b) => b,
        Err(_) => return bytes_str.to_string(),
    };
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB {
        format!("{:.0} GiB", bytes as f64 / GIB as f64)
    } else {
        format!("{:.0} MiB", bytes as f64 / MIB as f64)
    }
}

/// Log sccache stats if sccache is in use.
pub async fn log_sccache_stats() {
    if std::env::var("RUSTC_WRAPPER").is_ok() {
        let _ = run_command("sccache", &["--show-stats"], Path::new("/")).await;
    }
}

async fn wait_for_child(
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    child: &mut tokio::process::Child,
    pid: Option<u32>,
) -> Result<std::process::ExitStatus> {
    let started_at = Instant::now();
    let diagnostic_after_secs = default_diagnostic_after_secs();
    let diagnostic_interval_secs =
        env_u64("RUNNER_COMMAND_DIAGNOSTIC_INTERVAL_SECS", DEFAULT_DIAGNOSTIC_INTERVAL_SECS)
            .max(1);
    let timeout_secs = env_optional_u64("RUNNER_COMMAND_TIMEOUT_SECS");
    let mut next_diagnostic_at = diagnostic_after_secs;

    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }

        let elapsed = started_at.elapsed().as_secs();
        if let Some(pid) = pid {
            if elapsed >= next_diagnostic_at {
                emit_process_diagnostics(pid, cmd, args, cwd, elapsed).await;
                next_diagnostic_at = elapsed.saturating_add(diagnostic_interval_secs);
            }

            if let Some(timeout_secs) = timeout_secs {
                if elapsed >= timeout_secs {
                    warn!(cmd, ?args, pid, timeout_secs, "command timed out");
                    append_to_log(&format!(
                        "\n=== command timeout after {elapsed}s ===\ncmd={cmd} args={:?} cwd={}\n",
                        args,
                        cwd.display()
                    ))
                    .await;
                    emit_process_diagnostics(pid, cmd, args, cwd, elapsed).await;
                    dump_process_stack(pid).await;
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    anyhow::bail!(
                        "{cmd} {} timed out after {timeout_secs}s",
                        args.join(" ")
                    );
                }
            }
        }

        tokio::time::sleep(COMMAND_POLL_INTERVAL).await;
    }
}

async fn emit_process_diagnostics(
    pid: u32,
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    elapsed_secs: u64,
) {
    let status = read_proc_file(pid, "status").await;
    let wchan = read_proc_file(pid, "wchan").await;
    let io = read_proc_file(pid, "io").await;
    let threads = ps_threads(pid).await;

    let snapshot = format!(
        "\n=== long-running command diagnostics ===\n\
         elapsed_secs: {elapsed_secs}\n\
         pid: {pid}\n\
         cmd: {cmd}\n\
         args: {:?}\n\
         cwd: {}\n\
         -- /proc/{pid}/status --\n{}\n\
         -- /proc/{pid}/wchan --\n{}\n\
         -- /proc/{pid}/io --\n{}\n\
         -- ps threads --\n{}\n",
        args,
        cwd.display(),
        status.trim_end(),
        wchan.trim_end(),
        io.trim_end(),
        threads.trim_end(),
    );

    warn!(pid, elapsed_secs, cmd, ?args, "captured long-running command diagnostics");
    append_to_log(&snapshot).await;
}

async fn dump_process_stack(pid: u32) {
    let output = Command::new("gdb")
        .args([
            "-q",
            "-batch",
            "-ex",
            "set pagination off",
            "-ex",
            "thread apply all bt",
            "-p",
            &pid.to_string(),
        ])
        .output()
        .await;

    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            append_to_log(&format!(
                "\n=== gdb thread dump for pid {pid} ===\nstdout:\n{}\nstderr:\n{}\n",
                stdout.trim_end(),
                stderr.trim_end(),
            ))
            .await;
        }
        Err(error) => {
            append_to_log(&format!(
                "\n=== failed to run gdb for pid {pid}: {error} ===\n"
            ))
            .await;
        }
    }
}

async fn ps_threads(pid: u32) -> String {
    match Command::new("ps")
        .args([
            "-L",
            "-p",
            &pid.to_string(),
            "-o",
            "pid,tid,stat,etime,pcpu,pmem,wchan:32,comm",
        ])
        .output()
        .await
    {
        Ok(output) => String::from_utf8_lossy(&output.stdout).to_string(),
        Err(error) => format!("failed to run ps: {error}"),
    }
}

async fn read_proc_file(pid: u32, name: &str) -> String {
    tokio::fs::read_to_string(format!("/proc/{pid}/{name}"))
        .await
        .unwrap_or_else(|error| format!("<unavailable: {error}>"))
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_optional_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

fn default_diagnostic_after_secs() -> u64 {
    if let Some(deadline_secs) = env_optional_u64("RUNNER_JOB_DEADLINE_SECS") {
        return deadline_secs
            .saturating_sub(PRE_DEADLINE_DIAGNOSTIC_OFFSET_SECS)
            .max(60);
    }

    env_u64(
        "RUNNER_COMMAND_DIAGNOSTIC_AFTER_SECS",
        DEFAULT_DIAGNOSTIC_AFTER_SECS,
    )
}

fn temp_log_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{ts}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_file_path() {
        assert_eq!(OUTPUT_FILE, "/tmp/benchmark_output.txt");
    }

    #[tokio::test]
    async fn tail_log_empty_file() {
        let result = tail_log(20).await;
        let _ = result;
    }

    #[tokio::test]
    async fn uname_returns_something() {
        let result = uname().await;
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn run_command_echo() {
        let output = run_command("echo", &["hello"], Path::new("/tmp"))
            .await
            .unwrap();
        assert_eq!(output.trim(), "hello");
    }

    #[tokio::test]
    async fn run_command_failure() {
        let result = run_command("false", &[], Path::new("/tmp")).await;
        assert!(result.is_err());
    }

    #[test]
    fn format_bytes_gib() {
        // 65 GiB in bytes
        assert_eq!(format_bytes("69793218560"), "65 GiB");
    }

    #[test]
    fn format_bytes_mib() {
        // 512 MiB in bytes
        assert_eq!(format_bytes("536870912"), "512 MiB");
    }

    #[test]
    fn format_bytes_non_numeric() {
        assert_eq!(format_bytes("65Gi"), "65Gi");
    }
}
