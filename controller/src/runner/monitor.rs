//! Resource monitoring for benchmark execution.
//!
//! Memory is sampled from the benchmark command's **process subtree** by
//! walking `/proc/<pid>/status` (summing `VmRSS`) and excluding compiler /
//! build-tool processes (`rustc`, `cargo`, `cc`, `ld`, `sccache`, …). That is
//! what we report as the benchmark's memory. Reading the pod-wide cgroup
//! (`/sys/fs/cgroup/memory.current`) instead would also count page cache,
//! leftover build artifacts, and — for benchmarks whose `bench.sh run` step
//! compiles (e.g. `cargo bench`) — the compiler itself, which can dwarf the
//! actual query memory by tens of GB.
//!
//! CPU stats still come from cgroup v2 `cpu.stat`. When the per-process root
//! pid is unknown the sampler falls back to the cgroup memory figure. When
//! neither `/proc` nor the cgroup files are available (e.g. local macOS
//! development), reads return `None` and stats default to zero.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

const CGROUP_PATH: &str = "/sys/fs/cgroup";
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Resource usage statistics captured during a benchmark run.
#[derive(Debug, Clone, Default)]
pub struct ResourceStats {
    pub wall_time: Duration,
    pub peak_memory_bytes: u64,
    pub start_memory_bytes: u64,
    pub end_memory_bytes: u64,
    pub avg_memory_bytes: u64,
    pub cpu_user_usec: u64,
    pub cpu_sys_usec: u64,
    pub peak_spill_bytes: u64,
    pub sample_count: u32,
}

impl fmt::Display for ResourceStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "| Metric | Value |")?;
        writeln!(f, "|--------|-------|")?;
        writeln!(f, "| Wall time | {:.1}s |", self.wall_time.as_secs_f64())?;
        writeln!(
            f,
            "| Peak memory | {} |",
            format_bytes(self.peak_memory_bytes)
        )?;
        writeln!(
            f,
            "| Avg memory | {} |",
            format_bytes(self.avg_memory_bytes)
        )?;
        writeln!(
            f,
            "| CPU user | {:.1}s |",
            self.cpu_user_usec as f64 / 1_000_000.0
        )?;
        writeln!(
            f,
            "| CPU sys | {:.1}s |",
            self.cpu_sys_usec as f64 / 1_000_000.0
        )?;
        write!(
            f,
            "| Peak spill | {} |",
            format_bytes(self.peak_spill_bytes)
        )
    }
}

/// Format a byte count as a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.1} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Format a resource stats section for inclusion in a PR comment.
pub fn format_resource_comment(label: &str, stats: &ResourceStats) -> String {
    format!("**{label}**\n{stats}\n")
}

#[derive(Debug)]
struct CpuStat {
    user_usec: u64,
    system_usec: u64,
}

/// Monitors resource usage during benchmark execution.
pub struct CgroupMonitor {
    start_time: Instant,
    start_memory: u64,
    start_cpu: Option<CpuStat>,
    root_pid: Option<u32>,
    stop_flag: Arc<AtomicBool>,
    peak_memory: Arc<AtomicU64>,
    memory_sum: Arc<AtomicU64>,
    peak_spill: Arc<AtomicU64>,
    sample_count: Arc<AtomicU64>,
    // Per-process CPU consumed by the benchmark subtree (excluding build
    // tools), accumulated in the poll loop. Only meaningful when `root_pid` is
    // set; otherwise CPU is taken from the cgroup delta in `finish`.
    cpu_user_usec: Arc<AtomicU64>,
    cpu_sys_usec: Arc<AtomicU64>,
    poll_handle: JoinHandle<()>,
}

impl CgroupMonitor {
    /// Begin monitoring. `root_pid` is the spawned benchmark command's pid;
    /// both memory and CPU are sampled from its process subtree, excluding
    /// compiler/build-tool processes — so a `cargo bench` compile inside the
    /// monitored window is not attributed to the benchmark. When `root_pid` is
    /// `None`, falls back to the pod-wide cgroup figures.
    ///
    /// If `spill_dir` is provided, the polling loop will also sample the total
    /// size of files in that directory every second to track peak spill usage.
    pub fn start(root_pid: Option<u32>, spill_dir: Option<PathBuf>) -> Self {
        let start_memory = sample_memory(root_pid).unwrap_or(0);
        let start_cpu = read_cpu_stat();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let peak_memory = Arc::new(AtomicU64::new(start_memory));
        let memory_sum = Arc::new(AtomicU64::new(start_memory));
        let peak_spill = Arc::new(AtomicU64::new(0));
        let sample_count = Arc::new(AtomicU64::new(1));
        let cpu_user_usec = Arc::new(AtomicU64::new(0));
        let cpu_sys_usec = Arc::new(AtomicU64::new(0));

        let poll_handle = {
            let stop = stop_flag.clone();
            let peak = peak_memory.clone();
            let sum = memory_sum.clone();
            let spill_peak = peak_spill.clone();
            let count = sample_count.clone();
            let cpu_user = cpu_user_usec.clone();
            let cpu_sys = cpu_sys_usec.clone();

            tokio::spawn(async move {
                // Per-pid first/last cumulative CPU ticks, so each benchmark
                // process contributes only the CPU it burned during the window
                // (handles process churn as the bench binary starts/exits).
                let mut cpu_first: std::collections::HashMap<u32, (u64, u64)> =
                    std::collections::HashMap::new();
                let mut cpu_last: std::collections::HashMap<u32, (u64, u64)> =
                    std::collections::HashMap::new();

                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Some((rss, cpus)) = proc_tree_sample(root_pid) {
                        peak.fetch_max(rss, Ordering::Relaxed);
                        sum.fetch_add(rss, Ordering::Relaxed);
                        count.fetch_add(1, Ordering::Relaxed);

                        for (pid, utime, stime) in cpus {
                            cpu_first.entry(pid).or_insert((utime, stime));
                            cpu_last.insert(pid, (utime, stime));
                        }
                        // Recompute accumulated subtree CPU each poll.
                        let mut tu = 0u64;
                        let mut ts = 0u64;
                        for (pid, &(fu, fs)) in &cpu_first {
                            if let Some(&(lu, ls)) = cpu_last.get(pid) {
                                tu += lu.saturating_sub(fu);
                                ts += ls.saturating_sub(fs);
                            }
                        }
                        cpu_user.store(ticks_to_usec(tu), Ordering::Relaxed);
                        cpu_sys.store(ticks_to_usec(ts), Ordering::Relaxed);
                    } else if root_pid.is_none() {
                        // Fallback path: keep memory sampling via cgroup.
                        if let Some(current) = read_memory_current() {
                            peak.fetch_max(current, Ordering::Relaxed);
                            sum.fetch_add(current, Ordering::Relaxed);
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if let Some(ref dir) = spill_dir {
                        let size = dir_size(dir);
                        spill_peak.fetch_max(size, Ordering::Relaxed);
                    }
                }
            })
        };

        CgroupMonitor {
            start_time: Instant::now(),
            start_memory,
            start_cpu,
            root_pid,
            stop_flag,
            peak_memory,
            memory_sum,
            peak_spill,
            sample_count,
            cpu_user_usec,
            cpu_sys_usec,
            poll_handle,
        }
    }

    /// Stop monitoring and compute statistics.
    pub async fn finish(self) -> ResourceStats {
        let wall_time = self.start_time.elapsed();

        self.stop_flag.store(true, Ordering::Relaxed);
        let _ = self.poll_handle.await;

        let end_memory = sample_memory(self.root_pid).unwrap_or(0);

        let peak = self.peak_memory.load(Ordering::Relaxed).max(end_memory);
        let total_sum = self.memory_sum.load(Ordering::Relaxed) + end_memory;
        let total_count = self.sample_count.load(Ordering::Relaxed) + 1;
        let avg = total_sum.checked_div(total_count).unwrap_or(0);

        // CPU: per-process subtree accumulation when we have a root pid (so the
        // compile is excluded), otherwise the pod-wide cgroup delta.
        let (cpu_user, cpu_sys) = if self.root_pid.is_some() {
            (
                self.cpu_user_usec.load(Ordering::Relaxed),
                self.cpu_sys_usec.load(Ordering::Relaxed),
            )
        } else {
            match (self.start_cpu, read_cpu_stat()) {
                (Some(start), Some(end)) => (
                    end.user_usec.saturating_sub(start.user_usec),
                    end.system_usec.saturating_sub(start.system_usec),
                ),
                _ => (0, 0),
            }
        };

        let peak_spill = self.peak_spill.load(Ordering::Relaxed);

        ResourceStats {
            wall_time,
            peak_memory_bytes: peak,
            start_memory_bytes: self.start_memory,
            end_memory_bytes: end_memory,
            avg_memory_bytes: avg,
            cpu_user_usec: cpu_user,
            cpu_sys_usec: cpu_sys,
            peak_spill_bytes: peak_spill,
            sample_count: total_count as u32,
        }
    }
}

/// Clock ticks (USER_HZ) to microseconds. Linux USER_HZ is 100 on all common
/// configurations, so a tick is 10 ms = 10_000 µs.
fn ticks_to_usec(ticks: u64) -> u64 {
    ticks.saturating_mul(10_000)
}

// --- per-process sampling ---

/// Compiler / build-tool process names (as they appear in `/proc/<pid>/status`
/// `Name:` / `/proc/<pid>/stat` comm, truncated to 15 chars) whose usage should
/// NOT be attributed to the benchmark. A `cargo bench` step spawns these as
/// children of the monitored command; counting them would report compiler
/// memory and CPU as benchmark memory and CPU.
const BUILD_TOOL_NAMES: &[&str] = &[
    "cargo", "rustc", "sccache", "cc", "cc1", "cc1plus", "gcc", "g++", "c++", "clang", "clang++",
    "ld", "ld.lld", "lld", "ld.gold", "ld.bfd", "ld.mold", "mold", "collect2", "as", "ar",
    "rustdoc",
];

fn is_build_tool(name: &str) -> bool {
    BUILD_TOOL_NAMES.contains(&name)
        || name.starts_with("build-script")
        || name.starts_with("build_script")
}

/// Per-process cumulative CPU sample: `(pid, utime_ticks, stime_ticks)`.
type ProcCpu = (u32, u64, u64);

/// One subtree snapshot: `(total VmRSS bytes, per-process CPU)`.
type SubtreeSample = (u64, Vec<ProcCpu>);

/// Sample current memory: the benchmark process subtree RSS when `root_pid` is
/// known, otherwise the pod-wide cgroup figure.
fn sample_memory(root_pid: Option<u32>) -> Option<u64> {
    match root_pid {
        Some(pid) => proc_tree_sample(Some(pid)).map(|(rss, _)| rss),
        None => read_memory_current(),
    }
}

/// One snapshot of the process subtree rooted at `root_pid`, excluding
/// compiler/build-tool processes:
///   - total `VmRSS` in bytes
///   - per-process cumulative CPU `(pid, utime_ticks, stime_ticks)`
///
/// Returns `None` if `/proc` can't be read at all (e.g. macOS), or if
/// `root_pid` is `None`. Returns `Some((0, []))` if the subtree is already gone.
fn proc_tree_sample(root_pid: Option<u32>) -> Option<SubtreeSample> {
    let root_pid = root_pid?;
    let entries = std::fs::read_dir("/proc").ok()?;

    // pid -> (ppid, name, rss_bytes, utime_ticks, stime_ticks)
    let mut procs: std::collections::HashMap<u32, ProcInfo> = std::collections::HashMap::new();
    for entry in entries.flatten() {
        let pid: u32 = match entry.file_name().to_str().and_then(|s| s.parse().ok()) {
            Some(p) => p,
            None => continue, // non-numeric /proc entry
        };
        if let Some(info) = read_proc_info(pid) {
            procs.insert(pid, info);
        }
    }

    // children index
    let mut children: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    for (&pid, info) in &procs {
        children.entry(info.ppid).or_default().push(pid);
    }

    // BFS from root, collecting non-build-tool processes.
    let mut rss_total: u64 = 0;
    let mut cpus: Vec<(u32, u64, u64)> = Vec::new();
    let mut stack = vec![root_pid];
    while let Some(pid) = stack.pop() {
        if let Some(info) = procs.get(&pid) {
            if !is_build_tool(&info.name) {
                rss_total += info.rss_bytes;
                cpus.push((pid, info.utime_ticks, info.stime_ticks));
            }
        }
        if let Some(kids) = children.get(&pid) {
            stack.extend(kids);
        }
    }
    Some((rss_total, cpus))
}

struct ProcInfo {
    ppid: u32,
    name: String,
    rss_bytes: u64,
    utime_ticks: u64,
    stime_ticks: u64,
}

/// Read process info from `/proc/<pid>/status` (ppid, name, VmRSS) and
/// `/proc/<pid>/stat` (utime, stime).
fn read_proc_info(pid: u32) -> Option<ProcInfo> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let mut ppid = None;
    let mut name = None;
    let mut rss_kb = 0u64;
    for line in status.lines() {
        if let Some(v) = line.strip_prefix("Name:") {
            name = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("PPid:") {
            ppid = v.trim().parse().ok();
        } else if let Some(v) = line.strip_prefix("VmRSS:") {
            // e.g. "VmRSS:   123456 kB"
            rss_kb = v
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        }
    }

    let (utime_ticks, stime_ticks) = read_proc_cpu_ticks(pid).unwrap_or((0, 0));

    Some(ProcInfo {
        ppid: ppid?,
        name: name?,
        rss_bytes: rss_kb * 1024,
        utime_ticks,
        stime_ticks,
    })
}

/// Parse cumulative `(utime, stime)` clock ticks from `/proc/<pid>/stat`.
/// The comm field (field 2) can contain spaces and parentheses, so split on the
/// last `)`; the remaining whitespace-separated fields start at field 3
/// (`state`), making utime field 14 → index 11 and stime field 15 → index 12.
fn read_proc_cpu_ticks(pid: u32) -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let utime = fields.get(11).and_then(|s| s.parse().ok())?;
    let stime = fields.get(12).and_then(|s| s.parse().ok())?;
    Some((utime, stime))
}

// --- cgroup v2 file readers ---

fn read_memory_current() -> Option<u64> {
    std::fs::read_to_string(format!("{CGROUP_PATH}/memory.current"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn read_cpu_stat() -> Option<CpuStat> {
    let content = std::fs::read_to_string(format!("{CGROUP_PATH}/cpu.stat")).ok()?;
    let mut user_usec = 0u64;
    let mut system_usec = 0u64;

    for line in content.lines() {
        let mut parts = line.split_whitespace();
        match parts.next()? {
            "user_usec" => user_usec = parts.next()?.parse().ok()?,
            "system_usec" => system_usec = parts.next()?.parse().ok()?,
            _ => {}
        }
    }

    Some(CpuStat {
        user_usec,
        system_usec,
    })
}

/// Recursively sum the sizes of all files in a directory.
/// Returns 0 if the directory does not exist or cannot be read.
fn dir_size(path: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        let entries = match std::fs::read_dir(path) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_file() {
                *total += meta.len();
            } else if meta.is_dir() {
                walk(&entry.path(), total);
            }
        }
    }

    let mut total = 0;
    walk(path, &mut total);
    total
}

#[cfg(test)]
fn parse_memory_current(content: &str) -> Option<u64> {
    content.trim().parse().ok()
}

#[cfg(test)]
fn parse_cpu_stat(content: &str) -> Option<CpuStat> {
    let mut user_usec = 0u64;
    let mut system_usec = 0u64;

    for line in content.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next() {
            if let Some(val) = parts.next() {
                match key {
                    "user_usec" => user_usec = val.parse().ok()?,
                    "system_usec" => system_usec = val.parse().ok()?,
                    _ => {}
                }
            }
        }
    }

    Some(CpuStat {
        user_usec,
        system_usec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_values() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(1048576), "1.0 MiB");
        assert_eq!(format_bytes(1073741824), "1.0 GiB");
        assert_eq!(format_bytes(1288490189), "1.2 GiB");
    }

    #[test]
    fn test_resource_stats_display() {
        let stats = ResourceStats {
            wall_time: Duration::from_secs_f64(42.3),
            peak_memory_bytes: 1288490189,
            start_memory_bytes: 100_000_000,
            end_memory_bytes: 200_000_000,
            avg_memory_bytes: 858993459,
            cpu_user_usec: 38_100_000,
            cpu_sys_usec: 2_400_000,
            peak_spill_bytes: 536_870_912,
            sample_count: 42,
        };
        let output = stats.to_string();
        assert!(output.contains("| Wall time | 42.3s |"));
        assert!(output.contains("| Peak memory | 1.2 GiB |"));
        assert!(output.contains("| Avg memory |"));
        assert!(output.contains("| CPU user | 38.1s |"));
        assert!(output.contains("| CPU sys | 2.4s |"));
        assert!(output.contains("| Peak spill | 512.0 MiB |"));
    }

    #[test]
    fn test_parse_memory_current() {
        assert_eq!(parse_memory_current("123456789\n"), Some(123456789));
        assert_eq!(parse_memory_current("0\n"), Some(0));
        assert_eq!(parse_memory_current("not_a_number\n"), None);
    }

    #[test]
    fn test_parse_cpu_stat() {
        let content = "\
usage_usec 100000
user_usec 80000
system_usec 20000
nr_periods 0
nr_throttled 0
throttled_usec 0
";
        let stat = parse_cpu_stat(content).unwrap();
        assert_eq!(stat.user_usec, 80000);
        assert_eq!(stat.system_usec, 20000);
    }

    #[test]
    fn test_dir_size() {
        let tmp = std::env::temp_dir().join("test_dir_size_monitor");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(tmp.join("sub")).unwrap();
        std::fs::write(tmp.join("a.bin"), vec![0u8; 1024]).unwrap();
        std::fs::write(tmp.join("sub/b.bin"), vec![0u8; 2048]).unwrap();
        assert_eq!(dir_size(&tmp), 3072);
        // Non-existent directory returns 0
        assert_eq!(dir_size(Path::new("/nonexistent/path")), 0);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_format_resource_comment() {
        let stats = ResourceStats {
            wall_time: Duration::from_secs(10),
            ..Default::default()
        };
        let comment = format_resource_comment("base (merge-base)", &stats);
        assert!(comment.contains("**base (merge-base)**"));
        assert!(comment.contains("| Wall time | 10.0s |"));
    }

    #[tokio::test]
    async fn test_monitor_returns_stats() {
        let monitor = CgroupMonitor::start(None, None);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stats = monitor.finish().await;
        assert!(stats.wall_time >= Duration::from_millis(50));
        assert!(stats.sample_count >= 2);
    }

    // Multi-threaded runtime so the background poll task can sample while this
    // task burns CPU; CPU is a delta between samples, so the process must be
    // sampled at least twice (poll interval is 1s) — hence the >2s spin.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_monitor_self_subtree_has_cpu_and_mem() {
        // Monitor our own process subtree; we should observe non-zero RSS and,
        // after doing some work, non-zero CPU — proving the /proc sampler runs.
        let pid = std::process::id();
        let monitor = CgroupMonitor::start(Some(pid), None);
        // Burn CPU so the process's utime advances across poll samples.
        let mut x: u64 = 0;
        let spin_until = Instant::now() + Duration::from_millis(2500);
        while Instant::now() < spin_until {
            x = x.wrapping_add(1);
            std::hint::black_box(x);
        }
        let stats = monitor.finish().await;
        // On non-/proc platforms (macOS) these are zero; only assert there.
        if std::path::Path::new("/proc/self/status").exists() {
            assert!(stats.peak_memory_bytes > 0, "expected non-zero RSS");
            // CPU needs >= 2 poll samples for a non-zero delta (the first only
            // establishes the per-process baseline). sample_count = start + polls
            // + end, so >= 3 means at least two polls landed.
            if stats.sample_count >= 3 {
                assert!(
                    stats.cpu_user_usec > 0,
                    "expected non-zero user CPU for the spinning process"
                );
            }
        }
    }

    #[test]
    fn test_is_build_tool() {
        assert!(is_build_tool("rustc"));
        assert!(is_build_tool("cargo"));
        assert!(is_build_tool("sccache"));
        assert!(is_build_tool("ld.lld"));
        assert!(is_build_tool("build-script-build"));
        assert!(is_build_tool("build_script_build"));
        assert!(!is_build_tool("dfbench"));
        assert!(!is_build_tool("sql-712c5e60f8")); // criterion bench binary
    }

    #[test]
    fn test_ticks_to_usec() {
        assert_eq!(ticks_to_usec(0), 0);
        assert_eq!(ticks_to_usec(1), 10_000); // 1 tick = 10ms = 10_000us
        assert_eq!(ticks_to_usec(100), 1_000_000); // 100 ticks = 1s
    }

    #[test]
    fn test_read_proc_cpu_ticks_self() {
        // Only meaningful on Linux; skip elsewhere.
        if std::path::Path::new("/proc/self/stat").exists() {
            let pid = std::process::id();
            let (u, s) = read_proc_cpu_ticks(pid).expect("read self cpu ticks");
            // Cumulative ticks are monotonic and fit in u64; just sanity bound.
            assert!(u < u64::MAX && s < u64::MAX);
        }
    }
}
