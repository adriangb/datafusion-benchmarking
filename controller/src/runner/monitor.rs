//! Resource monitoring via cgroup v2 files.
//!
//! Captures memory, CPU, and I/O stats during benchmark execution by reading
//! cgroup v2 files at `/sys/fs/cgroup/`. When cgroup files are unavailable
//! (e.g. local macOS development), all reads return `None` and stats default
//! to zero.

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

/// Monitors cgroup v2 resource usage during benchmark execution.
pub struct CgroupMonitor {
    start_time: Instant,
    start_memory: u64,
    start_cpu: Option<CpuStat>,
    stop_flag: Arc<AtomicBool>,
    peak_memory: Arc<AtomicU64>,
    memory_sum: Arc<AtomicU64>,
    peak_spill: Arc<AtomicU64>,
    sample_count: Arc<AtomicU64>,
    poll_handle: JoinHandle<()>,
}

impl CgroupMonitor {
    /// Begin monitoring. Snapshots current cgroup values as baselines and
    /// spawns a background polling task for memory and spill directory tracking.
    ///
    /// If `spill_dir` is provided, the polling loop will also sample the total
    /// size of files in that directory every second to track peak spill usage.
    pub fn start(spill_dir: Option<PathBuf>) -> Self {
        let start_memory = read_memory_current().unwrap_or(0);
        let start_cpu = read_cpu_stat();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let peak_memory = Arc::new(AtomicU64::new(start_memory));
        let memory_sum = Arc::new(AtomicU64::new(start_memory));
        let peak_spill = Arc::new(AtomicU64::new(0));
        let sample_count = Arc::new(AtomicU64::new(1));

        let poll_handle = {
            let stop = stop_flag.clone();
            let peak = peak_memory.clone();
            let sum = memory_sum.clone();
            let spill_peak = peak_spill.clone();
            let count = sample_count.clone();

            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Some(current) = read_memory_current() {
                        peak.fetch_max(current, Ordering::Relaxed);
                        sum.fetch_add(current, Ordering::Relaxed);
                        count.fetch_add(1, Ordering::Relaxed);
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
            stop_flag,
            peak_memory,
            memory_sum,
            peak_spill,
            sample_count,
            poll_handle,
        }
    }

    /// Stop monitoring and compute delta statistics.
    pub async fn finish(self) -> ResourceStats {
        let wall_time = self.start_time.elapsed();

        self.stop_flag.store(true, Ordering::Relaxed);
        let _ = self.poll_handle.await;

        let end_memory = read_memory_current().unwrap_or(0);
        let end_cpu = read_cpu_stat();

        let peak = self.peak_memory.load(Ordering::Relaxed).max(end_memory);
        let total_sum = self.memory_sum.load(Ordering::Relaxed) + end_memory;
        let total_count = self.sample_count.load(Ordering::Relaxed) + 1;
        let avg = total_sum.checked_div(total_count).unwrap_or(0);

        let (cpu_user, cpu_sys) = match (self.start_cpu, end_cpu) {
            (Some(start), Some(end)) => (
                end.user_usec.saturating_sub(start.user_usec),
                end.system_usec.saturating_sub(start.system_usec),
            ),
            _ => (0, 0),
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
        let monitor = CgroupMonitor::start(None);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stats = monitor.finish().await;
        assert!(stats.wall_time >= Duration::from_millis(50));
        assert!(stats.sample_count >= 2);
    }
}
