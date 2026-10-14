//! The memory and CPU ceiling this process divides across its DuckLake pool.
//!
//! Nothing here is read by DuckDB. DuckDB sizes every database it opens from
//! the process's own cgroup — `memory_limit` at about 80% of it, `threads` from
//! `cpu.max` — so without a ceiling of our own, each concurrently open database
//! sizes itself against the whole host and N of them authorise N times the host.
//! The pool needs one number to divide instead.
//!
//! The precedence is the one `the-miint/RYpe` uses in `src/memory.rs`
//! (`detect_available_memory`), minus its SLURM branch: this process runs as a
//! systemd unit, so `scontrol show job` has no job to describe. Each detector
//! names itself so boot can log which one answered — the value alone does not
//! say whether it came from a cgroup or from a fallback.
//!
//! None of the detectors is compiled conditionally. Each one reads a path or
//! runs a tool that is simply absent on the other platform — `/proc` on macOS,
//! `hw.memsize` on Linux — so the ordering alone decides which can answer.

/// Where a detected memory ceiling came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySource {
    CgroupV2,
    CgroupV1,
    ProcMeminfo,
    MacOsSysctl,
    Fallback,
}

impl MemorySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CgroupV2 => "cgroup v2 memory.max",
            Self::CgroupV1 => "cgroup v1 memory.limit_in_bytes",
            Self::ProcMeminfo => "/proc/meminfo MemAvailable",
            Self::MacOsSysctl => "sysctl hw.memsize",
            Self::Fallback => "fallback default",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MemoryCeiling {
    pub bytes: u64,
    pub source: MemorySource,
}

/// Consulted only when every detector declines, which on Linux means no cgroup
/// limit and an unreadable `/proc/meminfo`. Small so that a pool sized from it
/// cannot exhaust a host whose size was never established.
const FALLBACK_CEILING_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// cgroup v1 writes a sentinel near `u64::MAX` for "no limit"; anything past a
/// terabyte is that sentinel rather than a configured ceiling.
const CGROUP_V1_UNLIMITED_ABOVE: u64 = 1024 * 1024 * 1024 * 1024;

pub fn detect_memory_ceiling() -> MemoryCeiling {
    let found = read_cgroup_v2()
        .map(|bytes| (bytes, MemorySource::CgroupV2))
        .or_else(|| read_cgroup_v1().map(|bytes| (bytes, MemorySource::CgroupV1)))
        .or_else(|| read_proc_meminfo().map(|bytes| (bytes, MemorySource::ProcMeminfo)))
        .or_else(|| read_macos_memsize().map(|bytes| (bytes, MemorySource::MacOsSysctl)));
    match found {
        Some((bytes, source)) => MemoryCeiling { bytes, source },
        None => MemoryCeiling {
            bytes: FALLBACK_CEILING_BYTES,
            source: MemorySource::Fallback,
        },
    }
}

/// CPU budget to divide across the pool. `available_parallelism` is what the
/// standard library will tell us about this process's own CPU allowance; it
/// yields 1 rather than an error so a pool can always be built.
pub fn detect_thread_budget() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn read_cgroup_v2() -> Option<u64> {
    let own_path = cgroup_v2_path(&std::fs::read_to_string("/proc/self/cgroup").ok()?)?;
    let content = std::fs::read_to_string(format!("/sys/fs/cgroup{own_path}/memory.max")).ok()?;
    parse_cgroup_v2_max(&content)
}

fn read_cgroup_v1() -> Option<u64> {
    let own_path = cgroup_v1_memory_path(&std::fs::read_to_string("/proc/self/cgroup").ok()?)?;
    let content = std::fs::read_to_string(format!(
        "/sys/fs/cgroup/memory{own_path}/memory.limit_in_bytes"
    ))
    .ok()?;
    parse_cgroup_v1_limit(&content)
}

fn read_proc_meminfo() -> Option<u64> {
    parse_meminfo_available(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

fn read_macos_memsize() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// The process's own v2 cgroup, from the single `0::<path>` line. A process in
/// the root cgroup has no limit of its own, so `/` declines.
fn cgroup_v2_path(proc_self_cgroup: &str) -> Option<String> {
    for line in proc_self_cgroup.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() == 3 && parts[0] == "0" && parts[1].is_empty() {
            let path = parts[2];
            return (!path.is_empty() && path != "/").then(|| path.to_string());
        }
    }
    None
}

/// The process's path under the v1 `memory` controller, from the line whose
/// middle field is that controller.
fn cgroup_v1_memory_path(proc_self_cgroup: &str) -> Option<String> {
    for line in proc_self_cgroup.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() == 3 && parts[1].split(',').any(|c| c == "memory") {
            return Some(parts[2].to_string());
        }
    }
    None
}

fn parse_cgroup_v2_max(content: &str) -> Option<u64> {
    let trimmed = content.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse().ok()
}

fn parse_cgroup_v1_limit(content: &str) -> Option<u64> {
    let value: u64 = content.trim().parse().ok()?;
    (value <= CGROUP_V1_UNLIMITED_ABOVE).then_some(value)
}

/// `MemAvailable`, in kB, is what the kernel expects to be usable without
/// swapping — the share a new pool may plan against, rather than `MemTotal`
/// which counts what the host's other services already hold. (The macOS
/// detector below has no equivalent and returns the machine's total; it is a
/// development path, never the deploy host.)
fn parse_meminfo_available(content: &str) -> Option<u64> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const V2_CGROUP: &str = "0::/system.slice/qiita-data-plane@50051.service\n";
    const V1_CGROUP: &str = "11:pids:/system.slice/x.service\n\
                             6:memory:/slurm/uid_1156392/job_3532212/step_0\n\
                             0::/system.slice/x.service\n";

    #[test]
    fn cgroup_v2_path_reads_the_unified_line() {
        assert_eq!(
            cgroup_v2_path(V2_CGROUP).as_deref(),
            Some("/system.slice/qiita-data-plane@50051.service")
        );
    }

    #[test]
    fn cgroup_v2_path_declines_the_root_cgroup() {
        assert_eq!(cgroup_v2_path("0::/\n"), None);
        assert_eq!(cgroup_v2_path("0::\n"), None);
    }

    #[test]
    fn cgroup_v1_path_reads_the_memory_controller_line() {
        assert_eq!(
            cgroup_v1_memory_path(V1_CGROUP).as_deref(),
            Some("/slurm/uid_1156392/job_3532212/step_0")
        );
    }

    #[test]
    fn cgroup_v1_path_matches_a_comounted_controller() {
        assert_eq!(
            cgroup_v1_memory_path("4:cpu,memory:/foo\n").as_deref(),
            Some("/foo")
        );
    }

    #[test]
    fn cgroup_v2_max_declines_when_unlimited() {
        assert_eq!(parse_cgroup_v2_max("max\n"), None);
        assert_eq!(
            parse_cgroup_v2_max("4294967296\n"),
            Some(4 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn cgroup_v1_limit_declines_the_unlimited_sentinel() {
        assert_eq!(parse_cgroup_v1_limit("9223372036854771712\n"), None);
        assert_eq!(
            parse_cgroup_v1_limit("4294967296\n"),
            Some(4 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn meminfo_reads_mem_available_not_mem_total() {
        let content = "MemTotal:       394718208 kB\n\
                       MemFree:         90000000 kB\n\
                       MemAvailable:   351653888 kB\n";
        assert_eq!(
            parse_meminfo_available(content),
            Some(351_653_888 * 1024),
            "MemAvailable, not MemTotal"
        );
    }

    #[test]
    fn meminfo_declines_when_the_field_is_absent() {
        assert_eq!(parse_meminfo_available("MemTotal: 1 kB\n"), None);
    }

    #[test]
    fn detection_always_yields_a_usable_ceiling_and_names_its_source() {
        let ceiling = detect_memory_ceiling();
        assert!(
            ceiling.bytes > 0,
            "a ceiling of 0 would divide into nothing"
        );
        assert!(!ceiling.source.as_str().is_empty());
        assert!(detect_thread_budget() >= 1);
    }
}
