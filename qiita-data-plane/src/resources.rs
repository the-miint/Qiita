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
//! None of the detectors is compiled conditionally: each one asks for something
//! the other platform does not have, so the ordering alone decides which answers.
//!
//! The compute orchestrator sizes its own DuckDB sessions from a cgroup too
//! (`qiita_compute_orchestrator.miint`), and takes the tightest limit across the
//! ancestor chain for the same reason. The two converge on that and on nothing
//! else: it collects v2 and v1 files together and minimises over both, where this
//! reads v2 and falls to v1 only when the unified line names no path; it reads the
//! mount root even when the process's own path is `/`, where `cgroup_v2_path`
//! declines; it treats a v1 value at or above the host total as unlimited, where
//! this has a fixed sentinel (`CGROUP_V1_UNLIMITED_ABOVE`); and its last resort is
//! the host's physical total, where this takes what is currently available —
//! because a job is sized before it runs and this pool is sized at boot. Neither
//! language can import the other, so a change to one precedence is a reason to
//! read the other rather than a shared constant.

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
    let own_path = cgroup_v2_path(&read_self_cgroup()?)?;
    tightest_limit(
        &own_path,
        "/sys/fs/cgroup",
        "memory.max",
        parse_cgroup_v2_max,
    )
}

fn read_cgroup_v1() -> Option<u64> {
    let own_path = cgroup_v1_memory_path(&read_self_cgroup()?)?;
    tightest_limit(
        &own_path,
        "/sys/fs/cgroup/memory",
        "memory.limit_in_bytes",
        parse_cgroup_v1_limit,
    )
}

fn read_proc_meminfo() -> Option<u64> {
    parse_meminfo_available(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// The machine's total, not an available share: macOS has no `MemAvailable`
/// equivalent, and this is a development path. On Linux the `hw.memsize` key does
/// not exist — `sysctl` itself may — and `/proc/meminfo` above has already
/// answered anyway.
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

fn read_self_cgroup() -> Option<String> {
    std::fs::read_to_string("/proc/self/cgroup").ok()
}

/// The tightest limit across the process's own cgroup and every ancestor up to
/// the mount root, which is not always its own: a slice above it can be the one
/// that caps memory, and under a systemd `MemoryMax=` on a parent slice the
/// process's own file reads unlimited. A file that is missing, unreadable or
/// unlimited is skipped rather than ending the walk, so detection steps down to
/// the next source only when no cgroup in the chain has a limit — which also
/// means an unreadable own file and an absent one are not told apart, and an
/// ancestor's looser limit answers where a step down to `MemAvailable` would
/// have.
fn tightest_limit(
    own_path: &str,
    mount: &str,
    file: &str,
    parse: impl Fn(&str) -> Option<u64>,
) -> Option<u64> {
    let mut tightest: Option<u64> = None;
    let mut path = own_path.to_string();
    loop {
        let candidate = std::fs::read_to_string(format!("{mount}{path}/{file}"))
            .ok()
            .and_then(|content| parse(&content));
        if let Some(value) = candidate {
            tightest = Some(tightest.map_or(value, |seen: u64| seen.min(value)));
        }
        if path.is_empty() {
            break;
        }
        // "/a/b" -> "/a" -> "" , and "" is the mount root, read once before the
        // loop ends.
        match path.rfind('/') {
            Some(cut) => path.truncate(cut),
            None => break,
        }
    }
    tightest
}

/// The path field of the `/proc/self/cgroup` line that `wanted` selects, where
/// `wanted` is given the line's hierarchy id and its comma-separated controller
/// list.
fn cgroup_path(proc_self_cgroup: &str, wanted: impl Fn(&str, &str) -> bool) -> Option<String> {
    for line in proc_self_cgroup.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() == 3 && wanted(parts[0], parts[1]) {
            return Some(parts[2].to_string());
        }
    }
    None
}

/// The process's own v2 cgroup, from the single `0::<path>` line. A process in
/// the root cgroup has no limit of its own, so `/` declines.
fn cgroup_v2_path(proc_self_cgroup: &str) -> Option<String> {
    let path = cgroup_path(proc_self_cgroup, |id, controllers| {
        id == "0" && controllers.is_empty()
    })?;
    (!path.is_empty() && path != "/").then_some(path)
}

/// The process's path under the v1 `memory` controller.
fn cgroup_v1_memory_path(proc_self_cgroup: &str) -> Option<String> {
    cgroup_path(proc_self_cgroup, |_, controllers| {
        controllers.split(',').any(|c| c == "memory")
    })
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
/// which counts what the host's other services already hold.
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

    /// A fake cgroup tree: `mount/<path>/<file>` for each entry.
    fn cgroup_tree(entries: &[(&str, &str)]) -> tempfile::TempDir {
        cgroup_tree_of("memory.max", entries)
    }

    fn cgroup_tree_of(file: &str, entries: &[(&str, &str)]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for (path, value) in entries {
            let dir = root
                .path()
                .join("cgroup")
                .join(path.trim_start_matches('/'));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(file), value).unwrap();
        }
        root
    }

    #[test]
    fn the_walk_takes_a_tighter_ancestor_over_the_process_s_own() {
        // The unit's own file says `max`; the slice above it is the real ceiling.
        let root = cgroup_tree(&[("slice", "4294967296\n"), ("slice/unit", "max\n")]);
        let mount = root.path().join("cgroup");
        assert_eq!(
            tightest_limit(
                "/slice/unit",
                mount.to_str().unwrap(),
                "memory.max",
                parse_cgroup_v2_max
            ),
            Some(4 * 1024 * 1024 * 1024),
            "a MemoryMax= on a parent slice is the limit this process runs under"
        );
    }

    #[test]
    fn the_walk_keeps_the_leaf_when_it_is_the_tighter_one() {
        let root = cgroup_tree(&[("slice", "8589934592\n"), ("slice/unit", "2147483648\n")]);
        let mount = root.path().join("cgroup");
        assert_eq!(
            tightest_limit(
                "/slice/unit",
                mount.to_str().unwrap(),
                "memory.max",
                parse_cgroup_v2_max
            ),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn the_walk_declines_when_no_cgroup_in_the_chain_has_a_limit() {
        let root = cgroup_tree(&[("slice/unit", "max\n")]);
        let mount = root.path().join("cgroup");
        assert_eq!(
            tightest_limit(
                "/slice/unit",
                mount.to_str().unwrap(),
                "memory.max",
                parse_cgroup_v2_max
            ),
            None,
            "an unlimited chain steps down to the next detector"
        );
    }

    #[test]
    fn the_walk_reads_the_v1_filename_and_its_unlimited_sentinel() {
        // v1 spells unlimited as a number above any real host, and the walk starts
        // at the mount root for a process whose own path is `/` — which is where
        // the v1 reader differs from the v2 one.
        let root = cgroup_tree_of(
            "memory.limit_in_bytes",
            &[
                (
                    "",
                    "9223372036854771712
",
                ),
                (
                    "slice",
                    "2147483648
",
                ),
                (
                    "slice/unit",
                    "9223372036854771712
",
                ),
            ],
        );
        let mount = root.path().join("cgroup");
        assert_eq!(
            tightest_limit(
                "/slice/unit",
                mount.to_str().unwrap(),
                "memory.limit_in_bytes",
                parse_cgroup_v1_limit
            ),
            Some(2 * 1024 * 1024 * 1024),
            "the sentinel is skipped at both ends and the slice's real limit answers"
        );
        assert_eq!(
            tightest_limit(
                "",
                mount.to_str().unwrap(),
                "memory.limit_in_bytes",
                parse_cgroup_v1_limit
            ),
            None,
            "a process at the mount root sees only the sentinel there"
        );
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
