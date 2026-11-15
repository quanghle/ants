//! Memory pressure: the system memory available, read on the pool's idle check, and whether it runs low.
//!
//! Each harness process takes a few hundred MB. While available memory is below [`Settings::min_available`] the pool
//! stops its idle harnesses at once and runs at most [`Settings::max_harnesses`] ([`crate::pool::Pool`]); it runs its
//! full cap again only once available memory is back above [`RECOVER_PERCENT`] of the threshold, so that memory
//! hovering around the threshold does not flip it at every check.
//!
//! Available memory is what the system can hand out without swapping: `MemAvailable` of `/proc/meminfo` on Linux,
//! `kern.memorystatus_level` (the share of memory available, which memory pressure is judged by) of `hw.memsize` on
//! macOS, and `GlobalMemoryStatusEx`'s available physical memory on Windows. Each is a read, never a process. Elsewhere
//! it is unknown, and memory is never low.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};

use crate::storage::{parse_size, show_size};

/// Available memory, as a share of the threshold, above which low memory has recovered.
pub const RECOVER_PERCENT: u64 = 150;

/// The `[memory_pressure]` table of the config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Settings {
    /// Available memory below which memory is low; 0 for never.
    #[serde(deserialize_with = "threshold")]
    pub min_available: Threshold,
    /// The most harnesses each ant runs at once while memory is low, at least 1.
    pub max_harnesses: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self { min_available: Threshold::Bytes(1 << 30), max_harnesses: 1 }
    }
}

impl Settings {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.max_harnesses == 0 {
            anyhow::bail!("[memory_pressure] max_harnesses must be at least 1");
        }
        Ok(())
    }

    pub fn is_off(&self) -> bool {
        matches!(self.min_available, Threshold::Bytes(0) | Threshold::Percent(0))
    }
}

/// How little available memory is low: a size, or a share of the system's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Threshold {
    Bytes(u64),
    Percent(u8),
}

impl Threshold {
    /// In bytes, of a system with `total` bytes of memory.
    pub fn of(self, total: u64) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Percent(percent) => share(total, u64::from(percent)),
        }
    }

    /// A size as [`parse_size`] reads it, or a whole percentage below 100 such as `10%`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let Some(percent) = text.strip_suffix('%') else { return parse_size(text).map(Self::Bytes) };
        match percent.trim().parse::<u8>() {
            Ok(percent) if percent < 100 => Ok(Self::Percent(percent)),
            _ => Err(format!("invalid share {text:?} (a whole percentage below 100, such as 10%)")),
        }
    }
}

/// `percent` percent of `total`.
fn share(total: u64, percent: u64) -> u64 {
    u64::try_from(u128::from(total) * u128::from(percent) / 100).unwrap_or(u64::MAX)
}

fn threshold<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Threshold, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Text {
        Bytes(u64),
        Text(String),
    }
    match Text::deserialize(deserializer)? {
        Text::Bytes(bytes) => Ok(Threshold::Bytes(bytes)),
        Text::Text(text) => Threshold::parse(&text).map_err(serde::de::Error::custom),
    }
}

/// The system's memory, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub available: u64,
    pub total: u64,
}

/// Reads the system's memory; `None` when it cannot be read.
pub type Probe = Arc<dyn Fn() -> Option<Memory> + Send + Sync>;

/// The probe of this system's memory ([`system`]).
pub fn system_probe() -> Probe {
    Arc::new(system)
}

/// Low memory, as last checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Low {
    /// When available memory fell below the threshold.
    pub since: DateTime<Utc>,
    /// Bytes available at the latest check.
    pub available: u64,
}

/// Whether memory is low, judged at each [`Gauge::check`].
pub struct Gauge {
    settings: Settings,
    probe: Probe,
    low: Option<Low>,
}

impl Gauge {
    pub fn new(settings: Settings, probe: Probe) -> Self {
        Self { settings, probe, low: None }
    }

    /// Read the memory: low once available memory falls below the threshold, until it is back above
    /// [`RECOVER_PERCENT`] of it. Memory that cannot be read is not low. Logs each change, with `cap` the harnesses run
    /// at most while memory is low.
    pub fn check(&mut self, now: DateTime<Utc>, cap: usize) -> Option<Low> {
        let memory = (self.probe)();
        if let Some(memory) = memory {
            crate::metrics::memory_available(memory.available);
        }
        let threshold = memory.map_or(0, |m| self.settings.min_available.of(m.total));
        let (Some(memory), 1..) = (memory, threshold) else {
            if self.low.take().is_some() {
                tracing::info!("memory can no longer be read; running harnesses as usual");
            }
            return None;
        };
        let available = memory.available;
        match &mut self.low {
            Some(_) if available >= threshold.saturating_mul(RECOVER_PERCENT) / 100 => {
                self.low = None;
                tracing::info!(available = show_size(available), "memory recovered; running harnesses as usual");
            }
            Some(low) => low.available = available,
            None if available < threshold => {
                self.low = Some(Low { since: now, available });
                tracing::warn!(
                    available = show_size(available),
                    min_available = show_size(threshold),
                    max_harnesses = cap,
                    "memory low; stopping idle harnesses at once and running fewer until it recovers"
                );
            }
            None => {}
        }
        self.low
    }
}

/// The memory of this system, if it can be read.
#[cfg(target_os = "linux")]
pub fn system() -> Option<Memory> {
    meminfo(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// The memory `/proc/meminfo` tells of: its `MemAvailable` and `MemTotal`, in kB.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn meminfo(text: &str) -> Option<Memory> {
    let field = |name: &str| {
        let line = text.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix(':'))?;
        let kb: u64 = line.trim().strip_suffix("kB")?.trim().parse().ok()?;
        kb.checked_mul(1024)
    };
    Some(Memory { available: field("MemAvailable")?, total: field("MemTotal")? })
}

/// The memory of this system, if it can be read.
#[cfg(target_os = "macos")]
pub fn system() -> Option<Memory> {
    fn sysctl<T: Copy + Default>(name: &std::ffi::CStr) -> Option<T> {
        let mut value = T::default();
        let mut len = size_of::<T>();
        // SAFETY: `name` is NUL-terminated and `value` is a writable `T` of `len` bytes, the most sysctlbyname writes.
        let status = unsafe {
            libc::sysctlbyname(name.as_ptr(), (&raw mut value).cast(), &raw mut len, std::ptr::null_mut(), 0)
        };
        (status == 0 && len == size_of::<T>()).then_some(value)
    }
    let total: u64 = sysctl(c"hw.memsize")?;
    let percent: libc::c_int = sysctl(c"kern.memorystatus_level")?;
    let percent = u64::try_from(percent).ok().filter(|p| *p <= 100)?;
    Some(Memory { available: share(total, percent), total })
}

/// The memory of this system, if it can be read.
#[cfg(windows)]
pub fn system() -> Option<Memory> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    // SAFETY: MEMORYSTATUSEX is plain data, valid zeroed.
    let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    status.dwLength = u32::try_from(size_of::<MEMORYSTATUSEX>()).ok()?;
    // SAFETY: `status` is a writable MEMORYSTATUSEX with its length set, as GlobalMemoryStatusEx requires.
    if unsafe { GlobalMemoryStatusEx(&raw mut status) } == 0 {
        return None;
    }
    Some(Memory { available: status.ullAvailPhys, total: status.ullTotalPhys })
}

/// The memory of this system, if it can be read: never here.
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn system() -> Option<Memory> {
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::sync::MutexExt as _;

    #[test]
    fn thresholds_read_as_sizes_or_shares() {
        assert_eq!(Threshold::parse("1GB"), Ok(Threshold::Bytes(1 << 30)));
        assert_eq!(Threshold::parse("0"), Ok(Threshold::Bytes(0)));
        assert_eq!(Threshold::parse("10%"), Ok(Threshold::Percent(10)));
        for bad in ["100%", "-1%", "x%", "1 parsec"] {
            assert!(Threshold::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(Threshold::Percent(10).of(16 << 30), (16 << 30) / 10);
        let read = |text: &str| toml::from_str::<Settings>(text);
        assert_eq!(read("").unwrap(), Settings::default());
        assert_eq!(read("min_available = 1024").unwrap().min_available, Threshold::Bytes(1024));
        let shared = read("min_available = \"5%\"\nmax_harnesses = 2").unwrap();
        assert_eq!((shared.min_available, shared.max_harnesses), (Threshold::Percent(5), 2));
        assert!(read("min_available = \"0\"").unwrap().is_off());
        assert!(read("max_harnesses = 0").unwrap().validate().is_err());
        assert!(read("min_free = \"1GB\"").is_err());
    }

    #[test]
    fn meminfo_tells_available_and_total_memory() {
        let text = "MemTotal:       16303428 kB\nMemFree:          512000 kB\nMemAvailable:    8151714 kB\n";
        assert_eq!(meminfo(text), Some(Memory { available: 8_151_714 * 1024, total: 16_303_428 * 1024 }));
        assert_eq!(meminfo("MemTotal: 1 kB\n"), None, "a kernel without MemAvailable");
    }

    #[test]
    fn this_system_s_memory_reads_where_supported() {
        if cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            let memory = system().expect("memory reads");
            assert!(memory.total > 0 && memory.available <= memory.total, "{memory:?}");
        }
    }

    #[test]
    fn memory_turns_low_below_the_threshold_and_recovers_well_above_it() {
        let available = Arc::new(Mutex::new(Some(4096)));
        let read = available.clone();
        let probe: Probe = Arc::new(move || read.locked().map(|available| Memory { available, total: 8192 }));
        let mut gauge = Gauge::new(Settings { min_available: Threshold::Bytes(1000), max_harnesses: 1 }, probe);
        let mut at = |bytes: Option<u64>, minute: i64| {
            *available.locked() = bytes;
            gauge.check(DateTime::UNIX_EPOCH + chrono::Duration::minutes(minute), 1)
        };
        assert_eq!(at(Some(1000), 0), None);
        let since = DateTime::UNIX_EPOCH + chrono::Duration::minutes(1);
        assert_eq!(at(Some(999), 1), Some(Low { since, available: 999 }));
        assert_eq!(at(Some(1400), 2), Some(Low { since, available: 1400 }), "not yet recovered");
        assert_eq!(at(Some(1500), 3), None);
        assert_eq!(at(Some(1200), 4), None, "low again only below the threshold");
        assert!(at(Some(10), 5).is_some());
        assert_eq!(at(None, 6), None, "memory that cannot be read is not low");
    }
}
