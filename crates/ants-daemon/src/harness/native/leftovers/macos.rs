//! The process table on macOS, read through libproc and sysctl.

use std::ffi::c_int;

use super::unix::Proc;

/// `SZOMB`, a zombie's `pbi_status`.
const ZOMBIE: u32 = 5;

/// Every process listed, zombies left out.
pub(super) fn table() -> Vec<Proc> {
    // SAFETY: with no buffer, proc_listallpids only counts the processes.
    let count = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    let Ok(count) = usize::try_from(count) else { return Vec::new() };
    // Room for the processes started since they were counted.
    let mut pids: Vec<libc::pid_t> = vec![0; count + 64];
    let size = c_int::try_from(std::mem::size_of_val(pids.as_slice())).unwrap_or(c_int::MAX);
    // SAFETY: the buffer holds `size` bytes.
    let listed = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), size) };
    pids.truncate(usize::try_from(listed).unwrap_or_default());
    pids.into_iter().filter(|&pid| pid > 0).filter_map(process).collect()
}

/// Process `pid`, unless gone or a zombie.
pub(super) fn process(pid: i32) -> Option<Proc> {
    // SAFETY: proc_bsdinfo is plain integers, for which zero is a value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
    // SAFETY: the buffer is a proc_bsdinfo of `size` bytes.
    let got = unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) };
    if got != size || info.pbi_status == ZOMBIE {
        return None;
    }
    Some(Proc {
        pid,
        ppid: i32::try_from(info.pbi_ppid).ok()?,
        pgid: i32::try_from(info.pbi_pgid).ok()?,
        uid: info.pbi_uid,
        start: info.pbi_start_tvsec.saturating_mul(1_000_000).saturating_add(info.pbi_start_tvusec),
    })
}

/// The arguments and environment variables a process started with.
struct Procargs<'a> {
    args: Vec<&'a [u8]>,
    env: Vec<&'a [u8]>,
}

/// The arguments and environment process `pid` started with, read into `buffer`: `KERN_PROCARGS2`'s argument count,
/// executable path, arguments and environment variables.
fn procargs(pid: i32, buffer: &mut Vec<u8>) -> Option<Procargs<'_>> {
    if buffer.is_empty() {
        let mut max: c_int = 0;
        let mut size = std::mem::size_of::<c_int>();
        let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
        // SAFETY: the output is one c_int.
        let read =
            unsafe { libc::sysctl(mib.as_mut_ptr(), 2, (&raw mut max).cast(), &raw mut size, std::ptr::null_mut(), 0) };
        buffer.resize(if read == 0 { usize::try_from(max).unwrap_or(1 << 20) } else { 1 << 20 }, 0);
    }
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    let mut size = buffer.len();
    // SAFETY: the output is the buffer, of `size` bytes.
    let read = unsafe {
        libc::sysctl(mib.as_mut_ptr(), 3, buffer.as_mut_ptr().cast(), &raw mut size, std::ptr::null_mut(), 0)
    };
    if read != 0 {
        return None;
    }
    let data = buffer.get(..size)?;
    let argc = usize::try_from(i32::from_ne_bytes(data.get(..4)?.try_into().ok()?)).ok()?;
    let mut strings = data.get(4..)?.split(|&b| b == 0);
    // The executable's path, then the padding after it.
    strings.next()?;
    let mut strings = strings.skip_while(|s| s.is_empty());
    let args = strings.by_ref().take(argc).collect();
    let env = strings.take_while(|s| !s.is_empty()).collect();
    Some(Procargs { args, env })
}

/// The value of the tag ([`super::TAG`]) in the environment process `pid` started with, if readable.
pub(super) fn tag(pid: i32, buffer: &mut Vec<u8>) -> Option<String> {
    let Procargs { env, .. } = procargs(pid, buffer)?;
    let prefix = format!("{}=", super::TAG);
    let value = env.iter().find_map(|var| var.strip_prefix(prefix.as_bytes()))?;
    Some(String::from_utf8_lossy(value).into_owned())
}

/// The command line of process `pid`, its arguments joined by spaces; empty if unreadable.
pub(super) fn command(pid: i32, buffer: &mut Vec<u8>) -> String {
    let Some(Procargs { args, .. }) = procargs(pid, buffer) else { return String::new() };
    let args: Vec<_> = args.into_iter().map(String::from_utf8_lossy).collect();
    args.join(" ")
}
