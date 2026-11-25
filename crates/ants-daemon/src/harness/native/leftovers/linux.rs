//! The process table on Linux, read of `/proc`.

use std::os::unix::fs::MetadataExt as _;

use super::unix::Proc;

/// Every process listed, zombies left out.
pub(super) fn table() -> Vec<Proc> {
    let Ok(entries) = std::fs::read_dir("/proc") else { return Vec::new() };
    entries.flatten().filter_map(|entry| entry.file_name().to_str()?.parse().ok()).filter_map(process).collect()
}

/// Process `pid`, unless gone or a zombie.
pub(super) fn process(pid: i32) -> Option<Proc> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name, in parentheses, may hold spaces and parentheses: the fields are those after the last `)`,
    // from field 3, the state, on: 4 the parent, 5 the process group, 22 the start time.
    let fields: Vec<&str> = stat.get(stat.rfind(')')? + 1..)?.split_whitespace().collect();
    if fields.first() == Some(&"Z") {
        return None;
    }
    let uid = std::fs::metadata(format!("/proc/{pid}")).ok()?.uid();
    Some(Proc {
        pid,
        ppid: fields.get(1)?.parse().ok()?,
        pgid: fields.get(2)?.parse().ok()?,
        uid,
        start: fields.get(19)?.parse().ok()?,
    })
}

/// The value of the tag ([`super::TAG`]) in the environment process `pid` started with, if readable.
pub(super) fn tag(pid: i32, _: &mut Vec<u8>) -> Option<String> {
    let environ = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
    let prefix = format!("{}=", super::TAG);
    let value = environ.split(|&b| b == 0).find_map(|var| var.strip_prefix(prefix.as_bytes()))?;
    Some(String::from_utf8_lossy(value).into_owned())
}

/// The command line of process `pid`, its arguments joined by spaces; empty if unreadable.
pub(super) fn command(pid: i32, _: &mut Vec<u8>) -> String {
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
    let args: Vec<_> = cmdline.split(|&b| b == 0).filter(|arg| !arg.is_empty()).map(String::from_utf8_lossy).collect();
    args.join(" ")
}
