//! The words an entry is noted with: a verdict, an escalation, how long a tool call took.

use std::time::Duration;

use ants_proto::{Escalation, UserId, Verdict};

/// How a verdict is printed.
pub fn spelling(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::AllowOnce => "allow-once",
        Verdict::AllowAlways => "allow-always",
        Verdict::Reject => "reject",
    }
}

/// How long a tool call took: `840ms`, `2.1s`, `3m`.
pub fn took(ms: u64) -> String {
    match ms {
        0..1000 => format!("{ms}ms"),
        1000..60_000 => format!("{:.1}s", Duration::from_millis(ms).as_secs_f64()),
        _ => ants_proto::duration::format(Duration::from_millis(ms)),
    }
}

/// An escalation in one line: `escalation level 1, turn_budget: dev-a stuck on t1, leader lead, visited dev-a > lead`.
pub fn escalation_note(x: &Escalation) -> String {
    let reason = match x.request_id {
        Some(id) => format!("{} {}/{id}", x.reason.as_str(), x.stuck),
        None => x.reason.as_str().to_owned(),
    };
    let visited = x.visited.iter().map(UserId::as_str).collect::<Vec<_>>().join(" > ");
    let (level, stuck, thread, leader) = (x.level, &x.stuck, &x.thread_id, &x.leader);
    let away = x.away.as_ref().map_or_default(|a| format!(", relayed while {} is away: answer {}", a.human, a.ant));
    format!("escalation level {level}, {reason}: {stuck} stuck on {thread}, leader {leader}, visited {visited}{away}")
}
