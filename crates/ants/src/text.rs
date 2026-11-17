//! Text helpers the commands and the terminal UI share: excerpts and one-line notes.

use ants_proto::{Escalation, UserId};

/// The first line of `text`, cut to `max` characters.
pub fn excerpt(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
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
