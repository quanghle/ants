//! Text helpers the commands and the terminal UI share: excerpts and one-line notes.

pub use ants_proto::one_line;
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

/// `text`, words someone else wrote, to follow a label's colon: ` text` when it is one line, else each of its lines on
/// a line of its own indented by two spaces; tabs as spaces, control characters and those reordering text left out,
/// blank lines at its ends dropped.
pub fn after_colon(text: &str) -> String {
    let text = ants_proto::lines(text);
    if text.is_empty() {
        return text;
    }
    if !text.contains('\n') {
        return format!(" {}", text.trim_start());
    }
    text.lines().fold(String::new(), |out, line| format!("{out}\n  {line}"))
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

#[cfg(test)]
mod tests {
    #[test]
    fn after_colon_puts_each_line_on_its_own_indented_without_control_characters() {
        assert_eq!(super::after_colon("  boom\u{1b}[8m \n\n"), " boom[8m");
        let text = "\nTraceback:\r\n\tline 3\u{9b}2J\u{2028}\u{202e}ValueError: x\u{7}\n";
        assert_eq!(super::after_colon(text), "\n  Traceback:\n   line 32J\n  ValueError: x");
        assert_eq!(super::after_colon(" \u{1b}\n"), "");
    }
}
