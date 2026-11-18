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

/// `text`, words someone else wrote, as one line cut to `max` characters, `…` marking a cut: each run of whitespace
/// one space, and control characters and those reordering text left out.
pub fn one_line(text: &str, max: usize) -> String {
    let kept = |c: &char| {
        !c.is_control()
            && !matches!(c, '\u{61c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    };
    let line = text.split_whitespace().map(|word| word.chars().filter(kept).collect::<String>());
    let line = line.filter(|word| !word.is_empty()).collect::<Vec<_>>().join(" ");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < line.len() {
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

#[cfg(test)]
mod tests {
    #[test]
    fn one_line_keeps_words_on_one_line_without_control_characters() {
        let text = "Good\tfor  NATS\r\nreconnects\u{2028}and \u{1b}[31mred\u{7} \u{202e}tests\u{2069}. \u{0}";
        assert_eq!(super::one_line(text, 100), "Good for NATS reconnects and [31mred tests.");
        assert_eq!(super::one_line(text, 13), "Good for NATS…");
        assert_eq!(super::one_line("  Sé  ", 2), "Sé");
    }
}
