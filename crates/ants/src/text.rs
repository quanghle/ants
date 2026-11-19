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
    let line = text.split_whitespace().map(|word| word.chars().filter(|&c| shown(c)).collect::<String>());
    let line = line.filter(|word| !word.is_empty()).collect::<Vec<_>>().join(" ");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < line.len() {
        out.push('…');
    }
    out
}

/// `text`, words someone else wrote, to follow a label's colon: ` text` when it is one line, else each of its lines on
/// a line of its own indented by two spaces; tabs as spaces, control characters and those reordering text left out,
/// blank lines at its ends dropped.
pub fn after_colon(text: &str) -> String {
    let lines: Vec<String> = text
        .replace("\r\n", "\n")
        .split(['\n', '\r', '\u{b}', '\u{c}', '\u{85}', '\u{2028}', '\u{2029}'])
        .map(|line| {
            let line: String = line.chars().map(|c| if c == '\t' { ' ' } else { c }).filter(|&c| shown(c)).collect();
            line.trim_end().to_owned()
        })
        .collect();
    let Some(first) = lines.iter().position(|line| !line.trim().is_empty()) else {
        return String::new();
    };
    let last = lines.iter().rposition(|line| !line.trim().is_empty()).unwrap_or(first);
    match &lines[first..=last] {
        [line] => format!(" {}", line.trim_start()),
        lines => lines.iter().fold(String::new(), |out, line| format!("{out}\n  {line}")),
    }
}

/// Whether `c` may reach the terminal in text someone else wrote: not a control character, nor one reordering text.
fn shown(c: char) -> bool {
    !c.is_control()
        && !matches!(c, '\u{61c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
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

    #[test]
    fn after_colon_puts_each_line_on_its_own_indented_without_control_characters() {
        assert_eq!(super::after_colon("  boom\u{1b}[8m \n\n"), " boom[8m");
        let text = "\nTraceback:\r\n\tline 3\u{9b}2J\u{2028}\u{202e}ValueError: x\u{7}\n";
        assert_eq!(super::after_colon(text), "\n  Traceback:\n   line 32J\n  ValueError: x");
        assert_eq!(super::after_colon(" \u{1b}\n"), "");
    }
}
