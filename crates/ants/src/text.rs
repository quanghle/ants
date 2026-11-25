//! Text helpers the commands and the terminal UI share: excerpts, one-line notes, tables in columns, how long ago
//! or how long from now a time is, and where a command's lines go ([`Say`]).

use std::time::Duration;

pub use ants_proto::one_line;
use chrono::{DateTime, TimeDelta, Utc};

/// The first line of `text`, words someone else wrote, cut to `max` characters, `…` marking what is left out: tabs
/// as spaces, control characters and those reordering text left out ([`ants_proto::lines`]).
pub fn excerpt(text: &str, max: usize) -> String {
    let text = ants_proto::lines(text);
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
}

/// `text`, words someone else wrote, as it comes, without control characters but line breaks, nor those reordering
/// text: for text printed piece by piece, as an ant streams it.
pub fn printable(text: &str) -> String {
    text.chars().filter(|&c| c == '\n' || ants_proto::shown(c)).collect()
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

/// Where a command the terminal UI runs too says what it does: printed line by line as the CLI runs it, or gathered
/// for the UI to show.
#[derive(Debug, Default)]
pub enum Say {
    #[default]
    Print,
    Gather(Vec<String>),
}

impl Say {
    pub fn line(&mut self, line: impl std::fmt::Display) {
        match self {
            Self::Print => println!("{line}"),
            Self::Gather(lines) => lines.push(line.to_string()),
        }
    }

    /// The lines gathered: none once printed.
    pub fn into_lines(self) -> Vec<String> {
        match self {
            Self::Print => Vec::new(),
            Self::Gather(lines) => lines,
        }
    }
}

/// `rows` as columns two spaces apart, each as wide as its widest cell, without trailing blanks.
pub fn columns(rows: &[impl AsRef<[String]>]) -> String {
    column_lines(rows).join("\n")
}

/// The lines of [`columns`], one per row, for a table with other lines between its rows.
pub fn column_lines(rows: &[impl AsRef<[String]>]) -> Vec<String> {
    let mut widths: Vec<usize> = Vec::new();
    for row in rows {
        let row = row.as_ref();
        widths.resize(widths.len().max(row.len()), 0);
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |row: &[String]| {
        let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, &width)| format!("{cell:width$}")).collect();
        cells.join("  ").trim_end().to_owned()
    };
    rows.iter().map(|row| line(row.as_ref())).collect()
}

/// Time from `then` to `now`, zero when a clock is ahead.
pub fn since(then: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    (now - then).to_std().unwrap_or_default()
}

/// `duration` in its largest whole unit: `7s`, `3m`, `2h`, `5d`.
pub fn short(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86_400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86_400),
    }
}

/// `then` from `now`, such as `5s ago`.
pub fn ago(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", short(since(then, now)))
}

/// Time left before `expires_at`, such as `4m05s`.
pub fn left(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let secs = (expires_at - now).max(TimeDelta::zero()).num_seconds();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        _ => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn excerpts_and_printable_text_leave_control_and_reordering_characters_out() {
        let text = "\u{1b}[2J\u{1b}]0;title\u{7}Fix\u{202e}ed\u{9b}1m\tnow\u{1b}]52;c;cHduZWQ=\u{7}\nmore";
        assert_eq!(super::excerpt(text, 100), "[2J]0;titleFixed1m now]52;c;cHduZWQ=…");
        assert_eq!(super::excerpt("\n\u{2066}short\u{2069} \n", 10), "short");
        assert_eq!(super::excerpt("long line", 4), "long…");
        assert_eq!(super::printable("a\u{1b}[2Jb\r\nc\u{202e}\u{0}"), "a[2Jb\nc");
    }

    #[test]
    fn after_colon_puts_each_line_on_its_own_indented_without_control_characters() {
        assert_eq!(super::after_colon("  boom\u{1b}[8m \n\n"), " boom[8m");
        let text = "\nTraceback:\r\n\tline 3\u{9b}2J\u{2028}\u{202e}ValueError: x\u{7}\n";
        assert_eq!(super::after_colon(text), "\n  Traceback:\n   line 32J\n  ValueError: x");
        assert_eq!(super::after_colon(" \u{1b}\n"), "");
    }

    #[test]
    fn time_left_reads_at_a_glance() {
        let now = chrono::Utc::now();
        let secs = |s| super::left(now + chrono::TimeDelta::seconds(s), now);
        assert_eq!([secs(-5), secs(45), secs(245), secs(3 * 3600 + 120)], ["0s", "45s", "4m05s", "3h02m"]);
    }
}
