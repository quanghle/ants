//! Text helpers the commands and the terminal UI share: excerpts and one-line notes.

pub use ants_proto::one_line;

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
}
