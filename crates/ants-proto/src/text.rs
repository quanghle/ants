//! Text someone else wrote, made safe to store and show: no control characters, nor characters reordering text.

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

/// `text`, words someone else wrote, as lines joined by `\n`: each line break one `\n`, tabs as spaces, control
/// characters and those reordering text left out, spaces ending a line and blank lines at its ends dropped.
pub fn lines(text: &str) -> String {
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
    lines[first..=last].join("\n")
}

/// Whether `c` may stand in text someone else wrote: not a control character, nor one reordering text (the bidi
/// marks, embeddings, overrides and isolates U+061C, U+200E, U+200F, U+202A-U+202E and U+2066-U+2069).
pub fn shown(c: char) -> bool {
    !c.is_control()
        && !matches!(c, '\u{61c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

/// Whether `c` may stand in one line of text someone else wrote: [`shown`], and no Unicode line or paragraph
/// separator.
pub(crate) fn shown_in_line(c: char) -> bool {
    shown(c) && !matches!(c, '\u{2028}' | '\u{2029}')
}

/// Whether `c` may stand in text of several lines someone else wrote: [`shown`], or whitespace such as a line break
/// or a tab.
pub(crate) fn shown_in_lines(c: char) -> bool {
    shown(c) || c.is_whitespace()
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
    fn lines_keep_their_breaks_without_control_characters() {
        assert_eq!(super::lines("  boom\u{1b}[8m \n\n"), "  boom[8m");
        let text = "\nTraceback:\r\n\tline 3\u{9b}2J\u{2028}\u{202e}ValueError: x\u{7}\n";
        assert_eq!(super::lines(text), "Traceback:\n line 32J\nValueError: x");
        assert_eq!(super::lines(" \u{1b}\n"), "");
    }
}
