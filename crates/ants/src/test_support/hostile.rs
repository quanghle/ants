//! Text another user may write to take over a reader's terminal, for the tests of what the CLI prints of it.

/// Escape sequences clearing the screen, retitling the window and writing the clipboard, a C1 control sequence, a
/// right-to-left override and an isolate, a NUL and a bell, across two lines.
pub(crate) const HOSTILE: &str =
    "\u{1b}[2J\u{1b}]0;pwned\u{7}\u{1b}]52;c;cHduZWQ=\u{7}\u{202e}spoof\u{9b}31m\u{0}\r\nline two\u{2066}\u{85}";

/// Panics unless `shown` holds no control character but line breaks, nor any reordering text.
#[track_caller]
pub(crate) fn assert_printable(shown: &str) {
    let bad: Vec<char> = shown.chars().filter(|&c| c != '\n' && !ants_proto::shown(c)).collect();
    assert!(bad.is_empty(), "{bad:?} in {shown:?}");
    assert!(shown.contains("spoof"), "the words themselves stay: {shown:?}");
}
