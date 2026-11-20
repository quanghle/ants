//! Finding text in the thread view's conversation (Ctrl-F): the text asked for in the footer, its matches marked in
//! the lines drawn, and the one stepped to.

use ratatui::style::{Color, Modifier, Style, Stylize as _};
use ratatui::text::{Line, Span};

use super::{Page, Scroll};
use crate::tui::model::Key;
use crate::tui::style::dim;

#[derive(Debug, Default)]
pub struct Find {
    pub text: String,
    /// Whether the footer asks for the text, which typing then changes.
    pub asking: bool,
    /// The match stepped to, by its place among those drawn last.
    pub at: Option<usize>,
}

impl Find {
    /// The footer while finding, `found` the matches drawn last.
    pub fn footer(&self, found: usize) -> Line<'static> {
        let count = match (found, self.at) {
            (0, _) if self.text.is_empty() => String::new(),
            (0, _) => " · no match".to_owned(),
            (n, Some(at)) => format!(" · {} of {n}", at % n + 1),
            (n, None) => format!(" · {n} found"),
        };
        if self.asking {
            Line::from(vec![
                Span::raw(format!(" find: {}", self.text)),
                Span::raw("_").slow_blink(),
                Span::styled(format!("{count} · Enter next · Shift-Enter previous · Esc end"), dim()),
            ])
        } else {
            Line::styled(format!(" find “{}”{count} · n next · N previous · Ctrl-F change · Esc end", self.text), dim())
        }
    }
}

/// Mark the matches of `text`, ignoring case, in `lines`, the `at`th of them apart from the others: the line of each
/// match, in order.
pub fn mark(lines: &mut [Line<'static>], text: &str, at: Option<usize>) -> Vec<usize> {
    let needle: Vec<char> = text.chars().map(fold).collect();
    let mut found = Vec::new();
    if needle.is_empty() {
        return found;
    }
    for (n, line) in lines.iter_mut().enumerate() {
        let chars: Vec<char> = line.spans.iter().flat_map(|s| s.content.chars()).map(fold).collect();
        let mut ranges = Vec::new();
        let mut from = 0;
        while let Some(start) = chars.get(from..).and_then(|rest| rest.windows(needle.len()).position(|w| w == needle))
        {
            let start = from + start;
            let current = at.is_some_and(|at| at == found.len());
            ranges.push((start, start + needle.len(), current));
            found.push(n);
            from = start + needle.len();
        }
        if !ranges.is_empty() {
            restyle(line, &ranges);
        }
    }
    found
}

fn fold(c: char) -> char {
    c.to_lowercase().next().unwrap_or(c)
}

/// `line` with the characters in each of `ranges` marked, the current match more.
fn restyle(line: &mut Line<'static>, ranges: &[(usize, usize, bool)]) {
    let marked = |current: bool| {
        let style = Style::new().add_modifier(Modifier::REVERSED);
        if current { style.fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { style }
    };
    let mut spans = Vec::new();
    let mut at = 0;
    for span in std::mem::take(&mut line.spans) {
        let mut piece = String::new();
        let mut piece_style = None;
        for c in span.content.chars() {
            let style = ranges
                .iter()
                .find(|(start, end, _)| (*start..*end).contains(&at))
                .map_or(span.style, |&(.., current)| span.style.patch(marked(current)));
            if piece_style.is_some_and(|s| s != style) {
                spans.push(Span::styled(std::mem::take(&mut piece), piece_style.unwrap_or_default()));
            }
            piece.push(c);
            piece_style = Some(style);
            at += 1;
        }
        if !piece.is_empty() {
            spans.push(Span::styled(piece, piece_style.unwrap_or_default()));
        }
    }
    line.spans = spans;
}

impl Page {
    /// Take `key` for finding text in the conversation: Ctrl-F starts asking for it in the footer, where typing changes
    /// it, Enter and Shift-Enter step to the next and previous match and another key stops asking; then n and N step,
    /// Ctrl-F asks again and Esc ends it. Whether it took `key`.
    pub fn find_key(&mut self, key: Key) -> bool {
        if self.listing() || self.answering.is_some() {
            return false;
        }
        let Some(find) = &mut self.find else {
            if key == Key::Ctrl('f') {
                self.find = Some(Find { asking: true, ..Find::default() });
            }
            return key == Key::Ctrl('f');
        };
        match key {
            Key::Ctrl('f') => find.asking = true,
            Key::Esc => self.find = None,
            Key::Char(c) if find.asking => {
                find.text.push(c);
                find.at = None;
            }
            Key::Backspace if find.asking => {
                find.text.pop();
                find.at = None;
            }
            Key::Enter if find.asking => self.step_find(true),
            Key::Newline if find.asking => self.step_find(false),
            Key::Char('n') => self.step_find(true),
            Key::Char('N') => self.step_find(false),
            _ => {
                find.asking = false;
                return false;
            }
        }
        true
    }

    /// Whether the footer asks for the text to find, which typing changes.
    pub fn finding(&self) -> bool {
        !self.listing() && self.answering.is_none() && self.find.as_ref().is_some_and(|f| f.asking)
    }

    /// The footer while finding text in the conversation.
    pub fn find_footer(&self) -> Option<Line<'static>> {
        if self.listing() || self.answering.is_some() {
            return None;
        }
        Some(self.find.as_ref()?.footer(self.laid.borrow().found.len()))
    }

    /// Step to the next match drawn last, or with `down` false the previous one, scrolling it into view by line.
    fn step_find(&mut self, down: bool) {
        let laid = self.laid.borrow();
        let (found, top, height) = (&laid.found, self.top.get(), laid.height);
        let Some(find) = self.find.as_mut().filter(|_| !found.is_empty()) else { return };
        let n = found.len();
        let at = match find.at.map(|at| at % n) {
            Some(at) if down => (at + 1) % n,
            Some(at) => (at + n - 1) % n,
            None if down => found.iter().position(|&line| line >= top).unwrap_or(0),
            None => found.iter().rposition(|&line| line < top + height).unwrap_or(n - 1),
        };
        find.at = Some(at);
        let line = found[at];
        if line < top || line >= top + height {
            let top = line.saturating_sub(height / 2).min(laid.lines.saturating_sub(height));
            self.top.set(top);
            self.scroll.set(Scroll::At(top));
        }
        drop(laid);
        self.at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_are_marked_ignoring_case_across_spans_and_the_current_one_apart() {
        let mut lines = vec![
            Line::from(vec![Span::raw("the Dep"), Span::styled("loy failed", dim())]),
            Line::raw("nothing"),
            Line::raw("deploy again, deploy"),
        ];
        assert_eq!(mark(&mut lines, "DEPLOY", Some(1)), [0, 2, 2]);
        let styled = |line: &Line, text: &str| line.spans.iter().find(|s| s.content == text).map(|s| s.style);
        let reversed = Style::new().add_modifier(Modifier::REVERSED);
        assert_eq!(styled(&lines[0], "Dep"), Some(reversed));
        assert_eq!(styled(&lines[0], "loy"), Some(dim().patch(reversed)), "its own style kept");
        assert_eq!(styled(&lines[0], " failed"), Some(dim()));
        assert_eq!(styled(&lines[2], "deploy").and_then(|s| s.fg), Some(Color::Yellow), "the current match");
        assert_eq!(lines[1], Line::raw("nothing"));
        assert!(mark(&mut lines, "", None).is_empty());
    }
}
