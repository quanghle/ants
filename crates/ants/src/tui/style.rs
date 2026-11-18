//! The look `ants chat` and `ants dash` share: dim text, bordered blocks, the selected row, the colours that carry a
//! meaning, and the ages, connection and notices of their headers and footers.

use chrono::{DateTime, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Block;

use crate::colony;
use crate::live::Link;

/// Something failed or was refused, or the connection is down.
pub const ERROR: Color = Color::Red;
/// Something may be stale, or asks for a second look.
pub const WARNING: Color = Color::Yellow;
/// Something waits to be taken, carried out or answered.
pub const PENDING: Color = Color::Yellow;
/// Connected, live, or carried out.
pub const CONNECTED: Color = Color::Green;
/// A notice that reports no error, and the border of the focused block.
pub const INFO: Color = Color::Cyan;

/// `then` from `now`, such as `5s ago`.
pub fn ago(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", colony::short(colony::since(then, now)))
}

pub fn dim() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

/// A bordered block titled `title`, its border bright when `focused`.
pub fn block(title: String, focused: bool) -> Block<'static> {
    let style = if focused { Style::new().fg(INFO) } else { dim() };
    Block::bordered().title(title).border_style(style)
}

/// The selected row's style.
pub fn highlight(focused: bool) -> Style {
    if focused { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new().add_modifier(Modifier::BOLD) }
}

/// The connection as a header shows it: `● connected`, else how long it has been down and `then`.
pub fn link(link: Link, now: DateTime<Utc>, then: &str) -> Span<'static> {
    match link {
        Link::Connected => Span::styled("● connected", Style::new().fg(CONNECTED)),
        Link::Down(since) => Span::styled(
            format!("○ disconnected {}, reconnecting{then}", colony::short(colony::since(since, now))),
            Style::new().fg(ERROR).bold(),
        ),
    }
}

/// What an error begins with, besides its colour.
pub const ERROR_MARK: &str = "✗ ";

/// A notice as a footer shows it on one line `width` wide, an ellipsis ending it when cut; in [`ERROR`] and after
/// [`ERROR_MARK`] when it reports an `error`.
pub fn notice(text: &str, error: bool, width: u16) -> Line<'static> {
    let text = if error { format!("{ERROR_MARK}{text}") } else { text.to_owned() };
    let text = cut(&text.replace('\n', " "), usize::from(width).saturating_sub(1));
    Line::styled(format!(" {text}"), Style::new().fg(if error { ERROR } else { INFO }))
}

/// An error as a footer shows it: after [`ERROR_MARK`], wrapped to `width` over [`ERROR_LINES`] at most, an
/// ellipsis ending the last when cut.
pub fn error(text: &str, width: u16) -> Vec<Line<'static>> {
    let width = usize::from(width).saturating_sub(1).max(1);
    let words = [Span::raw(format!("{ERROR_MARK}{}", text.replace('\n', " ")))];
    let mut rows: Vec<String> = super::markdown::wrap(&words, width)
        .into_iter()
        .map(|row| row.iter().map(|s| s.content.as_ref()).collect())
        .collect();
    if rows.len() > ERROR_LINES {
        let rest = rows.split_off(ERROR_LINES - 1).join(" ");
        rows.push(cut(&rest, width));
    }
    rows.into_iter().map(|row| Line::styled(format!(" {row}"), Style::new().fg(ERROR))).collect()
}

/// How many lines an error takes in a footer at most.
pub const ERROR_LINES: usize = 3;

/// `text` in at most `width` characters, an ellipsis ending it when cut.
pub fn cut(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}
