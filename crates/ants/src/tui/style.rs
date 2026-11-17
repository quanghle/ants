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

/// A notice as a footer shows it, in [`ERROR`] when it reports an `error`.
pub fn notice(text: &str, error: bool) -> Line<'static> {
    Line::styled(format!(" {text}"), Style::new().fg(if error { ERROR } else { INFO }))
}
