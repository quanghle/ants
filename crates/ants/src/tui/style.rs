//! The look `ants chat` and `ants dash` share: dim text, bordered blocks, the selected row, the colours that carry a
//! meaning, and the ages, connection and notices of their headers and footers.

use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, HighlightSpacing, List};

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

/// The selected row's style, in tables and tabs.
pub fn highlight(focused: bool) -> Style {
    if focused { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new().add_modifier(Modifier::BOLD) }
}

/// At the left of each line of the selected item of a list, bright when the list is `focused`, else dim; without
/// colour under `NO_COLOR`.
pub fn bar(focused: bool) -> Span<'static> {
    static COLOUR: OnceLock<bool> = OnceLock::new();
    bar_in(focused, *COLOUR.get_or_init(|| std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())))
}

fn bar_in(focused: bool, colour: bool) -> Span<'static> {
    let style = match (focused, colour) {
        (true, true) => Style::new().fg(INFO),
        (true, false) => Style::new(),
        (false, _) => dim(),
    };
    Span::styled("▌", style)
}

/// `items` as a list: the selected one with [`bar`] at the left of each of its lines and its first line bold, the
/// others with a blank there.
pub fn list<'a>(mut items: Vec<Text<'a>>, selected: Option<usize>, focused: bool) -> List<'a> {
    if let Some(first) = selected.and_then(|at| items.get_mut(at)).and_then(|item| item.lines.first_mut()) {
        first.style = first.style.add_modifier(Modifier::BOLD);
    }
    List::new(items)
        .highlight_symbol(bar(focused))
        .repeat_highlight_symbol(true)
        .highlight_spacing(HighlightSpacing::Always)
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

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::widgets::ListState;

    use super::*;

    #[test]
    fn a_list_marks_each_line_of_the_selected_item_with_a_bar_and_its_first_line_bold() {
        let draw = |focused: bool| {
            let items = vec![Text::from("one"), Text::from(vec![Line::raw("two"), Line::raw("more")])];
            let mut terminal = Terminal::new(TestBackend::new(8, 3)).unwrap();
            terminal
                .draw(|f| {
                    let list = list(items, Some(1), focused);
                    f.render_stateful_widget(list, f.area(), &mut ListState::default().with_selected(Some(1)));
                })
                .unwrap();
            terminal.backend().buffer().clone()
        };
        let buffer = draw(true);
        let row = |y: u16| (0..8).map(|x| buffer[(x, y)].symbol()).collect::<String>();
        assert_eq!([row(0), row(1), row(2)], [" one    ", "▌two    ", "▌more   "]);
        assert_eq!(bar_in(true, true).style.fg, Some(INFO));
        assert!(buffer[(1, 1)].modifier.contains(Modifier::BOLD) && !buffer[(1, 2)].modifier.contains(Modifier::BOLD));
        assert!((0..8).all(|x| (0..3).all(|y| !buffer[(x, y)].modifier.contains(Modifier::REVERSED))));
        assert!(draw(false)[(0, 1)].modifier.contains(Modifier::DIM), "a dim bar out of focus");
        assert_eq!(bar_in(true, false).style, Style::new(), "no colour under NO_COLOR");
    }
}
