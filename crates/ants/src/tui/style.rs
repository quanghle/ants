//! The look `ants chat` and `ants dash` share: dim text, bordered blocks, the selected row, the colours that carry a
//! meaning, the ages, connection and notices of their headers and footers, and text cut to fit, ending in an ellipsis.

use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, HighlightSpacing, List};
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

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

pub fn dim() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

/// A bordered block titled `title`, its border bright when `focused`.
pub fn block(title: String, focused: bool) -> Block<'static> {
    let style = if focused { Style::new().fg(INFO) } else { dim() };
    Block::bordered().title(title).border_style(style)
}

/// What a view with nothing to list shows until the stream is read.
pub const READING: &str = "reading the stream…";

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

/// The connection as a header shows it: `● connected`, else how long it, or the hub beyond it, has been out of reach
/// and `then`.
pub fn link(link: Link, now: DateTime<Utc>, then: &str) -> Span<'static> {
    let (what, since, doing) = match link {
        Link::Connected => return Span::styled("● connected", Style::new().fg(CONNECTED)),
        Link::Down(since) => ("disconnected", since, "reconnecting"),
        Link::Unreached(since) => ("hub out of reach", since, "retrying"),
    };
    let text = format!("○ {what} {}, {doing}{then}", crate::text::short(crate::text::since(since, now)));
    Span::styled(text, Style::new().fg(ERROR).bold())
}

/// What an error begins with, besides its colour.
pub const ERROR_MARK: &str = "✗ ";

/// A notice as a footer shows it on one line `width` wide, an ellipsis ending it when cut; in [`ERROR`] and after
/// [`ERROR_MARK`] when it reports an `error`.
pub fn notice(text: &str, error: bool, width: u16) -> Line<'static> {
    let text = if error { format!("{ERROR_MARK}{text}") } else { text.to_owned() };
    let text = fit(&text.replace('\n', " "), usize::from(width).saturating_sub(1));
    Line::styled(format!(" {text}"), Style::new().fg(if error { ERROR } else { INFO }))
}

/// An error as a footer shows it: after [`ERROR_MARK`], wrapped to `width` over [`ERROR_LINES`] at most, an
/// ellipsis ending the last when cut.
pub fn error(text: &str, width: u16) -> Vec<Line<'static>> {
    let width = usize::from(width).saturating_sub(1).max(1);
    let mut rows = rows(&format!("{ERROR_MARK}{}", text.replace('\n', " ")), width);
    if rows.len() > ERROR_LINES {
        let rest = rows.split_off(ERROR_LINES - 1).join(" ");
        rows.push(fit(&rest, width));
    }
    rows.into_iter().map(|row| Line::styled(format!(" {row}"), Style::new().fg(ERROR))).collect()
}

/// How many lines an error takes in a footer at most.
pub const ERROR_LINES: usize = 3;

/// `text` in at most `width` columns, an ellipsis ending it when cut.
pub fn fit(text: &str, width: usize) -> String {
    match width {
        _ if text.width() <= width => text.to_owned(),
        0 => String::new(),
        _ => ellipsed(text, width - 1),
    }
}

/// The start of `text` that fits in `room` columns, without the whitespace ending it, then an ellipsis.
fn ellipsed(text: &str, mut room: usize) -> String {
    let mut kept: String = text
        .chars()
        .take_while(|c| {
            let wide = c.width().unwrap_or(0);
            let fits = wide <= room;
            room = room.saturating_sub(wide);
            fits
        })
        .collect();
    kept.truncate(kept.trim_end().len());
    kept.push('…');
    kept
}

/// `text` in `width` columns: cut as [`fit`] cuts it, else spaces filling what it leaves.
pub fn column(text: &str, width: usize) -> String {
    let mut text = fit(text, width);
    text.extend(std::iter::repeat_n(' ', width.saturating_sub(text.width())));
    text
}

/// `line` in at most `width` columns, an ellipsis ending it when cut, each span keeping its style.
pub fn clip(line: Line<'static>, width: usize) -> Line<'static> {
    if line.width() <= width {
        return line;
    }
    let Line { style, alignment, spans: all } = line;
    let mut left = width.saturating_sub(1);
    let mut spans = Vec::new();
    for span in all.into_iter().take_while(|_| width > 0) {
        let wide = span.width();
        if wide <= left {
            left -= wide;
            spans.push(span);
            continue;
        }
        spans.push(Span::styled(ellipsed(&span.content, left), span.style));
        break;
    }
    Line { style, alignment, spans }
}

/// `lines` wrapped at words into rows at most `width` columns wide, each row keeping the style of its line.
pub fn wrapped(lines: Vec<Line<'static>>, width: usize) -> Vec<Line<'static>> {
    let rows = |line: Line<'static>| {
        let style = line.style;
        super::markdown::wrap(&line.spans, width.max(1)).into_iter().map(move |spans| Line::from(spans).style(style))
    };
    lines.into_iter().flat_map(rows).collect()
}

/// `text`, one line, wrapped at words into rows at most `width` columns wide.
pub fn rows(text: &str, width: usize) -> Vec<String> {
    let rows = super::markdown::wrap(&[Span::raw(text.to_owned())], width.max(1));
    rows.into_iter().map(|row| row.iter().map(|s| s.content.as_ref()).collect()).collect()
}

/// The hints of `keys`, apart by ` · `, that fit whole in `width` columns: the last ones dropped.
pub fn hints(keys: &str, width: usize) -> String {
    let mut fitting = String::new();
    for hint in keys.split(" · ") {
        let next = if fitting.is_empty() { hint.to_owned() } else { format!("{fitting} · {hint}") };
        if next.width() > width {
            break;
        }
        fitting = next;
    }
    fitting
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

    #[test]
    fn what_is_cut_ends_in_an_ellipsis_within_the_width() {
        assert_eq!(fit("deploy the site", 15), "deploy the site");
        assert_eq!(fit("deploy the site", 11), "deploy the…");
        assert_eq!(fit("デプロイする", 7), "デプロ…", "wide characters count two columns");
        assert_eq!(fit("deploy", 0), "");
        let line = Line::from(vec![Span::raw("waiting "), Span::styled("deploy the site", dim())]);
        let clipped = clip(line, 16);
        assert_eq!(clipped.to_string(), "waiting deploy…");
        assert_eq!(clipped.spans[1].style, dim());
    }

    #[test]
    fn a_line_of_keys_drops_its_last_hints_whole() {
        assert_eq!(hints("a allow · r reject · Enter details", 34), "a allow · r reject · Enter details");
        assert_eq!(hints("a allow · r reject · Enter details", 30), "a allow · r reject");
        assert_eq!(hints("a allow · r reject", 5), "");
    }
}
