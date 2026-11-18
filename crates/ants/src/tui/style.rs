//! The look `ants chat` and `ants dash` share: dim text, bordered blocks, the selected row, the colours that carry a
//! meaning, the ages, connection and notices of their headers and footers, and text cut to fit, ending in an ellipsis.

use chrono::{DateTime, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Block;
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

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

/// `text` in at most `width` columns, an ellipsis ending it when cut.
pub fn fit(text: &str, width: usize) -> String {
    if text.width() <= width {
        return text.to_owned();
    }
    let mut left = width.saturating_sub(1);
    let mut kept: String = text
        .chars()
        .take_while(|c| {
            let wide = c.width().unwrap_or(0);
            let fits = wide <= left;
            left = left.saturating_sub(wide);
            fits
        })
        .collect();
    if width > 0 {
        kept.truncate(kept.trim_end().len());
        kept.push('…');
    }
    kept
}

/// `line` in at most `width` columns, an ellipsis ending it when cut, each span keeping its style.
pub fn clip(line: Line<'static>, width: usize) -> Line<'static> {
    if line.width() <= width {
        return line;
    }
    let mut left = width;
    let mut spans = Vec::new();
    let Line { style, alignment, spans: all } = line;
    for span in all {
        let wide = span.width();
        if wide < left {
            left -= wide;
            spans.push(span);
            continue;
        }
        spans.push(Span::styled(fit(&span.content, left), span.style));
        break;
    }
    Line { style, alignment, spans }
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
    use super::*;

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
