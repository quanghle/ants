//! The tool request Enter shows in full over the screen: who asks, what it asks ([`super::text`]), what allow-always
//! allows, when it was asked and where it stands, the body scrolling with ↑↓, PgUp and PgDn and saying how many lines
//! are below, and the keys answering it in the bottom border, always drawn.

use ants_proto::Verdict;
use chrono::{DateTime, Local, Utc};
use ratatui::Frame;
use ratatui::layout::{Margin, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};

use super::super::{model::Key, select::step, style};
use super::text::asked;
use super::{Asking, how};
use crate::tools::{self, State};

/// The keys answering a pending request, as offered, from the longest form to the shortest: the widest fitting the
/// border is drawn.
fn keys(asking: &Asking) -> [String; 3] {
    let offered = |v: &Verdict| asking.request.options.contains(v);
    let allow = |once, always| {
        [(Verdict::AllowOnce, once), (Verdict::AllowAlways, always)].into_iter().filter(|(v, _)| offered(v))
    };
    let form = |allow: &mut dyn Iterator<Item = (Verdict, &'static str)>, rest: [&'static str; 4]| {
        allow.map(|(_, key)| key).chain(rest).collect::<Vec<_>>().join(" · ")
    };
    [
        form(
            &mut allow("a allow", "A always"),
            ["r reject", "n reject with a note", "Enter open its thread", "Esc back"],
        ),
        form(&mut allow("a allow", "A always"), ["r reject", "n note", "Enter thread", "Esc back"]),
        form(&mut allow("a", "A"), ["r", "n", "Enter", "Esc"]),
    ]
}

/// What the bottom border says: the keys answering a pending request, that its decision is being stored, or that it
/// was answered.
fn border(asking: &Asking, width: usize) -> String {
    let forms = if asking.storing {
        ["storing the decision… · Esc back".to_owned(), "storing… · Esc".to_owned(), String::new()]
    } else if asking.state == State::Pending {
        keys(asking)
    } else {
        [
            "answered already · Enter open its thread · Esc back".to_owned(),
            "answered · Enter · Esc".to_owned(),
            String::new(),
        ]
    };
    let fits = |form: &&String| form.chars().count() + 2 <= width;
    let form = forms.iter().find(fits).unwrap_or(&forms[forms.len() - 1]);
    format!(" {form} ")
}

/// The body's lines, unwrapped.
fn body(asking: &Asking, now: DateTime<Utc>) -> Vec<Line<'static>> {
    let request = &asking.request;
    let head = format!("{} asks to run {}", request.ant, request.tool.kind);
    let mut lines = vec![
        Line::from(vec![
            Span::styled(head, Style::new().bold()),
            Span::styled(format!(" · thread {}", request.thread_id), style::dim()),
        ]),
        Line::raw(""),
    ];
    lines.extend(asked(request));
    lines.push(Line::raw(""));
    if !request.tool.locations.is_empty() {
        lines.push(Line::styled(format!("files    {}", request.tool.locations.join(", ")), style::dim()));
    }
    if let Some(always) = request.label(Verdict::AllowAlways).filter(|_| asking.state == State::Pending) {
        lines.push(Line::styled(format!("A always {always}"), style::dim()));
    }
    let asked_at = request.requested_at.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S");
    lines.push(Line::styled(format!("asked    {asked_at}"), style::dim()));
    lines.push(match &asking.state {
        State::Pending => Line::styled(format!("expires  in {}", tools::left(request.expires_at, now)), style::dim()),
        state => Line::styled(format!("settled  {}", how(state, &request.ant)), Style::new().fg(style::PENDING)),
    });
    lines
}

/// Where `key` scrolls the body from `top`, `page` rows showing: none for a key that does not scroll.
pub(super) fn scrolled(key: Key, top: usize, page: usize) -> Option<usize> {
    step(key, top, Some(page.max(1)))
}

/// The request being answered in full over the screen.
pub fn draw(f: &mut Frame, asking: &Asking, now: DateTime<Utc>) {
    let screen = f.area();
    let area = screen.inner(Margin { horizontal: screen.width / 20, vertical: screen.height / 10 });
    let id = tools::qualified(&asking.request);
    let block = Block::bordered()
        .title(format!(" Tool request {id} "))
        .title_bottom(Line::styled(border(asking, usize::from(area.width)), Style::new().fg(style::PENDING).bold()))
        .border_style(Style::new().fg(style::PENDING));
    let inner = block.inner(area);
    f.render_widget(Clear, area);
    f.render_widget(block, area);
    let rows = style::wrapped(body(asking, now), usize::from(inner.width));
    let height = usize::from(inner.height);
    let (shown, below) = window(rows.len(), height, asking);
    let mut lines: Vec<Line> = rows[shown.0..shown.1].to_vec();
    if below > 0 {
        let what = if below == 1 { "line" } else { "lines" };
        lines.push(Line::styled(format!("↓ {below} more {what} below · ↑↓ PgUp PgDn scroll"), style::dim()));
    }
    f.render_widget(Paragraph::new(lines), Rect { height: inner.height, ..inner });
}

/// The rows of `total` drawn in `height`, as a range, and how many are below them; the top kept within the rows and
/// noted, with the page, for the keys to come.
fn window(total: usize, height: usize, asking: &Asking) -> ((usize, usize), usize) {
    if total <= height {
        asking.top.set(0);
        asking.page.set(height);
        return ((0, total), 0);
    }
    let last = total - height;
    let top = asking.top.get().min(last);
    asking.top.set(top);
    let page = height.saturating_sub(1).max(1);
    asking.page.set(page);
    if top == last {
        return ((top, total), 0);
    }
    let end = (top + page).min(total);
    ((top, end), total - end)
}

#[cfg(test)]
mod tests {
    use ants_proto::Verdict;

    use super::super::Answer;
    use super::*;
    use crate::test_support::live::{at, request};
    use crate::test_support::render::rendered;

    #[test]
    fn a_long_request_keeps_its_keys_in_sight_and_scrolls_to_every_line() {
        let words: Vec<String> = (0..400).map(|n| format!("w{n:03}")).collect();
        let mut input = serde_json::json!({ "command": words.join(" ") }).to_string();
        input.truncate(2000);
        input.push('…');
        let mut push = request("dev-a", "deploy", vec![Verdict::AllowOnce, Verdict::AllowAlways]);
        push.tool.input = Some(input);
        let mut asking = Asking::full(push);
        let draw = |asking: &Asking| rendered(80, 24, |f| draw(f, asking, at(10)));
        let first = draw(&asking);
        assert!(first.contains("a allow · A always · r reject · n note · Enter thread · Esc back"), "{first}");
        assert!(first.contains("more lines below · ↑↓ PgUp PgDn scroll"), "{first}");
        assert_eq!(first.matches("w000").count(), 1, "the command shows once: {first}");
        let mut seen = first.clone();
        for _ in 0..40 {
            assert_eq!(asking.key(Key::Down), Answer::Keep);
            let screen = draw(&asking);
            assert!(screen.contains("Esc back"), "{screen}");
            seen.push_str(&screen);
        }
        let shown = words.iter().take_while(|w| first.contains(w.as_str()) || seen.contains(w.as_str())).count();
        assert_eq!(shown, 397, "every word before the cut can be scrolled to");
        assert!(seen.contains("cut at 2,000 characters") && seen.contains("expires  in "), "{seen}");
        assert_eq!(asking.key(Key::PageUp), Answer::Keep);
        assert_eq!(asking.key(Key::Home), Answer::Keep);
        assert_eq!(draw(&asking), first, "Home goes back to the top");
        assert_eq!(asking.key(Key::Char('a')), Answer::Decide(Verdict::AllowOnce), "the keys still answer");
    }

    #[test]
    fn the_body_scrolls_by_row_by_the_rows_showing_and_to_either_end() {
        assert_eq!(scrolled(Key::Up, 0, 8), Some(0));
        assert_eq!(scrolled(Key::Char('j'), 3, 8), Some(4));
        assert_eq!(scrolled(Key::Char('k'), 3, 8), Some(2));
        assert_eq!(scrolled(Key::PageDown, 3, 8), Some(11));
        assert_eq!(scrolled(Key::PageUp, 3, 8), Some(0));
        assert_eq!(scrolled(Key::PageDown, 3, 0), Some(4), "a page is a row at least");
        assert_eq!((scrolled(Key::Home, 3, 8), scrolled(Key::End, 3, 8)), (Some(0), Some(usize::MAX)));
        assert_eq!(scrolled(Key::Char('a'), 3, 8), None);
    }
}
