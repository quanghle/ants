//! The frame around the views: a header with the human, the server, the place shown and the connection, the nav beside
//! the shown view (over it when folded and focused, [`super::fold`]) with the command bar under it, a footer with the
//! view's keys, the latest notice or the command bar's keys, and the help over it all.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};
use unicode_width::UnicodeWidthStr;

use super::model::{Focus, Level, Model, Notice, Page};
use super::style::{self, dim};
use super::{approvals, catchup, compact, keymap, nav, palette};

/// How wide the nav is.
const NAV_WIDTH: u16 = 24;
/// The footer after a first Ctrl-C with nothing open and the bar empty.
pub const QUIT_AGAIN: &str = "Ctrl-C again to quit";

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    model.data.mouse.clear();
    if model.compact {
        return compact::draw(frame, model, now);
    }
    let width = frame.area().width;
    let error = error_shown(model, now).map(|error| style::error(&error.text, width));
    let tall = error.as_ref().map_or(1, |lines| u16::try_from(lines.len()).unwrap_or(1));
    let [header, body, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(3), Constraint::Length(tall)]).areas(frame.area());
    frame.render_widget(Paragraph::new(header_line(model, now, header.width)), header);
    model.fold.drawn(width);
    let nav_focused = model.focus == Focus::Nav && !model.bar.focused;
    let right = if model.fold.nav(width) {
        let [left, right] = Layout::horizontal([Constraint::Length(NAV_WIDTH), Constraint::Min(20)]).areas(body);
        nav::draw(frame, left, &model.data, &model.nav, now, nav_focused);
        right
    } else {
        body
    };
    let [view, bar] =
        Layout::vertical([Constraint::Min(3), Constraint::Length(model.bar.height(right.width))]).areas(right);
    let focused = model.focus == Focus::View && !model.bar.focused;
    let data = &model.data;
    data.mouse.add(view, super::mouse::Target::View);
    match model.page() {
        Page::Home(page) => page.draw(frame, view, model, now, focused),
        Page::Work(page) => page.draw(frame, view, data, now, focused),
        Page::Channel(page) => page.draw(frame, view, data, now, focused),
        Page::Thread(page) => page.draw(frame, view, model, now, focused),
        Page::Dm(page) => page.draw(frame, view, data, now, focused),
        Page::Search(page) => page.draw(frame, view, data, now, focused),
        Page::Tree(page) => page.draw(frame, view, data, now, focused),
        Page::Ant(page) => page.draw(frame, view, data, now, focused),
        Page::Credentials(page) => page.draw(frame, view, data, now, focused),
        Page::Held(page) => page.draw(frame, view, data, now, focused),
    }
    model.bar.draw(frame, bar, &model.completions(), &model.bar_title(), &model.enter_hint());
    let completions = model.completions();
    if !completions.is_empty() {
        super::mouse::completions(data, bar, completions.len(), model.bar.pick.selected, false);
    }
    let palette = model.palette(now);
    if !palette.is_empty() {
        palette::draw(frame, bar, &palette, model.bar.pick.selected);
        super::mouse::completions(data, bar, palette.len(), model.bar.pick.selected, true);
    }
    if nav_focused && !model.fold.nav(width) {
        let over = Rect { width: NAV_WIDTH.min(view.width), ..view };
        frame.render_widget(Clear, over);
        nav::draw(frame, over, &model.data, &model.nav, now, true);
    }
    // After the view: a thread's find counts the matches it drew.
    let footer_lines = error.unwrap_or_else(|| vec![footer_line(model, now, width)]);
    frame.render_widget(Paragraph::new(footer_lines), footer);
    if let Some(asking) = model.asking.as_ref().filter(|a| a.full) {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
        approvals::draw_full(frame, asking, now);
    }
    if model.catchup.shown() {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
    }
    catchup::draw_digest(frame, model, now);
    if let Some(planned) = &model.starting {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
        planned.draw(frame);
    }
    if let Some(help) = &model.help {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
        help.draw(frame, model, now);
    }
    if let Some(log) = &model.log {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
        log.draw(frame, &model.notices);
    }
    if let Some(copying) = &model.copying {
        data.mouse.add(frame.area(), super::mouse::Target::Overlay);
        copying.draw(frame, data);
    }
}

/// The error the footer shows, wrapped, if one waits for a key and no prompt takes the footer.
fn error_shown(model: &Model, now: DateTime<Utc>) -> Option<&Notice> {
    let prompt = model.quitting_at(now)
        || model.picking.is_some()
        || model.interrupting.is_some()
        || model.dropping.is_some()
        || model.starting.is_some();
    let error = model.notice_at(now).filter(|n| n.level == Level::Error);
    error.filter(|_| !prompt && model.snoozing.is_none())
}

/// Who the UI runs as, on which server, the place shown, and the connection.
/// The header, its place cut first so the connection's state stays on screen.
fn header_line(model: &Model, now: DateTime<Utc>, width: u16) -> Line<'static> {
    let data = &model.data;
    let place = match model.page() {
        Page::Thread(page) => page.place(data),
        page => page.name().to_owned(),
    };
    let who = format!(" ants · {}@{}", data.me(), data.server);
    let link = style::link(data.live.link, now, "");
    let room = usize::from(width).saturating_sub(who.width() + link.width() + 6);
    let spans = vec![
        Span::styled(who, Style::new().bold()),
        Span::styled(format!(" · {} · ", style::fit(&place, room.max(1))), dim()),
        link,
    ];
    style::clip(Line::from(spans), usize::from(width))
}

/// The footer at most `width` wide: a prompt taking the keys, the latest notice, the keys of the bar or of a focused
/// question card, a confirmation or draft of the view, else the hints of the key table ([`keymap::footer`]).
fn footer_line(model: &Model, now: DateTime<Utc>, width: u16) -> Line<'static> {
    if model.quitting_at(now) {
        return style::notice(QUIT_AGAIN, false, width);
    }
    if let Some(picker) = &model.picking {
        return picker.line();
    }
    if let Some(confirm) = &model.interrupting {
        return confirm.line();
    }
    if let Some(letter) = &model.dropping {
        return letter.line();
    }
    if let Some(planned) = &model.starting {
        return planned.line();
    }
    if let Some(prompt) = &model.snoozing {
        return prompt.line(&model.data);
    }
    if let Some(notice) = model.notice_at(now) {
        return style::notice(&notice.text, notice.level == Level::Error, width);
    }
    if !model.palette(now).is_empty() {
        return keymap::prompt("↑↓ select · Enter run · Tab complete · Esc close", width, false);
    }
    if model.bar.focused && !model.completions().is_empty() {
        return keymap::prompt("↑↓ select · Tab/Enter complete · Esc close the list", width, false);
    }
    if model.data.history.searching().is_some() {
        return keymap::prompt(
            "type to search the texts sent · Ctrl-R earlier · Enter take · Esc put back",
            width,
            false,
        );
    }
    if model.bar.focused {
        let keys = "Enter send · Ctrl-J new line · Ctrl-G editor · / palette · Esc leave";
        return keymap::prompt(keys, width, false);
    }
    if model.focus == Focus::View
        && let Page::Thread(page) = model.page()
        && let Some(footer) = page.find_footer()
    {
        return style::clip(footer, usize::from(width));
    }
    if model.focus == Focus::View {
        let card = match model.page() {
            Page::Home(page) => page.card(&model.data, now).map(|(cursor, _)| cursor),
            Page::Thread(page) => page.card(&model.data, now),
            _ => None,
        };
        if let Some(cursor) = card {
            return keymap::prompt(cursor.keys(), width, false);
        }
        if let Page::Home(page) = model.page()
            && let Some(keys) = page.prompt()
        {
            return keymap::prompt(&keys, width, true);
        }
        let (prompt, help) = match model.page() {
            Page::Channel(page) => (page.prompt(), false),
            Page::Ant(page) => (page.prompt(), page.armed().is_some()),
            Page::Tree(page) => (page.prompt(), true),
            Page::Credentials(page) => (page.prompt(), true),
            Page::Held(page) => (page.prompt(), true),
            _ => (None, false),
        };
        if let Some(keys) = prompt {
            return keymap::prompt(keys, width, help);
        }
    }
    keymap::footer(model, now, width)
}

#[cfg(test)]
mod tests;
