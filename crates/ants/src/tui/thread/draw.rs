//! Drawing the thread view: its cards, the conversation scrolled as the keys left it, and the side panel.

use std::cell::{Cell, RefCell};

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use super::cards::draw_cards;
use super::conversation::{self, Item, Shown, Timed, glyphs, rows};
use super::{Laid, PANEL_WIDTH, Page, Picked, Scroll, Spot, find};
use crate::tui::model::{Data, Model};
use crate::tui::style::{self, ago};
use crate::tui::{catchup, images, work};

impl Page {
    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let (data, ui) = (&model.data, &model.ui);
        if let Some(list) = &self.listing {
            list.draw(frame, area, self, data, &ui.mouse, now);
            return;
        }
        let thread = &self.history.thread;
        let (area, panel) = if model.ui.fold.panel(frame.area().width) {
            let [left, right] = Layout::horizontal([Constraint::Min(20), Constraint::Length(PANEL_WIDTH)]).areas(area);
            (left, Some(right))
        } else {
            (area, None)
        };
        let cursor = self.card(data, now);
        let cards = draw_cards(frame, area, model, thread, &self.history.entries, cursor, now);
        self.card_room.set(Some(cards.room));
        let area = cards.rest;
        let block = style::block(self.title(data, usize::from(area.width.saturating_sub(4))), focused);
        let mut inner = block.inner(area);
        if panel.is_none() {
            let [brief, rest] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(inner);
            frame.render_widget(Paragraph::new(self.brief(data, usize::from(brief.width))), brief);
            inner = rest;
        }
        let (outer, area) = (area, catchup::draw_tldr(frame, inner, model, thread, now));
        let [gutter, area] = Layout::horizontal([Constraint::Length(1), Constraint::Min(0)]).areas(area);
        let width = usize::from(area.width);
        let mut lines = Vec::new();
        if panel.is_some() || work::reason(data, thread).is_some() {
            lines.push(style::clip(work::why(data, thread), width));
        }
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", style::dim()));
        }
        let marked = self.attached().map(|e| (e.seq, self.mark.at(e.seq, e.attachments.len())));
        let busy = data.live.progress.contains_key(thread);
        let shown = Shown {
            data,
            now,
            busy,
            width,
            marked,
            placed: RefCell::default(),
            carded: cards.carded,
            spun: Cell::new(false),
        };
        let calls = data.calls.get(thread).map_or(&[][..], |c| c.calls());
        let ends = data.calls.get(thread).map_or(&[][..], |c| c.ends());
        let timed = Timed { calls, ends, steps: data.steps.of(thread) };
        let rows = rows(&self.history.entries, timed, ui.tools, busy, |u| data.is_human(u));
        let held = self.held();
        let (picks, range) = self.lay(&mut lines, &shown, &rows);
        let height = usize::from(area.height);
        let found = match &self.find {
            Some(find) => find::mark(&mut lines, &find.text, find.at),
            None => Vec::new(),
        };
        let top = self.scrolled(range, held.is_some(), lines.len(), height);
        self.spinning.set(shown.spun.get());
        let below = |(picked, start, _): &&Spot| matches!(picked, Picked::Message(_)) && *start >= top + height;
        let newer = picks.iter().filter(below).count();
        message_hits(&ui.mouse, &picks, area, top, height);
        *self.laid.borrow_mut() = Laid { height, lines: lines.len(), rows: picks, found };
        let block = match newer {
            0 => block,
            n => block.title_bottom(
                Line::styled(format!(" ↓ {n} newer · End shows them "), Style::new().fg(style::INFO)).right_aligned(),
            ),
        };
        frame.render_widget(block, outer);
        let paragraph = Paragraph::new(lines).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
        if let Some((start, end)) = range {
            let bars =
                (top..top + height).map(|at| if (start..end).contains(&at) { style::bar(focused) } else { "".into() });
            frame.render_widget(Paragraph::new(bars.map(Line::from).collect::<Vec<_>>()), gutter);
        }
        images::draw(frame, &data.images, &shown.placed.borrow(), top, area);
        if let Some(panel) = panel {
            let lines = self.panel(data, calls, now, usize::from(PANEL_WIDTH.saturating_sub(4)));
            frame.render_widget(Paragraph::new(lines).block(style::block(String::new(), false)), panel);
        }
    }

    /// Add the lines of `rows`, drawn with `shown`, the first line of the selected one bold: the messages and steps
    /// among them with the lines each takes, and those the selected one takes.
    fn lay(
        &self,
        lines: &mut Vec<Line<'static>>,
        shown: &Shown,
        rows: &[conversation::Row],
    ) -> (Vec<Spot>, Option<(usize, usize)>) {
        let selected = match &self.selected {
            Some(step @ Picked::Step(_)) => Some(step.clone()),
            _ => self.picked().and_then(|_| self.selection()).map(|e| Picked::Message(e.seq)),
        };
        let (mut picks, mut range) = (Vec::new(), None);
        for (at, (row, (first, rest))) in rows.iter().zip(glyphs(rows)).enumerate() {
            if at > 0 && row.depth == 0 {
                lines.push(Line::raw(""));
            }
            let start = lines.len();
            shown.row(lines, row, &self.history.entries, &first, &rest);
            let picked = match &row.item {
                Item::Message(e) => Picked::Message(e.seq),
                Item::Step(step) => Picked::Step(step.thread.clone()),
                Item::Call(_) | Item::End(_) | Item::Turn(_) => continue,
            };
            if Some(&picked) == selected.as_ref() {
                lines[start].style = lines[start].style.add_modifier(Modifier::BOLD);
                range = Some((start, lines.len()));
            }
            picks.push((picked, start, lines.len()));
        }
        (picks, range)
    }

    /// The conversation's border title in `room` columns: the thread's title, cut to leave room for its channels.
    fn title(&self, data: &Data, room: usize) -> String {
        let summary = data.live.summary(&self.history.thread);
        let channels = summary.filter(|s| !s.channels.is_empty()).map_or_else(String::new, |s| {
            let channels: Vec<String> = s.channels.iter().map(|c| format!("#{c}")).collect();
            format!(" · {}", channels.join(" "))
        });
        let title =
            style::fit(&data.title(&self.history.thread), room.saturating_sub(channels.chars().count()).max(10));
        format!(" {} ", style::fit(&format!("{title}{channels}"), room))
    }

    /// The first line of the conversation to show, `height` lines of its `lines`, as the keys left it: the selected
    /// message's lines, from `range`, at the top while `held`; the scroll settled where it reveals them.
    fn scrolled(&self, range: Option<(usize, usize)>, held: bool, lines: usize, height: usize) -> usize {
        let bottom = lines.saturating_sub(height);
        let top = match (range, self.scroll.get()) {
            (Some((start, _)), _) if held => start.min(bottom),
            (_, Scroll::Latest) => bottom,
            (_, Scroll::At(top)) => top.min(bottom),
            (None, Scroll::Reveal { .. }) => self.top.get().min(bottom),
            (Some((start, end)), Scroll::Reveal { up }) => reveal(self.top.get(), (start, end), height, up).min(bottom),
        };
        if let Scroll::Reveal { .. } = self.scroll.get() {
            self.scroll.set(Scroll::At(top));
        }
        self.top.set(top);
        top
    }
}

fn message_hits(mouse: &crate::tui::mouse::Regions, rows: &[Spot], area: Rect, top: usize, height: usize) {
    for (picked, start, end) in rows {
        let from = (*start).max(top);
        let to = (*end).min(top + height);
        if from < to {
            let target = match picked {
                Picked::Message(seq) => crate::tui::mouse::Target::Message(*seq),
                Picked::Step(thread) => crate::tui::mouse::Target::Step(thread.clone()),
            };
            mouse.add(Rect { y: area.y + (from - top) as u16, height: (to - from) as u16, ..area }, target);
        }
    }
}

/// The first line to show of a view `height` lines high, at `top` before, to bring into it the message on lines
/// `start..end`: whole if it fits, else from its last line when moving `up`, from its first otherwise; the view moving
/// no further than it has to.
fn reveal(top: usize, (start, end): (usize, usize), height: usize, up: bool) -> usize {
    if end - start <= height {
        if start < top {
            start
        } else if end > top + height {
            end - height
        } else {
            top
        }
    } else if up {
        if end > top && end <= top + height { top } else { end - height }
    } else if start >= top && start < top + height {
        top
    } else {
        start
    }
}
