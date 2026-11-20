//! The thread view's side panel, with what else is known of the thread, and its brief line when folded.

use ants_proto::UserId;
use chrono::{DateTime, Utc};
use ratatui::style::Style;
use ratatui::text::{Line, Span};

use super::Page;
use super::conversation::{busy, cut, hm};
use crate::threads::thread::{Entry, Kind};
use crate::tui::model::Data;
use crate::tui::style;
use crate::tui::timeline::Call;

impl Page {
    /// Where the thread is, as the header names it: its channels and its title.
    pub fn place(&self, data: &Data) -> String {
        let thread = &self.history.thread;
        let title = data.title(thread);
        match data.live.summary(thread).filter(|s| !s.channels.is_empty()) {
            Some(s) => {
                let channels: Vec<String> = s.channels.iter().map(|c| format!("#{c}")).collect();
                format!("{} · {title}", channels.join(" "))
            }
            None => title,
        }
    }

    /// The ants on the thread: those it names, those that wrote or were written to, those that made `calls`.
    fn ants<'a>(&'a self, data: &'a Data, calls: &'a [Call]) -> Vec<&'a UserId> {
        let mut ants: Vec<&UserId> =
            data.live.summary(&self.history.thread).into_iter().flat_map(|s| &s.users).collect();
        ants.extend(self.messages().flat_map(|e| e.from.iter().chain(&e.to)));
        ants.extend(calls.iter().map(|c| &c.ant));
        ants.retain(|u| !data.is_human(u));
        ants.sort();
        ants.dedup();
        ants
    }

    /// The turns left, as the latest message carrying them says.
    fn turns(&self) -> Option<u32> {
        self.messages().rev().find_map(|e| e.turns)
    }

    /// The spend on the thread in cost and tokens, once read.
    fn spend(&self) -> Option<(f64, u64)> {
        let spend = self.about.as_ref().and_then(|a| a.spend.as_ref())?;
        Some(spend.iter().fold((0.0, 0), |(c, t), r| (c + r.spent.cost, t + r.spent.tokens)))
    }

    /// The side panel's state, ants, turns and spend on one line in `width` columns, for when it is folded.
    pub(super) fn brief(&self, data: &Data, width: usize) -> Line<'static> {
        let thread = &self.history.thread;
        let state = data.state(thread);
        let calls = data.calls.get(thread).map_or(&[][..], |c| c.calls());
        let mut rest: Vec<String> = Vec::new();
        let ants = self.ants(data, calls);
        if !ants.is_empty() {
            rest.push(ants.iter().map(|a| a.as_str()).collect::<Vec<_>>().join(" "));
        }
        if let Some(turns) = self.turns() {
            rest.push(format!("{turns} turns left"));
        }
        if let Some((cost, tokens)) = self.spend() {
            rest.push(format!("${cost:.2} · {} tokens", count(tokens)));
        }
        let label = state.label();
        let rest = rest.iter().map(|r| format!(" · {r}")).collect::<String>();
        Line::from(vec![
            Span::styled(label, Style::new().fg(state.color())),
            Span::styled(cut(&rest, width.saturating_sub(label.chars().count())), style::dim()),
        ])
    }

    /// The side panel: the thread's state, owner, ants and what each does, turns left, spend, age, origin, how the
    /// human holds it and the paths its tool calls touched.
    pub(super) fn panel(&self, data: &Data, calls: &[Call], now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let label = |name: &str| Span::styled(format!("{name:<7}"), style::dim());
        let field =
            |name: &str, value: String| Line::from(vec![label(name), Span::raw(cut(&value, width.saturating_sub(7)))]);
        let state = data.state(thread);
        let mut lines =
            vec![Line::from(vec![label("STATE"), Span::styled(state.label(), Style::new().fg(state.color()))])];
        let owner = summary.and_then(|s| s.owner.clone()).or_else(|| self.messages().find_map(|e| e.owner.clone()));
        lines.push(field("OWNER", owner.map_or_else(|| "none".to_owned(), |o| o.to_string())));
        let ants = self.ants(data, calls);
        lines.push(Line::from(label("ANTS")));
        if ants.is_empty() {
            lines.push(Line::styled("  none", style::dim()));
        }
        let progress = data.live.progress.get(thread);
        for ant in ants {
            match progress.and_then(|p| p.status.of(ant)) {
                Some(stand) => lines.push(Line::from(vec![
                    Span::raw(format!("  {ant} ")),
                    Span::styled(cut(&format!("◐ {stand}"), width.saturating_sub(ant.as_str().len() + 3)), busy()),
                ])),
                None => lines.push(Line::raw(format!("  {ant}"))),
            }
        }
        if let Some(turns) = self.turns() {
            lines.push(field("TURNS", format!("{turns} left")));
        }
        if let Some((cost, tokens)) = self.spend() {
            lines.push(field("SPEND", format!("${cost:.2} · {} tokens", count(tokens))));
        }
        let first = self.messages().next();
        if let Some(at) = summary.map(|s| s.first_at).or(first.map(|e| e.stored_at)) {
            lines.push(field("AGE", crate::colony::short(crate::colony::since(at, now))));
        }
        if let Some(first) = first {
            lines.push(field("FROM", format!("{} {}", origin(first, data), hm(first.stored_at))));
        }
        let flags = self.flags(data, now);
        let snoozed = crate::tui::home::snoozed(data, now).contains_key(thread);
        let held: Vec<&str> = [
            (flags.pinned, "pinned"),
            (flags.muted, "muted"),
            (flags.archived, "archived"),
            (flags.done, "done"),
            (snoozed, "snoozed"),
        ]
        .into_iter()
        .filter_map(|(on, name)| on.then_some(name))
        .collect();
        if !held.is_empty() {
            lines.push(field("HELD", held.join(" · ")));
        }
        let paths = data.calls.get(thread).map_or_default(crate::tui::timeline::Calls::paths);
        if !paths.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(label("FILES")));
            lines.extend(paths.into_iter().map(|p| Line::raw(format!("  {}", cut_start(p, width.saturating_sub(2))))));
        }
        lines
    }
}

/// Where the thread came from, as `first`, its first message read, tells: a channel post, a webhook or schedule
/// (which send alike), a provider's wake, a human's ask, or an ant.
fn origin(first: &Entry, data: &Data) -> String {
    let from = first.acting.label(first.from.as_ref().map_or("a human", UserId::as_str));
    match (&first.channel, first.kind) {
        (Some(channel), Kind::Post | Kind::Mention) => format!("#{channel} post"),
        _ if first.webhook => format!("webhook {from}"),
        _ if let Some(provider) = &first.provider => format!("{provider} wake"),
        _ if first.from.as_ref().is_none_or(|u| data.is_human(u)) => format!("ask by {from}"),
        _ => from,
    }
}

/// `path` in at most `width` characters, an ellipsis beginning it when cut, so its file name shows.
fn cut_start(path: &str, width: usize) -> String {
    let count = path.chars().count();
    if count <= width {
        return path.to_owned();
    }
    let skip = count - width.saturating_sub(1);
    format!("…{}", path.chars().skip(skip).collect::<String>())
}

/// `n` tokens, short: `950`, `61k`, `1.2M`.
fn count(n: u64) -> String {
    match n {
        0..1000 => n.to_string(),
        1000..1_000_000 => format!("{}k", n / 1000),
        _ => format!("{}.{}M", n / 1_000_000, n % 1_000_000 / 100_000),
    }
}
