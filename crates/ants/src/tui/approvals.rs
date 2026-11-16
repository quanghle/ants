//! Tool request cards: the pending tool requests, on home and on their thread, with the ant, the tool and its input,
//! answered with a, A or r then y through the path of `ants tools approve` and `reject`, the first decision winning
//! ([`crate::live::Feed::decide`]); Enter shows a request in full as `ants dash` does. A request settled while the UI
//! runs keeps its card a while ([`SETTLED_FOR`]), saying how it was settled and by whom, unless the human decided it
//! here.

use ants_proto::{ApprovalRequest, ThreadId, Verdict};
use chrono::{DateTime, Local, TimeDelta, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph, Wrap};

use super::frame;
use super::model::{Data, Key};
use crate::live;
use crate::tools::{self, State};

/// How long the card of a request settled while the UI runs stays.
pub const SETTLED_FOR: TimeDelta = TimeDelta::minutes(2);
/// How many settled requests are kept at most.
const SETTLED_KEEP: usize = 64;

/// The requests settled while the UI runs, the latest last: each with how it was settled and when word of it came;
/// and the requests the human answered here, the latest last.
#[derive(Debug, Default)]
pub struct Settled(Vec<(ApprovalRequest, State, DateTime<Utc>)>, Vec<ApprovalRequest>);

impl Settled {
    /// Take note that the human answers `request` here.
    pub fn deciding(&mut self, request: &ApprovalRequest) {
        self.1.retain(|r| !same(r, request));
        self.1.push(request.clone());
        let over = self.1.len().saturating_sub(SETTLED_KEEP);
        self.1.drain(..over);
    }

    /// Whether `me` settled `request` as `state` with an answer given here: its card need not stay.
    pub fn decided_here(&self, request: &ApprovalRequest, state: &State, me: &ants_proto::UserId) -> bool {
        matches!(state, State::Decided(d) if d.by.as_ref() == Some(me)) && self.1.iter().any(|r| same(r, request))
    }

    pub fn add(&mut self, request: ApprovalRequest, state: State, now: DateTime<Utc>) {
        self.0.retain(|(r, ..)| !same(r, &request));
        self.0.push((request, state, now));
        let over = self.0.len().saturating_sub(SETTLED_KEEP);
        self.0.drain(..over);
    }
}

/// Whether `a` and `b` are the same request: ids are unique per ant only.
fn same(a: &ApprovalRequest, b: &ApprovalRequest) -> bool {
    a.ant == b.ant && a.id == b.id
}

/// A request's card: pending (no state), or settled lately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Card<'a> {
    pub request: &'a ApprovalRequest,
    pub settled: Option<&'a State>,
}

/// The cards at `now`, of `thread` or of every thread: the pending requests oldest first, then those settled lately.
pub fn cards<'a>(data: &'a Data, thread: Option<&ThreadId>, now: DateTime<Utc>) -> Vec<Card<'a>> {
    let on = |r: &ApprovalRequest| thread.is_none_or(|t| r.thread_id == *t);
    let mut pending: Vec<Card> =
        data.live.approvals.iter().filter(|r| on(r)).map(|request| Card { request, settled: None }).collect();
    pending.sort_by_key(|c| c.request.requested_at);
    let settled = data.settled.0.iter().filter(|(r, _, at)| on(r) && now - *at < SETTLED_FOR);
    pending.extend(settled.map(|(request, state, _)| Card { request, settled: Some(state) }));
    pending
}

/// How `state` settled a request of `ant`, as a card says it: `allowed once by alice at 10:05`.
pub fn how(state: &State, ant: &ants_proto::UserId) -> String {
    let verdict = |v: Verdict| match v {
        Verdict::AllowOnce => "allowed once",
        Verdict::AllowAlways => "allowed always",
        Verdict::Reject => "rejected",
    };
    let hm = |at: DateTime<Utc>| at.with_timezone(&Local).format("%H:%M").to_string();
    match state {
        State::Pending => "pending".to_owned(),
        State::Decided(d) => {
            let by = d.by.as_ref().map_or("a human", ants_proto::UserId::as_str);
            format!("{} by {by} at {}", verdict(d.verdict), hm(d.decided_at))
        }
        State::DecidedByAnt(by, d) => {
            format!("{} by {} at {}", verdict(d.verdict), crate::tools::deciders(by, d), hm(d.decided_at))
        }
        State::Expired => format!("expired unanswered: {ant} rejected it"),
    }
}

/// The keys answering `request`, as offered.
fn keys(request: &ApprovalRequest) -> String {
    [(Verdict::AllowOnce, "a allow"), (Verdict::AllowAlways, "A always")]
        .into_iter()
        .filter(|(v, _)| request.options.contains(v))
        .map(|(_, key)| key)
        .chain(["r reject", "Enter details"])
        .collect::<Vec<_>>()
        .join(" · ")
}

/// The lines of `card`, at most `width` wide: the tool, its input if any, and its keys (with `keyed`, the card the
/// keys answer), the answer being asked or stored, or how it was settled.
pub fn lines(card: Card, asking: Option<&Asking>, keyed: bool, now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
    let request = card.request;
    let tool = format!("{}: {}", request.tool.kind, request.tool.title.replace('\n', " "));
    let mut lines = vec![Line::raw(cut(&tool, width))];
    if let Some(input) = &request.tool.input {
        lines.push(Line::styled(cut(&format!("input {}", input.replace('\n', " ")), width), frame::dim()));
    } else if !request.tool.locations.is_empty() {
        lines.push(Line::styled(cut(&format!("files {}", request.tool.locations.join(", ")), width), frame::dim()));
    }
    let prompt = Style::new().fg(Color::Yellow).bold();
    let asked = asking.filter(|a| same(&a.request, request));
    let last = match (card.settled, asked) {
        (Some(state), _) => Line::styled(cut(&how(state, &request.ant), width), frame::dim()),
        (None, Some(Asking { storing: true, .. })) => Line::styled("storing the decision…", prompt),
        (None, Some(Asking { verdict: Some(v), .. })) => {
            Line::styled(cut(&format!("{}? y confirms · any other key cancels", tools::spelling(*v)), width), prompt)
        }
        (None, _) => {
            let left = format!("expires in {}", tools::left(request.expires_at, now));
            if keyed {
                Line::from(vec![
                    Span::styled(keys(request), Style::new().fg(Color::Yellow)),
                    Span::styled(format!(" · {left}"), frame::dim()),
                ])
            } else {
                Line::styled(left, frame::dim())
            }
        }
    };
    lines.push(last);
    lines
}

/// The title of `card`'s box: who asks, or that it is settled.
pub fn title(card: Card) -> String {
    match card.settled {
        None => format!(" {} asks to run a tool ", card.request.ant),
        Some(_) => format!(" tool request of {} · settled ", card.request.ant),
    }
}

/// A request being answered: its verdict waiting for y, or the request shown in full over the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asking {
    pub request: ApprovalRequest,
    /// Where the request stands, as last known.
    pub state: State,
    /// The verdict asked for, waiting for its confirmation.
    pub verdict: Option<Verdict>,
    /// Whether the request shows in full over the view.
    pub full: bool,
    /// Whether the decision is being stored.
    pub storing: bool,
}

/// What a key does to the request being answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Nothing more, the request still being answered.
    Keep,
    /// The answering ends, with a notice saying so if any.
    Close(Option<String>),
    /// A notice to show, the request still shown in full.
    Note(String),
    /// The verdict is refused, and why.
    Refused(String),
    /// Store the decision on the request.
    Decide(Verdict),
    /// Open the request's thread.
    Open(ThreadId),
}

impl Asking {
    /// Answer the pending `request`: with `verdict`, waiting for y; without, shown in full. Refused for a verdict the
    /// request does not offer.
    pub fn new(request: ApprovalRequest, verdict: Option<Verdict>) -> Result<Self, String> {
        let state = State::Pending;
        if let Some(refusal) = verdict.and_then(|v| live::refusal(&request, &state, v)) {
            return Err(refusal);
        }
        Ok(Self { full: verdict.is_none(), request, state, verdict, storing: false })
    }

    /// Whether keys go to it rather than to the view: while shown in full or waiting for y.
    pub fn takes_keys(&self) -> bool {
        self.full || (self.verdict.is_some() && !self.storing)
    }

    pub fn key(&mut self, key: Key) -> Answer {
        if self.storing {
            if self.full && matches!(key, Key::Esc | Key::Char('q')) {
                self.full = false;
            }
            return Answer::Keep;
        }
        let id = tools::qualified(&self.request);
        if let Some(verdict) = self.verdict.take() {
            if key == Key::Char('y') {
                self.verdict = Some(verdict);
                self.storing = true;
                return Answer::Decide(verdict);
            }
            let line = format!("request {id} not answered");
            return if self.full { Answer::Note(line) } else { Answer::Close(Some(line)) };
        }
        let verdict = match key {
            Key::Esc | Key::Char('q') => return Answer::Close(None),
            Key::Enter => return Answer::Open(self.request.thread_id.clone()),
            Key::Char('a') => Verdict::AllowOnce,
            Key::Char('A') => Verdict::AllowAlways,
            Key::Char('r') => Verdict::Reject,
            _ => return Answer::Keep,
        };
        match live::refusal(&self.request, &self.state, verdict) {
            Some(refusal) => Answer::Refused(refusal),
            None => {
                self.verdict = Some(verdict);
                Answer::Keep
            }
        }
    }

    /// Take word that `request` was settled as `state`: whether the answering goes on, shown in full or storing.
    pub fn settled(&mut self, request: &ApprovalRequest, state: &State) -> bool {
        if !same(&self.request, request) {
            return true;
        }
        self.state = state.clone();
        if !self.storing {
            self.verdict = None;
        }
        self.full || self.storing
    }

    /// The footer while it waits for y, not shown in full.
    pub fn footer(&self) -> Option<Line<'static>> {
        let verdict = self.verdict.filter(|_| !self.full && !self.storing)?;
        let text = format!(
            " Answer {} with {}? y confirms, any other key cancels",
            tools::qualified(&self.request),
            tools::spelling(verdict)
        );
        Some(Line::styled(text, Style::new().fg(Color::Yellow).bold()))
    }
}

/// The request being answered in full over the screen, as `ants dash` shows it.
pub fn draw_full(f: &mut Frame, asking: &Asking, now: DateTime<Utc>) {
    let [_, area, _] =
        Layout::vertical([Constraint::Percentage(15), Constraint::Percentage(70), Constraint::Percentage(15)])
            .areas(f.area());
    let [_, area, _] =
        Layout::horizontal([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)])
            .areas(area);
    let request = &asking.request;
    let mut lines: Vec<Line> =
        tools::details(request, &asking.state, now).lines().map(|l| Line::raw(l.to_owned())).collect();
    if asking.state != State::Pending {
        lines.push(Line::raw(format!("settled  {}", how(&asking.state, &request.ant))));
    }
    lines.push(Line::raw(""));
    let id = tools::qualified(request);
    let prompt = Style::new().fg(Color::Yellow).bold();
    if asking.storing {
        lines.push(Line::styled("storing the decision…", prompt));
    } else if let Some(verdict) = asking.verdict {
        let verdict = tools::spelling(verdict);
        lines.push(Line::styled(format!("Answer {id} with {verdict}? y confirms, any other key cancels"), prompt));
    } else if asking.state == State::Pending {
        let keys = keys(request).replace("Enter details", "Enter open its thread · Esc back");
        lines.push(Line::styled(keys, prompt));
    } else {
        lines.push(Line::styled("answered already · Enter open its thread · Esc back", prompt));
    }
    f.render_widget(Clear, area);
    let block = Block::bordered().title(format!(" Tool request {id} ")).border_style(Style::new().fg(Color::Yellow));
    f.render_widget(Paragraph::new(lines).block(block).wrap(Wrap { trim: false }), area);
}

/// `text` in at most `width` characters, an ellipsis ending it when cut.
fn cut(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
mod tests {
    use ants_proto::ApprovalDecision;

    use super::*;
    use crate::live::tests::{at, request, user};
    use crate::tui::model::tests::model;

    fn text(lines: &[Line]) -> Vec<String> {
        lines.iter().map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect()).collect()
    }

    #[test]
    fn a_verdict_waits_for_y_and_any_other_key_cancels_it() {
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        let id = tools::qualified(&push);
        assert_eq!(
            Asking::new(push.clone(), Some(Verdict::AllowAlways)),
            Err(format!("allow-always is not offered for request {id}"))
        );
        let mut asking = Asking::new(push.clone(), Some(Verdict::AllowOnce)).unwrap();
        assert!(asking.takes_keys() && !asking.full);
        assert!(asking.footer().is_some_and(|l| l.to_string().contains("with allow-once? y confirms")));
        assert_eq!(asking.key(Key::Char('n')), Answer::Close(Some(format!("request {id} not answered"))));

        let mut asking = Asking::new(push, Some(Verdict::Reject)).unwrap();
        assert_eq!(asking.key(Key::Char('y')), Answer::Decide(Verdict::Reject));
        assert!(asking.storing && !asking.takes_keys(), "keys go back to the view while it is stored");
    }

    #[test]
    fn shown_in_full_it_answers_opens_the_thread_and_follows_word_of_a_decision() {
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        let mut asking = Asking::new(push.clone(), None).unwrap();
        assert!(asking.full && asking.takes_keys());
        assert_eq!(asking.key(Key::Char('x')), Answer::Keep);
        assert_eq!(asking.key(Key::Enter), Answer::Open(push.thread_id.clone()));
        assert_eq!(asking.key(Key::Char('a')), Answer::Keep);
        assert_eq!(asking.verdict, Some(Verdict::AllowOnce));
        assert!(matches!(asking.key(Key::Char('n')), Answer::Note(_)), "it stays shown");
        asking.key(Key::Char('a'));
        let decision = ApprovalDecision::new(push.id, push.ant.clone(), Verdict::Reject);
        let state = State::DecidedByAnt(user("lead"), decision);
        assert!(asking.settled(&push, &state), "shown in full, it stays");
        assert_eq!((&asking.state, asking.verdict), (&state, None));
        assert!(matches!(asking.key(Key::Char('a')), Answer::Refused(e) if e.contains("answered already")));
        assert_eq!(asking.key(Key::Esc), Answer::Close(None));

        let mut asking = Asking::new(push.clone(), Some(Verdict::AllowOnce)).unwrap();
        assert!(!asking.settled(&push, &State::Expired), "waiting for y, it ends");
    }

    #[test]
    fn cards_list_the_pending_requests_then_those_settled_lately_with_how() {
        let mut m = model();
        let (push, rm) = (request("dev-a", "git push", vec![Verdict::AllowOnce]), request("dev-b", "rm -rf x", vec![]));
        m.data.live.approvals = vec![push.clone(), rm.clone()];
        let mut decision = ApprovalDecision::new(rm.id, rm.ant.clone(), Verdict::Reject);
        decision.by = Some(user("bob"));
        m.data.settled.add(rm.clone(), State::Decided(decision), at(0));
        m.data.live.approvals.retain(|r| r.id != rm.id);
        let shown = cards(&m.data, None, at(10));
        assert_eq!(
            shown.iter().map(|c| (&c.request.ant, c.settled.is_some())).collect::<Vec<_>>(),
            [(&push.ant, false), (&rm.ant, true)]
        );
        assert_eq!(cards(&m.data, Some(&push.thread_id), at(10)).len(), 1);
        assert_eq!(cards(&m.data, None, at(200)).len(), 1, "a settled card goes after a while");

        let pending = text(&lines(shown[0], None, true, at(10), 60));
        assert_eq!(pending[0], "execute: git push");
        assert!(pending[1].starts_with("a allow · r reject · Enter details · expires in "), "{pending:?}");
        let settled = text(&lines(shown[1], None, false, at(10), 60));
        assert!(settled[1].starts_with("rejected by bob at "), "{settled:?}");
        let asking = Asking::new(push, Some(Verdict::AllowOnce)).unwrap();
        assert_eq!(
            text(&lines(shown[0], Some(&asking), true, at(10), 60))[1],
            "allow-once? y confirms · any other key cancels"
        );
    }
}
