//! Tool request cards: the pending tool requests, on home and on their thread, with the ant, the tool and its input,
//! answered at once with a, A or r through the path of `ants tools approve` and `reject`, the first decision winning
//! ([`crate::live::Feed::decide`]); Enter shows a request in full ([`full`]), where a, A or r answer it too. n opens
//! the command bar on a note: Enter rejects the request, and once the rejection is stored sends the note to the ant on
//! the request's thread, as `ants tools reject --note` does; refused, the note comes back into the bar.
//! A request settled while the UI runs keeps its card a while ([`SETTLED_FOR`]), saying how it was settled and by
//! whom, unless the human decided it here.

use std::cell::Cell;

use ants_proto::{ApprovalRequest, ThreadId, Verdict};
use chrono::{DateTime, Local, TimeDelta, Utc};
use ratatui::Frame;
use ratatui::style::Style;
use ratatui::text::{Line, Span};

use super::model::{Data, Key};
use super::style;
use crate::live;
use crate::tools::{self, State};

mod full;
mod text;

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
        .chain(["r reject", "n reject with a note", "Enter details"])
        .collect::<Vec<_>>()
        .join(" · ")
}

/// How many lines the card the keys answer gives what its request asks, at most.
pub const CARD_LINES: usize = 6;
/// How wide the compact pane's one line shows a request, no card being drawn there.
const COMPACT_WIDTH: usize = 100;

/// What `request` asks as its card shows it, wrapped at `width`: the tool's kind, then its command, its file and content
/// or its input ([`text::asked`]).
fn card_text(request: &ApprovalRequest, width: usize) -> Vec<Line<'static>> {
    let mut asked = text::asked(request);
    if let Some(first) = asked.first_mut() {
        first.spans.insert(0, Span::raw(format!("{}: ", request.tool.kind)));
    }
    text::wrapped(&asked, width)
}

/// Whether what `request` asks shows whole on the card the keys answer, as last drawn: `width` wide over `rows` lines;
/// with no card drawn, on the compact pane's one line. A key answers at once only a request that does.
pub fn fits(request: &ApprovalRequest, room: Option<(usize, usize)>) -> bool {
    let (width, rows) = room.unwrap_or((COMPACT_WIDTH, 1));
    card_text(request, width).len() <= rows
}

/// The lines of `card`, at most `width` wide: with `keyed`, the card the keys answer, what the request asks over `rows`
/// lines at most, how many more are left, and its keys; else the tool on one line; then that its answer is being stored,
/// or how it was settled.
pub fn lines(
    card: Card,
    asking: Option<&Asking>,
    keyed: bool,
    now: DateTime<Utc>,
    (width, rows): (usize, usize),
) -> Vec<Line<'static>> {
    let request = card.request;
    let mut lines = if keyed && card.settled.is_none() {
        let mut lines = card_text(request, width);
        let left = lines.len().saturating_sub(rows);
        lines.truncate(rows);
        if left > 0 {
            let what = if left == 1 { "line" } else { "lines" };
            lines.push(Line::styled(format!("… {left} more {what}: Enter shows them"), style::dim()));
        }
        lines
    } else {
        let tool = format!("{}: {}", request.tool.kind, request.tool.title.replace('\n', " "));
        vec![Line::raw(cut(&tool, width))]
    };
    if keyed && request.tool.input.is_none() && !request.tool.locations.is_empty() {
        lines.push(Line::styled(cut(&format!("files {}", request.tool.locations.join(", ")), width), style::dim()));
    }
    if let Some(always) = request.label(Verdict::AllowAlways).filter(|_| card.settled.is_none()) {
        lines.push(Line::styled(cut(&format!("A always: {always}"), width), style::dim()));
    }
    let storing = asking.filter(|a| a.storing && same(&a.request, request));
    let last = match (card.settled, storing) {
        (Some(state), _) => Line::styled(cut(&how(state, &request.ant), width), style::dim()),
        (None, Some(asking)) => Line::styled(asking.storing_what(), Style::new().fg(style::PENDING).bold()),
        (None, None) => {
            let left = format!("expires in {}", tools::left(request.expires_at, now));
            if keyed {
                Line::from(vec![
                    Span::styled(
                        style::hints(&keys(request), width.saturating_sub(left.chars().count() + 3)),
                        Style::new().fg(style::PENDING),
                    ),
                    Span::styled(format!(" · {left}"), style::dim()),
                ])
            } else {
                Line::styled(left, style::dim())
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

/// A request being answered: its decision being stored, or the request shown in full over the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asking {
    pub request: ApprovalRequest,
    /// Where the request stands, as last known.
    pub state: State,
    /// Whether the request shows in full over the view.
    pub full: bool,
    /// Whether the decision is being stored.
    pub storing: bool,
    /// The note to send the ant once the rejection being stored is stored.
    pub note: Option<String>,
    /// The first row of the body shown in full, and how many rows a page scrolls, as last drawn.
    top: Cell<usize>,
    page: Cell<usize>,
}

/// What a key does to the request being answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Nothing more, the request still being answered.
    Keep,
    /// The answering ends.
    Close,
    /// The verdict is refused, and why.
    Refused(String),
    /// Store the decision on the request.
    Decide(Verdict),
    /// Open the request's thread.
    Open(ThreadId),
    /// Open the command bar on a note rejecting the request.
    Note,
}

impl Asking {
    /// Show the pending `request` in full.
    pub fn full(request: ApprovalRequest) -> Self {
        Self {
            request,
            state: State::Pending,
            full: true,
            storing: false,
            note: None,
            top: Cell::default(),
            page: Cell::default(),
        }
    }

    /// Answer the pending `request` with `verdict` at once, its decision then being stored. Refused for a verdict the
    /// request does not offer.
    pub fn decide(request: ApprovalRequest, verdict: Verdict) -> Result<Self, String> {
        let state = State::Pending;
        if let Some(refusal) = live::refusal(&request, &state, verdict) {
            return Err(refusal);
        }
        Ok(Self { request, state, full: false, storing: true, note: None, top: Cell::default(), page: Cell::default() })
    }

    /// Whether keys go to it rather than to the view: while shown in full.
    pub fn takes_keys(&self) -> bool {
        self.full
    }

    pub fn key(&mut self, key: Key) -> Answer {
        if self.full
            && let Some(top) = full::scrolled(key, self.top.get(), self.page.get())
        {
            self.top.set(top);
            return Answer::Keep;
        }
        if self.storing {
            if self.full && matches!(key, Key::Esc | Key::Char('q')) {
                self.full = false;
            }
            return Answer::Keep;
        }
        let verdict = match key {
            Key::Esc | Key::Char('q') => return Answer::Close,
            Key::Enter => return Answer::Open(self.request.thread_id.clone()),
            Key::Char('n') => {
                return live::refusal(&self.request, &self.state, Verdict::Reject)
                    .map_or(Answer::Note, Answer::Refused);
            }
            Key::Char('a') => Verdict::AllowOnce,
            Key::Char('A') => Verdict::AllowAlways,
            Key::Char('r') => Verdict::Reject,
            _ => return Answer::Keep,
        };
        match live::refusal(&self.request, &self.state, verdict) {
            Some(refusal) => Answer::Refused(refusal),
            None => {
                self.storing = true;
                Answer::Decide(verdict)
            }
        }
    }

    /// Take word that `request` was settled as `state`: whether the answering goes on, shown in full or storing.
    pub fn settled(&mut self, request: &ApprovalRequest, state: &State) -> bool {
        if !same(&self.request, request) {
            return true;
        }
        self.state = state.clone();
        self.full || self.storing
    }

    /// The line saying its decision is being stored, not shown in full: for the compact pane, which shows no card.
    pub fn status(&self) -> Option<Line<'static>> {
        (self.storing && !self.full).then(|| {
            let id = tools::qualified(&self.request);
            let text = match self.note {
                Some(_) => format!(" storing the rejection of request {id}, then sending the note…"),
                None => format!(" storing the decision on request {id}…"),
            };
            Line::styled(text, Style::new().fg(style::PENDING).bold())
        })
    }

    /// What is being stored, said on the request's card.
    fn storing_what(&self) -> &'static str {
        if self.note.is_some() { "storing the rejection, then sending the note…" } else { "storing the decision…" }
    }
}

/// The request being answered in full over the screen.
pub fn draw_full(f: &mut Frame, asking: &Asking, now: DateTime<Utc>) {
    full::draw(f, asking, now);
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
    use crate::test_support::live::{at, request, user};
    use crate::tui::test_support::model;

    fn text(lines: &[Line]) -> Vec<String> {
        lines.iter().map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect()).collect()
    }

    #[test]
    fn a_verdict_offered_is_stored_at_once() {
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        let id = tools::qualified(&push);
        assert_eq!(
            Asking::decide(push.clone(), Verdict::AllowAlways),
            Err(format!("allow-always is not offered for request {id}"))
        );
        let asking = Asking::decide(push, Verdict::AllowOnce).unwrap();
        assert!(asking.storing && !asking.full && !asking.takes_keys(), "keys stay with the view while it is stored");
        assert!(asking.status().is_some_and(|l| l.to_string() == format!(" storing the decision on request {id}…")));

        let always = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::AllowAlways]);
        let mut asking = Asking::full(always);
        assert_eq!(asking.key(Key::Char('A')), Answer::Decide(Verdict::AllowAlways), "allow always needs no more");
        assert!(asking.storing && asking.full && asking.status().is_none());
        assert_eq!(asking.key(Key::Char('r')), Answer::Keep, "one decision at a time");
    }

    #[test]
    fn shown_in_full_it_answers_opens_the_thread_and_follows_word_of_a_decision() {
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        let mut asking = Asking::full(push.clone());
        assert!(asking.full && asking.takes_keys());
        assert_eq!(asking.key(Key::Char('x')), Answer::Keep);
        assert_eq!(asking.key(Key::Enter), Answer::Open(push.thread_id.clone()));
        assert_eq!(asking.key(Key::Char('n')), Answer::Note);
        assert!(matches!(asking.key(Key::Char('A')), Answer::Refused(e) if e.contains("not offered")));
        let decision = ApprovalDecision::new(push.id, push.ant.clone(), Verdict::Reject);
        let state = State::DecidedByAnt(user("lead"), decision);
        assert!(asking.settled(&push, &state), "shown in full, it stays");
        assert_eq!(asking.state, state);
        assert!(matches!(asking.key(Key::Char('a')), Answer::Refused(e) if e.contains("answered already")));
        assert!(matches!(asking.key(Key::Char('n')), Answer::Refused(e) if e.contains("answered already")));
        assert_eq!(asking.key(Key::Esc), Answer::Close);

        let mut asking = Asking::decide(push.clone(), Verdict::AllowOnce).unwrap();
        assert!(asking.settled(&push, &State::Expired), "storing, it waits for the outcome");
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

        let pending = text(&lines(shown[0], None, true, at(10), (80, CARD_LINES)));
        assert_eq!(pending[0], "execute: git push");
        assert!(
            pending[1].starts_with("a allow · r reject · n reject with a note · Enter details · expires in "),
            "{pending:?}"
        );
        let narrow = text(&lines(shown[0], None, true, at(10), (60, CARD_LINES)));
        assert_eq!(narrow[1], "a allow · r reject · n reject with a note · expires in 0s", "the last hints go whole");
        let settled = text(&lines(shown[1], None, false, at(10), (60, CARD_LINES)));
        assert!(settled[1].starts_with("rejected by bob at "), "{settled:?}");
        let asking = Asking::decide(push, Verdict::AllowOnce).unwrap();
        assert_eq!(text(&lines(shown[0], Some(&asking), true, at(10), (60, CARD_LINES)))[1], "storing the decision…");
    }

    #[test]
    fn a_pending_card_and_the_request_in_full_name_what_allow_always_allows() {
        let always = "Yes, and don't ask again for git push commands in /work";
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::AllowAlways])
            .labelled([(Verdict::AllowAlways, always.to_owned())].into());
        let card = Card { request: &push, settled: None };
        let shown = text(&lines(card, None, true, at(10), (80, CARD_LINES)));
        assert_eq!(shown[1], format!("A always: {always}"), "{shown:?}");
        assert!(shown[2].starts_with("a allow · A always · r reject"), "{shown:?}");
        assert_eq!(text(&lines(card, None, true, at(10), (20, CARD_LINES)))[1], "A always: Yes, and …");
        let full = tools::details(&push, &State::Pending, at(10));
        assert!(full.contains(&format!("\nalways   {always}\n")), "{full}");

        let expired = Card { request: &push, settled: Some(&State::Expired) };
        assert!(
            !text(&lines(expired, None, false, at(10), (80, CARD_LINES))).iter().any(|l| l.contains(always)),
            "settled"
        );
    }
}
