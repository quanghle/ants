//! What the human's assistant did for it that waits for its consent: the admin orders machines' daemons hold, gathered
//! every while as `ants assistant confirm` lists them, and the decisions on tool requests their ants hold, as following
//! the tool requests tells them ([`crate::tools::follow`]). Counted on home, and listed on the page `o` opens there,
//! where `a` confirms the selected one and `r` denies it once `y` gives the answer, as `ants assistant confirm ACT`
//! (with `--deny`) does.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{Act, ActAnswer, AdminOrder, ApprovalRequest, ORDER_HOLD, UserId, Uuid};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};

use super::model::{Action, Data, Key, To};
use super::select::Selection;
use super::style::{self, ago};
use crate::live::maybe;
use crate::tools::{self, HeldAct};

/// How often the held orders are gathered.
const GATHER_EVERY: Duration = Duration::from_secs(15);

/// A decision the human's assistant made for it on a tool request, which the request's ant holds until the human
/// consents or the request expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub request: ApprovalRequest,
    pub act: HeldAct,
}

/// What waits for the human's consent.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Order(Box<AdminOrder>),
    Decision(Box<Decision>),
}

// An order's values are JSON, whose equality is total.
impl Eq for Item {}

impl Item {
    fn id(&self) -> Uuid {
        match self {
            Self::Order(order) => order.id,
            Self::Decision(decision) => decision.request.id,
        }
    }

    /// The assistant that sent it.
    fn assistant(&self) -> &UserId {
        match self {
            Self::Order(order) => &order.admin,
            Self::Decision(decision) => &decision.act.assistant,
        }
    }

    fn sent_at(&self) -> DateTime<Utc> {
        match self {
            Self::Order(order) => order.sent_at,
            Self::Decision(decision) => decision.act.sent_at,
        }
    }

    fn expires_at(&self) -> DateTime<Utc> {
        match self {
            Self::Order(order) => order.sent_at + ORDER_HOLD,
            Self::Decision(decision) => decision.request.expires_at,
        }
    }

    /// Whether `other` is the same: the same order, or the same decision on the same request.
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Order(a), Self::Order(b)) => a.id == b.id,
            (Self::Decision(a), Self::Decision(b)) => a == b,
            _ => false,
        }
    }

    /// As the command line names it: `MACHINE/ANT/ID` for an order, `ANT/ID` for a decision.
    fn qualified(&self) -> String {
        match self {
            Self::Order(o) => Act::Order { machine: o.machine.clone(), ant: o.ant.clone(), id: o.id }.qualified(),
            Self::Decision(decision) => tools::qualified(&decision.request),
        }
    }

    /// What it is, in a few words: `pause of dev-a on m1`, `allow-once on dev-a's execute`.
    fn what(&self) -> String {
        match self {
            Self::Order(o) => format!("{} of {} on {}", o.op.describe(), o.ant, o.machine),
            Self::Decision(d) => {
                format!("{} on {}'s {}", tools::spelling(d.act.verdict), d.request.ant, d.request.tool.kind)
            }
        }
    }

    /// Whom it is from, and what it is: `pa's order m1/dev-a/<id> (pause)`, `pa's allow-once on request dev-a/<id>
    /// (execute: git push)`.
    fn describe(&self) -> String {
        match self {
            Self::Order(order) => format!("{}'s order {} ({})", order.admin, self.qualified(), order.op.describe()),
            Self::Decision(d) => format!(
                "{}'s {} on request {} ({}: {})",
                d.act.assistant,
                tools::spelling(d.act.verdict),
                self.qualified(),
                d.request.tool.kind,
                d.request.tool.title.lines().next().unwrap_or_default()
            ),
        }
    }
}

/// What waits for the human's consent, oldest first: the orders held for it, and the decisions its assistant made on
/// the pending tool requests.
pub fn items(data: &Data) -> Vec<Item> {
    let me = data.me();
    let mut items: Vec<Item> = data.held.orders.iter().map(|order| Item::Order(Box::new(order.clone()))).collect();
    for request in &data.live.approvals {
        let acts = data.live.held.get(&(request.ant.clone(), request.id)).into_iter().flatten();
        for act in acts.filter(|act| act.human == *me) {
            items.push(Item::Decision(Box::new(Decision { request: request.clone(), act: act.clone() })));
        }
    }
    items.sort_by_key(Item::sent_at);
    items
}

/// What waits for an answer, as home's line counts it; None when nothing does.
pub fn line(data: &Data) -> Option<Line<'static>> {
    let waiting: Vec<Item> = items(data).into_iter().filter(|item| data.held.answer(item).is_none()).collect();
    let first = waiting.first()?;
    let orders = waiting.iter().filter(|item| matches!(item, Item::Order(_))).count();
    let noun = match (orders, waiting.len() - orders) {
        (1, 0) => "order",
        (_, 0) => "orders",
        (0, 1) => "decision",
        (0, _) => "decisions",
        _ => "orders and decisions",
    };
    let what: Vec<String> = waiting.iter().map(Item::what).collect();
    let text = format!(
        "{} {noun} of {} wait{} for confirmation: {} · o lists them",
        waiting.len(),
        first.assistant(),
        if waiting.len() == 1 { "s" } else { "" },
        what.join(", ")
    );
    Some(Line::styled(text, Style::new().fg(style::PENDING)))
}

/// An answer to what waits for the human's consent, as the key `y` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub held: Item,
    pub answer: ActAnswer,
}

impl Answer {
    /// The answer, as the key `y` gives it: `confirm pa's order m1/dev-a/<id> (pause)`.
    pub fn describe(&self) -> String {
        let verb = match self.answer {
            ActAnswer::Confirm => "confirm",
            ActAnswer::Deny => "deny",
        };
        format!("{verb} {}", self.held.describe())
    }
}

/// The time left until `expires`, in days and hours past a day.
fn left(expires: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let delta = expires - now;
    match delta.num_days() {
        0 => crate::tools::left(expires, now),
        days => format!("{days}d{:02}h", delta.num_hours() % 24),
    }
}

/// The orders held for the human, how the latest gathering went, and the answers to them and to the decisions held.
#[derive(Debug, Default)]
pub struct Held {
    /// Oldest first.
    pub orders: Vec<AdminOrder>,
    /// When the orders were last gathered, or why that failed.
    pub read: Option<Result<DateTime<Utc>, String>>,
    /// The answer being recorded.
    pub busy: Option<Answer>,
    /// The answers recorded: the daemon holding an order, or the ant holding a decision, decides it soon after, and
    /// drops it.
    answered: Vec<Answer>,
}

impl Held {
    /// The answer recorded to `item`, if any.
    fn answer(&self, item: &Item) -> Option<ActAnswer> {
        self.answered.iter().find(|a| a.held.same(item)).map(|a| a.answer)
    }

    /// Forget the answers to decisions no longer waiting among `items`, decided or expired since.
    pub fn settled(&mut self, items: &[Item]) {
        self.answered.retain(|a| matches!(a.held, Item::Order(_)) || items.iter().any(|item| a.held.same(item)));
    }

    pub fn take(&mut self, event: Event, now: DateTime<Utc>) -> Option<Result<String, String>> {
        match event {
            Event::Read(Ok(orders)) => {
                self.answered.retain(|a| match &a.held {
                    Item::Order(order) => orders.iter().any(|o| o.id == order.id),
                    Item::Decision(_) => true,
                });
                self.orders = orders;
                self.read = Some(Ok(now));
                None
            }
            Event::Read(Err(e)) => {
                self.read = Some(Err(e));
                None
            }
            Event::Done(answer, done) => {
                if self.busy.as_ref() == Some(&*answer) {
                    self.busy = None;
                }
                if done.is_ok() {
                    self.answered.push(*answer);
                }
                Some(done)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Page {
    selected: Selection<Uuid>,
    /// The answer the next `y` gives.
    pub armed: Option<Answer>,
}

impl Page {
    pub const ARMED_KEYS: &str = "y gives the answer above · any other key leaves it held";

    /// Drop the confirmation asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.armed.take().is_some()
    }

    /// The keys of the answer `a` or `r` asked to confirm, while it waits.
    pub fn prompt(&self) -> Option<&'static str> {
        self.armed.as_ref().map(|_| Self::ARMED_KEYS)
    }

    fn ids(items: &[Item]) -> Vec<Uuid> {
        items.iter().map(Item::id).collect()
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let held = &data.held;
        let items = items(data);
        let ids = Self::ids(&items);
        let at = self.selected.at(&ids);
        let armed = self.armed.take();
        let to = match key {
            Key::Char('a' | 'r') if held.busy.is_none() => {
                let answer = if key == Key::Char('r') { ActAnswer::Deny } else { ActAnswer::Confirm };
                self.armed = items
                    .get(at)
                    .filter(|item| held.answer(item).is_none())
                    .map(|item| Answer { held: item.clone(), answer });
                return None;
            }
            Key::Char('y') => return armed.map(|answer| To::Do(Action::Answer(answer))),
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.selected.select(&ids, to);
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let held = &data.held;
        let items = items(data);
        let block = style::block(format!(" Held for confirmation ({}) ", items.len()), focused);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let read = match &held.read {
            None => "gathering the orders held for confirmation…".to_owned(),
            Some(Ok(at)) => format!(
                "orders of the assistant held by the machines' daemons until confirmed, for {} days at most, \
                 gathered {}; its decisions on tool requests, held by their ants until the requests expire",
                ORDER_HOLD.num_days(),
                ago(*at, now)
            ),
            Some(Err(e)) => format!("gathering the held orders: {e}"),
        };
        let mut about = vec![Line::styled(read, style::dim())];
        if let Some(answer) = &held.busy {
            about.push(Line::styled(format!("{}…", answer.describe()), Style::new().fg(Color::Cyan)));
        }
        if let Some(answer) = &self.armed {
            about.push(Line::styled(format!("y to {}", answer.describe()), Style::new().fg(Color::Yellow).bold()));
        }
        if items.is_empty() {
            if matches!(held.read, Some(Ok(_))) {
                about.push(Line::raw("nothing held"));
            }
            frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), inner);
            return;
        }
        // The lines but a long reason for a failed gathering fit one row each.
        let tall = u16::try_from(about.len() * 2).unwrap_or(u16::MAX);
        let [top, list] = Layout::vertical([Constraint::Max(tall), Constraint::Min(0)]).areas(inner);
        frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), top);
        let rows: Vec<Text> = items
            .iter()
            .map(|item| {
                let order = matches!(item, Item::Order(_));
                let status = match held.answer(item) {
                    Some(ActAnswer::Confirm) => {
                        let then = if order { "to be carried out" } else { "to take effect" };
                        Span::styled(format!("confirmed, {then}"), Style::new().fg(style::CONNECTED))
                    }
                    Some(ActAnswer::Deny) => {
                        let then = if order { "to be refused" } else { "to be dropped" };
                        Span::styled(format!("denied, {then}"), Style::new().fg(style::ERROR))
                    }
                    None => Span::styled("a confirm · r deny", style::dim()),
                };
                let (kind, what) = match item {
                    Item::Order(o) => {
                        (o.op.kind(), format!("{} ordered {} of {} on {}", o.admin, o.op.describe(), o.ant, o.machine))
                    }
                    Item::Decision(d) => (
                        "tool",
                        format!(
                            "{} chose {} on {}'s {}: {}",
                            d.act.assistant,
                            tools::spelling(d.act.verdict),
                            d.request.ant,
                            d.request.tool.kind,
                            d.request.tool.title.lines().next().unwrap_or_default()
                        ),
                    ),
                };
                let when = format!("  {} · expires in {}", ago(item.sent_at(), now), left(item.expires_at(), now));
                Text::from(vec![
                    Line::from(vec![
                        Span::styled(format!("{kind:<8}"), Style::new().fg(Color::Yellow)),
                        Span::raw(what),
                        Span::styled(when, style::dim()),
                    ]),
                    Line::from(vec![Span::styled(format!("        {} · ", item.qualified()), style::dim()), status]),
                ])
            })
            .collect();
        let selected = self.selected.at(&Self::ids(&items));
        super::mouse::list(frame, list, data, "held", rows, selected, focused);
    }
}

/// What a gathering or an answer brought.
pub enum Event {
    Read(Result<Vec<AdminOrder>, String>),
    /// What recording the answer did, or why it failed.
    Done(Box<Answer>, Result<String, String>),
}

/// The gatherings of the held orders and the answers.
pub struct Io {
    js: jetstream::Context,
    me: UserId,
    /// The account of the team, whose key signs the grant naming the assistant.
    account: Option<String>,
    /// The human device's credentials that sign the answers, or why there are none.
    creds: Result<PathBuf, String>,
    every: tokio::time::Interval,
    gather: Option<LocalBoxFuture<'static, Event>>,
    answer: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: jetstream::Context, me: UserId, account: Option<String>, creds: Result<PathBuf, String>) -> Self {
        let every = tokio::time::interval(GATHER_EVERY);
        Self { js, me, account, creds, every, gather: None, answer: None }
    }

    /// The next gathering or answer done. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.gather.is_none() {
                        self.refresh();
                    }
                }
                event = maybe(&mut self.gather) => {
                    self.gather = None;
                    return event;
                }
                event = maybe(&mut self.answer) => {
                    self.answer = None;
                    self.refresh();
                    return event;
                }
            }
        }
    }

    /// Gather again now.
    pub fn refresh(&mut self) {
        let (js, me) = (self.js.clone(), self.me.clone());
        self.gather = Some(
            async move {
                Event::Read(crate::people::assistant::orders_held_for(&js, &me).await.map_err(|e| format!("{e:#}")))
            }
            .boxed_local(),
        );
    }

    /// Record `answer`, unless another one is being recorded; whether it starts.
    pub fn answer(&mut self, answer: Answer) -> bool {
        if self.answer.is_some() {
            return false;
        }
        let (js, me, account, creds) = (self.js.clone(), self.me.clone(), self.account.clone(), self.creds.clone());
        self.answer = Some(
            async move {
                let done = match creds {
                    Ok(creds) => recorded(&js, &creds, account.as_deref(), &me, &answer).await,
                    Err(e) => Err(e),
                };
                let done = done.map_err(|e| format!("{}: {e}", answer.describe()));
                Event::Done(Box::new(answer), done)
            }
            .boxed_local(),
        );
        true
    }
}

/// Record `answer` of `me`, signed with the device credentials `creds` of the team of `account`, as `ants assistant
/// confirm` does: what was done, in a line.
async fn recorded(
    js: &jetstream::Context,
    creds: &Path,
    account: Option<&str>,
    me: &UserId,
    answer: &Answer,
) -> Result<String, String> {
    use crate::people::assistant::{answer_decision, answer_order};

    let done = match &answer.held {
        Item::Order(order) => answer_order(js, creds, account, me, order, answer.answer).await,
        Item::Decision(d) => {
            answer_decision(js, creds, account, me, &d.request, std::slice::from_ref(&d.act), answer.answer).await
        }
    };
    done.map_err(|e| format!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use ants_proto::{AdminOp, Verdict};

    use super::*;
    use crate::test_support::live::{at, user};
    use crate::tools::Update;
    use crate::tui::model::{Focus, Model};
    use crate::tui::test_support::model;

    fn screen(m: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(140, 30, |f| crate::tui::frame::draw(f, m, now))
    }

    fn order(op: AdminOp, ant: &str, sent: i64) -> AdminOrder {
        let order =
            AdminOrder::new(user("pa"), "m1".parse().unwrap(), user(ant), op, "").on_behalf(Some(user("alice")));
        AdminOrder { sent_at: at(sent), ..order }
    }

    #[test]
    fn held_orders_show_on_home_and_y_gives_the_answer() {
        let mut m = model();
        let (pause, restart) = (order(AdminOp::Pause, "dev-a", 1), order(AdminOp::Restart { now: false }, "dev-b", 2));
        assert_eq!(m.data.held.take(Event::Read(Ok(vec![pause.clone(), restart.clone()])), at(3)), None);
        let shown = screen(&m, at(3));
        assert!(
            shown.contains(
                "2 orders of pa wait for confirmation: pause of dev-a on m1, restart of dev-b on m1 · o lists"
            ),
            "{shown}"
        );

        m.focus = Focus::View;
        m.key(Key::Char('o'), at(3));
        assert_eq!(m.page().name(), "held for confirmation");
        let shown = screen(&m, at(3));
        assert!(shown.contains("pa ordered pause of dev-a on m1") && shown.contains("expires in 6d23h"), "{shown}");
        assert!(shown.contains(&format!("m1/dev-a/{} · a confirm · r deny", pause.id)), "{shown}");
        assert!(shown.contains("a confirm · r deny · Esc back · ↑↓ select"), "{shown}");

        assert_eq!(m.key(Key::Char('y'), at(3)), None, "y alone gives no answer");
        m.key(Key::Char('a'), at(3));
        let shown = screen(&m, at(3));
        let confirm = Answer { held: Item::Order(Box::new(pause.clone())), answer: ActAnswer::Confirm };
        assert!(shown.contains(&format!("y to {}", confirm.describe())) && shown.contains(Page::ARMED_KEYS), "{shown}");
        m.key(Key::Char('n'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "any other key disarms");
        m.key(Key::Down, at(3));
        m.key(Key::Char('r'), at(3));
        let deny = Answer { held: Item::Order(Box::new(restart)), answer: ActAnswer::Deny };
        assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Answer(deny.clone())));
        assert_eq!(m.data.held.busy, Some(deny.clone()));
        m.key(Key::Char('a'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "one answer at a time");

        m.held(Event::Done(Box::new(deny), Ok("denied: pa's order … is refused".to_owned())), at(4));
        assert!(screen(&m, at(4)).contains("denied: pa's order … is refused"), "the outcome is a notice");
        assert!(m.data.held.busy.is_none());
        let shown = screen(&m, at(4));
        assert!(shown.contains("denied, to be refused"), "{shown}");
        m.key(Key::Char('r'), at(4));
        assert_eq!(m.key(Key::Char('y'), at(4)), None, "an order answered is not answered again");

        m.key(Key::Up, at(4));
        m.key(Key::Char('a'), at(4));
        assert_eq!(m.key(Key::Char('y'), at(4)), Some(Action::Answer(confirm.clone())));
        m.held(Event::Done(Box::new(confirm), Err("refused".to_owned())), at(5));
        assert_eq!(
            m.data.held.answer(&Item::Order(Box::new(pause.clone()))),
            None,
            "a failed answer leaves the order waiting"
        );

        m.held(Event::Read(Ok(vec![pause])), at(6));
        assert!(m.data.held.answered.is_empty(), "the order the daemon decided goes");
        m.key(Key::Esc, at(6));
        let shown = screen(&m, at(6));
        assert!(shown.contains("1 order of pa waits for confirmation: pause of dev-a on m1 · o lists them"), "{shown}");
    }

    /// The decisions pa made for alice on pending tool requests wait with its orders, oldest first, counted on home;
    /// `r` then `y` denies one, shown answered until the ant drops it. A decision for another human is not alice's
    /// to answer.
    #[test]
    fn the_assistant_s_decisions_on_tool_requests_wait_with_its_orders() {
        let mut m = model();
        let request = |title: &str, sent: i64| ApprovalRequest {
            requested_at: at(sent),
            expires_at: at(sent + 300),
            ..crate::test_support::live::request("dev-a", title, vec![Verdict::AllowOnce])
        };
        let act = |human: &str, sent: i64| HeldAct {
            human: user(human),
            assistant: user("pa"),
            verdict: Verdict::AllowOnce,
            sent_at: at(sent),
        };
        let (push, rm, pause) = (request("git push", 2), request("rm -rf build", 4), order(AdminOp::Pause, "dev-b", 1));
        m.data.held.take(Event::Read(Ok(vec![pause.clone()])), at(5));
        for (request, acts) in [(&push, vec![act("alice", 3)]), (&rm, vec![act("bob", 5)])] {
            m.take(crate::live::Event::Approval(Update::Pending(request.clone())), at(5));
            m.take(crate::live::Event::Approval(Update::Held(request.clone(), acts)), at(5));
        }
        let decision = Item::Decision(Box::new(Decision { request: push.clone(), act: act("alice", 3) }));
        assert_eq!(
            items(&m.data),
            [Item::Order(Box::new(pause.clone())), decision.clone()],
            "bob's is for bob to answer"
        );
        let shown = screen(&m, at(6));
        let line = "2 orders and decisions of pa wait for confirmation: pause of dev-b on m1, allow-once on dev-a's \
                    execute";
        assert!(shown.contains(line), "{shown}");
        assert!(shown.contains("pa chose allow-once for alice: o lists it"), "the request's card: {shown}");

        m.focus = Focus::View;
        m.key(Key::Char('o'), at(6));
        let shown = screen(&m, at(6));
        assert!(shown.contains("tool    pa chose allow-once on dev-a's execute: git push"), "{shown}");
        assert!(shown.contains("expires in 4m56s"), "{shown}");
        assert!(shown.contains(&format!("dev-a/{} · a confirm · r deny", push.id)), "{shown}");
        m.key(Key::Down, at(6));
        m.key(Key::Char('r'), at(6));
        let deny = Answer { held: decision.clone(), answer: ActAnswer::Deny };
        let shown = screen(&m, at(6));
        let described = format!("deny pa's allow-once on request dev-a/{} (execute: git push)", push.id);
        assert!(shown.contains(&format!("y to {described}")), "{shown}");
        assert_eq!(m.key(Key::Char('y'), at(6)), Some(Action::Answer(deny.clone())));
        m.held(Event::Done(Box::new(deny), Ok("denied".to_owned())), at(7));
        let shown = screen(&m, at(7));
        assert!(shown.contains("denied, to be dropped"), "{shown}");
        m.key(Key::Char('a'), at(7));
        assert_eq!(m.key(Key::Char('y'), at(7)), None, "a decision answered is not answered again");

        m.held(Event::Read(Ok(vec![pause.clone()])), at(8));
        assert_eq!(m.data.held.answer(&decision), Some(ActAnswer::Deny), "gathering the orders keeps it");
        m.take(crate::live::Event::Approval(Update::Held(push, Vec::new())), at(8));
        assert!(m.data.held.answered.is_empty(), "the decision dropped goes, its answer with it");
        assert_eq!(items(&m.data), [Item::Order(Box::new(pause))]);
    }

    /// The page's answers to pa's decisions are recorded as `ants assistant confirm` records them: the request
    /// confirmed is decided for alice, the one denied waits on, its decision dropped.
    #[tokio::test]
    async fn the_page_s_answers_to_decisions_are_recorded_as_ants_assistant_confirm_records_them() {
        use ants_daemon::bus;
        use ants_proto::{ApprovalDecision, Envelope, Role};

        use crate::tools::State;

        async fn recorded(io: &mut Io, answer: Answer) -> Result<String, String> {
            assert!(io.answer(answer.clone()));
            loop {
                if let Event::Done(answered, done) = io.next().await {
                    assert_eq!(*answered, answer);
                    return done;
                }
            }
        }

        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, pa, dev_a) = (user("alice"), user("pa"), user("dev-a"));
        let creds = server.creds_as(&alice, Role::Human);
        let account = crate::people::devices::account_of(Some(&creds)).unwrap();
        let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
        crate::people::humans::ensure(&js).await.unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        crate::people::assistant::designate(&js, &creds, &alice, &pa).await.unwrap();
        let (_, ant_js) = server.connect(&dev_a).await;
        let (_, pa_js) = server.connect(&pa).await;
        let [push, rm] = ["git push", "rm -rf build"]
            .map(|title| crate::test_support::live::request("dev-a", title, vec![Verdict::AllowOnce]));
        for r in [&push, &rm] {
            bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
            let decision = ApprovalDecision::new(r.id, dev_a.clone(), Verdict::AllowOnce);
            let envelope = Envelope::decision(r.thread_id.clone(), pa.clone(), decision).on_behalf(Some(alice.clone()));
            bus::publish_json(&pa_js, envelope.subject(), &envelope, None).await.unwrap();
        }
        let held = tools::held(&js, &stream, Utc::now()).await.unwrap();
        let answer = |request: &ApprovalRequest, answer| {
            let (_, acts) = held.iter().find(|(r, _)| r.id == request.id).expect("held for alice");
            let decision = Decision { request: request.clone(), act: acts[0].clone() };
            Answer { held: Item::Decision(Box::new(decision)), answer }
        };

        let mut io = Io::new(js.clone(), alice.clone(), account, Ok(creds.clone()));
        let confirmed = recorded(&mut io, answer(&push, ActAnswer::Confirm)).await.unwrap();
        assert_eq!(
            confirmed,
            format!("confirmed: pa's allow-once on request dev-a/{} takes effect for alice", push.id)
        );
        let denied = recorded(&mut io, answer(&rm, ActAnswer::Deny)).await.unwrap();
        assert_eq!(denied, format!("denied: pa's allow-once on request dev-a/{} is dropped", rm.id));

        let state_of = async |r: &ApprovalRequest| {
            let (seq, _) = tools::find(&stream, &tools::qualified(r)).await.unwrap();
            tools::state(&js, &stream, seq, r, Utc::now()).await.unwrap()
        };
        let State::DecidedByAnt(by, decision) = state_of(&push).await else { panic!("git push decided") };
        assert_eq!((by, decision.on_behalf_of, decision.verdict), (pa, Some(alice), Verdict::AllowOnce));
        assert_eq!(state_of(&rm).await, State::Pending, "denied, waiting for a decision");
        assert!(tools::held(&js, &stream, Utc::now()).await.unwrap().is_empty(), "nothing waits for alice");
    }
}
