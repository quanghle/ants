//! The admin orders of the human's assistant that machines' daemons hold until the human consents: gathered every
//! while as `ants assistant confirm` lists them, counted on home, and listed on the page `o` opens there, where `a`
//! confirms the selected one and `r` denies it once `y` gives the answer, as `ants assistant confirm MACHINE/ANT/ID`
//! (with `--deny`) does.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{Act, ActAnswer, AdminOrder, ORDER_HOLD, UserId, Uuid};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph, Wrap};

use super::model::{Action, Data, Key, To};
use super::select::Selection;
use super::style::{self, ago};
use crate::live::maybe;

/// How often the held orders are gathered.
const GATHER_EVERY: Duration = Duration::from_secs(15);

/// An answer to a held order, as the key `y` gives it.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub order: AdminOrder,
    pub answer: ActAnswer,
}

// An order's values are JSON, whose equality is total.
impl Eq for Answer {}

impl Answer {
    /// The answer, as the key `y` gives it: `confirm pa's order m1/dev-a/<id> (pause)`.
    pub fn describe(&self) -> String {
        let verb = match self.answer {
            ActAnswer::Confirm => "confirm",
            ActAnswer::Deny => "deny",
        };
        format!("{verb} {}'s order {} ({})", self.order.admin, qualified(&self.order), self.order.op.describe())
    }
}

/// The order as the command line names it: `MACHINE/ANT/ID`.
fn qualified(order: &AdminOrder) -> String {
    Act::Order { machine: order.machine.clone(), ant: order.ant.clone(), id: order.id }.qualified()
}

/// The time left until `expires`, in days and hours past a day.
fn left(expires: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let delta = expires - now;
    match delta.num_days() {
        0 => crate::tools::left(expires, now),
        days => format!("{days}d{:02}h", delta.num_hours() % 24),
    }
}

/// The orders held for the human, and how the latest gathering and answer went.
#[derive(Debug, Default)]
pub struct Orders {
    /// Oldest first.
    pub orders: Vec<AdminOrder>,
    /// When the orders were last gathered, or why that failed.
    pub read: Option<Result<DateTime<Utc>, String>>,
    /// The answer being recorded.
    pub busy: Option<Answer>,
    /// The answers recorded, by order: the daemon holding it decides it soon after, and drops it from its list.
    pub answered: HashMap<Uuid, ActAnswer>,
}

impl Orders {
    fn waiting(&self) -> impl Iterator<Item = &AdminOrder> {
        self.orders.iter().filter(|o| !self.answered.contains_key(&o.id))
    }

    /// The orders waiting for an answer, as home's line counts them; None when none does.
    pub fn line(&self) -> Option<Line<'static>> {
        let waiting: Vec<&AdminOrder> = self.waiting().collect();
        let first = waiting.first()?;
        let what: Vec<String> =
            waiting.iter().map(|o| format!("{} of {} on {}", o.op.describe(), o.ant, o.machine)).collect();
        let text = format!(
            "{} {} of {} wait{} for confirmation: {} · o lists them",
            waiting.len(),
            if waiting.len() == 1 { "order" } else { "orders" },
            first.admin,
            if waiting.len() == 1 { "s" } else { "" },
            what.join(", ")
        );
        Some(Line::styled(text, Style::new().fg(style::PENDING)))
    }

    pub fn take(&mut self, event: Event, now: DateTime<Utc>) -> Option<Result<String, String>> {
        match event {
            Event::Read(Ok(orders)) => {
                self.answered.retain(|id, _| orders.iter().any(|o| o.id == *id));
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
                    self.answered.insert(answer.order.id, answer.answer);
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
    pub const KEYS: &str = "↑↓ select · a confirm · r deny · Esc back";
    pub const ARMED_KEYS: &str = "y gives the answer above · any other key leaves the order held";

    pub fn keys(&self) -> &'static str {
        if self.armed.is_some() { Self::ARMED_KEYS } else { Self::KEYS }
    }

    fn ids(orders: &[AdminOrder]) -> Vec<Uuid> {
        orders.iter().map(|o| o.id).collect()
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let held = &data.held;
        let ids = Self::ids(&held.orders);
        let at = self.selected.at(&ids);
        let armed = self.armed.take();
        let to = match key {
            Key::Char('a' | 'r') if held.busy.is_none() => {
                let answer = if key == Key::Char('r') { ActAnswer::Deny } else { ActAnswer::Confirm };
                self.armed = held
                    .orders
                    .get(at)
                    .filter(|o| !held.answered.contains_key(&o.id))
                    .map(|order| Answer { order: order.clone(), answer });
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
        let block = style::block(format!(" Held orders ({}) ", held.orders.len()), focused);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let read = match &held.read {
            None => "gathering the orders held for confirmation…".to_owned(),
            Some(Ok(at)) => format!(
                "orders of the assistant held by the machines' daemons until confirmed, for {} days at most; \
                 gathered {}",
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
        if held.orders.is_empty() {
            if matches!(held.read, Some(Ok(_))) {
                about.push(Line::raw("no orders held"));
            }
            frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), inner);
            return;
        }
        // The lines but a long reason for a failed gathering fit one row each.
        let tall = u16::try_from(about.len() * 2).unwrap_or(u16::MAX);
        let [top, list] = Layout::vertical([Constraint::Max(tall), Constraint::Min(0)]).areas(inner);
        frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), top);
        let rows: Vec<ListItem> = held
            .orders
            .iter()
            .map(|o| {
                let status = match held.answered.get(&o.id) {
                    Some(ActAnswer::Confirm) => {
                        Span::styled("confirmed, to be carried out", Style::new().fg(style::CONNECTED))
                    }
                    Some(ActAnswer::Deny) => Span::styled("denied, to be refused", Style::new().fg(style::ERROR)),
                    None => Span::styled("a confirm · r deny", style::dim()),
                };
                ListItem::new(vec![
                    Line::from(vec![
                        Span::styled(format!("{:<8}", o.op.kind()), Style::new().fg(Color::Yellow)),
                        Span::raw(format!("{} ordered {} of {} on {}", o.admin, o.op.describe(), o.ant, o.machine)),
                        Span::styled(
                            format!("  {} · expires in {}", ago(o.sent_at, now), left(o.sent_at + ORDER_HOLD, now)),
                            style::dim(),
                        ),
                    ]),
                    Line::from(vec![Span::styled(format!("        {} · ", qualified(o)), style::dim()), status]),
                ])
            })
            .collect();
        let selected = self.selected.at(&Self::ids(&held.orders));
        let list_widget = List::new(rows).highlight_style(style::highlight(focused));
        frame.render_stateful_widget(list_widget, list, &mut ListState::default().with_selected(Some(selected)));
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
                    Ok(creds) => crate::people::assistant::answer_order(
                        &js,
                        &creds,
                        account.as_deref(),
                        &me,
                        &answer.order,
                        answer.answer,
                    )
                    .await
                    .map_err(|e| format!("{e:#}")),
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

#[cfg(test)]
mod tests {
    use ants_proto::AdminOp;

    use super::*;
    use crate::test_support::live::{at, user};
    use crate::tui::model::Focus;
    use crate::tui::test_support::model;

    fn screen(m: &crate::tui::model::Model, now: DateTime<Utc>) -> String {
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
        assert_eq!(m.page().name(), "held orders");
        let shown = screen(&m, at(3));
        assert!(shown.contains("pa ordered pause of dev-a on m1") && shown.contains("expires in 6d23h"), "{shown}");
        assert!(shown.contains(&format!("m1/dev-a/{} · a confirm · r deny", pause.id)), "{shown}");
        assert!(shown.contains(Page::KEYS), "{shown}");

        assert_eq!(m.key(Key::Char('y'), at(3)), None, "y alone gives no answer");
        m.key(Key::Char('a'), at(3));
        let shown = screen(&m, at(3));
        let confirm = Answer { order: pause.clone(), answer: ActAnswer::Confirm };
        assert!(shown.contains(&format!("y to {}", confirm.describe())) && shown.contains(Page::ARMED_KEYS), "{shown}");
        m.key(Key::Char('n'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "any other key disarms");
        m.key(Key::Down, at(3));
        m.key(Key::Char('r'), at(3));
        let deny = Answer { order: restart, answer: ActAnswer::Deny };
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
        assert!(!m.data.held.answered.contains_key(&pause.id), "a failed answer leaves the order waiting");

        m.held(Event::Read(Ok(vec![pause])), at(6));
        assert!(m.data.held.answered.is_empty(), "the order the daemon decided goes");
        m.key(Key::Esc, at(6));
        let shown = screen(&m, at(6));
        assert!(shown.contains("1 order of pa waits for confirmation: pause of dev-a on m1 · o lists them"), "{shown}");
    }
}
