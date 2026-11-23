//! The settings pane of the ant view: the ant's settings as the config file of its machine has them, read with an
//! admin order as `ants ant show` reads them ([`crate::admin::settings`]), and the admin orders to it, each with where it
//! stands, pending ones included while its daemon is away. Enter on a field writes a change to it, `n` one to another
//! field, sent as `ants ant set` sends it, expecting the value read; its outcome shows below.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{AdminOp, JsDomain, Machine, OrderState, UserId, Uuid};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{List, ListItem, Paragraph, Wrap};

use super::input::Input;
use super::model::{Action, Key};
use super::select::step;
use super::style::{self, ago};
use crate::admin::orders::{self, Listed};
use crate::admin::settings::{self, Assignment, Reading};
use crate::live::maybe;

/// How often the orders to the shown ant are listed again.
const LIST_EVERY: Duration = Duration::from_secs(10);

/// How many of the latest orders the pane lists.
const ORDERS_SHOWN: usize = 6;

/// What the pane asks of the ant's machine.
#[derive(Debug, Clone, PartialEq)]
pub enum Ask {
    /// Read the ant's settings afresh.
    Read(UserId),
    /// Send the order to the ant's machine.
    Set(UserId, Machine, AdminOp),
}

// An order's values are JSON, whose equality is total.
impl Eq for Ask {}

/// How the order sent last stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent {
    Sending,
    Applied(Uuid),
    /// Not taken yet by the daemon of the machine.
    Pending(Uuid, Machine),
    Refused(Uuid, String),
    /// Not sent, or no change asked for that could be.
    Failed(String),
}

/// What the pane's reads and orders brought.
pub enum Event {
    Read(UserId, Result<Reading, String>),
    Listed(UserId, Result<Vec<Listed>, String>),
    Sent(UserId, Sent),
}

#[derive(Debug, Default)]
pub struct Pane {
    /// None while read.
    pub reading: Option<Result<Reading, String>>,
    /// The field selected.
    at: usize,
    /// The `KEY = VALUE` being written, if any.
    pub draft: Option<Input>,
    pub sent: Option<Sent>,
}

impl Pane {
    pub const DRAFT_KEYS: &str = "KEY = VALUE, a TOML value or a word; KEY = removes · Enter send · Esc cancel";

    fn read(&self) -> Option<&settings::Read> {
        self.reading.as_ref()?.as_ref().ok()?.read.as_ref()
    }

    /// The fields, each with its value as TOML has it, and whether it hides a secret.
    fn fields(&self) -> Vec<(Vec<String>, String, bool)> {
        let Some(read) = self.read() else { return Vec::new() };
        let settings = &read.settings;
        let leaves = settings.leaves().into_iter();
        leaves
            .map(|(f, v)| (f.clone(), ants_daemon::config::edit::toml_text(v), settings.digest(&f).is_some()))
            .collect()
    }

    /// How many fields are read, as the pane's tab counts them.
    pub fn count(&self) -> String {
        if self.read().is_some() { self.fields().len().to_string() } else { "?".to_owned() }
    }

    pub fn key(&mut self, key: Key) {
        let fields = self.fields();
        match key {
            Key::Enter => {
                let Some((field, value, secret)) = fields.get(self.at) else { return };
                let field = ants_daemon::config::edit::field_text(field);
                let draft = if *secret { format!("{field} = ") } else { format!("{field} = {value}") };
                self.draft = Some(Input::from(draft.as_str()));
                self.drop_failure();
            }
            Key::Char('n') => {
                self.draft = Some(Input::default());
                self.drop_failure();
            }
            _ => {
                if let Some(to) = step(key, self.at, None) {
                    self.at = to.min(fields.len().saturating_sub(1));
                }
            }
        }
    }

    /// Take `key` into the change being written, edited as in the command bar: None when none is, the key being the
    /// pane's then. `Enter` sends it as an order expecting the value read, `Esc` drops it.
    pub fn write(&mut self, key: Key, ant: &UserId) -> Option<Option<Action>> {
        let draft = self.draft.as_mut()?;
        match key {
            Key::Esc => self.draft = None,
            Key::Enter => return Some(self.send(ant)),
            key => _ = draft.key(key),
        }
        Some(None)
    }

    /// Forget a change that could not be sent, as another is written.
    fn drop_failure(&mut self) {
        if matches!(self.sent, Some(Sent::Failed(_))) {
            self.sent = None;
        }
    }

    fn send(&mut self, ant: &UserId) -> Option<Action> {
        let text = self.draft.as_ref().map_or_else(String::new, Input::text);
        let assignment = match text.parse::<Assignment>() {
            Ok(assignment) => assignment,
            Err(e) => {
                self.sent = Some(Sent::Failed(format!("{e:#}")));
                return None;
            }
        };
        let Some(read) = self.read() else {
            self.sent = Some(Sent::Failed("the settings are not read: r reads them".to_owned()));
            return None;
        };
        let shared = self.reading.as_ref().and_then(|r| r.as_ref().ok()).and_then(settings::Reading::verified_shared);
        let op = settings::order(&read.settings, shared, &[assignment]);
        let machine = read.machine.clone();
        self.draft = None;
        self.sent = Some(Sent::Sending);
        Some(Action::Settings(Ask::Set(ant.clone(), machine, op)))
    }

    /// The keys of the change being written, which takes every key, if one is.
    pub fn prompt(&self) -> Option<&'static str> {
        self.draft.as_ref().map(|_| Self::DRAFT_KEYS)
    }

    /// Take in what a read or an order brought.
    pub fn take(&mut self, event: Event) {
        match event {
            Event::Read(_, reading) => self.reading = Some(reading),
            Event::Listed(_, listed) => {
                if let Some(Ok(reading)) = &mut self.reading
                    && let Ok(listed) = listed
                {
                    reading.orders = listed;
                }
            }
            Event::Sent(_, sent) => self.sent = Some(sent),
        }
    }

    pub fn draw(
        &self,
        frame: &mut Frame,
        area: Rect,
        ant: &UserId,
        now: DateTime<Utc>,
        focused: bool,
        mouse: &super::mouse::Regions,
    ) {
        let orders = match &self.reading {
            Some(Ok(reading)) => &reading.orders[reading.orders.len().saturating_sub(ORDERS_SHOWN)..],
            _ => &[],
        };
        let tall = u16::try_from(orders.len().max(1) + 2).unwrap_or(u16::MAX);
        let [fields, below, input] =
            Layout::vertical([Constraint::Min(3), Constraint::Length(tall), Constraint::Length(3)]).areas(area);
        self.draw_fields(frame, fields, ant, now, focused, mouse);
        let items: Vec<ListItem> = if orders.is_empty() {
            vec![ListItem::new(Line::styled(format!("no admin orders to {ant}"), style::dim()))]
        } else {
            orders.iter().rev().map(|o| ListItem::new(order_line(o, now))).collect()
        };
        frame.render_widget(
            List::new(items).block(style::block(" admin orders, latest first ".to_owned(), false)),
            below,
        );
        let (text, style, title) = match (&self.draft, &self.sent) {
            (Some(draft), _) => (draft.caret(usize::from(input.width.saturating_sub(2))), Style::new(), " change "),
            (None, Some(sent)) => {
                let (text, color) = sent_line(sent);
                (text, Style::new().fg(color), " last order ")
            }
            (None, None) => ("Enter changes the field selected".to_owned(), style::dim(), " change "),
        };
        let mut block = style::block(title.to_owned(), focused && self.draft.is_some());
        if self.draft.is_some()
            && let Some(Sent::Failed(e)) = &self.sent
        {
            block = block.title_bottom(Line::styled(format!(" {e} "), Style::new().fg(style::ERROR)));
        }
        frame.render_widget(Paragraph::new(Span::styled(text, style)).block(block), input);
    }

    fn draw_fields(
        &self,
        frame: &mut Frame,
        area: Rect,
        ant: &UserId,
        now: DateTime<Utc>,
        focused: bool,
        mouse: &super::mouse::Regions,
    ) {
        let reading = match &self.reading {
            None => Err(format!("reading the settings of {ant}…")),
            Some(Err(e)) => Err(format!("reading the settings: {e}")),
            Some(Ok(reading)) => Ok(reading),
        };
        let reading = match reading {
            Ok(reading) => reading,
            Err(text) => {
                let block = style::block(" settings ".to_owned(), focused);
                frame.render_widget(Paragraph::new(text).block(block).wrap(Wrap { trim: false }), area);
                return;
            }
        };
        let title = match &reading.read {
            Some(read) => format!(" config.toml on {}, read {} ", read.machine, ago(read.read_at, now)),
            None => " settings never read ".to_owned(),
        };
        let mut block = style::block(title, focused && self.draft.is_none());
        if let Some(stale) = &reading.stale {
            block = block.title_bottom(Line::styled(format!(" {stale} "), Style::new().fg(style::WARNING)));
        }
        let items: Vec<Text> = self
            .fields()
            .into_iter()
            .map(|(field, value, secret)| {
                let mut spans = vec![
                    Span::styled(ants_daemon::config::edit::field_text(&field), Style::new().fg(Color::Cyan)),
                    Span::raw(" = "),
                    Span::raw(value),
                ];
                if secret {
                    spans.push(Span::styled("  (secret)", style::dim()));
                }
                Text::from(Line::from(spans))
            })
            .collect();
        let inner = block.inner(area);
        frame.render_widget(block, area);
        super::mouse::list(
            frame,
            inner,
            mouse,
            super::mouse::Viewport::AntSettings,
            items,
            self.at,
            focused && self.draft.is_none(),
        );
    }
}

/// An order on one line: when, who, what, and where it stands, in its colour.
fn order_line(o: &Listed, now: DateTime<Utc>) -> Line<'static> {
    let color = match o.state {
        OrderState::Pending | OrderState::Held => style::PENDING,
        OrderState::Applied => style::CONNECTED,
        OrderState::Refused => style::ERROR,
    };
    let state = match &o.reason {
        Some(reason) => format!("{}: {reason}", o.state.as_str()),
        None => o.state.as_str().to_owned(),
    };
    Line::from(vec![
        Span::styled(o.sent_at.map_or_else(|| "-".to_owned(), |at| ago(at, now)), style::dim()),
        Span::raw(format!(
            "  {} · {} on {}  ",
            o.by(),
            o.op.as_ref().map_or_else(|| "-".to_owned(), AdminOp::describe),
            o.machine
        )),
        Span::styled(state, Style::new().fg(color)),
    ])
}

fn sent_line(sent: &Sent) -> (String, Color) {
    match sent {
        Sent::Sending => ("sending…".to_owned(), style::PENDING),
        Sent::Applied(id) => (format!("order {id} applied"), style::CONNECTED),
        Sent::Pending(id, machine) => {
            (format!("order {id} pending: the daemon of {machine} has not taken it"), style::PENDING)
        }
        Sent::Refused(id, reason) => (format!("order {id} refused: {reason}"), style::ERROR),
        Sent::Failed(e) => (e.clone(), style::ERROR),
    }
}

/// The reads of the shown ant's settings and orders, and the orders sent.
pub struct Io {
    js: jetstream::Context,
    url: String,
    creds: Option<PathBuf>,
    domain: Option<JsDomain>,
    watched: Option<UserId>,
    /// The ant whose settings [`Io::read`] reads, if it reads any rather than its orders.
    reading: Option<UserId>,
    every: tokio::time::Interval,
    read: Option<LocalBoxFuture<'static, Event>>,
    set: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: jetstream::Context, url: &str, creds: Option<PathBuf>, domain: Option<JsDomain>) -> Self {
        let every = tokio::time::interval(LIST_EVERY);
        Self { js, url: url.to_owned(), creds, domain, watched: None, reading: None, every, read: None, set: None }
    }

    /// The next read or order's outcome. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.read.is_none() && let Some(ant) = self.watched.clone() {
                        let js = self.js.clone();
                        let listed = async move {
                            let listed = orders::orders_of(&js, &ant).await.map_err(|e| format!("{e:#}"));
                            Event::Listed(ant, listed)
                        };
                        self.read = Some(listed.boxed_local());
                    }
                }
                event = maybe(&mut self.read) => {
                    (self.read, self.reading) = (None, None);
                    return event;
                }
                event = maybe(&mut self.set) => {
                    self.set = None;
                    if let Event::Sent(ant, Sent::Applied(_)) = &event {
                        self.ask(Ask::Read(ant.clone()));
                    }
                    return event;
                }
            }
        }
    }

    /// List the orders to the ant `shown` in the pane every while, and read its settings now if `unread` and not
    /// being read; with None, of none.
    pub fn watch(&mut self, shown: Option<UserId>, unread: bool) {
        if unread
            && let Some(ant) = &shown
            && self.reading.as_ref() != Some(ant)
        {
            self.ask(Ask::Read(ant.clone()));
        }
        self.watched = shown;
    }

    pub fn ask(&mut self, ask: Ask) {
        let (js, url, creds, domain) = (self.js.clone(), self.url.clone(), self.creds.clone(), self.domain.clone());
        match ask {
            Ask::Read(ant) => {
                self.reading = Some(ant.clone());
                let to = ant.clone();
                let read = async move {
                    let nats = crate::connection::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    settings::read(&nats, &js, &to, settings::READ_WAIT).await
                };
                let read = async move { Event::Read(ant, read.await.map_err(|e| format!("{e:#}"))) };
                self.read = Some(read.boxed_local());
            }
            Ask::Set(ant, machine, op) => {
                let to = ant.clone();
                let sent = async move {
                    let nats = crate::connection::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    if !settings::may_order(&nats) {
                        anyhow::bail!("changing settings needs a human device's admin credentials");
                    }
                    let admin = orders::Admin::of(&nats)?;
                    Ok(match admin.send(&js, Some(machine), &to, op, settings::SET_WAIT).await? {
                        orders::Sent::Applied(order, _) => Sent::Applied(order.id),
                        orders::Sent::Refused(order, o) => Sent::Refused(order.id, o.reason.unwrap_or_default()),
                        orders::Sent::Pending(order) => Sent::Pending(order.id, order.machine),
                    })
                };
                let sent = async move {
                    Event::Sent(ant, sent.await.unwrap_or_else(|e: anyhow::Error| Sent::Failed(format!("{e:#}"))))
                };
                self.set = Some(sent.boxed_local());
            }
        }
    }
}

impl Event {
    pub fn ant(&self) -> &UserId {
        match self {
            Self::Read(ant, _) | Self::Listed(ant, _) | Self::Sent(ant, _) => ant,
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{FieldChange, Hidden, Settings};
    use serde_json::json;

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, user};
    use crate::tui::input::{Typed, exercise};
    use crate::tui::model::Focus;
    use crate::tui::nav;
    use crate::tui::test_support::{model, screen};

    fn reading() -> Reading {
        let mut fields = serde_json::Map::new();
        fields.insert("env".to_owned(), json!({ "API_TOKEN": "[redacted]" }));
        fields.insert("model".to_owned(), json!("gpt-5"));
        let field = vec!["env".to_owned(), "API_TOKEN".to_owned()];
        let hidden = vec![Hidden { field, digest: "d1".to_owned() }];
        let settings = Settings { fields, hidden };
        let machine: Machine = "m1".parse().unwrap();
        let read = settings::Read { machine: machine.clone(), order: Uuid::now_v7(), read_at: at(1), settings };
        let pending = Listed {
            id: Uuid::now_v7(),
            machine,
            ant: user("dev-a"),
            admin: Some(user("alice")),
            on_behalf_of: None,
            op: Some(AdminOp::Restart { now: false }),
            sent_at: Some(at(2)),
            state: OrderState::Pending,
            reason: None,
            result: None,
            decided_at: None,
        };
        Reading {
            read: Some(read),
            stale: Some("the read waits for the daemon of m1".to_owned()),
            orders: vec![pending],
            shared: None,
        }
    }

    #[test]
    fn the_settings_pane_shows_what_was_read_and_sends_a_change_expecting_it() {
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.focus = Focus::View;
        m.key(Key::Left, at(3));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), true)), "the pane asks for a read");
        assert!(screen(&m, 160, 36, at(3)).contains("reading the settings of dev-a…"));
        m.settings_event(Event::Read(user("dev-a"), Ok(reading())));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), false)));
        let shown = screen(&m, 160, 36, at(3));
        assert!(shown.contains("settings (2)") && shown.contains("config.toml on m1, read 2s ago"), "{shown}");
        assert!(shown.contains("env.API_TOKEN = \"[redacted]\"  (secret)"), "{shown}");
        assert!(shown.contains("model = \"gpt-5\""), "{shown}");
        assert!(shown.contains("the read waits for the daemon of m1"), "{shown}");
        assert!(shown.contains("alice · restart on m1  pending"), "{shown}");
        assert!(shown.contains("Enter change · n new field · Esc back · ↑↓ field"), "{shown}");

        m.key(Key::Down, at(3));
        m.key(Key::Enter, at(3));
        assert!(screen(&m, 160, 36, at(3)).contains("model = \"gpt-5\"▏"), "a change of the field selected");
        for _ in 0.."\"gpt-5\"".len() {
            m.key(Key::Backspace, at(3));
        }
        "o3 w".chars().for_each(|c| _ = m.key(Key::Char(c), at(3)));
        assert_eq!(m.page().name(), "ant", "keys go to the change");
        m.key(Key::Backspace, at(3));
        m.key(Key::Backspace, at(3));
        let Some(Action::Settings(Ask::Set(ant, machine, op))) = m.key(Key::Enter, at(3)) else { panic!("an order") };
        assert_eq!((ant, machine.as_str()), (user("dev-a"), "m1"));
        let model = vec!["model".to_owned()];
        let expected = FieldChange {
            field: model,
            expected: Some(json!("gpt-5")),
            expected_digest: None,
            value: Some(json!("o3")),
        };
        assert_eq!(op, AdminOp::Set { changes: vec![expected] });
        assert!(screen(&m, 160, 36, at(3)).contains("sending…"));
        let id = Uuid::now_v7();
        m.settings_event(Event::Sent(
            user("dev-a"),
            Sent::Refused(id, "model changed since the order was sent".to_owned()),
        ));
        assert!(screen(&m, 160, 36, at(3)).contains(&format!("order {id} refused: model changed")));

        m.key(Key::Up, at(3));
        m.key(Key::Enter, at(3));
        assert!(screen(&m, 160, 36, at(3)).contains("env.API_TOKEN = ▏"), "a secret is not shown to change");
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "ant", "Esc drops the change alone");
        m.key(Key::Char('n'), at(3));
        "nope".chars().for_each(|c| _ = m.key(Key::Char(c), at(3)));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(screen(&m, 160, 36, at(3)).contains("\"nope\" sets nothing"), "{}", screen(&m, 160, 36, at(3)));
        m.key(Key::Esc, at(3));
        assert_eq!(m.key(Key::Char('R'), at(3)), Some(Action::Refresh));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), true)));
    }

    #[test]
    fn a_change_edits_as_the_bar_does() {
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.focus = Focus::View;
        m.key(Key::Left, at(3));
        m.settings_event(Event::Read(user("dev-a"), Ok(reading())));
        m.key(Key::Char('n'), at(3));
        let want = exercise(|typed| match typed {
            Typed::Key(key) => _ = m.key(key, at(3)),
            Typed::Paste(text) => m.paste(text, at(3)),
        });
        assert!(
            screen(&m, 160, 36, at(3)).contains(&format!("│{}▏{}", &want[..1], &want[1..])),
            "{}",
            screen(&m, 160, 36, at(3))
        );
        assert_eq!(m.page().name(), "ant", "keys go to the change");
    }
}
