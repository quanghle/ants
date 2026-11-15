//! The settings pane of the ant view: the ant's settings as the config file of its machine has them, read with an
//! admin order as `ants ant show` reads them ([`crate::settings`]), and the admin orders to it, each with where it
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
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph, Wrap};

use super::frame::{self, ago};
use super::model::{Action, Key, To};
use crate::live::maybe;
use crate::orders::{self, Listed};
use crate::settings::{self, Assignment, Reading};

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
    pub draft: Option<String>,
    pub sent: Option<Sent>,
}

impl Pane {
    pub const KEYS: &str = "↑↓ field · Enter change · n new field · u read again · ←→ pane · a ask · w work · r \
                            restart · p (un)pause · X retire · Esc back";
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

    pub fn key(&mut self, key: Key, ant: &UserId) -> Option<To> {
        let fields = self.fields();
        match key {
            Key::Up | Key::Char('k') => self.at = self.at.saturating_sub(1),
            Key::Down | Key::Char('j') => self.at = (self.at + 1).min(fields.len().saturating_sub(1)),
            Key::Home => self.at = 0,
            Key::End => self.at = fields.len().saturating_sub(1),
            Key::Enter => {
                let (field, value, secret) = fields.get(self.at)?;
                let field = ants_daemon::config::edit::field_text(field);
                self.draft = Some(if *secret { format!("{field} = ") } else { format!("{field} = {value}") });
                self.drop_failure();
            }
            Key::Char('n') => {
                self.draft = Some(String::new());
                self.drop_failure();
            }
            Key::Char('u') => {
                self.reading = None;
                return Some(To::Do(Action::Settings(Ask::Read(ant.clone()))));
            }
            _ => {}
        }
        None
    }

    /// Take `key` into the change being written: None when none is, the key being the pane's then. `Enter` sends it
    /// as an order expecting the value read, `Esc` drops it.
    pub fn write(&mut self, key: Key, ant: &UserId) -> Option<Option<Action>> {
        let draft = self.draft.as_mut()?;
        match key {
            Key::Char(c) => draft.push(c),
            Key::Backspace => _ = draft.pop(),
            Key::Esc => self.draft = None,
            Key::Enter => return Some(self.send(ant)),
            _ => {}
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
        let text = self.draft.as_deref().unwrap_or_default();
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
        let op = settings::order(&read.settings, &[assignment]);
        let machine = read.machine.clone();
        self.draft = None;
        self.sent = Some(Sent::Sending);
        Some(Action::Settings(Ask::Set(ant.clone(), machine, op)))
    }

    pub fn keys(&self) -> &'static str {
        if self.draft.is_some() { Self::DRAFT_KEYS } else { Self::KEYS }
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

    pub fn draw(&self, frame: &mut Frame, area: Rect, ant: &UserId, now: DateTime<Utc>, focused: bool) {
        let orders = match &self.reading {
            Some(Ok(reading)) => &reading.orders[reading.orders.len().saturating_sub(ORDERS_SHOWN)..],
            _ => &[],
        };
        let tall = u16::try_from(orders.len().max(1) + 2).unwrap_or(u16::MAX);
        let [fields, below, input] =
            Layout::vertical([Constraint::Min(3), Constraint::Length(tall), Constraint::Length(3)]).areas(area);
        self.draw_fields(frame, fields, ant, now, focused);
        let items: Vec<ListItem> = if orders.is_empty() {
            vec![ListItem::new(Line::styled(format!("no admin orders to {ant}"), frame::dim()))]
        } else {
            orders.iter().rev().map(|o| ListItem::new(order_line(o, now))).collect()
        };
        frame.render_widget(
            List::new(items).block(frame::block(" admin orders, latest first ".to_owned(), false)),
            below,
        );
        let (text, style, title) = match (&self.draft, &self.sent) {
            (Some(draft), _) => (format!("{draft}▏"), Style::new(), " change "),
            (None, Some(sent)) => {
                let (text, color) = sent_line(sent);
                (text, Style::new().fg(color), " last order ")
            }
            (None, None) => ("Enter changes the field selected".to_owned(), frame::dim(), " change "),
        };
        let mut block = frame::block(title.to_owned(), focused && self.draft.is_some());
        if self.draft.is_some()
            && let Some(Sent::Failed(e)) = &self.sent
        {
            block = block.title_bottom(Line::styled(format!(" {e} "), Style::new().fg(Color::Red)));
        }
        frame.render_widget(Paragraph::new(Span::styled(text, style)).block(block), input);
    }

    fn draw_fields(&self, frame: &mut Frame, area: Rect, ant: &UserId, now: DateTime<Utc>, focused: bool) {
        let reading = match &self.reading {
            None => Err(format!("reading the settings of {ant}…")),
            Some(Err(e)) => Err(format!("reading the settings: {e}")),
            Some(Ok(reading)) => Ok(reading),
        };
        let reading = match reading {
            Ok(reading) => reading,
            Err(text) => {
                let block = frame::block(" settings ".to_owned(), focused);
                frame.render_widget(Paragraph::new(text).block(block).wrap(Wrap { trim: false }), area);
                return;
            }
        };
        let title = match &reading.read {
            Some(read) => format!(" config.toml on {}, read {} ", read.machine, ago(read.read_at, now)),
            None => " settings never read ".to_owned(),
        };
        let mut block = frame::block(title, focused && self.draft.is_none());
        if let Some(stale) = &reading.stale {
            block = block.title_bottom(Line::styled(format!(" {stale} "), Style::new().fg(Color::Yellow)));
        }
        let items: Vec<ListItem> = self
            .fields()
            .into_iter()
            .map(|(field, value, secret)| {
                let mut spans = vec![
                    Span::styled(ants_daemon::config::edit::field_text(&field), Style::new().fg(Color::Cyan)),
                    Span::raw(" = "),
                    Span::raw(value),
                ];
                if secret {
                    spans.push(Span::styled("  (secret)", frame::dim()));
                }
                ListItem::new(Line::from(spans))
            })
            .collect();
        let list = List::new(items).block(block).highlight_style(frame::highlight(focused && self.draft.is_none()));
        let mut state = ListState::default().with_selected(Some(self.at));
        frame.render_stateful_widget(list, area, &mut state);
    }
}

/// An order on one line: when, who, what, and where it stands, in its colour.
fn order_line(o: &Listed, now: DateTime<Utc>) -> Line<'static> {
    let color = match o.state {
        OrderState::Pending => Color::Yellow,
        OrderState::Applied => Color::Green,
        OrderState::Refused => Color::Red,
    };
    let state = match &o.reason {
        Some(reason) => format!("{}: {reason}", o.state.as_str()),
        None => o.state.as_str().to_owned(),
    };
    Line::from(vec![
        Span::styled(o.sent_at.map_or_else(|| "-".to_owned(), |at| ago(at, now)), frame::dim()),
        Span::raw(format!(
            "  {} · {} on {}  ",
            o.admin.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
            o.op.as_ref().map_or_else(|| "-".to_owned(), AdminOp::describe),
            o.machine
        )),
        Span::styled(state, Style::new().fg(color)),
    ])
}

fn sent_line(sent: &Sent) -> (String, Color) {
    match sent {
        Sent::Sending => ("sending…".to_owned(), Color::Yellow),
        Sent::Applied(id) => (format!("order {id} applied"), Color::Green),
        Sent::Pending(id, machine) => {
            (format!("order {id} pending: the daemon of {machine} has not taken it"), Color::Yellow)
        }
        Sent::Refused(id, reason) => (format!("order {id} refused: {reason}"), Color::Red),
        Sent::Failed(e) => (e.clone(), Color::Red),
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
                    let nats = crate::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    settings::read(&nats, &js, &to, settings::READ_WAIT).await
                };
                let read = async move { Event::Read(ant, read.await.map_err(|e| format!("{e:#}"))) };
                self.read = Some(read.boxed_local());
            }
            Ask::Set(ant, machine, op) => {
                let to = ant.clone();
                let sent = async move {
                    let nats = crate::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    if !settings::may_order(&nats) {
                        anyhow::bail!("changing settings needs a human device's admin credentials");
                    }
                    let (order, outcome) = orders::send(&nats, &js, Some(machine), &to, op, settings::SET_WAIT).await?;
                    Ok(match outcome {
                        Some(o) if o.state == OrderState::Applied => Sent::Applied(order.id),
                        Some(o) => Sent::Refused(order.id, o.reason.unwrap_or_default()),
                        None => Sent::Pending(order.id, order.machine),
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
    use crate::live::tests::{at, user};
    use crate::roster::tests::entry;
    use crate::tui::model::Focus;
    use crate::tui::model::tests::model;
    use crate::tui::nav;

    fn screen(m: &super::super::model::Model) -> String {
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 36)).unwrap();
        terminal.draw(|f| crate::tui::frame::draw(f, m, at(3))).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(140).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

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
        }
    }

    #[test]
    fn the_settings_pane_shows_what_was_read_and_sends_a_change_expecting_it() {
        let mut m = model();
        m.data.live.roster(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.focus = Focus::View;
        m.key(Key::Left, at(3));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), true)), "the pane asks for a read");
        assert!(screen(&m).contains("reading the settings of dev-a…"));
        m.settings_event(Event::Read(user("dev-a"), Ok(reading())));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), false)));
        let shown = screen(&m);
        assert!(shown.contains("settings (2)") && shown.contains("config.toml on m1, read 2s ago"), "{shown}");
        assert!(shown.contains("env.API_TOKEN = \"[redacted]\"  (secret)"), "{shown}");
        assert!(shown.contains("model = \"gpt-5\""), "{shown}");
        assert!(shown.contains("the read waits for the daemon of m1"), "{shown}");
        assert!(shown.contains("alice · restart on m1  pending"), "{shown}");
        assert!(shown.contains(Pane::KEYS), "{shown}");

        m.key(Key::Down, at(3));
        m.key(Key::Enter, at(3));
        assert!(screen(&m).contains("model = \"gpt-5\"▏"), "a change of the field selected");
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
        assert!(screen(&m).contains("sending…"));
        let id = Uuid::now_v7();
        m.settings_event(Event::Sent(
            user("dev-a"),
            Sent::Refused(id, "model changed since the order was sent".to_owned()),
        ));
        assert!(screen(&m).contains(&format!("order {id} refused: model changed")));

        m.key(Key::Up, at(3));
        m.key(Key::Enter, at(3));
        assert!(screen(&m).contains("env.API_TOKEN = ▏"), "a secret is not shown to change");
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "ant", "Esc drops the change alone");
        m.key(Key::Char('n'), at(3));
        "nope".chars().for_each(|c| _ = m.key(Key::Char(c), at(3)));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(screen(&m).contains("\"nope\" sets nothing"), "{}", screen(&m));
        m.key(Key::Esc, at(3));
        assert_eq!(m.key(Key::Char('u'), at(3)), Some(Action::Settings(Ask::Read(user("dev-a")))));
        assert_eq!(m.settings_shown(), Some((user("dev-a"), true)));
    }
}
