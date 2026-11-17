//! The MCP pane of the ant view: the MCP servers of the ant's harness as `ants ant mcp` shows them ([`crate::admin::mcp`]),
//! which [`Io`] asks the daemon of its machine for with an admin order when the pane first shows and on `u`, and their
//! OAuth sign-in: `s` asks the harness to sign in to the selected server, which brings the address to open in a
//! browser, and `v` hands over the address a browser on another machine came back to.

use std::path::PathBuf;

use ants_proto::{JsDomain, McpReport, McpSignIn, McpStatus, UserId};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use super::model::{Action, Key};
use super::style;
use crate::live::maybe;

/// The ant's MCP servers, as last asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Unasked,
    Asking,
    Failed(String),
    Read(McpReport),
}

/// A sign-in to one server, as it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signing {
    pub server: String,
    pub state: Sign,
    /// The address to open in a browser, once the harness gave it.
    pub url: Option<(String, Option<DateTime<Utc>>)>,
    /// The address the browser came back to, when one is being handed over.
    callback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sign {
    /// Waiting for the daemon.
    Asking,
    /// Waiting for the browser to come back.
    Waiting,
    Failed(String),
    /// The server as it stands after signing in.
    Done(McpStatus, Option<String>),
}

/// The pane's servers, how many times they were asked for, the selected one, and its sign-in.
#[derive(Debug)]
pub struct Page {
    pub state: State,
    pub asked: u32,
    selected: usize,
    pub signing: Option<Signing>,
    /// How many sign-in orders were sent.
    pub signs: u32,
    /// The address the browser came back to, being written.
    draft: Option<String>,
}

impl Page {
    pub const KEYS: &str = "↑↓ server · s sign in · u read again · ←→ pane · a ask · w work · r restart · \
         p (un)pause · X retire · Esc back";
    pub const SIGNING_KEYS: &str = "↑↓ server · y copy the address · v hand back where the browser came back to · \
         s sign in again · u read again · ←→ pane · Esc back";
    pub const DRAFT_KEYS: &str =
        "type or paste the address the browser came back to · Enter hand it over · Esc drop it";

    pub fn new() -> Self {
        Self { state: State::Unasked, asked: 0, selected: 0, signing: None, signs: 0, draft: None }
    }

    pub fn keys(&self) -> &'static str {
        match (&self.draft, &self.signing) {
            (Some(_), _) => Self::DRAFT_KEYS,
            (None, Some(Signing { url: Some(_), .. })) => Self::SIGNING_KEYS,
            _ => Self::KEYS,
        }
    }

    /// Ask for the servers, unless they were already.
    pub fn shown(&mut self) {
        if self.state == State::Unasked {
            self.ask();
        }
    }

    fn ask(&mut self) {
        self.state = State::Asking;
        self.asked += 1;
    }

    fn servers(&self) -> &[ants_proto::McpServerState] {
        match &self.state {
            State::Read(McpReport { servers: Some(servers), .. }) => servers,
            _ => &[],
        }
    }

    /// Take in the servers of the `asked`th asking, unless asked again since.
    pub fn read(&mut self, asked: u32, read: &Result<McpReport, String>) {
        if asked != self.asked {
            return;
        }
        self.state = match read {
            Ok(report) => State::Read(report.clone()),
            Err(e) => State::Failed(e.clone()),
        };
        self.selected = self.selected.min(self.servers().len().saturating_sub(1));
    }

    /// Take in what the `sign`th sign-in order brought, unless another was sent since.
    pub fn signed(&mut self, sign: u32, signed: &Result<McpSignIn, String>) {
        let Some(signing) = self.signing.as_mut().filter(|_| sign == self.signs) else { return };
        signing.callback = None;
        let signed = match signed {
            Ok(signed) => signed,
            Err(e) => {
                signing.state = Sign::Failed(e.clone());
                return;
            }
        };
        if let Some(url) = &signed.authorization_url {
            signing.url = Some((url.clone(), signed.expires_at));
            signing.state = Sign::Waiting;
            return;
        }
        let server = &signed.server;
        signing.state = Sign::Done(server.status, server.error.clone());
        if let State::Read(McpReport { servers: Some(servers), .. }) = &mut self.state
            && let Some(read) = servers.iter_mut().find(|s| s.name == server.name)
        {
            *read = server.clone();
        }
    }

    /// The sign-in order the pane wants sent: its number, the server, and the address to hand over.
    pub fn sign_wanted(&self) -> Option<(u32, String, Option<String>)> {
        let signing = self.signing.as_ref()?;
        (signing.state == Sign::Asking).then(|| (self.signs, signing.server.clone(), signing.callback.clone()))
    }

    /// How many servers the tab counts.
    pub fn count(&self) -> String {
        match &self.state {
            State::Read(McpReport { servers: Some(servers), .. }) => servers.len().to_string(),
            State::Read(_) => "-".to_owned(),
            _ => "?".to_owned(),
        }
    }

    pub fn key(&mut self, key: Key) -> Option<Action> {
        let last = self.servers().len().saturating_sub(1);
        self.selected = match key {
            Key::Char('u') => {
                self.ask();
                return None;
            }
            Key::Char('s') | Key::Enter => {
                let server = self.servers().get(self.selected)?.name.clone();
                self.signs += 1;
                self.signing = Some(Signing { server, state: Sign::Asking, url: None, callback: None });
                return None;
            }
            Key::Char('y') => return self.signing.as_ref()?.url.as_ref().map(|(url, _)| Action::Copy(url.clone())),
            Key::Char('v') => {
                if self.signing.as_ref().is_some_and(|s| s.url.is_some()) {
                    self.draft = Some(String::new());
                }
                return None;
            }
            Key::Up | Key::Char('k') => self.selected.saturating_sub(1),
            Key::Down | Key::Char('j') => self.selected + 1,
            Key::Home => 0,
            Key::End => last,
            _ => return None,
        }
        .min(last);
        None
    }

    /// Take `key` into the address being written: None when none is, the key being the pane's then. `Enter` hands
    /// it over to the sign-in waiting for it, `Esc` drops it.
    pub fn write(&mut self, key: Key) -> Option<Option<Action>> {
        let draft = self.draft.as_mut()?;
        match key {
            Key::Char(c) => draft.push(c),
            Key::Backspace => _ = draft.pop(),
            Key::Esc => self.draft = None,
            Key::Enter => {
                let callback = self.draft.take().map(|d| d.trim().to_owned()).filter(|d| !d.is_empty());
                if let (Some(callback), Some(signing)) = (callback, self.signing.as_mut()) {
                    self.signs += 1;
                    signing.state = Sign::Asking;
                    signing.callback = Some(callback);
                }
            }
            _ => {}
        }
        Some(None)
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, ant: &UserId, focused: bool) {
        let signing = self.signing.as_ref().map(|s| self.signing_lines(s));
        let inner = usize::from(area.width.saturating_sub(2)).max(1);
        let tall = signing.as_ref().map_or(0, |lines| {
            let rows: usize = lines.iter().map(|line| line.width().div_ceil(inner).max(1)).sum();
            u16::try_from(rows + 2).unwrap_or(u16::MAX)
        });
        let [servers, sign] = Layout::vertical([Constraint::Min(3), Constraint::Max(tall)]).areas(area);
        let block = style::block(format!(" MCP servers of {ant} "), focused && self.draft.is_none());
        let lines = match &self.state {
            State::Unasked | State::Asking => {
                vec![Line::raw(format!("asking the daemon of {ant} for its MCP servers…"))]
            }
            State::Failed(e) => vec![Line::raw(format!("reading the MCP servers: {e}"))],
            State::Read(report) => self.server_lines(report, focused),
        };
        let scroll = u16::try_from(self.selected.saturating_sub(usize::from(servers.height.saturating_sub(4))))
            .unwrap_or(u16::MAX);
        let paragraph = Paragraph::new(lines).block(block).wrap(Wrap { trim: false }).scroll((scroll, 0));
        frame.render_widget(paragraph, servers);
        if let (Some(signing), Some(lines)) = (&self.signing, signing) {
            let title = format!(" sign in to {} ", signing.server);
            let block = style::block(title, focused && self.draft.is_some());
            frame.render_widget(Paragraph::new(lines).block(block).wrap(Wrap { trim: false }), sign);
        }
    }

    fn server_lines(&self, report: &McpReport, focused: bool) -> Vec<Line<'static>> {
        let mut lines = vec![Line::styled(
            format!(
                "harness {} · protocol {} · read {}",
                report.harness,
                report.protocol,
                report.read_at.format("%H:%M:%S")
            ),
            style::dim(),
        )];
        let Some(servers) = &report.servers else {
            lines.push(Line::raw(
                "not reported by this harness: only protocols claude, codex and copilot-sdk report them",
            ));
            return lines;
        };
        if servers.is_empty() {
            lines.push(Line::raw("none"));
        }
        let width = servers.iter().map(|s| s.name.chars().count()).max().unwrap_or(0);
        for (at, server) in servers.iter().enumerate() {
            let color = match server.status {
                McpStatus::Connected => style::CONNECTED,
                McpStatus::Pending => style::PENDING,
                McpStatus::NeedsAuth | McpStatus::Failed => style::ERROR,
                McpStatus::Disabled | McpStatus::Unknown => Color::DarkGray,
            };
            let mut spans = vec![
                Span::raw(format!("{:<width$}  ", server.name)),
                Span::styled(format!("{:<10}", server.status.name()), Style::new().fg(color)),
            ];
            if let Some(tools) = server.tools {
                spans.push(Span::raw(format!("  {tools} tools")));
            }
            if let Some(source) = &server.source {
                spans.push(Span::styled(format!("  ({source})"), style::dim()));
            }
            let mut line = Line::from(spans);
            if at == self.selected {
                line = line.style(style::highlight(focused && self.draft.is_none()));
            }
            lines.push(line);
            if let Some(error) = &server.error {
                lines.push(Line::styled(format!("{:width$}  {error}", ""), style::dim()));
            }
        }
        if report.truncated {
            lines.push(Line::raw("and more, left out"));
        }
        lines
    }

    fn signing_lines(&self, signing: &Signing) -> Vec<Line<'static>> {
        let mut lines = match &signing.state {
            Sign::Asking if signing.callback.is_some() => vec![Line::raw("handing the address over…")],
            Sign::Asking => vec![Line::raw("asking the harness to sign in…")],
            Sign::Failed(e) => vec![Line::raw(format!("signing in: {e}"))],
            Sign::Done(status, error) => {
                let mut lines = vec![Line::raw(format!("now {}", status.name()))];
                lines.extend(error.iter().map(|e| Line::styled(e.clone(), style::dim())));
                lines
            }
            Sign::Waiting => Vec::new(),
        };
        if let (Sign::Waiting | Sign::Failed(_), Some((url, until))) = (&signing.state, &signing.url) {
            let until = until.map_or_default(|at| format!(", until {}", at.format("%H:%M UTC")));
            lines.push(Line::raw(format!("open in a browser to sign in{until} (y copies it):")));
            lines.push(Line::raw(url.clone()));
            lines.push(Line::styled(
                "a browser on the ant's machine comes back on its own; one elsewhere ends on an address that fails to \
                 load: v hands it over",
                style::dim(),
            ));
        }
        if let Some(draft) = &self.draft {
            lines.push(Line::raw(format!("address: {draft}▏")));
        }
        lines
    }
}

/// What an order about an ant's MCP servers brought: the ant, which asking or sign-in it was, and what came or why
/// nothing did.
pub enum Event {
    Read(UserId, u32, Result<McpReport, String>),
    Signed(UserId, u32, Result<McpSignIn, String>),
}

/// The admin orders about the shown ant's MCP servers, signed with the chat's credentials.
pub struct Io {
    js: jetstream::Context,
    url: String,
    creds: Option<PathBuf>,
    domain: Option<JsDomain>,
    /// The ant and asking last read.
    asked: Option<(UserId, u32)>,
    read: Option<LocalBoxFuture<'static, Event>>,
    /// The ant and sign-in last sent.
    signed: Option<(UserId, u32)>,
    sign: Option<LocalBoxFuture<'static, Event>>,
}

/// What the view wants of the shown ant's MCP servers: the ant, its asking, and the sign-in to send.
pub struct Wanted {
    pub ant: UserId,
    pub asked: u32,
    pub sign: Option<(u32, String, Option<String>)>,
}

impl Io {
    pub fn new(nats: &crate::connection::Nats<'_>, js: jetstream::Context) -> Self {
        let (url, creds, domain) = (nats.url.to_owned(), nats.creds.map(PathBuf::from), nats.domain.cloned());
        Self { js, url, creds, domain, asked: None, read: None, signed: None, sign: None }
    }

    /// The next read or sign-in. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        tokio::select! {
            event = maybe(&mut self.read) => {
                self.read = None;
                event
            }
            event = maybe(&mut self.sign) => {
                self.sign = None;
                event
            }
        }
    }

    /// Read the servers of the ant the view `wanted` them for at the asking it names, and send the sign-in it names,
    /// unless done already.
    pub fn watch(&mut self, wanted: Option<Wanted>) {
        let Some(Wanted { ant, asked, sign }) = wanted else { return };
        let nats = (self.js.clone(), self.url.clone(), self.creds.clone(), self.domain.clone());
        if self.asked.as_ref() != Some(&(ant.clone(), asked)) {
            self.asked = Some((ant.clone(), asked));
            let (js, url, creds, domain) = nats.clone();
            let ant = ant.clone();
            self.read = Some(
                async move {
                    let read = async {
                        let nats = crate::connection::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                        crate::admin::mcp::read(&nats, &js, None, &ant).await
                    };
                    Event::Read(ant.clone(), asked, read.await.map_err(|e| format!("{e:#}")))
                }
                .boxed_local(),
            );
        }
        let Some((sign, server, callback)) = sign else { return };
        if self.signed.as_ref() == Some(&(ant.clone(), sign)) {
            return;
        }
        self.signed = Some((ant.clone(), sign));
        let (js, url, creds, domain) = nats;
        self.sign = Some(
            async move {
                let signed = async {
                    let nats = crate::connection::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    crate::admin::mcp::sign_in(&nats, &js, None, &ant, &server, callback).await
                };
                Event::Signed(ant.clone(), sign, signed.await.map_err(|e| format!("{e:#}")))
            }
            .boxed_local(),
        );
    }
}
