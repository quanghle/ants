//! The memories pane of the ant view: the ant's memories as `ants ant memories` shows them ([`crate::memories`]),
//! read-only, which [`Io`] asks the daemon of its machine for with an admin order when the pane first shows and on
//! `u`.

use std::path::PathBuf;

use ants_proto::{JsDomain, Memories, UserId};
use async_nats::jetstream;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Paragraph, Wrap};

use super::model::Key;
use super::style;
use crate::live::maybe;

/// The ant's memories, as last asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Unasked,
    Asking,
    Failed(String),
    /// The memories as text, and how many files and bd memories they hold.
    Read(String, usize),
}

/// The pane's memories, how many times they were asked for, and how far down they scroll.
#[derive(Debug)]
pub struct Page {
    pub state: State,
    pub asked: u32,
    scroll: u16,
}

impl Page {
    pub const KEYS: &str =
        "↑↓ scroll · u read again · ←→ pane · a ask · w work · r restart · p (un)pause · X retire · Esc back";

    pub fn new() -> Self {
        Self { state: State::Unasked, asked: 0, scroll: 0 }
    }

    /// Ask for the memories, unless they were already.
    pub fn shown(&mut self) {
        if self.state == State::Unasked {
            self.ask();
        }
    }

    fn ask(&mut self) {
        self.state = State::Asking;
        self.asked += 1;
    }

    /// Take in the memories of the `asked`th asking, unless asked again since.
    pub fn read(&mut self, ant: &UserId, asked: u32, read: &Result<Memories, String>) {
        if asked != self.asked {
            return;
        }
        self.state = match read {
            Ok(memories) => {
                let held = memories.files.len() + memories.bd.as_ref().map_or(0, |bd| bd.memories.len());
                State::Read(crate::memories::render(ant, memories), held)
            }
            Err(e) => State::Failed(e.clone()),
        };
        self.scroll = 0;
    }

    /// How many files and bd memories the tab counts.
    pub fn count(&self) -> String {
        match &self.state {
            State::Read(_, held) => held.to_string(),
            _ => "?".to_owned(),
        }
    }

    pub fn key(&mut self, key: Key) {
        let lines = match &self.state {
            State::Read(text, _) => u16::try_from(text.lines().count()).unwrap_or(u16::MAX),
            _ => 0,
        };
        self.scroll = match key {
            Key::Char('u') => {
                self.ask();
                0
            }
            Key::Up | Key::Char('k') => self.scroll.saturating_sub(1),
            Key::Down | Key::Char('j') => self.scroll + 1,
            Key::PageUp => self.scroll.saturating_sub(10),
            Key::PageDown => self.scroll.saturating_add(10),
            Key::Home => 0,
            Key::End => u16::MAX,
            _ => return,
        }
        .min(lines.saturating_sub(1));
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, ant: &UserId, focused: bool) {
        let block = style::block(format!(" memories of {ant} · read-only "), focused);
        let text = match &self.state {
            State::Unasked | State::Asking => format!("asking the daemon of {ant} for its memories…"),
            State::Failed(e) => format!("reading the memories: {e}"),
            State::Read(text, _) => text.clone(),
        };
        let paragraph = Paragraph::new(text).block(block).wrap(Wrap { trim: false }).scroll((self.scroll, 0));
        frame.render_widget(paragraph, area);
    }
}

/// What a read of an ant's memories brought: the ant, which asking of it, and the memories or why none came.
pub struct Event(pub UserId, pub u32, pub Result<Memories, String>);

/// The reads of the shown ant's memories, through admin orders signed with the chat's credentials.
pub struct Io {
    js: jetstream::Context,
    url: String,
    creds: Option<PathBuf>,
    domain: Option<JsDomain>,
    /// The ant and asking last read.
    asked: Option<(UserId, u32)>,
    read: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(nats: &crate::connection::Nats<'_>, js: jetstream::Context) -> Self {
        let (url, creds, domain) = (nats.url.to_owned(), nats.creds.map(PathBuf::from), nats.domain.cloned());
        Self { js, url, creds, domain, asked: None, read: None }
    }

    /// The next read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        let event = maybe(&mut self.read).await;
        self.read = None;
        event
    }

    /// Read the memories of the ant the view `wanted` them for, at the asking it names, unless read already.
    pub fn watch(&mut self, wanted: Option<(UserId, u32)>) {
        if wanted.is_none() || wanted == self.asked {
            return;
        }
        self.asked.clone_from(&wanted);
        let Some((ant, asked)) = wanted else { return };
        let (js, url, creds, domain) = (self.js.clone(), self.url.clone(), self.creds.clone(), self.domain.clone());
        self.read = Some(
            async move {
                let read = async {
                    let nats = crate::connection::Nats::new(&url, creds.as_deref(), domain.as_ref())?;
                    crate::memories::read(&nats, &js, None, &ant).await
                };
                Event(ant.clone(), asked, read.await.map_err(|e| format!("{e:#}")))
            }
            .boxed_local(),
        );
    }
}
