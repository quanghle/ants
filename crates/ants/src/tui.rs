//! `ants chat`, which bare `ants` runs on a terminal: the full-screen chat UI. A nav of home (what waits for the
//! human), work (the threads by state), the human's channels, the ants and the people, beside the view selected in it,
//! kept live from what the CLI's own connection reads and hears ([`live::Feed`], [`reads::Reads`], [`people::Io`]),
//! and searched ([`search::Searches`]).
//!
//! What it shows and how keys change it is the [`model::Model`]; each view and component has a module of its own,
//! which draws it and takes its keys.

mod acts;
mod approvals;
mod attach;
mod cards;
mod channel;
mod command;
mod compact;
mod complete;
mod dm;
mod exec;
mod frame;
mod home;
mod images;
mod interrupt;
mod markdown;
mod model;
mod nav;
mod people;
mod react;
mod reads;
mod search;
mod status;
mod thread;
mod timeline;
mod tree;
mod work;

use std::io::{IsTerminal as _, Write as _};
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use ants_proto::Following;
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::ConnectErrorKind;
use chrono::Utc;
use clap::CommandFactory as _;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::crossterm::{execute, terminal};

use self::acts::Acts;
use self::exec::Exec;
use self::model::{Action, Key, Level, Model};
use self::reads::Reads;
use self::search::Searches;
use crate::live::{self, Feed, Screen};
use crate::{compose, presence, progress, sealed, thread_meta};

pub use self::status::run as status;

/// How far back `ants chat` reads threads unless told otherwise, as `ants` alone does.
pub const SINCE: Duration = Duration::from_hours(7 * 24);
/// How long reaching the server may take before the first-run hint shows.
const CONNECT_WITHIN: Duration = Duration::from_secs(5);
/// How often keys are read and the screen redrawn when something changed.
const TICK: Duration = Duration::from_millis(50);
/// How often the screen is redrawn anyway, for the ages it shows.
const REDRAW: Duration = Duration::from_secs(1);

/// For bare `ants` without a terminal on stdin and stdout: print the usage and exit 2, as a missing command does.
pub fn usage_without_terminal() {
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        eprint!("{}", crate::Cli::command().render_help());
        std::process::exit(2);
    }
}

/// Refuse to run without a terminal to draw on and read keys from, naming the commands that do the same in scripts.
fn terminal_check(stdin: bool, stdout: bool) -> Result<()> {
    if stdin && stdout {
        return Ok(());
    }
    bail!(
        "`ants chat` needs a terminal on stdin and stdout; without one, `ants inbox` reads the replies, `ants thread \
         list` and `ants thread show THREAD` the threads, `ants channel read CHANNEL` a channel, `ants roster` the \
         ants, `ants tools list` the tool requests, and `ants ask` and `ants post` send"
    )
}

/// Why connecting failed, when it is likely for want of a server or of credentials, as on a first run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unreached {
    /// No server answered in time, or none listens there.
    NoServer,
    /// The server refused the connection, and no credentials were given.
    Refused,
}

impl Unreached {
    /// How connecting failed with `e`, if for want of a server or of credentials; `creds` tells whether any were given.
    fn of(e: &anyhow::Error, creds: bool) -> Option<Self> {
        let kind = e.chain().find_map(|e| e.downcast_ref::<async_nats::ConnectError>())?.kind();
        match kind {
            ConnectErrorKind::Io | ConnectErrorKind::TimedOut | ConnectErrorKind::Dns => Some(Self::NoServer),
            ConnectErrorKind::AuthorizationViolation | ConnectErrorKind::Authentication if !creds => {
                Some(Self::Refused)
            }
            _ => None,
        }
    }

    /// What to do about it, the server at `url`.
    fn hint(self, url: &str) -> String {
        let what = match self {
            Self::NoServer => format!("no ants server answered at {} within 5s", host(url)),
            Self::Refused => format!("the server at {} refused a connection without credentials", host(url)),
        };
        format!(
            "{what}. On a first machine, `ants setup` sets up a local server and an ant; to join a hub, `ants login`; \
             `ants --help` lists the commands"
        )
    }
}

/// The server `url` names, without its scheme, any user and password, and the servers after the first.
fn host(url: &str) -> &str {
    let first = url.split(',').next().unwrap_or(url);
    let rest = first.split_once("://").map_or(first, |(_, rest)| rest);
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    rest.split(['/', '?', '#']).next().unwrap_or(rest)
}

/// `ants chat`, as the two-line pane when `compact`.
pub async fn run(nats: &crate::Nats<'_>, since: Duration, compact: bool) -> Result<()> {
    terminal_check(std::io::stdin().is_terminal(), std::io::stdout().is_terminal())?;
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; `ants chat` shows what a human reads and answers");
    }
    let (client, js) = match tokio::time::timeout(CONNECT_WITHIN, nats.connect_only("ants-chat")).await {
        Ok(Ok(connected)) => connected,
        Ok(Err(e)) => match Unreached::of(&e, nats.creds.is_some()) {
            Some(unreached) => return Err(anyhow!(unreached.hint(nats.url))),
            None => return Err(e),
        },
        Err(_) => return Err(anyhow!(Unreached::NoServer.hint(nats.url))),
    };
    nats.prepare(&js).await;
    let meta = thread_meta::open(nats, &js).await.map_err(|e| format!("{e:#}"));
    let sealing = sealed::Sealing::of(nats);
    let (feed, state) = live::start(nats, client.clone(), js.clone(), since).await?;
    let ctx = exec::Ctx {
        client,
        js: js.clone(),
        me: nats.me.clone(),
        domain: nats.domain.cloned(),
        meta,
        sealing: sealing.clone(),
        keyring: feed.keyring(),
    };
    let feed = feed.with_timelines();
    let reads = Reads::new(js.clone(), nats.domain.cloned(), me.clone(), crate::channel::reads::allowed(nats));
    let acts = Acts::new(js.clone(), nats.domain.cloned(), me.clone(), thread_meta::open(nats, &js).await);
    let may_read = nats.rights.may_publish(&presence::online_right());
    let people = people::Io::new(js.clone(), nats.domain.cloned(), nats.me.clone(), sealing, may_read);
    let searches = Searches::new(js.clone(), feed.keyring());
    let trees = tree::Io::new(js.clone(), nats.domain.cloned(), me.clone());
    let cards = cards::Io::new(cards::Source::open(nats, &js).await);
    let mut model = Model::new(state, host(nats.url).to_owned());
    model.compact = compact;
    let inline_images = crate::defaults::inline_images()?;
    model.data.images = images::Images::new(inline_images);
    // The UI shows the replies and the tool requests as they come, so notifications spare this device theirs meanwhile.
    let shown = async {
        let mut screen = Screen::open()?;
        let images = inline_images.then(|| images::Io::new(images::picker(), js.clone(), feed.keyring()));
        let exec = Exec::new(ctx);
        let chat = Chat { feed, reads, acts, people, searches, trees, cards, images, exec, model, edit: None };
        Box::pin(chat.run(&mut screen.terminal)).await
    };
    let (domain, device) = (nats.domain, nats.device);
    let approvals = presence::while_following(&js, domain, me, device, Following::Approvals, shown);
    Box::pin(presence::while_following(&js, domain, me, device, Following::Inbox, Box::pin(approvals))).await
}

/// The key `key` is to the UI, if any.
fn key_of(key: KeyEvent) -> Option<Key> {
    if key.code == KeyCode::Enter && key.modifiers.intersects(KeyModifiers::ALT | KeyModifiers::SHIFT) {
        return Some(Key::Newline);
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c') => Some(Key::Interrupt),
            KeyCode::Char(c) => Some(Key::Ctrl(c)),
            _ => None,
        };
    }
    Some(match key.code {
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        KeyCode::Tab | KeyCode::BackTab => Key::Tab,
        KeyCode::Enter => Key::Enter,
        KeyCode::Esc => Key::Esc,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Delete => Key::Delete,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::Char(c) => Key::Char(c),
        _ => return None,
    })
}

/// The UI: what it shows, and what it reads it from.
struct Chat {
    feed: Feed,
    reads: Reads,
    exec: Exec,
    acts: Acts,
    people: people::Io,
    searches: Searches,
    trees: tree::Io,
    cards: cards::Io,
    /// The images drawn, with `[cli]` `inline_images` on.
    images: Option<images::Io>,
    model: Model,
    /// The command bar's text to edit in the editor.
    edit: Option<String>,
}

impl Chat {
    async fn run(mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (mut dirty, mut drawn, mut logged) = (true, Instant::now(), None);
        loop {
            if let Some(action) = self.model.link(self.feed.connected(), Utc::now())
                && self.act(action).is_break()
            {
                return Ok(());
            }
            let actions = tokio::select! {
                _ = ticks.tick() => {
                    if self.read_terminal(&mut dirty)?.is_break() {
                        return Ok(());
                    }
                    let log = progress::last_held_log();
                    if log != logged {
                        if let Some(warning) = log.as_deref().and_then(live::warning) {
                            self.model.notify(warning, Level::Error, Utc::now());
                            dirty = true;
                        }
                        logged = log;
                    }
                    self.cards.threads(&self.model.data.live.order);
                    if dirty || drawn.elapsed() >= REDRAW {
                        terminal.draw(|f| frame::draw(f, &self.model, Utc::now())).context("drawing")?;
                        (dirty, drawn) = (false, Instant::now());
                    }
                    Vec::new()
                }
                event = self.feed.next() => {
                    dirty = true;
                    self.model.take(event, Utc::now())
                }
                read = self.reads.next() => {
                    dirty = true;
                    self.model.read(read, Utc::now());
                    Vec::new()
                }
                outcome = self.exec.next() => {
                    dirty = true;
                    self.model.outcome(outcome).into_iter().collect()
                }
                acted = self.acts.next() => {
                    dirty = true;
                    self.model.acted(acted, Utc::now());
                    Vec::new()
                }
                event = self.people.next() => {
                    dirty = true;
                    self.model.people(event, Utc::now());
                    Vec::new()
                }
                found = self.searches.next() => {
                    dirty = true;
                    self.model.found(found);
                    Vec::new()
                }
                read = self.cards.next() => {
                    dirty = true;
                    self.model.cards(read, Utc::now());
                    Vec::new()
                }
                event = self.trees.next() => {
                    dirty = true;
                    self.model.tree_event(event, Utc::now());
                    Vec::new()
                }
                (handle, image) = next_image(self.images.as_mut()) => {
                    dirty = true;
                    self.model.data.images.insert(handle, image);
                    Vec::new()
                }
            };
            for action in actions.into_iter().chain(self.model.mark_shown()) {
                if self.act(action).is_break() {
                    return Ok(());
                }
            }
            if let Some(text) = self.edit.take() {
                self.model.edited(edit(terminal, text).await);
                dirty = true;
                if let Some(images) = &mut self.images {
                    images.reset();
                    self.model.data.images.clear();
                }
            }
            self.acts.watch(self.model.open().cloned());
            self.trees.watch(self.model.tree_shown());
            if let Some(images) = &mut self.images {
                let wanted = images::wanted(&self.model);
                images.want(&wanted);
                self.model.data.images.retain(&wanted);
            }
        }
    }

    /// Act on the keys pressed, setting `dirty` when the screen is to be drawn again; breaks on quitting.
    fn read_terminal(&mut self, dirty: &mut bool) -> Result<ControlFlow<()>> {
        while event::poll(Duration::ZERO).context("reading the terminal")? {
            match event::read().context("reading the terminal")? {
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    *dirty = true;
                    if let Some(action) = key_of(key).and_then(|key| self.model.key(key, Utc::now()))
                        && self.act(action).is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Event::Resize(..) => *dirty = true,
                _ => {}
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn act(&mut self, action: Action) -> ControlFlow<()> {
        match action {
            Action::Quit => return ControlFlow::Break(()),
            Action::Open(thread) => {
                self.feed.load_history(thread.clone(), true);
                self.reads.mark_read(thread);
            }
            Action::Change(thread, change, last) => self.acts.change(thread, change, last),
            Action::Copy(text) => {
                let copied =
                    std::io::stdout().write_all(osc52(&text).as_bytes()).and_then(|()| std::io::stdout().flush());
                let line = match copied {
                    Ok(()) => (format!("copied {} characters", text.chars().count()), Level::Info),
                    Err(e) => (format!("copying: {e}"), Level::Error),
                };
                self.model.notify(line.0, line.1, Utc::now());
            }
            Action::MarkRead(thread) => self.reads.mark_read(thread),
            Action::Run(job, text) => self.exec.run(job, text),
            Action::Decide(request, verdict) => self.feed.decide(*request, verdict, self.model.data.me().clone()),
            Action::Edit(text) => self.edit = Some(text),
            Action::MarkChannel(channel, marks) => self.reads.mark_channel(channel, marks),
            Action::Reactions(messages) => self.reads.watch_reactions(messages),
            Action::React(message, reaction, remove) => self.reads.react(message, reaction, remove),
            Action::Send(draft) => self.people.send(draft),
            Action::Search(search) => self.searches.start(search),
            Action::CancelSubtree(prompts) => {
                let text = format!("asking {} ants to cancel their prompts…", prompts.len());
                self.model.notify(text, Level::Info, Utc::now());
                self.trees.cancel(prompts);
            }
            Action::Refresh => {
                self.feed.refresh();
                self.reads.refresh();
                self.acts.refresh();
                self.people.refresh();
                self.trees.refresh();
                self.cards.refresh();
                if let Some(thread) = self.model.open_thread() {
                    self.feed.load_history(thread, true);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// The next image encoded, when images are drawn; pending otherwise.
async fn next_image(
    images: Option<&mut images::Io>,
) -> (ants_proto::AttachmentHandle, Option<ratatui_image::protocol::Protocol>) {
    match images {
        Some(images) => images.next().await,
        None => std::future::pending().await,
    }
}

/// Edit `text` in the editor, as `ants ask --edit` does, the terminal handed over to it meanwhile.
async fn edit(terminal: &mut DefaultTerminal, text: String) -> Result<String> {
    ratatui::restore();
    let edited = tokio::task::spawn_blocking(move || compose::edit_text(&text)).await;
    terminal::enable_raw_mode().context("setting up the terminal")?;
    execute!(std::io::stdout(), terminal::EnterAlternateScreen).context("setting up the terminal")?;
    terminal.clear().context("drawing")?;
    edited.context("running the editor")?
}

/// The OSC 52 sequence that has the terminal put `text` on its clipboard, through SSH and tmux alike.
fn osc52(text: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(text.len().div_ceil(3) * 4);
    for chunk in text.as_bytes().chunks(3) {
        let bytes = [chunk[0], chunk.get(1).copied().unwrap_or(0), chunk.get(2).copied().unwrap_or(0)];
        let n = u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]);
        for at in 0..4 {
            let sextet = (n >> (18 - 6 * at)) & 63;
            encoded.push(if at <= chunk.len() { char::from(ALPHABET[sextet as usize]) } else { '=' });
        }
    }
    format!("\x1b]52;c;{encoded}\x07")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_copy_is_the_text_in_base64_in_an_osc_52_sequence() {
        assert_eq!(osc52(""), "\x1b]52;c;\x07");
        assert_eq!(osc52("f"), "\x1b]52;c;Zg==\x07");
        assert_eq!(osc52("fo"), "\x1b]52;c;Zm8=\x07");
        assert_eq!(osc52("foo"), "\x1b]52;c;Zm9v\x07");
        assert_eq!(osc52("héllo wörld"), "\x1b]52;c;aMOpbGxvIHfDtnJsZA==\x07");
    }

    #[test]
    fn without_a_terminal_chat_names_the_commands_for_scripts() {
        assert!(terminal_check(true, true).is_ok());
        for (stdin, stdout) in [(false, true), (true, false), (false, false)] {
            let e = terminal_check(stdin, stdout).unwrap_err().to_string();
            assert!(e.starts_with("`ants chat` needs a terminal"), "{e}");
            assert!(e.contains("`ants inbox`") && e.contains("`ants thread list`"), "{e}");
        }
    }

    #[test]
    fn ctrl_keys_but_ctrl_c_are_left_to_the_views() {
        let key = |code, modifiers| key_of(KeyEvent::new(code, modifiers));
        assert_eq!(key(KeyCode::Char('c'), KeyModifiers::CONTROL), Some(Key::Interrupt));
        assert_eq!(key(KeyCode::Char('k'), KeyModifiers::CONTROL), Some(Key::Ctrl('k')));
        assert_eq!(key(KeyCode::Char('?'), KeyModifiers::SHIFT), Some(Key::Char('?')));
        assert_eq!(key(KeyCode::BackTab, KeyModifiers::SHIFT), Some(Key::Tab));
        assert_eq!(key(KeyCode::F(1), KeyModifiers::NONE), None);
        assert_eq!(key(KeyCode::Enter, KeyModifiers::ALT), Some(Key::Newline));
        assert_eq!(key(KeyCode::Enter, KeyModifiers::NONE), Some(Key::Enter));
        assert_eq!(key(KeyCode::Backspace, KeyModifiers::NONE), Some(Key::Backspace));
    }

    #[test]
    fn the_server_shows_without_scheme_user_or_other_servers() {
        assert_eq!(host("nats://127.0.0.1:4222"), "127.0.0.1:4222");
        assert_eq!(host("tls://alice:secret@hub.example:4222,tls://b:4222"), "hub.example:4222");
        assert_eq!(host("wss://hub.example/nats"), "hub.example");
        assert_eq!(host("hub:4222"), "hub:4222");
    }

    #[tokio::test]
    async fn a_first_run_without_a_server_gets_a_hint_instead_of_the_connection_error() {
        // Nothing listens on port 1.
        let me = "human".parse().unwrap();
        let e = ants_daemon::bus::connect("nats://127.0.0.1:1", None, None, &me, "test").await.unwrap_err();
        assert_eq!(Unreached::of(&e, false), Some(Unreached::NoServer), "{e:#}");
        assert_eq!(Unreached::of(&e, true), Some(Unreached::NoServer), "{e:#}");
        let hint = Unreached::NoServer.hint("nats://127.0.0.1:1");
        assert!(hint.starts_with("no ants server answered at 127.0.0.1:1 within 5s."), "{hint}");
        assert!(hint.contains("`ants setup`") && hint.contains("`ants login`") && hint.contains("`ants --help`"));
        assert_eq!(Unreached::of(&anyhow!("reading the stream"), false), None);
    }
}
