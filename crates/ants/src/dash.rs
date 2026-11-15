//! `ants dash`: an interactive terminal dashboard of the recent threads (but for those the human archived, by hand
//! or for having been idle) and their history, the ants' live progress and
//! the roster, and the pending tool requests, answered as `ants tools approve` and `reject` do. It reads with the
//! CLI's own connection, as `ants thread`, `ants watch` and `ants tools list --follow` do, and keeps no service.

mod model;
mod view;

use std::io::IsTerminal as _;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use ants_daemon::reading::{self, Reading};
use ants_proto::{
    ApprovalRequest, Following, RosterEntry, STREAM, StatusEvent, ThreadId, ThreadMarks, UserId, Verdict, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::connection::State as Connection;
use async_nats::jetstream::{self, consumer};
use chrono::Utc;
use futures::future::LocalBoxFuture;
use futures::{FutureExt as _, StreamExt as _};
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use self::model::{Action, Key, Level, Model};
use crate::thread::{self, Entry, Parser};
use crate::{presence, progress, roster, thread_meta, tools};

/// How often keys are read and the screen redrawn when something changed.
const TICK: Duration = Duration::from_millis(50);
/// How often the screen is redrawn anyway, for the ages it shows.
const REDRAW: Duration = Duration::from_secs(1);
/// How often the stream is read for what was stored without being heard, such as a leaf node's sourced messages, and
/// following the tool requests starts again after it ended.
const REREAD: Duration = Duration::from_secs(5);
/// How often the roster is read.
const ROSTER_EVERY: Duration = Duration::from_secs(10);

/// Refuse to run without a terminal to draw on and read keys from, naming the commands that do the same in scripts.
fn terminal_check(stdin: bool, stdout: bool) -> Result<()> {
    if stdin && stdout {
        return Ok(());
    }
    bail!(
        "`ants dash` needs a terminal on stdin and stdout; without one, `ants thread list` and `ants thread show \
         THREAD` read the threads, `ants roster` and `ants watch ANT` the ants and their progress, and `ants tools \
         list --follow`, `ants tools show ID`, `ants tools approve ID` and `ants tools reject ID` the tool requests"
    )
}

pub async fn run(nats: &crate::Nats<'_>, since: Duration) -> Result<()> {
    terminal_check(std::io::stdin().is_terminal(), std::io::stdout().is_terminal())?;
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; `ants dash` shows what a human reads and answers");
    }
    let auto_archive = crate::defaults::auto_archive()?;
    let (client, js) = nats.connect("ants-dash").await?;
    let marks = async { thread_meta::open(nats, &js).await?.marks().await };
    let marks = marks.await.unwrap_or_else(|e| {
        tracing::warn!("not leaving archived threads out: {e:#}");
        ThreadMarks::default()
    });
    // The dashboard shows the tool requests as they come, so notifications spare this device theirs meanwhile.
    let shown = async {
        let mut screen = Screen::open()?;
        let mut model = Model::new(me.clone(), since);
        (model.marks, model.auto_archive) = (marks, auto_archive);
        Box::pin(Dash::new(client, js.clone(), model).run(&mut screen.terminal)).await
    };
    Box::pin(presence::while_following(&js, nats.domain, me, nats.device, Following::Approvals, shown)).await
}

/// The terminal, drawn on full screen while this lives; log lines are kept meanwhile and written once it is restored.
struct Screen {
    terminal: DefaultTerminal,
}

impl Screen {
    fn open() -> Result<Self> {
        progress::hold_logs();
        match ratatui::try_init() {
            Ok(terminal) => Ok(Self { terminal }),
            Err(e) => {
                ratatui::restore();
                release_logs();
                Err(e).context("setting up the terminal")
            }
        }
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        ratatui::restore();
        release_logs();
    }
}

fn release_logs() {
    for line in progress::release_logs() {
        eprintln!("{line}");
    }
}

/// `text` without its terminal escape sequences, such as a log line's colours.
fn plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // A CSI sequence ends with a letter.
            if chars.next() == Some('[') {
                chars.by_ref().find(char::is_ascii_alphabetic);
            }
        } else if !c.is_control() {
            out.push(c);
        }
    }
    out
}

/// A log line worth showing on the screen, a warning or an error, without its colours.
fn warning(line: &str) -> Option<String> {
    let line = plain(line);
    line.split_whitespace().take(2).any(|word| word == "WARN" || word == "ERROR").then_some(line)
}

/// The dashboard key `key` is, if any.
fn key_of(key: KeyEvent) -> Option<Key> {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return matches!(key.code, KeyCode::Char('c')).then_some(Key::Interrupt);
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
        KeyCode::Char(c) => Key::Char(c),
        _ => return None,
    })
}

type Tail = LocalBoxFuture<'static, (Parser, Result<(Vec<Entry>, u64)>)>;

/// Read the messages stored after sequence `after`, or else those of the window `since`, with `parser`, which keeps
/// the threads of the tool requests read for their decisions. Returns them and the sequence read up to.
fn tail(js: jetstream::Context, mut parser: Parser, after: Option<u64>, since: Duration) -> Tail {
    async move {
        let read = async {
            let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
            let mut last = stream.cached_info().state.last_sequence;
            let start = match after {
                Some(after) if after >= last => return Ok((Vec::new(), after)),
                Some(after) => consumer::DeliverPolicy::ByStartSequence { start_sequence: after + 1 },
                None => thread::window(since),
            };
            let mut entries = Vec::new();
            reading::read_back(&js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
                last = last.max(seq);
                entries.extend(parser.entry(seq, stored.into(), subject, payload));
                ControlFlow::Continue(())
            })
            .await
            .context("reading the stream")?;
            Ok((entries, last))
        }
        .await;
        (parser, read)
    }
    .boxed_local()
}

/// Store `verdict` on `request` as `ants tools approve` or `reject` does, a decision stored first winning; returns the
/// line saying so, with the allow rule an approval suggests.
async fn decide(js: jetstream::Context, request: ApprovalRequest, verdict: Verdict, by: UserId) -> Result<String> {
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = tools::find(&stream, &tools::qualified(&request)).await?;
    let decision = tools::answer_request(&js, &stream, seq, &request, verdict, &by, Utc::now()).await?;
    let mut text = format!("answered request {}/{}: {}", decision.ant, decision.request_id, tools::spelling(verdict));
    if verdict != Verdict::Reject
        && let Ok(Some(suggestion)) = tools::suggestion(&stream, seq, &request).await
    {
        text = format!("{text}; {}", tools::suggestion_text(&request, &suggestion));
    }
    Ok(text)
}

/// Await `work` while there is one; it is taken once done.
async fn maybe<T>(work: &mut Option<LocalBoxFuture<'static, T>>) -> T {
    match work {
        Some(work) => work.await,
        None => std::future::pending().await,
    }
}

/// The dashboard's connection and its reads in flight, at most one of each kind.
struct Dash {
    client: async_nats::Client,
    js: jetstream::Context,
    model: Model,
    /// The parser for the next read of the stream, None while one is in flight.
    parser: Option<Parser>,
    tail: Option<Tail>,
    /// Something was heard that may have been stored.
    tail_wanted: bool,
    history: Option<LocalBoxFuture<'static, (ThreadId, Result<Vec<Entry>>)>>,
    decision: Option<LocalBoxFuture<'static, Result<String>>>,
    follow: Option<LocalBoxFuture<'static, Result<()>>>,
    /// When following the tool requests may start again.
    follow_at: Instant,
    roster: Option<LocalBoxFuture<'static, Result<Vec<RosterEntry>>>>,
}

impl Dash {
    fn new(client: async_nats::Client, js: jetstream::Context, model: Model) -> Self {
        Self {
            client,
            js,
            model,
            parser: Some(Parser::default()),
            tail: None,
            tail_wanted: true,
            history: None,
            decision: None,
            follow: None,
            follow_at: Instant::now(),
            roster: None,
        }
    }

    async fn run(mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        let mut statuses = self.client.subscribe(subject::ALL).await.context("following ants traffic")?;
        let (updates, mut updated) = tokio::sync::mpsc::unbounded_channel();
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut rereads = tokio::time::interval(REREAD);
        let mut rosters = tokio::time::interval(ROSTER_EVERY);
        let (mut dirty, mut drawn, mut logged) = (true, Instant::now(), None);
        loop {
            let now = Utc::now();
            let connected = self.client.connection_state() == Connection::Connected;
            if let Some(action) = self.model.link(connected, now)
                && self.act(action).is_break()
            {
                return Ok(());
            }
            if connected
                && self.tail_wanted
                && let Some(parser) = self.parser.take()
            {
                self.tail_wanted = false;
                self.tail = Some(tail(self.js.clone(), parser, self.model.stored_seq, self.model.since));
            }
            if connected && self.follow.is_none() && Instant::now() >= self.follow_at {
                self.follow_tools(&updates);
                dirty = true;
            }
            tokio::select! {
                _ = ticks.tick() => {
                    if self.read_terminal(&mut dirty)?.is_break() {
                        return Ok(());
                    }
                    let log = progress::last_held_log();
                    if log != logged {
                        if let Some(warning) = log.as_deref().and_then(warning) {
                            self.model.notify(warning, Level::Error, Utc::now());
                            dirty = true;
                        }
                        logged = log;
                    }
                    if dirty || drawn.elapsed() >= REDRAW {
                        terminal.draw(|frame| view::draw(frame, &self.model, Utc::now())).context("drawing")?;
                        (dirty, drawn) = (false, Instant::now());
                    }
                }
                Some(msg) = statuses.next() => dirty |= self.heard(&msg, now),
                Some(update) = updated.recv() => {
                    self.model.approval(update);
                    dirty = true;
                }
                _ = rereads.tick() => self.tail_wanted = true,
                _ = rosters.tick() => self.read_roster(),
                (parser, read) = maybe(&mut self.tail) => {
                    self.tail = None;
                    self.parser = Some(parser);
                    let action = match read {
                        Ok((entries, last)) => self.model.stored(entries, last, Utc::now()),
                        Err(e) => {
                            self.model.notify(format!("{e:#}"), Level::Error, Utc::now());
                            None
                        }
                    };
                    if let Some(action) = action && self.act(action).is_break() {
                        return Ok(());
                    }
                    dirty = true;
                }
                (thread, read) = maybe(&mut self.history) => {
                    self.history = None;
                    self.model.history_loaded(&thread, read, Utc::now());
                    dirty = true;
                }
                outcome = maybe(&mut self.decision) => {
                    self.decision = None;
                    self.model.decided(outcome, Utc::now());
                    dirty = true;
                }
                followed = maybe(&mut self.follow) => {
                    self.follow = None;
                    self.follow_at = Instant::now() + REREAD;
                    if let Err(e) = followed {
                        self.model.notify(format!("following the tool requests: {e:#}"), Level::Error, Utc::now());
                    }
                    dirty = true;
                }
                read = maybe(&mut self.roster) => {
                    self.roster = None;
                    match read {
                        Ok(entries) => self.model.roster(entries, Utc::now()),
                        Err(e) => self.model.notify(format!("reading the roster: {e:#}"), Level::Error, Utc::now()),
                    }
                    dirty = true;
                }
            }
        }
    }

    /// Follow the tool requests, sending their updates to `updates`.
    fn follow_tools(&mut self, updates: &tokio::sync::mpsc::UnboundedSender<tools::Update>) {
        self.model.approvals_reset();
        let (client, js, updates) = (self.client.clone(), self.js.clone(), updates.clone());
        let follow = async move {
            tools::follow(&client, &js, |update| {
                let _ = updates.send(update.clone());
                Ok(())
            })
            .await
        };
        self.follow = Some(follow.boxed_local());
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

    /// Take in `msg`, heard on the ants traffic: whether the screen is to be drawn again.
    fn heard(&mut self, msg: &async_nats::Message, now: chrono::DateTime<Utc>) -> bool {
        if msg.subject.starts_with("ants.status.") {
            if let Ok(event) = serde_json::from_slice::<StatusEvent>(&msg.payload)
                && event.check_subject(&msg.subject).is_ok()
            {
                self.model.status(&event, now);
                return true;
            }
        } else {
            self.tail_wanted = true;
        }
        false
    }

    fn read_roster(&mut self) {
        if self.roster.is_none() {
            let js = self.js.clone();
            self.roster = Some(async move { roster::list(&js).await }.boxed_local());
        }
    }

    fn act(&mut self, action: Action) -> ControlFlow<()> {
        match action {
            Action::Quit => return ControlFlow::Break(()),
            Action::LoadHistory(thread) => {
                let js = self.js.clone();
                self.history = Some(
                    async move {
                        let read = thread::history(&js, &thread, None).await;
                        (thread, read)
                    }
                    .boxed_local(),
                );
            }
            Action::Decide(request, verdict) => {
                self.decision = Some(decide(self.js.clone(), *request, verdict, self.model.me.clone()).boxed_local());
            }
            Action::Refresh => {
                self.follow = None;
                self.follow_at = Instant::now();
                self.tail_wanted = true;
                self.roster = None;
                self.read_roster();
                if let Some(action) = self.model.reload_history() {
                    return self.act(action);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_terminal_the_dashboard_names_the_commands_for_scripts() {
        assert!(terminal_check(true, true).is_ok());
        for (stdin, stdout) in [(false, true), (true, false), (false, false)] {
            let e = terminal_check(stdin, stdout).unwrap_err().to_string();
            assert!(e.contains("needs a terminal"), "{e}");
            assert!(e.contains("`ants thread list`") && e.contains("`ants tools approve ID`"), "{e}");
        }
    }

    #[test]
    fn warnings_logged_show_without_their_colours() {
        let warn = "2026-11-14T22:08:00Z \x1b[33m WARN\x1b[0m ants: lost\r";
        assert_eq!(warning(warn).as_deref(), Some("2026-11-14T22:08:00Z  WARN ants: lost"));
        assert_eq!(warning("2026-11-14T22:08:00Z \x1b[32m INFO\x1b[0m async_nats: event: connected"), None);
    }

    #[test]
    fn ctrl_c_quits_and_other_keys_map_to_the_dashboard_s() {
        let key = |code, modifiers| key_of(KeyEvent::new(code, modifiers));
        assert_eq!(key(KeyCode::Char('c'), KeyModifiers::CONTROL), Some(Key::Interrupt));
        assert_eq!(key(KeyCode::Char('x'), KeyModifiers::CONTROL), None);
        assert_eq!(key(KeyCode::Char('A'), KeyModifiers::SHIFT), Some(Key::Char('A')));
        assert_eq!(key(KeyCode::BackTab, KeyModifiers::SHIFT), Some(Key::Tab));
        assert_eq!(key(KeyCode::F(1), KeyModifiers::NONE), None);
    }
}
