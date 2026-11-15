//! `ants dash`: an interactive terminal dashboard of the recent threads (but for those the human archived, by hand
//! or for having been idle) and their history, the ants' live progress and
//! the roster, and the pending tool requests, answered as `ants tools approve` and `reject` do. It reads with the
//! CLI's own connection, as `ants thread`, `ants watch` and `ants tools list --follow` do, and keeps no service.

mod model;
mod view;

use std::io::IsTerminal as _;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use ants_proto::Following;
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use self::model::{Action, Key, Level, Model};
use crate::live::{self, Feed, Screen};
use crate::{presence, progress};

/// How often keys are read and the screen redrawn when something changed.
const TICK: Duration = Duration::from_millis(50);
/// How often the screen is redrawn anyway, for the ages it shows.
const REDRAW: Duration = Duration::from_secs(1);

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
    let (feed, live) = live::open(nats, "ants-dash", since).await?;
    let js = feed.js().clone();
    // The dashboard shows the tool requests as they come, so notifications spare this device theirs meanwhile.
    let shown = async {
        let mut screen = Screen::open()?;
        Box::pin(Dash { feed, model: Model::new(live) }.run(&mut screen.terminal)).await
    };
    Box::pin(presence::while_following(&js, nats.domain, me, nats.device, Following::Approvals, shown)).await
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

/// The dashboard: what it shows, and the feed it reads it from.
struct Dash {
    feed: Feed,
    model: Model,
}

impl Dash {
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
            tokio::select! {
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
                    if dirty || drawn.elapsed() >= REDRAW {
                        terminal.draw(|frame| view::draw(frame, &self.model, Utc::now())).context("drawing")?;
                        (dirty, drawn) = (false, Instant::now());
                    }
                }
                event = self.feed.next() => {
                    dirty = true;
                    if let Some(action) = self.take(event) && self.act(action).is_break() {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Take in what the feed read or heard.
    fn take(&mut self, event: live::Event) -> Option<Action> {
        let (model, now) = (&mut self.model, Utc::now());
        match event {
            live::Event::Stored(Ok(entries)) => return model.stored(entries, now),
            live::Event::Stored(Err(e)) => model.notify(format!("{e:#}"), Level::Error, now),
            live::Event::Status(event) => model.live.status(&event, now),
            live::Event::Following => model.approvals_reset(),
            live::Event::Approval(update) => model.approval(update),
            live::Event::FollowEnded(Ok(())) => {}
            live::Event::FollowEnded(Err(e)) => {
                model.notify(format!("following the tool requests: {e:#}"), Level::Error, now);
            }
            live::Event::Roster(Ok(entries)) => model.live.roster(entries, now),
            live::Event::Roster(Err(e)) => model.notify(format!("reading the roster: {e:#}"), Level::Error, now),
            live::Event::History(thread, read) => model.history_loaded(&thread, read, now),
            live::Event::Decided(outcome) => model.decided(outcome, now),
        }
        None
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
            Action::LoadHistory(thread) => self.feed.load_history(thread, false),
            Action::Decide(request, verdict) => self.feed.decide(*request, verdict, self.model.live.me.clone()),
            Action::Refresh => {
                self.feed.refresh();
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
    fn ctrl_c_quits_and_other_keys_map_to_the_dashboard_s() {
        let key = |code, modifiers| key_of(KeyEvent::new(code, modifiers));
        assert_eq!(key(KeyCode::Char('c'), KeyModifiers::CONTROL), Some(Key::Interrupt));
        assert_eq!(key(KeyCode::Char('x'), KeyModifiers::CONTROL), None);
        assert_eq!(key(KeyCode::Char('A'), KeyModifiers::SHIFT), Some(Key::Char('A')));
        assert_eq!(key(KeyCode::BackTab, KeyModifiers::SHIFT), Some(Key::Tab));
        assert_eq!(key(KeyCode::F(1), KeyModifiers::NONE), None);
    }
}
