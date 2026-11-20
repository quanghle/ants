//! Replaying and dropping dead letters from the command bar, as `ants dead replay` and `ants dead drop` do: `/dead
//! replay|drop [SEQ]` acts on the dead letter at SEQ, else on the one selected in a thread (its `gave up` line, or the
//! daemon's notice of giving up on the message), else on the latest of the thread open or selected, such as home's
//! stuck item; a drop waits for `y`. A dead letter replayed or dropped, here or before the read, and the notice of it,
//! leave the thread stuck no more.

use ants_proto::NoticeKind;
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::command::{Dead, Tone};
use super::exec::Job;
use super::model::{Action, Key, Model, Page, Prompt};
use crate::text::excerpt;
use crate::threads::thread::{Entry, Kind};

/// A dead letter `/dead` acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Letter {
    /// Its stream sequence, as `ants dead list` lists it.
    pub seq: u64,
    /// The text of the message given up on, when read.
    pub text: Option<String>,
}

impl Letter {
    fn of(entry: &Entry) -> Self {
        Self { seq: entry.seq, text: Some(entry.text.clone()) }
    }

    /// The footer asking for `y` to drop it.
    pub fn line(&self) -> Line<'static> {
        let letter = match &self.text {
            Some(text) => format!("dead letter {} (\"{}\")", self.seq, excerpt(text, 40)),
            None => format!("dead letter {}", self.seq),
        };
        let text = format!(" {letter}: y drops it unreplayed; any other key keeps it");
        Line::styled(text, Style::new().fg(Color::Yellow))
    }

    /// Take `key`: `y` drops it, another key keeps it.
    pub fn key(self, key: Key, model: &mut Model) -> Option<Action> {
        if key != Key::Char('y') {
            return None;
        }
        model.bar.set_status(format!("dropping dead letter {}…", self.seq), Tone::Busy);
        Some(Action::Run(Job::DeadDrop(self.seq), String::new()))
    }
}

impl Model {
    /// The dead letter `/dead` acts on: the one at `seq`, else the one selected in the thread view, else the latest of
    /// the thread acted on ([`Model::target`]) not replayed or dropped here.
    pub(super) fn letter(&self, seq: Option<u64>) -> Result<Letter, String> {
        let (entries, chosen) = match self.page() {
            Page::Thread(page) => (&page.history.entries[..], page.chosen()),
            _ => (&[][..], None),
        };
        let dead = |e: &&Entry| e.kind == Kind::Dead;
        let picked = match (seq, chosen) {
            (Some(seq), _) => {
                Some(entries.iter().filter(dead).find(|e| e.seq == seq).map_or(Letter { seq, text: None }, Letter::of))
            }
            (None, Some(e)) if e.kind == Kind::Dead => Some(Letter::of(e)),
            // The notice of giving up on a message follows its dead letter.
            (None, Some(e)) if e.notice == Some(NoticeKind::GaveUp) && e.in_reply_to.is_some() => {
                entries.iter().rev().filter(dead).find(|d| d.seq < e.seq).map(Letter::of)
            }
            _ => None,
        };
        let cleared = &self.data.dead_cleared;
        if let Some(letter) = picked {
            if cleared.contains(&letter.seq) {
                return Err(format!("dead letter {} is replayed or dropped already", letter.seq));
            }
            return Ok(letter);
        }
        let thread = self.target()?;
        let read = entries.iter().rev().filter(dead).find(|e| !e.forged && !cleared.contains(&e.seq)).map(Letter::of);
        read.or_else(|| self.data.dead_letter(&thread).map(|a| Letter { seq: a.seq, text: Some(a.text.clone()) }))
            .ok_or_else(|| format!("no dead letter on thread {thread}"))
    }

    /// Do what `/dead` asks: replay the dead letter at once, or ask for `y` to drop it.
    pub(super) fn dead(&mut self, dead: Dead) -> Result<Option<Action>, String> {
        match dead {
            Dead::Replay(seq) => Ok(Some(Action::Run(Job::DeadReplay(self.letter(seq)?.seq), String::new()))),
            Dead::Drop(seq) => {
                self.prompt = Some(Prompt::Drop(self.letter(seq)?));
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::exec::{Done, Outcome};
    use crate::tui::home::{self, Why};
    use crate::tui::test_support::model;
    use crate::tui::thread;

    /// What a thread alice asked dev-a on holds once dev-a gave up: the dead letter at 2, the daemon's notice at 3.
    fn gave_up(thread: &ThreadId) -> Vec<Entry> {
        let ask = message(1, thread, "alice", "dev-a", "build it");
        let mut dead = message(2, thread, "alice", "dev-a", "build it");
        (dead.kind, dead.id, dead.note) = (Kind::Dead, None, Some("after 5 deliveries: harness crashed".into()));
        let mut notice = message(3, thread, "dev-a", "alice", "Gave up on the message from alice after 5 deliveries");
        (notice.notice, notice.in_reply_to) = (Some(NoticeKind::GaveUp), ask.id);
        vec![ask, dead, notice]
    }

    fn run(m: &mut Model, text: &str) -> Option<Action> {
        m.follow_drafts();
        m.bar.focused = true;
        m.bar.set_text(text);
        m.key(Key::Enter, at(9))
    }

    #[test]
    fn dead_replays_the_stuck_thread_s_dead_letter_or_drops_it_once_y_confirms() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(gave_up(&thread), at(3));
        let stuck = |m: &Model| home::waiting(&m.data, at(9)).iter().any(|i| i.why == Why::Stuck);
        assert!(stuck(&m));
        let replay = Some(Action::Run(Job::DeadReplay(2), "/dead replay".into()));
        assert_eq!(run(&mut m, "/dead replay"), replay, "home's stuck item");
        assert_eq!(run(&mut m, "/dead drop"), None);
        let asks = "dead letter 2 (\"build it\"): y drops it unreplayed; any other key keeps it";
        assert_eq!(m.prompt_line().map(|l| l.to_string()), Some(format!(" {asks}")));
        assert_eq!(m.key(Key::Char('n'), at(9)), None);
        assert!(m.prompt.is_none(), "any other key keeps it");
        run(&mut m, "/dead drop");
        assert_eq!(m.key(Key::Char('y'), at(9)), Some(Action::Run(Job::DeadDrop(2), String::new())));
        assert_eq!(m.bar.status.as_ref().unwrap().text, "dropping dead letter 2…");

        let done = Done { line: "dropped dead letter 2".into(), dead: Some(2), ..Done::default() };
        m.outcome(Outcome { result: Ok(done), text: String::new(), quoting: None });
        assert_eq!(m.bar.status.as_ref().unwrap().text, "dropped dead letter 2");
        assert!(!stuck(&m), "neither the dead letter nor the notice of it holds the thread");
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        assert_eq!(run(&mut m, "/dead replay"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, format!("no dead letter on thread {thread}"));
    }

    #[test]
    fn the_notice_of_a_dead_letter_gone_before_the_read_holds_the_thread_no_more() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut entries = gave_up(&thread);
        entries.remove(1);
        m.stored(entries, at(3));
        assert!(home::waiting(&m.data, at(9)).iter().all(|i| i.why != Why::Stuck));
    }

    #[test]
    fn dead_acts_on_the_gave_up_line_or_notice_selected_else_the_thread_s_latest() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut entries = gave_up(&thread);
        let mut again = message(5, &thread, "alice", "dev-a", "and test it");
        (again.kind, again.id) = (Kind::Dead, None);
        entries.extend([message(4, &thread, "alice", "dev-a", "and test it"), again]);
        m.stored(entries.clone(), at(5));
        let mut page = thread::Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        m.pages.push(Page::Thread(page));
        let replay = |seq| Some(Action::Run(Job::DeadReplay(seq), "/dead replay".into()));
        assert_eq!(run(&mut m, "/dead replay"), replay(5), "the latest");
        let select = |m: &mut Model, seq| {
            let Some(Page::Thread(page)) = m.pages.last_mut() else { panic!() };
            page.mouse_select(seq);
        };
        select(&mut m, 3);
        assert_eq!(run(&mut m, "/dead replay"), replay(2), "the notice tells of the dead letter before it");
        select(&mut m, 2);
        assert_eq!(run(&mut m, "/dead replay"), replay(2), "the gave up line");
        assert_eq!(run(&mut m, "/dead replay 9"), Some(Action::Run(Job::DeadReplay(9), "/dead replay 9".into())));

        m.data.dead_cleared.extend([2, 5]);
        assert_eq!(run(&mut m, "/dead replay"), None);
        assert_eq!(m.bar.status.as_ref().unwrap().text, "dead letter 2 is replayed or dropped already");
        select(&mut m, 4);
        assert_eq!(m.unfit("dead", at(9)), Some(format!("no dead letter on thread {thread}")));
    }
}
