//! Steering and cancelling from the thread and work views: `s` opens the command bar on `/steer` for the ant at work
//! on the thread, and `x` or Ctrl-X cancels the prompt running there as `ants cancel` does, each asking for `y` first
//! while an ant on the thread runs a tool call. The bar's status line then tells whether a prompt was interrupted or
//! none was running.

use ants_proto::{ThreadId, UserId};
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::command::Tone;
use super::exec::Job;
use super::model::{Action, Key, Model};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    Steer,
    Cancel,
}

/// A steer or cancel waiting for `y` while an ant runs a tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirm {
    intent: Intent,
    thread: ThreadId,
    ant: UserId,
    tool: String,
}

impl Confirm {
    /// The footer asking for `y`.
    pub fn line(&self) -> Line<'static> {
        let what = match self.intent {
            Intent::Steer => "y opens /steer anyway, any other key does not",
            Intent::Cancel => "y cancels the prompt, any other key keeps it running",
        };
        let text = format!(" {} is running a tool call, {}: {what}", self.ant, self.tool);
        Line::styled(text, Style::new().fg(Color::Yellow))
    }
}

impl Model {
    /// Steer or cancel the prompt running on `thread`: at once, unless an ant there runs a tool call, then once `y`
    /// confirms.
    pub fn interrupt(&mut self, intent: Intent, thread: ThreadId) -> Option<Action> {
        let progress = self.data.live.progress.get(&thread);
        let tool = progress.and_then(|p| p.status.tools().next().map(|(ant, tool)| (ant.clone(), tool.to_owned())));
        if let Some((ant, tool)) = tool {
            self.interrupting = Some(Confirm { intent, thread, ant, tool });
            return None;
        }
        self.interrupted(intent, thread)
    }

    /// Take `key` while a steer or cancel waits for `y`.
    pub fn confirm_key(&mut self, key: Key) -> Option<Action> {
        let confirm = self.interrupting.take()?;
        if key != Key::Char('y') {
            return None;
        }
        self.interrupted(confirm.intent, confirm.thread)
    }

    fn interrupted(&mut self, intent: Intent, thread: ThreadId) -> Option<Action> {
        match intent {
            Intent::Steer => {
                let draft = self.bar.text();
                if !draft.starts_with("/steer ") {
                    let text = if draft.starts_with('/') { String::new() } else { draft };
                    self.bar.set_text(&format!("/steer {text}"));
                }
                self.bar.focused = true;
                None
            }
            Intent::Cancel => {
                let ants = self.running(&thread);
                if ants.is_empty() {
                    self.bar.set_status(format!("no ant runs a prompt on thread {thread}"), Tone::Error);
                    return None;
                }
                self.bar.set_status("cancelling…", Tone::Busy);
                Some(Action::Run(Job::Cancel { thread, ants }, String::new()))
            }
        }
    }

    /// The ants to ask to cancel their prompt on `thread`: those at work there, else every ant on it, as the
    /// progress heard may have missed a prompt that started before the UI.
    fn running(&self, thread: &ThreadId) -> Vec<UserId> {
        let busy: Vec<UserId> =
            self.data.live.progress.get(thread).map_or_default(|p| p.status.busy().cloned().collect());
        if !busy.is_empty() {
            return busy;
        }
        let users = self.data.live.summary(thread).map(|s| s.users.iter());
        users.into_iter().flatten().filter(|u| !self.data.is_human(u)).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{StatusEvent, StatusUpdate};

    use super::*;
    use crate::live;
    use crate::live::tests::{at, message, user};
    use crate::tui::exec::Outcome;
    use crate::tui::model::tests::model;
    use crate::tui::model::{Focus, Page};
    use crate::tui::nav;
    use crate::tui::timeline::tests::tool;

    #[test]
    fn x_cancels_at_once_unless_an_ant_runs_a_tool_call_then_y_confirms() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.select(nav::Item::Work(None));
        m.focus = Focus::View;
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        let cancel = |ants: &[&str]| {
            let job = Job::Cancel { thread: thread.clone(), ants: ants.iter().map(|a| user(a)).collect() };
            Some(Action::Run(job, String::new()))
        };
        assert_eq!(m.key(Key::Char('x'), at(2)), cancel(&["dev-a"]), "no progress heard: ask the thread's ants");
        assert_eq!(m.bar.status.as_ref().map(|s| s.text.as_str()), Some("cancelling…"));

        let thinking = StatusEvent::new(user("dev-b"), thread.clone(), StatusUpdate::Thought { text: "hm".into() });
        m.take(live::Event::Status(thinking), at(3));
        assert_eq!(m.key(Key::Ctrl('x'), at(3)), cancel(&["dev-b"]), "the ants at work");

        m.take(live::Event::Status(tool(&thread, "dev-b", "c1", Some("git push"), "in_progress", 4)), at(4));
        assert_eq!(m.key(Key::Char('x'), at(4)), None);
        let line = m.interrupting.as_ref().map(|c| c.line().to_string());
        let asks = "dev-b is running a tool call, git push: y cancels the prompt, any other key keeps it running";
        assert_eq!(line.as_deref().map(str::trim), Some(asks));
        assert_eq!(m.key(Key::Char('n'), at(5)), None);
        assert_eq!(m.interrupting, None, "any other key drops it");
        m.key(Key::Char('x'), at(5));
        assert_eq!(m.key(Key::Char('y'), at(5)), cancel(&["dev-b"]));

        let outcome = Outcome {
            result: Err(anyhow::anyhow!("dev-b has no prompt running on thread {thread}")),
            text: String::new(),
        };
        m.outcome(outcome);
        assert_eq!(
            m.bar.status.as_ref().map(|s| s.text.clone()),
            Some(format!("dev-b has no prompt running on thread {thread}"))
        );
        assert!(!m.bar.focused && m.bar.is_empty(), "the bar stays as it was");
    }

    #[test]
    fn s_opens_the_bar_on_steer_once_confirmed_while_a_tool_call_runs() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.select(nav::Item::Work(None));
        m.focus = Focus::View;
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.key(Key::Enter, at(1));
        assert!(matches!(m.page(), Page::Thread(_)));
        m.bar.set_text("left, not right");
        assert_eq!(m.key(Key::Char('s'), at(2)), None);
        assert_eq!((m.bar.focused, m.bar.text()), (true, "/steer left, not right".to_owned()), "a draft is the steer");

        m.key(Key::Esc, at(2));
        m.bar.set_text("/done");
        m.take(live::Event::Status(tool(&thread, "dev-a", "c1", Some("cargo test"), "in_progress", 3)), at(3));
        m.key(Key::Char('s'), at(3));
        assert!(!m.bar.focused && m.interrupting.is_some());
        m.key(Key::Char('y'), at(3));
        assert_eq!((m.bar.focused, m.bar.text()), (true, "/steer ".to_owned()));
    }
}
