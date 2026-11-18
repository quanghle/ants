//! The drafts of the conversations: the bar's text belongs to the conversation it was typed in (a thread, a channel, a
//! person, or none), the drafts of the others kept apart until they show again.

use std::collections::HashMap;

use ants_proto::{ChannelId, ThreadId, UserId};
use ratatui::style::Style;
use ratatui::text::Span;
use serde::{Deserialize, Serialize};

use super::Draft;
use crate::tui::model::{Model, Page};
use crate::tui::style;

/// What the nav and the lists show by a conversation holding a draft.
const MARK: &str = " ✎";

/// What the bar's text belongs to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Conversation {
    /// No conversation: home, work and the other lists.
    #[default]
    None,
    Thread(ThreadId),
    /// A channel's feed.
    Channel(ChannelId),
    /// The DMs with another human.
    Person(UserId),
}

#[derive(Debug, Default)]
pub struct Drafts {
    /// The conversation the bar's text belongs to.
    here: Conversation,
    /// Whether the bar holds text.
    held: bool,
    /// The drafts of the other conversations.
    kept: HashMap<Conversation, Draft>,
    /// The texts being sent, each with the conversation it was typed in.
    sending: Vec<(String, Conversation)>,
}

impl Drafts {
    /// The conversation the bar's text belongs to.
    pub fn here(&self) -> &Conversation {
        &self.here
    }

    /// Whether `conversation` holds a draft.
    pub fn holds(&self, conversation: &Conversation) -> bool {
        if *conversation == self.here {
            return self.held;
        }
        self.kept.get(conversation).is_some_and(|draft| !draft.is_empty())
    }

    /// The mark of a draft by `conversation`, if it holds one.
    pub fn mark(&self, conversation: &Conversation) -> Option<Span<'static>> {
        self.holds(conversation).then(|| Span::styled(MARK, Style::new().fg(style::INFO)))
    }

    /// Remember that `text` being sent was typed in `conversation`.
    pub fn sending(&mut self, text: &str, conversation: Conversation) {
        self.sending.push((text.to_owned(), conversation));
    }

    /// Take that `text` was sent or failed: the conversation it was typed in, if it was being sent.
    pub fn sent(&mut self, text: &str) -> Option<Conversation> {
        let at = self.sending.iter().position(|(sending, _)| sending == text)?;
        Some(self.sending.remove(at).1)
    }

    /// The drafts of the conversations not shown.
    pub fn kept(&self) -> impl Iterator<Item = (&Conversation, &Draft)> {
        self.kept.iter()
    }

    /// Keep `draft` for `conversation` unless it holds one already: whether it was kept.
    pub fn keep(&mut self, conversation: Conversation, draft: Draft) -> bool {
        if self.holds(&conversation) {
            return false;
        }
        self.kept.insert(conversation, draft);
        true
    }
}

impl Model {
    /// The conversation shown, which the bar's text belongs to.
    pub fn conversation(&self) -> Conversation {
        match self.page() {
            Page::Thread(page) => Conversation::Thread(page.history.thread.clone()),
            Page::Channel(page) => Conversation::Channel(page.channel.clone()),
            Page::Dm(page) => Conversation::Person(page.with.clone()),
            _ => Conversation::None,
        }
    }

    /// Have the bar hold the draft of the conversation shown, keeping that of the one it held.
    pub fn follow_drafts(&mut self) {
        let here = self.conversation();
        let drafts = &mut self.data.drafts;
        if drafts.here != here {
            let draft = drafts.kept.remove(&here).unwrap_or_default();
            let left = self.bar.swap(draft);
            let there = std::mem::replace(&mut drafts.here, here);
            if !left.is_empty() {
                drafts.kept.insert(there, left);
            }
        }
        drafts.held = !self.bar.is_empty();
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::exec::{Job, Outcome};
    use crate::tui::model::{Action, Key};
    use crate::tui::test_support::model;
    use crate::tui::work::Filter;
    use crate::tui::{thread, work};

    fn typed(m: &mut Model, text: &str) {
        text.chars().for_each(|c| _ = m.key(Key::Char(c), at(1)));
    }

    fn show(m: &mut Model, thread: &ThreadId) {
        m.pages.truncate(1);
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        m.follow_drafts();
    }

    #[test]
    fn a_draft_stays_with_the_conversation_it_was_typed_in() {
        let (a, b) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.stored(vec![message(1, &a, "alice", "dev-a", "deploy"), message(2, &b, "alice", "dev-b", "docs")], at(1));
        show(&mut m, &a);
        m.bar.focused = true;
        typed(&mut m, "ship it");
        assert!(m.data.drafts.holds(&Conversation::Thread(a.clone())));
        show(&mut m, &b);
        assert!(m.bar.is_empty(), "thread B shows its own draft");
        assert_eq!(m.bar_title(), "reply on \"docs\"");
        m.bar.focused = true;
        assert_eq!(m.key(Key::Enter, at(1)), None, "nothing to send on B");
        assert!(m.data.drafts.holds(&Conversation::Thread(a.clone())) && !m.data.drafts.holds(&Conversation::None));
        m.pages.push(Page::Work(work::Page::new(Filter::default())));
        m.follow_drafts();
        let shown = crate::test_support::render::rendered(120, 32, |f| crate::tui::frame::draw(f, &m, at(1)));
        assert!(shown.contains("deploy ✎"), "the lists mark it: {shown}");
        show(&mut m, &a);
        assert_eq!(m.bar.text(), "ship it", "back on A");
        let Some(Action::Run(Job::Reply { thread, .. }, _)) = m.key(Key::Enter, at(1)) else { panic!() };
        assert_eq!(thread, a);
    }

    #[test]
    fn a_failed_send_comes_back_as_the_draft_of_its_conversation() {
        let (a, b) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        show(&mut m, &a);
        m.bar.focused = true;
        typed(&mut m, "ship it");
        let Some(Action::Run(_, text)) = m.key(Key::Enter, at(1)) else { panic!() };
        show(&mut m, &b);
        typed(&mut m, "and the docs");
        m.outcome(Outcome { result: Err(anyhow::anyhow!("no route")), text, quoting: None });
        assert_eq!(m.bar.text(), "and the docs", "B keeps its draft");
        show(&mut m, &a);
        assert_eq!(m.bar.text(), "ship it", "A has the text back");
    }
}
