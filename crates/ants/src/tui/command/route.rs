//! Where Enter sends the bar's text, which the bar's title names: the one routing both read, so that a send goes where
//! the title said when Enter was pressed.

use ants_proto::{ChannelId, ThreadId, UserId};

use super::Note;
use crate::text::excerpt;
use crate::tui::exec::{Job, dm_with, reply_to};
use crate::tui::model::{Data, Model, Page};
use crate::tui::thread;

/// How much of a thread's title the bar's title shows.
const TITLE_CHARS: usize = 40;

/// Where Enter sends the bar's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Nowhere: a slash command, by its name, runs.
    Command(String),
    /// Nowhere yet: the @user or #channel typed first names no one yet.
    Unnamed,
    /// To the user, on a new thread, else on the thread of the message quoted.
    Ask { to: UserId, thread: Option<ThreadId> },
    /// To the channel, on a new thread, else on the channel's thread.
    Post { channel: ChannelId, thread: Option<ThreadId> },
    /// On the open thread, to whom its messages say.
    Reply { thread: ThreadId, to: Replied },
    /// On a new thread to the human's assistant, else the messenger.
    Unaddressed,
}

/// Whom a reply on a thread goes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replied {
    /// The thread's messages are still being read: the send finds out.
    Reading,
    /// The ant that last messaged the human there, else the other human of the thread.
    To(UserId),
    /// The human's assistant, else the messenger, as no one else is there to reply to.
    Unaddressed,
}

impl Route {
    /// What the bar's title says of the route.
    pub fn describe(&self, data: &Data) -> String {
        let title = |thread: &ThreadId| format!("\"{}\"", excerpt(&data.title(thread), TITLE_CHARS));
        let sealed = |to: &UserId| if data.is_human(to) && to != data.me() { ", sealed" } else { "" };
        match self {
            Self::Command(name) => format!("run /{name}"),
            Self::Unnamed => String::new(),
            Self::Ask { to, thread: None } => format!("to {to} on a new thread{}", sealed(to)),
            Self::Ask { to, thread: Some(thread) } => format!("to {to} on {}{}", title(thread), sealed(to)),
            Self::Post { channel, thread: None } => format!("new thread in #{channel}"),
            Self::Post { channel, thread: Some(thread) } => format!("post in #{channel} on {}", title(thread)),
            Self::Reply { thread, to: Replied::Reading } => format!("reply on {}", title(thread)),
            Self::Reply { thread, to: Replied::To(to) } => format!("reply to {to} on {}{}", title(thread), sealed(to)),
            Self::Reply { thread, to: Replied::Unaddressed } => {
                format!("reply to the assistant or the messenger on {}", title(thread))
            }
            Self::Unaddressed => "to the assistant or the messenger on a new thread".to_owned(),
        }
    }
}

impl Model {
    /// Where Enter sends `text` from the view shown: an @user typed first starts a thread with the user (on a
    /// channel's thread or feed, posts there), a #channel posts a new thread there, a slash command runs, and else
    /// the text goes where the view says ([`Self::said`]).
    pub fn route(&self, text: &str) -> Route {
        let text = text.trim_start();
        let word = |after: &str| after.split(char::is_whitespace).next().unwrap_or_default().to_owned();
        if text.starts_with("//") {
            return self.said();
        }
        if let Some(command) = text.strip_prefix('/') {
            let name = word(command).to_ascii_lowercase();
            return if name == "later" { self.said() } else { Route::Command(name) };
        }
        if let Some(name) = text.strip_prefix('@')
            && self.channel_on_view().is_none()
        {
            return match word(name).parse() {
                Ok(to) => Route::Ask { to, thread: self.quoting().map(|(thread, _)| thread) },
                Err(_) => Route::Unnamed,
            };
        }
        if let Some(name) = text.strip_prefix('#') {
            return match word(name).parse() {
                Ok(channel) => Route::Post { channel, thread: None },
                Err(_) => Route::Unnamed,
            };
        }
        self.said()
    }

    /// Where text naming no one goes: posted on the open channel thread, or as a new thread on the channel on view;
    /// else replied on the open thread, else to the human of the DM view, else to the human's assistant, else the
    /// messenger.
    pub(super) fn said(&self) -> Route {
        if let Some((channel, thread)) = self.channel_on_view() {
            return Route::Post { channel, thread };
        }
        match self.page() {
            Page::Thread(page) => Route::Reply { thread: page.history.thread.clone(), to: self.replied(page) },
            Page::Dm(page) => Route::Ask { to: page.with.clone(), thread: None },
            _ => Route::Unaddressed,
        }
    }

    /// Whom a reply on the thread of `page` goes to, as its messages read say.
    fn replied(&self, page: &thread::Page) -> Replied {
        if page.history.loading {
            return Replied::Reading;
        }
        let (entries, me) = (&page.history.entries, self.data.me());
        let human = |user: &UserId| self.data.is_human(user);
        reply_to(entries, me, human).or_else(|| dm_with(entries, me, human)).map_or(Replied::Unaddressed, Replied::To)
    }

    /// The job sending `text` the way `route` goes, quoting the message `>` picked when it goes on its thread.
    pub(super) fn job(&self, route: Route, text: String) -> Result<Job, String> {
        let quote = self.quoting().map(|(_, quote)| quote);
        Ok(match route {
            Route::Ask { to, thread } => Job::Ask { quote: thread.as_ref().and(quote), to, thread, text },
            Route::Post { channel, thread } => Job::Post { quote: thread.as_ref().and(quote), channel, thread, text },
            Route::Reply { thread, to } => {
                let to = match to {
                    Replied::To(to) => Some(to),
                    Replied::Reading | Replied::Unaddressed => None,
                };
                Job::Reply { thread, to, text, quote }
            }
            Route::Unaddressed => Job::Unaddressed { text },
            Route::Command(_) | Route::Unnamed => return Err("nothing to send".to_owned()),
        })
    }

    /// What the bar's title says Enter does with the text as it stands.
    pub fn bar_title(&self) -> String {
        if let Some(searching) = self.ui.history.searching() {
            return searching;
        }
        match &self.ui.bar.noting {
            Some(Note::Reject(request)) => {
                format!("reject {} with a note to {}", crate::tools::qualified(request), request.ant)
            }
            Some(Note::Reply(request)) => format!("to {} on the request's thread", request.ant),
            None => self.route(&self.ui.bar.text()).describe(&self.data),
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::exec::Job;
    use crate::tui::model::{Action, Key};
    use crate::tui::test_support::model;
    use crate::tui::{channel, dm};

    #[test]
    fn the_title_names_where_enter_sends_and_the_send_goes_there() {
        let thread = ThreadId::generate();
        let mut m = model();
        let title = |m: &mut Model, text: &str| {
            m.follow_drafts();
            m.ui.bar.set_text(text);
            m.bar_title()
        };
        assert_eq!(title(&mut m, "who owns billing?"), "to the assistant or the messenger on a new thread");
        assert_eq!(title(&mut m, "@bob lunch?"), "to bob on a new thread, sealed");
        assert_eq!(title(&mut m, "@dev-a"), "to dev-a on a new thread", "as typed");
        assert_eq!(title(&mut m, "@"), "");
        assert_eq!(title(&mut m, "#ops status?"), "new thread in #ops");
        assert_eq!(title(&mut m, "/done"), "run /done");

        let entries =
            vec![message(1, &thread, "alice", "dev-b", "deploy"), message(2, &thread, "dev-b", "alice", "ok")];
        m.stored(entries.clone(), at(1));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        assert_eq!(title(&mut m, "go on"), "reply on \"deploy\"", "whom to while the thread is read");
        let Some(Page::Thread(page)) = m.pages.last_mut() else { panic!() };
        page.history.loaded(&thread, Ok(entries)).unwrap();
        assert_eq!(title(&mut m, "go on"), "reply to dev-b on \"deploy\"");
        m.ui.bar.focused = true;
        let reply = Job::Reply { thread: thread.clone(), to: Some(user("dev-b")), text: "go on".into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(reply, "go on".into())));
        assert_eq!(title(&mut m, "#ops look"), "new thread in #ops", "a #channel typed first changes it");

        m.pages.push(Page::Channel(channel::Page::new("ops".parse().unwrap())));
        assert_eq!(title(&mut m, "@dev-b look"), "new thread in #ops", "an @mention posts on a channel");
        m.pages.push(Page::Dm(dm::Page::new(user("bob"))));
        assert_eq!(title(&mut m, "lunch?"), "to bob on a new thread, sealed");
        let ask = Job::Ask { to: user("bob"), thread: None, text: "lunch?".into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(ask, "lunch?".into())));
    }

    #[test]
    fn the_bar_shows_its_title() {
        let mut m = model();
        m.ui.bar.set_text("@dev-a deploy");
        let shown = crate::test_support::render::rendered(120, 32, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(shown.contains("┌ > to dev-a on a new thread ─"), "{shown}");
    }
}
