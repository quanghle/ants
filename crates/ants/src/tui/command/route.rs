//! Where Enter sends the bar's text, which the bar's title names: the routing every client reads
//! ([`ants_client::sending::bar`]), from the view shown, so that a send goes where the title said when Enter was
//! pressed; and the bar's own titles, of a note on a tool request and of a search of its history.

use ants_client::sending::Addressed;
use ants_client::sending::bar::{self, Names, Outgoing, Replied, Route, Target, View};
use ants_proto::{TeamDirectory, UserId};

use super::Note;
use crate::tui::exec::Job;
use crate::tui::model::{Data, Model, Page};
use crate::tui::thread;

impl Names for Data {
    fn me(&self) -> &UserId {
        Data::me(self)
    }

    fn is_human(&self, user: &UserId) -> bool {
        Data::is_human(self, user)
    }

    fn teams(&self) -> Option<&TeamDirectory> {
        self.teams.as_ref()
    }
}

impl Model {
    /// The view the bar sends from, as every client routes from it ([`bar::View`]).
    pub(super) fn bar_view(&self) -> View {
        match self.page() {
            Page::Thread(page) => {
                let thread = page.history.thread.clone();
                let channel = self.channel_on_view().map(|(channel, _)| channel);
                let quoting = self.quoting().is_some();
                View::Thread { title: self.data.title(&thread), thread, channel, to: self.replied(page), quoting }
            }
            Page::Channel(page) => View::Channel { channel: page.channel.clone() },
            Page::Dm(page) => View::Dm { with: page.with.clone() },
            _ => View::Elsewhere,
        }
    }

    /// Where text naming no one goes from the view shown ([`bar::said`]).
    pub(super) fn said(&self) -> Route {
        bar::said(&self.bar_view())
    }

    /// Whom a reply on the thread of `page` goes to, as its messages read say.
    fn replied(&self, page: &thread::Page) -> Replied {
        if page.history.loading {
            return Replied::Reading;
        }
        bar::replied(&page.history.entries, self.data.me(), |user| self.data.is_human(user))
    }

    /// The job sending the message of the bar's `text` where its route goes ([`bar::outgoing`]), quoting the message
    /// `>` picked when it goes on its thread.
    pub(super) fn job(&self, text: &str) -> Result<Job, String> {
        let Outgoing { target, text, queue, .. } = bar::outgoing(text, &self.bar_view(), &self.data)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "nothing to send".to_owned())?;
        let quote = self.quoting().map(|(_, quote)| quote);
        let job = match target {
            Target::Ask { to, team, thread } => Job::Ask { quote: thread.as_ref().and(quote), to, thread, text, team },
            Target::Post { channel, thread } => Job::Post { quote: thread.as_ref().and(quote), channel, thread, text },
            Target::Reply { thread, to } => Job::Reply { thread, to, text, quote },
            Target::Unaddressed => Job::Unaddressed { text },
        };
        Ok(if queue { Job::Later(Box::new(job)) } else { job })
    }

    /// Where a message to `to` goes: to its lead when `to` names a team of the directory read and no human, carrying
    /// the team; a team with no lead, nor any above it, refused ([`ants_client::sending::addressed`]).
    pub(super) fn addressed(&self, to: UserId) -> Result<Addressed, String> {
        match &self.data.teams {
            Some(teams) => {
                ants_client::sending::addressed(teams, |u| self.data.is_human(u), to).map_err(|e| e.to_string())
            }
            None => Ok(Addressed { to, team: None }),
        }
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
            None => {
                let view = self.bar_view();
                bar::route(&self.ui.bar.text(), &view).describe(&view, &self.data)
            }
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
        let ask = Job::Ask { to: user("bob"), thread: None, text: "lunch?".into(), quote: None, team: None };
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
