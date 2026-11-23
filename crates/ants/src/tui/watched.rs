//! What the views are read from besides the live feed, the inbox and channel reads and the bar's jobs: each source
//! told what the views show ([`Watched::watch`]), heard from in turn ([`Watched::next`]), and what it brought taken
//! in by the model ([`Model::view_event`]).

use ants_proto::{AttachmentHandle, ThreadId, UserId};
use chrono::{DateTime, Utc};
use ratatui_image::protocol::Protocol;

use super::acts::{self, Acted, Acts};
use super::model::{Action, Level, Model, Page};
use super::search::{self, Searches};
use super::{
    ant, cards, catchup, credentials, faces, held, images, mcp, memories, people, settings, steps, thread, tree,
};
use crate::avatars::Identities;
use crate::text::excerpt;
use crate::threads::thread::Entry;

pub struct Watched {
    /// The done states, snoozes and reminders changed, and the open thread's state.
    pub acts: Acts,
    pub people: people::Io,
    pub searches: Searches,
    pub trees: tree::Io,
    /// The reads of the steps of the runs reporting on the thread shown.
    pub steps: steps::Io,
    /// The reads of the shown ant's queue.
    pub ants: ant::Io,
    /// The reads of the shown ant's memories.
    pub memories: memories::Io,
    /// The reads and sign-ins of the shown ant's MCP servers.
    pub mcp: mcp::Io,
    /// The reads of the shown ant's settings and orders, and the orders sent.
    pub settings: settings::Io,
    /// The credentials asked for, where the team's account key is.
    pub credentials: credentials::Io,
    /// The admin orders of the human's assistant held for its consent.
    pub held: held::Io,
    pub cards: cards::Io,
    pub summaries: catchup::Io,
    /// The images drawn, with `[cli]` `inline_images` on.
    pub images: Option<images::Io>,
    /// The reads of the ants' identity records, for their avatars.
    pub looks: faces::Io,
}

/// What a source of the views brought.
pub enum ViewEvent {
    Acted(Acted),
    People(people::Event),
    Found(search::Found),
    Cards(cards::Read),
    Summarised(catchup::Summarised),
    Tree(tree::Event),
    Steps(steps::Event),
    Ant(ant::Event),
    Memories(memories::Event),
    Mcp(mcp::Event),
    Settings(settings::Event),
    Credentials(credentials::Event),
    Held(held::Event),
    Looks(Identities),
    Image(AttachmentHandle, Option<Protocol>),
}

impl Watched {
    /// Follow what the views of `model` show: the open thread's state and the steps of the runs reporting there, the
    /// delegation tree's links and spend, the ant's queue, memories, MCP servers and settings, the images.
    pub fn watch(&mut self, model: &mut Model) {
        self.acts.watch(model.open().cloned());
        self.trees.watch(model.tree_wanted());
        self.steps.watch(model.steps_wanted());
        self.ants.watch(model.ant_wanted());
        self.memories.watch(model.memories_wanted());
        self.mcp.watch(model.mcp_wanted());
        let (shown, unread) = model.settings_wanted().unzip();
        self.settings.watch(shown, unread.unwrap_or(false));
        if let Some(images) = &mut self.images {
            let wanted = images::wanted(model);
            images.want(&wanted);
            model.data.images.retain(&wanted);
        }
    }

    /// Wait for word from any of the sources. Cancel-safe, as each source's is.
    pub async fn next(&mut self) -> ViewEvent {
        tokio::select! {
            acted = self.acts.next() => {
                if matches!(acted, acts::Acted::Changed(..)) {
                    // Home shows done states and snoozes: read them back at once.
                    self.cards.refresh();
                }
                ViewEvent::Acted(acted)
            }
            event = self.people.next() => ViewEvent::People(event),
            found = self.searches.next() => ViewEvent::Found(found),
            read = self.cards.next() => ViewEvent::Cards(read),
            summarised = self.summaries.next() => ViewEvent::Summarised(summarised),
            event = self.trees.next() => ViewEvent::Tree(event),
            read = self.steps.next() => ViewEvent::Steps(read),
            event = self.ants.next() => ViewEvent::Ant(event),
            event = self.memories.next() => ViewEvent::Memories(event),
            event = self.mcp.next() => ViewEvent::Mcp(event),
            event = self.settings.next() => ViewEvent::Settings(event),
            event = self.credentials.next() => ViewEvent::Credentials(event),
            event = self.held.next() => ViewEvent::Held(event),
            identities = self.looks.next() => ViewEvent::Looks(identities),
            (handle, image) = next_image(self.images.as_mut()) => ViewEvent::Image(handle, image),
        }
    }

    /// Read again what may have missed changes.
    pub fn refresh(&mut self) {
        self.acts.refresh();
        self.people.refresh();
        self.trees.refresh();
        self.steps.refresh();
        self.ants.refresh();
        self.credentials.refresh();
        self.held.refresh();
        self.cards.refresh();
        self.looks.refresh();
    }

    /// Have the images drawn again, after the terminal was handed over.
    pub fn redraw_images(&mut self, model: &mut Model) {
        if let Some(images) = &mut self.images {
            images.reset();
            model.data.images.clear();
        }
    }
}

impl Model {
    /// Take in what a source of the views brought: the actions it calls for.
    pub fn view_event(&mut self, event: ViewEvent, now: DateTime<Utc>) -> Vec<Action> {
        match event {
            ViewEvent::Acted(acted) => self.acts_event(acted, now),
            ViewEvent::People(event) => self.people_event(event, now),
            ViewEvent::Found(found) => self.search_event(found),
            ViewEvent::Cards(read) => {
                self.cards_event(read, now);
                // TL;DRs ahead of time follow what was read.
                return self.ahead(now).into_iter().collect();
            }
            ViewEvent::Summarised(summarised) => {
                self.summary_event(summarised, now);
                return self.ahead(now).into_iter().collect();
            }
            ViewEvent::Tree(event) => self.tree_event(event, now),
            ViewEvent::Steps(read) => self.steps_event(read, now),
            ViewEvent::Ant(event) => self.ant_event(event),
            ViewEvent::Memories(event) => self.memories_event(event),
            ViewEvent::Mcp(event) => self.mcp_event(event),
            ViewEvent::Settings(event) => self.settings_event(event),
            ViewEvent::Credentials(event) => self.credentials_event(event, now),
            ViewEvent::Held(event) => self.held_event(event, now),
            ViewEvent::Looks(identities) => self.data.looks.set(identities),
            ViewEvent::Image(handle, image) => self.data.images.insert(handle, image),
        }
        Vec::new()
    }

    /// The root of the delegation tree shown, and the threads known of it, for its links and spend to be read.
    pub fn tree_wanted(&self) -> Option<(ThreadId, Vec<ThreadId>)> {
        let Page::Tree(page) = self.page() else { return None };
        Some((page.root.clone(), self.data.delegations.threads(&page.root)))
    }

    /// The thread shown and the ants known on it, for the steps of the runs they conduct reporting there to be read.
    pub fn steps_wanted(&self) -> Option<(ThreadId, Vec<UserId>)> {
        let Page::Thread(page) = self.page() else { return None };
        let thread = &page.history.thread;
        let data = &self.data;
        let summary = data.live.summary(thread).into_iter().flat_map(|s| &s.users);
        let senders = page.history.entries.iter().filter_map(|e| e.from.as_ref());
        let conductors = data.steps.of(thread).iter().map(|s| &s.conductor);
        let mut ants: Vec<UserId> =
            summary.chain(senders).chain(conductors).filter(|u| !data.is_human(u)).cloned().collect();
        ants.sort();
        ants.dedup();
        Some((thread.clone(), ants))
    }

    /// The ant shown, for its queue to be read.
    pub fn ant_wanted(&self) -> Option<UserId> {
        let Page::Ant(page) = self.page() else { return None };
        Some(page.ant.clone())
    }

    /// The ant whose memories the view wants read, and at which asking: the shown ant's, on its memories pane.
    pub fn memories_wanted(&self) -> Option<(UserId, u32)> {
        let Page::Ant(page) = self.page() else { return None };
        (page.pane == ant::Pane::Memories).then(|| (page.ant.clone(), page.memories.asked))
    }

    /// What the view wants of an ant's MCP servers: the shown ant's, on its MCP pane.
    pub fn mcp_wanted(&self) -> Option<mcp::Wanted> {
        let Page::Ant(page) = self.page() else { return None };
        (page.pane == ant::Pane::Mcp).then(|| mcp::Wanted {
            ant: page.ant.clone(),
            asked: page.mcp.asked,
            sign: page.mcp.sign_wanted(),
        })
    }

    /// The ant whose settings pane shows, and whether its settings are to be read.
    pub fn settings_wanted(&self) -> Option<(UserId, bool)> {
        let Page::Ant(page) = self.page() else { return None };
        (page.pane == ant::Pane::Settings).then(|| (page.ant.clone(), page.settings.reading.is_none()))
    }

    /// Take what was done to or read of the open thread besides its messages.
    pub fn acts_event(&mut self, acted: Acted, now: DateTime<Utc>) {
        match acted {
            Acted::About(thread, Ok(about)) => {
                if self.data.live.marks != about.marks {
                    self.data.live.marks.clone_from(&about.marks);
                    self.data.live.reorder(now);
                }
                match &about.done {
                    Some(done) => self.data.cards.done.insert(thread.clone(), done.clone()),
                    None => self.data.cards.done.remove(&thread),
                };
                for page in self.pages_mut::<thread::Page>().filter(|page| page.history.thread == thread) {
                    page.about = Some(about.clone());
                }
            }
            Acted::About(thread, Err(e)) => tracing::debug!(%thread, "reading the thread's marks: {e:#}"),
            Acted::Changed(thread, change, Ok(changed)) => {
                let line = format!("thread {} {}", excerpt(&self.data.title(&thread), 40), change.done_line(changed));
                self.notify(line, Level::Info, now);
            }
            Acted::Changed(thread, _, Err(e)) => {
                self.notify(format!("changing thread {thread}: {e:#}"), Level::Error, now);
            }
        }
    }

    /// Take what was read of the other humans.
    pub fn people_event(&mut self, event: people::Event, now: DateTime<Utc>) {
        let people::Event::Online(read) = event;
        if let Some(text) = self.data.people.read(read) {
            self.notify(text, Level::Error, now);
        }
    }

    /// Take the matches of the search run last.
    pub fn search_event(&mut self, found: anyhow::Result<(Vec<Entry>, usize)>) {
        if let Some(page) = self.pages_mut::<search::Page>().rfind(|page| page.found.is_none()) {
            page.found(found);
        }
    }

    /// Take what was read of the threads besides their messages.
    pub fn cards_event(&mut self, read: cards::Read, now: DateTime<Utc>) {
        if let Some(failed) = self.data.cards.take(read) {
            self.notify(failed, Level::Error, now);
        }
    }

    /// Take in what the delegation trees' reads and cancels brought.
    pub fn tree_event(&mut self, event: tree::Event, now: DateTime<Utc>) {
        match event {
            tree::Event::Read(root, links, spend) => {
                for page in self.pages_mut::<tree::Page>().filter(|page| page.root == root) {
                    if let Some(links) = &links {
                        page.links.clone_from(links);
                    }
                    if spend.is_some() {
                        page.spend.clone_from(&spend);
                    }
                }
            }
            tree::Event::Cancelled(outcomes) => {
                let (text, level) = tree::notice(&outcomes);
                self.notify(text, level, now);
            }
        }
    }

    /// Take the steps read of the runs reporting on a thread.
    pub fn steps_event(&mut self, steps::Event(thread, read): steps::Event, now: DateTime<Utc>) {
        match read {
            Ok(read) => self.data.steps.read(&thread, read, now),
            Err(e) => tracing::debug!(%thread, "reading the steps of the runs reporting on the thread: {e:#}"),
        }
    }

    /// Take in what a read of an ant's queue brought.
    pub fn ant_event(&mut self, event: ant::Event) {
        let ant::Event::Queue(ant, queue) = event;
        for page in self.pages_mut::<ant::Page>().filter(|page| page.ant == ant) {
            page.queue = queue.clone();
        }
    }

    /// Take in what a read of an ant's memories brought.
    pub fn memories_event(&mut self, memories::Event(ant, asked, read): memories::Event) {
        for page in self.pages_mut::<ant::Page>().filter(|page| page.ant == ant) {
            page.memories.read(&ant, asked, &read);
        }
    }

    /// Take in what an order about an ant's MCP servers brought.
    pub fn mcp_event(&mut self, event: mcp::Event) {
        for page in self.pages_mut::<ant::Page>() {
            match &event {
                mcp::Event::Read(ant, asked, read) if *ant == page.ant => page.mcp.read(*asked, read),
                mcp::Event::Signed(ant, sign, signed) if *ant == page.ant => page.mcp.signed(*sign, signed),
                _ => {}
            }
        }
    }

    /// Take in what a read of an ant's settings or orders, or an order changing them, brought.
    pub fn settings_event(&mut self, event: settings::Event) {
        if let Some(page) = self.pages_mut::<ant::Page>().rfind(|page| event.ant() == &page.ant) {
            page.settings.take(event);
        }
    }

    /// Take in what a gathering of the credentials asked for, or an answer to one, brought.
    pub fn credentials_event(&mut self, event: credentials::Event, now: DateTime<Utc>) {
        match self.data.credentials.take(event, now) {
            Some(Ok(done)) => self.notify(done, Level::Info, now),
            Some(Err(e)) => self.notify(e, Level::Error, now),
            None => {}
        }
    }

    /// Take in what a gathering of the held admin orders, or an answer to one or to a held decision, brought.
    pub fn held_event(&mut self, event: held::Event, now: DateTime<Utc>) {
        let done = self.data.held.take(event, now);
        let held = held::items(&self.data);
        self.data.held.settled(&held);
        match done {
            Some(Ok(done)) => self.notify(done, Level::Info, now),
            Some(Err(e)) => self.notify(e, Level::Error, now),
            None => {}
        }
    }
}

/// The next image encoded, when images are drawn; pending otherwise.
async fn next_image(images: Option<&mut images::Io>) -> (AttachmentHandle, Option<Protocol>) {
    match images {
        Some(images) => images.next().await,
        None => std::future::pending().await,
    }
}
