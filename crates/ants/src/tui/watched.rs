//! What the views are read from besides the live feed, the inbox and channel reads and the bar's jobs: each source
//! told what the views show ([`Watched::watch`]), heard from in turn ([`Watched::next`]), and what it brought taken
//! in by the model ([`Model::view_event`]).

use ants_proto::AttachmentHandle;
use chrono::{DateTime, Utc};
use ratatui_image::protocol::Protocol;

use super::acts::{self, Acted, Acts};
use super::model::{Action, Model};
use super::search::{self, Searches};
use super::{ant, cards, catchup, credentials, faces, held, images, mcp, memories, people, settings, steps, tree};
use crate::avatars::Identities;

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
        self.trees.watch(model.tree_shown());
        self.steps.watch(model.steps_wanted());
        self.ants.watch(model.ant_shown());
        self.memories.watch(model.memories_wanted());
        self.mcp.watch(model.mcp_wanted());
        let (shown, unread) = model.settings_shown().unzip();
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
            ViewEvent::Acted(acted) => self.acted(acted, now),
            ViewEvent::People(event) => self.people(event, now),
            ViewEvent::Found(found) => self.found(found),
            ViewEvent::Cards(read) => {
                self.cards(read, now);
                // TL;DRs ahead of time follow what was read.
                return self.ahead(now).into_iter().collect();
            }
            ViewEvent::Summarised(summarised) => {
                self.summarised(summarised, now);
                return self.ahead(now).into_iter().collect();
            }
            ViewEvent::Tree(event) => self.tree_event(event, now),
            ViewEvent::Steps(read) => self.steps_read(read, now),
            ViewEvent::Ant(event) => self.ant_event(event),
            ViewEvent::Memories(event) => self.memories_event(event),
            ViewEvent::Mcp(event) => self.mcp_event(event),
            ViewEvent::Settings(event) => self.settings_event(event),
            ViewEvent::Credentials(event) => self.credentials(event, now),
            ViewEvent::Held(event) => self.held(event, now),
            ViewEvent::Looks(identities) => self.data.looks.set(identities),
            ViewEvent::Image(handle, image) => self.data.images.insert(handle, image),
        }
        Vec::new()
    }
}

/// The next image encoded, when images are drawn; pending otherwise.
async fn next_image(images: Option<&mut images::Io>) -> (AttachmentHandle, Option<Protocol>) {
    match images {
        Some(images) => images.next().await,
        None => std::future::pending().await,
    }
}
