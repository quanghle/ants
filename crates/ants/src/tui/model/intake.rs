//! What the model takes in besides keys: what the live feed read or heard ([`Model::take`]), what the UI read of the
//! inbox, the channels and the reactions ([`Model::read`]), and the connection's state.

use ants_proto::ChannelEntry;
use chrono::{DateTime, Utc};

use super::{Action, Level, Model, Page};
use crate::live;
use crate::text::excerpt;
use crate::threads::thread::Entry;
use crate::tools::Update;
use crate::tui::command::{self, Tone};
use crate::tui::reads::Read;
use crate::tui::{approvals, held, react, thread, work};

impl Model {
    /// Take in what the live feed read or heard.
    pub fn take(&mut self, event: live::Event, now: DateTime<Utc>) -> Vec<Action> {
        match event {
            live::Event::Stored(Ok(entries)) => {
                let reached = self.reached(now);
                return reached.into_iter().chain(self.stored(entries, now)).collect();
            }
            live::Event::Stored(Err(e)) => self.failed("", &e, now),
            live::Event::Status(event) => {
                self.data.live.status(&event, now);
                self.data.calls.status(&event);
                self.data.steps.status(&event);
            }
            live::Event::Following => self.data.live.approvals_reset(),
            live::Event::Approval(update) => self.approval(update, now),
            live::Event::Decided(outcome) => return self.answered(outcome, now).into_iter().collect(),
            live::Event::FollowEnded(Ok(())) => {}
            live::Event::FollowEnded(Err(e)) => self.failed("following the tool requests", &e, now),
            live::Event::Colony(Ok(entries)) => self.data.live.colony(entries, now),
            live::Event::Colony(Err(e)) => self.failed("reading the colony", &e, now),
            live::Event::History(thread, read) => {
                let read = read.map(|entries| self.tools(entries));
                for entry in read.iter().flatten() {
                    self.data.delegations.add(entry);
                }
                let page = self.pages_mut::<thread::Page>().rfind(|page| page.history.thread == thread);
                if let Some(page) = page
                    && let Err(e) = page.history.loaded(&thread, read)
                {
                    self.failed(&format!("reading thread {thread}"), &e, now);
                }
                if self.open() == Some(&thread) {
                    return self.shown_reactions().into_iter().collect();
                }
            }
        }
        Vec::new()
    }

    /// Take the messages read from the stream. Marks read the human's new messages on an open thread, and reads the
    /// reactions to a shown channel's new posts.
    pub fn stored(&mut self, entries: Vec<Entry>, now: DateTime<Utc>) -> Vec<Action> {
        let entries = self.tools(entries);
        let data = &mut self.data;
        data.live.stored(&entries, now);
        let me = data.live.me.clone();
        let mut posted = false;
        for entry in &entries {
            data.facts.add(entry, &me);
            data.people.dms.add(entry, &me);
            posted |= data.feeds.add(entry);
            data.activity.stored(entry, &data.feeds);
            data.delegations.add(entry);
        }
        let mut actions = Vec::new();
        for page in self.pages_mut::<thread::Page>() {
            let thread = page.history.thread.clone();
            if entries.iter().any(|e| e.thread_id == thread && work::to(e, &me)) {
                actions.push(Action::MarkRead(thread));
            }
            page.history.stored(entries.clone());
        }
        let shown = match self.page() {
            Page::Channel(_) => posted,
            Page::Thread(page) => entries.iter().any(|e| e.thread_id == page.history.thread),
            _ => false,
        };
        actions.extend(self.shown_reactions().filter(|_| shown));
        actions
    }

    /// Take the tool calls and ends with no reply among `entries` into those of their threads, an end into what its
    /// thread's messages tell too: the other entries.
    pub(super) fn tools(&mut self, entries: Vec<Entry>) -> Vec<Entry> {
        let (tools, entries): (Vec<Entry>, Vec<Entry>) = entries.into_iter().partition(|e| e.kind.on_timeline());
        let me = self.data.live.me.clone();
        for tool in &tools {
            self.data.calls.stored(tool);
            if tool.kind == crate::threads::thread::Kind::NoReply {
                self.data.facts.add(tool, &me);
            }
        }
        entries
    }

    /// Take word from following the tool requests: a settled request keeps its card a while, unless decided here.
    pub(super) fn approval(&mut self, update: Update, now: DateTime<Utc>) {
        self.data.live.approval(&update);
        let held = held::items(&self.data);
        self.data.held.settled(&held);
        if let Update::Resolved(request, state) = update {
            if let Some(asking) = self.asking_mut()
                && !asking.settled(&request, &state)
            {
                self.unask();
                let how = approvals::how(&state, &request.ant);
                self.notify(format!("request {} {how}", crate::tools::qualified(&request)), Level::Info, now);
            }
            if !self.data.settled.decided_here(&request, &state, self.data.me()) {
                self.data.settled.add(request, state, now);
            }
        }
    }

    /// Take the outcome of storing a decision: the line saying so, or why it was not stored, the first decision
    /// winning. A rejection with a note sends the note once stored; refused, the note comes back into the bar, to go as
    /// a reply on the request's thread if wanted.
    pub(super) fn answered(&mut self, outcome: anyhow::Result<String>, now: DateTime<Utc>) -> Option<Action> {
        let Some(asking) = self.asking_mut().filter(|a| a.storing) else {
            self.decided(outcome);
            return None;
        };
        asking.storing = false;
        let note = asking.note.take().map(|note| (note, Box::new(asking.request.clone())));
        if outcome.is_ok() || !asking.full {
            self.unask();
        }
        match (outcome, note) {
            (Ok(line), None) => self.notify(line, Level::Info, now),
            (Err(e), None) => self.notify(format!("{e:#}"), Level::Error, now),
            (Ok(line), Some((note, request))) => {
                self.notify(line, Level::Info, now);
                self.ui.bar.set_status(format!("sending the note to {}…", request.ant), Tone::Busy);
                let job = command::note_job(&request, &note);
                self.ui.bar.sending_note(&note, request);
                return Some(Action::Run(job, note));
            }
            (Err(e), Some((note, request))) => {
                self.notify(format!("{e:#}"), Level::Error, now);
                let why = format!("{e:#}; the note was not sent");
                if !self.ui.bar.is_empty() {
                    self.ui.bar.set_status(why, Tone::Error);
                    return None;
                }
                self.ui.bar.set_status(format!("{why}: Enter sends it to {} as a reply", request.ant), Tone::Error);
                self.ui.bar.set_text(&note);
                self.ui.bar.noting = Some(command::Note::Reply(request));
                self.ui.bar.focused = true;
            }
        }
        None
    }

    /// Take what the UI read besides the live feed.
    pub fn read(&mut self, read: Read, now: DateTime<Utc>) {
        let data = &mut self.data;
        match read {
            Read::Marker(Ok(marker)) => data.marker = Some(marker),
            Read::Marker(Err(e)) => self.failed("reading what was read", &e, now),
            Read::ChannelMarkers(Ok(markers)) | Read::ChannelMarked(_, Ok(markers)) => {
                data.channel_reads = Some(markers);
            }
            Read::ChannelMarkers(Err(e)) => self.failed("reading which channel posts were read", &e, now),
            Read::ChannelMarked(channel, Err(e)) => {
                self.notify(format!("marking #{channel} read: {e:#}"), Level::Error, now);
            }
            Read::Directory(Ok((channels, humans))) => {
                let me = data.me().clone();
                let channels: Vec<ChannelEntry> = channels.into_iter().filter(|c| c.members.contains(&me)).collect();
                data.activity.directory(&data.channels, &channels, now);
                data.channels = channels;
                data.humans = Some(humans);
            }
            Read::Directory(Err(e)) => self.failed("reading the channels", &e, now),
            Read::Levels(levels) => data.channel_levels = levels,
            Read::Marked(_, Ok(())) => {}
            Read::Unmarked(thread, Ok(())) => {
                let line = format!("thread {} marked unread", excerpt(&self.data.title(&thread), 40));
                self.notify(line, Level::Info, now);
            }
            Read::Unmarked(thread, Err(e)) => {
                self.notify(format!("marking thread {thread} unread: {e:#}"), Level::Error, now);
            }
            Read::Marked(thread, Err(e)) => {
                self.notify(format!("marking thread {thread} read: {e:#}"), Level::Error, now);
            }
            Read::Reactions(watched, reactions) => {
                data.activity.reactions(&watched, &reactions, &data.feeds, now);
                data.reactions = reactions;
            }
            Read::Reacted(message, reaction, remove, Ok(changed)) => {
                self.notify(react::done_line(message, &reaction, remove, changed), Level::Info, now);
            }
            Read::Reacted(message, reaction, _, Err(e)) => {
                self.notify(format!("reacting {reaction} to message {message}: {e:#}"), Level::Error, now);
            }
        }
    }

    /// Look at `now` at the states of the threads of the human's channels, for their activity.
    pub fn look(&mut self, now: DateTime<Utc>) {
        let mut activity = std::mem::take(&mut self.data.activity);
        activity.look(&self.data, now);
        self.data.activity = activity;
    }

    /// Take the connection's state at `now`: once it is back, what was missed meanwhile is read again.
    pub fn link(&mut self, connected: bool, now: DateTime<Utc>) -> Option<Action> {
        if !self.data.live.link(connected, now) {
            return None;
        }
        self.notify("reconnected: reading what changed meanwhile", Level::Info, now);
        Some(Action::Refresh)
    }

    /// Take a read the hub's JetStream answered at `now`: once it is back in reach, what was missed meanwhile is read
    /// again.
    pub(super) fn reached(&mut self, now: DateTime<Utc>) -> Option<Action> {
        if !self.data.live.reached() {
            return None;
        }
        self.notify("hub reached again: reading what changed meanwhile", Level::Info, now);
        Some(Action::Refresh)
    }

    /// Tell that `what` failed with `e` at `now`, unless the hub's JetStream did not answer: the header shows it out of
    /// reach instead, and the read is tried again on its own.
    pub(in crate::tui) fn failed(&mut self, what: &str, e: &anyhow::Error, now: DateTime<Utc>) {
        if let Some(text) = self.data.live.failed(what, e, now) {
            self.notify(text, Level::Error, now);
        }
    }
}
