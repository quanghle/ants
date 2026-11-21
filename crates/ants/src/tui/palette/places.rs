//! The places the palette lists and Ctrl-T steps through: the human's channels, the ants, the other humans and the
//! threads work lists, each with what is unread or waiting there.

use ants_proto::ThreadId;
use chrono::{DateTime, Utc};

use crate::tui::command::Conversation;
use crate::tui::model::{Action, Focus, Level, Model, Page};
use crate::tui::work::{self, Filter, ThreadState};
use crate::tui::{dm, nav, thread};

/// Where a place's entry goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place {
    /// A channel, an ant or a person, as the nav selects it.
    Nav(nav::Item),
    Thread(ThreadId),
}

/// A place as the palette lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spot {
    pub place: Place,
    /// How it shows: `#channel`, `@ant`, `@person`, or a thread's title.
    pub word: String,
    /// What it is narrowed by: its name without a sigil, or a thread's title.
    pub name: String,
    pub detail: String,
    /// Whether something is unread there.
    pub unread: bool,
    /// Whether something is unread there or waits on the human.
    pub first: bool,
}

impl Model {
    /// The places, those with something unread or waiting first: the channels, the ants, the other humans, then the
    /// threads in the order work lists them.
    pub fn places(&self, now: DateTime<Utc>) -> Vec<Spot> {
        let data = &self.data;
        let mut spots = Vec::new();
        for item in nav::items(data, &self.nav, now) {
            let (word, name, detail, unread) = match &item {
                nav::Item::Channel(channel) => {
                    let u = nav::unread(data, channel);
                    let mut detail = vec!["channel".to_owned()];
                    if u.posts > 0 {
                        detail.push(format!("{} unread", u.posts));
                    }
                    if u.mentions > 0 {
                        detail.push(format!("{} @mentions", u.mentions));
                    }
                    (format!("#{channel}"), channel.to_string(), detail.join(" · "), u.posts + u.mentions > 0)
                }
                nav::Item::Ant(ant) => (format!("@{ant}"), ant.to_string(), "ant".to_owned(), false),
                nav::Item::Person(person) => {
                    let n = dm::unread(data, person);
                    let detail = if n > 0 { format!("person · {n} unread") } else { "person".to_owned() };
                    (format!("@{person}"), person.to_string(), detail, n > 0)
                }
                _ => continue,
            };
            spots.push(Spot { place: Place::Nav(item), word, name, detail, unread, first: unread });
        }
        for (thread, state) in work::Page::new(Filter::default()).rows(data) {
            let title = data.title(&thread);
            let n = data.unread_on(&thread).count();
            let mut detail = vec!["thread".to_owned(), state.label().to_owned()];
            if n > 0 {
                detail.push(format!("{n} unread"));
            }
            spots.push(Spot {
                place: Place::Thread(thread),
                word: title.clone(),
                name: title,
                detail: detail.join(" · "),
                unread: n > 0,
                first: n > 0 || state == ThreadState::WaitingOnMe,
            });
        }
        spots.sort_by_key(|s| !s.first);
        spots
    }

    /// Show `place`, its draft in the bar.
    pub(in crate::tui) fn visit(&mut self, place: Place) -> Option<Action> {
        self.focus = Focus::View;
        let action = match place {
            Place::Nav(item) => self.select(item),
            Place::Thread(thread) if self.here().as_ref() == Some(&Place::Thread(thread.clone())) => None,
            Place::Thread(thread) => {
                self.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
        };
        self.follow_drafts();
        action
    }

    /// Ctrl-T: show the next conversation after the one shown with something unread: a channel, a person or a
    /// thread.
    pub(in crate::tui) fn next_unread(&mut self, now: DateTime<Utc>) -> Option<Action> {
        let here = self.here();
        let unread: Vec<Place> = self.places(now).into_iter().filter(|s| s.unread).map(|s| s.place).collect();
        let at = here.as_ref().and_then(|here| unread.iter().position(|p| p == here)).map_or(0, |at| at + 1);
        let len = unread.len();
        let Some(next) = unread.into_iter().cycle().skip(at).take(len).find(|p| Some(p) != here.as_ref()) else {
            self.notify("nothing unread elsewhere", Level::Info, now);
            return None;
        };
        self.visit(next)
    }

    /// The conversation shown, as a place.
    fn here(&self) -> Option<Place> {
        match self.conversation() {
            Conversation::Thread(thread) => Some(Place::Thread(thread)),
            Conversation::Channel(channel) => Some(Place::Nav(nav::Item::Channel(channel))),
            Conversation::Person(person) => Some(Place::Nav(nav::Item::Person(person))),
            Conversation::None => None,
        }
    }
}
