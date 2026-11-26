//! What the bar of a client does with the text typed in it: where Enter sends it ([`route`]), which the bar's title
//! names ([`Route::describe`]), and the message it sends there ([`outgoing`]), one routing all of them read, so that a
//! send goes where the title said when Enter was pressed. A binding gets both from an owned [`Context`]
//! ([`describe_send`], [`resolve_send`]).
//!
//! An `@user` typed first starts a thread with the user (on a channel's thread or feed, the text posts there), a
//! `#channel` posts a new thread there, a slash command is the client's to run but `/later`, which waits behind the
//! prompt running on the thread, `//` begins a text with `/`, and else the text goes where the view says ([`said`]).

use ants_proto::{ChannelId, TeamDirectory, ThreadId, UserId};
use serde::{Deserialize, Serialize};

use super::{Addressed, addressed, one_recipient};
use crate::entry::{Entry, Kind};
use crate::error::Error;
use crate::text::excerpt;

/// How much of a thread's title the bar's title shows.
const TITLE_CHARS: usize = 40;

/// What a refusal of the bar's text was doing.
const WHAT: &str = "sending the message";

/// The view the bar sends from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum View {
    /// None of those below: home, a list, a search.
    Elsewhere,
    /// An open thread.
    Thread {
        thread: ThreadId,
        /// Its title, as the client shows it.
        title: String,
        /// The channel it is posted in, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        channel: Option<ChannelId>,
        /// Whom a reply on it goes to ([`replied`]).
        to: Replied,
        /// Whether the bar quotes a message of it, which an `@user` then answers on it.
        quoting: bool,
    },
    /// A channel's feed.
    Channel { channel: ChannelId },
    /// The messages with another human.
    Dm { with: UserId },
}

impl View {
    /// The channel the bar posts to: that of the open channel thread, with the thread, else the channel whose feed is
    /// on view.
    fn channel(&self) -> Option<(ChannelId, Option<ThreadId>)> {
        match self {
            Self::Thread { thread, channel: Some(channel), .. } => Some((channel.clone(), Some(thread.clone()))),
            Self::Channel { channel } => Some((channel.clone(), None)),
            _ => None,
        }
    }

    /// The title of `thread`, as the view shows it.
    fn title(&self, of: &ThreadId) -> String {
        match self {
            Self::Thread { thread, title, .. } if thread == of => title.clone(),
            _ => of.to_string(),
        }
    }
}

/// Where Enter sends the bar's text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Route {
    /// Nowhere: a slash command, by its name, runs.
    Command { name: String },
    /// Nowhere yet: the @user or #channel typed first names no one yet.
    Unnamed,
    /// To the user, on a new thread, else on the thread of the message quoted.
    Ask {
        to: UserId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<ThreadId>,
    },
    /// To the channel, on a new thread, else on the channel's thread.
    Post {
        channel: ChannelId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<ThreadId>,
    },
    /// On the open thread, to whom its messages say.
    Reply { thread: ThreadId, to: Replied },
    /// On a new thread to the human's assistant, else the messenger ([`super::unaddressed`]).
    Unaddressed,
}

/// Whom a reply on a thread goes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Replied {
    /// The thread's messages are still being read: the send finds out.
    Reading,
    /// The ant that last messaged the human there, else the other human of the thread.
    To { user: UserId },
    /// The human's assistant, else the messenger, as no one else is there to reply to.
    Unaddressed,
}

/// Who the users the bar names are, as the client knows them.
pub trait Names {
    /// The human writing.
    fn me(&self) -> &UserId;
    /// Whether `user` is a human of the team.
    fn is_human(&self, user: &UserId) -> bool;
    /// The team directory, once read; none on a bus without one.
    fn teams(&self) -> Option<&TeamDirectory>;
}

/// What the bar knows, as a binding gives it ([`describe_send`], [`resolve_send`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context {
    pub me: UserId,
    pub view: View,
    /// The team's humans, once read.
    pub humans: Vec<UserId>,
    /// The team directory, once read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub teams: Option<TeamDirectory>,
}

impl Names for Context {
    fn me(&self) -> &UserId {
        &self.me
    }

    fn is_human(&self, user: &UserId) -> bool {
        *user == self.me || self.humans.contains(user)
    }

    fn teams(&self) -> Option<&TeamDirectory> {
        self.teams.as_ref()
    }
}

/// Where the bar's text goes, and what its title says of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Described {
    pub route: Route,
    pub description: String,
}

/// Where Enter sends `text`, as typed so far, and what the bar's title says of it, from `context`.
pub fn describe_send(text: &str, context: &Context) -> Described {
    let route = route(text, &context.view);
    let description = route.describe(&context.view, context);
    Described { route, description }
}

/// The message Enter sends with `text`, from `context` ([`outgoing`]).
pub fn resolve_send(text: &str, context: &Context) -> Result<Option<Outgoing>, Error> {
    outgoing(text, &context.view, context)
}

/// Where Enter sends `text` from `view` (see the [module](self)).
pub fn route(text: &str, view: &View) -> Route {
    let text = text.trim_start();
    let word = |after: &str| after.split(char::is_whitespace).next().unwrap_or_default().to_owned();
    if text.starts_with("//") {
        return said(view);
    }
    if let Some(command) = text.strip_prefix('/') {
        let name = word(command).to_ascii_lowercase();
        return if name == "later" { said(view) } else { Route::Command { name } };
    }
    if let Some(name) = text.strip_prefix('@')
        && view.channel().is_none()
    {
        let thread = match view {
            View::Thread { thread, quoting: true, .. } => Some(thread.clone()),
            _ => None,
        };
        return word(name).parse().map_or(Route::Unnamed, |to| Route::Ask { to, thread });
    }
    if let Some(name) = text.strip_prefix('#') {
        return word(name).parse().map_or(Route::Unnamed, |channel| Route::Post { channel, thread: None });
    }
    said(view)
}

/// Where text naming no one goes from `view`: posted on the open channel thread, or as a new thread on the channel on
/// view; else replied on the open thread, else to the human of the DM view, else to the human's assistant, else the
/// messenger.
pub fn said(view: &View) -> Route {
    if let Some((channel, thread)) = view.channel() {
        return Route::Post { channel, thread };
    }
    match view {
        View::Thread { thread, to, .. } => Route::Reply { thread: thread.clone(), to: to.clone() },
        View::Dm { with } => Route::Ask { to: with.clone(), thread: None },
        View::Elsewhere | View::Channel { .. } => Route::Unaddressed,
    }
}

impl Route {
    /// What the bar's title says of the route from `view`: whom to and on which thread, a team with its lead ([`named`]),
    /// and whether it goes sealed, to a human other than the sender.
    pub fn describe(&self, view: &View, names: &dyn Names) -> String {
        let title = |thread: &ThreadId| format!("\"{}\"", excerpt(&view.title(thread), TITLE_CHARS));
        let sealed = |to: &UserId| if names.is_human(to) && to != names.me() { ", sealed" } else { "" };
        match self {
            Self::Command { name } => format!("run /{name}"),
            Self::Unnamed => String::new(),
            Self::Ask { to, thread: None } => format!("to {} on a new thread{}", named(to, names), sealed(to)),
            Self::Ask { to, thread: Some(thread) } => {
                format!("to {} on {}{}", named(to, names), title(thread), sealed(to))
            }
            Self::Post { channel, thread: None } => format!("new thread in #{channel}"),
            Self::Post { channel, thread: Some(thread) } => format!("post in #{channel} on {}", title(thread)),
            Self::Reply { thread, to: Replied::Reading } => format!("reply on {}", title(thread)),
            Self::Reply { thread, to: Replied::To { user } } => {
                format!("reply to {user} on {}{}", title(thread), sealed(user))
            }
            Self::Reply { thread, to: Replied::Unaddressed } => {
                format!("reply to the assistant or the messenger on {}", title(thread))
            }
            Self::Unaddressed => "to the assistant or the messenger on a new thread".to_owned(),
        }
    }
}

/// `to` as the bar's title names it: a team with its lead, its own or one above it, where the message goes, or that
/// it has none; a human's name is the human's.
pub fn named(to: &UserId, names: &dyn Names) -> String {
    let Some(teams) = names.teams().filter(|teams| teams.is_team(to) && !names.is_human(to)) else {
        return to.to_string();
    };
    match teams.lead_source(to) {
        Some(source) if source.team == *to => format!("team {to} (lead {})", source.lead.as_ref().expect("a lead")),
        Some(source) => {
            format!("team {to} (lead {}, from team {})", source.lead.as_ref().expect("a lead"), source.team)
        }
        None => format!("team {to}, which has no lead"),
    }
}

/// What the bar's text says, as Enter takes it ([`draft`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Draft {
    /// A slash command but `/later`: the text after its `/`, the client's to run.
    Command { line: String },
    /// Start a thread with the user.
    Ask { to: UserId, text: String },
    /// Post a new thread to the channel.
    Post { channel: ChannelId, text: String },
    /// Send where the view says ([`said`]).
    Say { text: String },
    /// Reply on the open thread, waiting behind the prompt running there rather than steering it.
    Later { text: String },
}

/// `text`'s first word and the rest, trimmed.
fn first_word(text: &str) -> (&str, &str) {
    text.split_once(char::is_whitespace).map_or((text, ""), |(word, rest)| (word, rest.trim()))
}

/// What the bar's `text` says, trimmed, or why it says nothing that can be sent: an `@user` or `#channel` with no
/// text after it, a name that is none, `/later` with no text.
pub fn draft(text: &str) -> Result<Draft, Error> {
    let invalid = |why: String| Error::invalid(WHAT, why);
    let text = text.trim();
    if text.is_empty() {
        return Err(invalid("nothing to send".to_owned()));
    }
    if let Some(literal) = text.strip_prefix("//") {
        return Ok(Draft::Say { text: format!("/{literal}") });
    }
    if let Some(command) = text.strip_prefix('/') {
        return match first_word(command) {
            (name, "") if name.eq_ignore_ascii_case("later") => Err(invalid("usage: /later TEXT".to_owned())),
            (name, later) if name.eq_ignore_ascii_case("later") => Ok(Draft::Later { text: later.to_owned() }),
            _ => Ok(Draft::Command { line: command.to_owned() }),
        };
    }
    if let Some(rest) = text.strip_prefix('@') {
        let (name, text) = first_word(rest);
        let to: UserId = name.parse().map_err(|e| invalid(format!("{e}")))?;
        if text.is_empty() {
            return Err(invalid(format!("nothing to send to @{to}")));
        }
        return Ok(Draft::Ask { to, text: text.to_owned() });
    }
    if let Some(rest) = text.strip_prefix('#') {
        let (name, text) = first_word(rest);
        let channel: ChannelId = name.parse().map_err(|e| invalid(format!("{e}")))?;
        if text.is_empty() {
            return Err(invalid(format!("nothing to post to #{channel}")));
        }
        return Ok(Draft::Post { channel, text: text.to_owned() });
    }
    Ok(Draft::Say { text: text.to_owned() })
}

/// Where a message the bar sends goes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    /// To the user, the lead of the team it names carrying the team ([`addressed`]), on a new thread, else on that of
    /// the message quoted.
    Ask {
        to: UserId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        team: Option<UserId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<ThreadId>,
    },
    /// To the channel, on a new thread, else on the channel's thread.
    Post {
        channel: ChannelId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<ThreadId>,
    },
    /// On the thread, to the user; none while the thread's messages are read or with no one there to reply to, the
    /// send then finding out from them, else sending to the human's assistant, else the messenger.
    Reply {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<UserId>,
    },
    /// On a new thread to the human's assistant, else the messenger ([`super::unaddressed`]).
    Unaddressed,
}

/// The message the bar sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outgoing {
    pub target: Target,
    pub text: String,
    /// Wait behind the prompt running on the thread rather than steering it: `/later`.
    pub queue: bool,
    /// Whether it goes to a human other than the sender, so sealed, as the bar's title says.
    pub sealed: bool,
}

/// The message Enter sends with `text` from `view`, where [`route`] says, its text as [`draft`] reads it (on a
/// channel's thread or feed, an `@user` typed first is part of the post); none for a slash command, the client's to
/// run. Refused before anything is sent: what [`draft`] refuses, `/later` with no thread open, a team with no lead in
/// it nor above it ([`addressed`]), and a message to one recipient @mentioning several users ([`one_recipient`]).
pub fn outgoing(text: &str, view: &View, names: &dyn Names) -> Result<Option<Outgoing>, Error> {
    let invalid = |why: String| Error::invalid(WHAT, why);
    let drafted = match view.channel() {
        Some(_) if text.trim_start().starts_with('@') => Draft::Say { text: text.trim().to_owned() },
        _ => draft(text)?,
    };
    let (said, queue) = match drafted {
        Draft::Command { .. } => return Ok(None),
        Draft::Ask { text, .. } | Draft::Post { text, .. } | Draft::Say { text } => (text, false),
        Draft::Later { text } => (text, true),
    };
    if queue && !matches!(view, View::Thread { .. }) {
        return Err(invalid("/later replies on an open thread: open one first".to_owned()));
    }
    let target = match route(text, view) {
        Route::Ask { to, thread } => {
            let Addressed { to, team } = match names.teams() {
                Some(teams) => addressed(teams, |u| names.is_human(u), to).map_err(|e| invalid(e.to_string()))?,
                None => Addressed { to, team: None },
            };
            Target::Ask { to, team, thread }
        }
        Route::Post { channel, thread } => Target::Post { channel, thread },
        Route::Reply { thread, to } => {
            let to = match to {
                Replied::To { user } => Some(user),
                Replied::Reading | Replied::Unaddressed => None,
            };
            Target::Reply { thread, to }
        }
        Route::Unaddressed => Target::Unaddressed,
        Route::Command { .. } | Route::Unnamed => return Err(invalid("nothing to send".to_owned())),
    };
    let to = match &target {
        Target::Ask { to, .. } | Target::Reply { to: Some(to), .. } => Some(to),
        Target::Post { .. } | Target::Reply { to: None, .. } | Target::Unaddressed => None,
    };
    if let Some(to) = to {
        one_recipient(names.me(), to, &said)?;
    }
    let sealed = to.is_some_and(|to| names.is_human(to) && to != names.me());
    Ok(Some(Outgoing { target, text: said, queue, sealed }))
}

/// Whom a reply on a thread whose `entries` are these goes to: [`reply_to`], else [`dm_with`], else the human's
/// assistant or the messenger; `human` tells the humans.
pub fn replied(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Replied {
    reply_to(entries, me, &human)
        .or_else(|| dm_with(entries, me, &human))
        .map_or(Replied::Unaddressed, |user| Replied::To { user })
}

/// Who a reply on a thread whose `entries` are these goes to: the last ant that messaged `me` there, else the last ant
/// `me` messaged there; `human` tells the humans.
pub fn reply_to(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let messages = || entries.iter().rev().filter(|e| matches!(e.kind, Kind::Message | Kind::Escalation) && !e.forged);
    let ant = |user: &Option<UserId>| user.as_ref().filter(|u| *u != me && !human(u)).cloned();
    messages()
        .filter(|e| e.to.as_ref() == Some(me))
        .find_map(|e| ant(&e.from))
        .or_else(|| messages().filter(|e| e.from.as_ref() == Some(me)).find_map(|e| ant(&e.to)))
}

/// The other human of the latest message between `me` and another human among `entries`, for a reply on a thread
/// with no ant; `human` tells the humans.
pub fn dm_with(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let other = |from: &UserId, to: &UserId| match (from == me, to == me) {
        (true, false) => Some(to.clone()),
        (false, true) => Some(from.clone()),
        _ => None,
    };
    entries
        .iter()
        .rev()
        .filter(|e| e.kind == Kind::Message && !e.forged)
        .find_map(|e| other(e.from.as_ref()?, e.to.as_ref()?).filter(|u| human(u)))
}

#[cfg(test)]
mod tests;
