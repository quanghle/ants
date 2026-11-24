//! What a message quotes ([`Quote`]), the channel post a mention notice tells of ([`Mention`]), the @mentions of a
//! text ([`parse_mentions`]) and those a message outside a channel cannot carry ([`several_mentions`]).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use super::Envelope;
#[cfg(doc)]
use crate::ChannelPost;
use crate::{ChannelId, UserId};

/// A short quote of the message or post a human's reply answers ([`Envelope::quote`], [`ChannelPost::quote`]): its
/// author and an excerpt of its text, so the recipient sees what is answered even when it never held that message.
/// It is the sender's word only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub from: UserId,
    /// The excerpt, none when the quoted message was sealed and the reply is not ([`Quote::withheld`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl Quote {
    /// How many characters of the quoted text an excerpt keeps.
    pub const MAX: usize = 160;

    /// The quote of `text` by `from`: its whitespace collapsed to single spaces, cut to [`Quote::MAX`] characters with
    /// an ellipsis.
    pub fn new(from: UserId, text: &str) -> Self {
        let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
        let text = match line.char_indices().nth(Self::MAX) {
            Some((cut, _)) => format!("{}…", &line[..cut]),
            None => line,
        };
        Self { from, text: Some(text) }
    }

    /// The quote without its excerpt: a sealed message's text never goes in the clear.
    #[must_use]
    pub fn withheld(self) -> Self {
        Self { text: None, ..self }
    }

    /// The line a view shows above the reply: `↪ replying to <author>: <excerpt>`, or without the excerpt.
    pub fn line(&self) -> String {
        match &self.text {
            Some(text) => format!("↪ replying to {}: {text}", self.from),
            None => format!("↪ replying to {}", self.from),
        }
    }
}

/// Where the channel post a mention notice tells of is ([`Envelope::mention`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mention {
    pub channel: ChannelId,
    /// The post's id.
    pub post: Uuid,
    /// The post's author, when the notice is relayed by another ([`ChannelPost::relayed_notice`]): the envelope's
    /// `from` sent it. It is the sender's word only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<UserId>,
}

/// The users `text` @mentions, in the order of their first mention.
///
/// A mention is `@` and a user id, with the `@` at the start of the text or after whitespace or an opening bracket
/// or quote, so an address such as `a@b` mentions nobody. Punctuation after the id (`,.:;!?)` and the like) ends it;
/// a letter, `_`, `@` or a dot followed by a letter or digit (`@host.example`) means it was no user id.
pub fn parse_mentions(text: &str) -> Vec<UserId> {
    let mut found: Vec<UserId> = Vec::new();
    let mut prev: Option<char> = None;
    for (i, c) in text.char_indices() {
        if c == '@' && prev.is_none_or(|p| p.is_whitespace() || "([{<\"'`".contains(p)) {
            let rest = &text[i + 1..];
            let end =
                rest.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')).unwrap_or(rest.len());
            let (name, after) = rest.split_at(end);
            let mut after = after.chars();
            let glued = match after.next() {
                Some(c) if c.is_alphanumeric() || c == '_' || c == '@' => true,
                Some('.') => after.next().is_some_and(char::is_alphanumeric),
                _ => false,
            };
            if !glued
                && let Ok(id) = name.parse::<UserId>()
                && !found.contains(&id)
            {
                found.push(id);
            }
        }
        prev = Some(c);
    }
    found
}

/// The users other than `from` that `text` @mentions ([`parse_mentions`]), when they are more than one: a message
/// outside a channel has one recipient, so several mentions in it would read as addressing users it never reaches.
pub fn several_mentions(from: &UserId, text: &str) -> Option<Vec<UserId>> {
    let mentioned: Vec<UserId> = parse_mentions(text).into_iter().filter(|m| m != from).collect();
    (mentioned.len() > 1).then_some(mentioned)
}

/// Refuse a message from `from` to `to` outside a channel whose `text` @mentions several users
/// ([`several_mentions`]), saying why: it reaches `to` alone, so addressing several takes a channel post, as
/// `post_with` makes one, whose mentions wake or notify them, else one message to each.
pub fn one_recipient(from: &UserId, to: &UserId, text: &str, post_with: &str) -> Result<(), String> {
    let Some(mentioned) = several_mentions(from, text) else { return Ok(()) };
    let names: Vec<String> = mentioned.iter().map(|u| format!("@{u}")).collect();
    let (last, rest) = names.split_last().unwrap_or((&names[0], &[]));
    Err(format!(
        "Not sent: the text mentions {} and {last}, but a message has one recipient, {to}, and the others would \
         receive nothing. To address several, post to a channel ({post_with}), whose mentions reach them; else send \
         one message to each.",
        rest.join(", ")
    ))
}
