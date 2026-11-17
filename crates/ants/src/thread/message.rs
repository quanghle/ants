//! The MESSAGE of `ants react`: a whole message id, or a unique start of one among the messages a human can read
//! back; and the message an id names, which `--reply-to` quotes ([`find`]).

use std::collections::HashSet;

use ants_daemon::channel::CLOCK_SLACK;

use super::pick::{own_subjects, scan_made};
use super::*;

/// What a MESSAGE argument picks: a whole message id (a UUID), taken as given, or a start of one, among the messages
/// to or from the CLI's user stored in the last [`RECENT`], else among any user's stored then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagePick {
    Id(Uuid),
    Prefix(String),
}

impl std::str::FromStr for MessagePick {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let text = text.trim().to_ascii_lowercase();
        if let Ok(id) = Uuid::try_parse(&text) {
            return Ok(Self::Id(id));
        }
        if !text.is_empty() && text.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            return Ok(Self::Prefix(text));
        }
        bail!("{text} is no message id or start of one")
    }
}

impl MessagePick {
    /// The message this picks for `me`, reading the stream with `js` unless given a whole id. Only a human's
    /// credentials get stream messages by sequence, so only a human picks by a start of an id.
    pub async fn resolve(self, js: &jetstream::Context, me: &crate::humans::Me) -> Result<Uuid> {
        let prefix = match self {
            Self::Id(id) => return Ok(id),
            Self::Prefix(prefix) => prefix,
        };
        if !me.role.is_human() {
            bail!("the credentials of {} are an ant's, which read no message back: give the whole message id", me.id);
        }
        let own = recent_messages(js, &me.id).await?;
        if let Some(id) = select(&own, &prefix, &me.id)? {
            return Ok(id);
        }
        let mut any = Vec::new();
        scan_made(js, &prefix, |entry| {
            if entry.id.is_some_and(|id| id.hyphenated().to_string().starts_with(&prefix)) {
                any.push(entry);
            }
        })
        .await?;
        any.reverse();
        select_any(&any, &own, &prefix, &me.id)
    }
}

/// The message or post `id` names, a sealed one opened with `keyring`, read back from when its time-ordered id was made,
/// less the clocks' skew, up to [`LATE`] after; None when none is kept, or only a forged one.
pub async fn find(js: &jetstream::Context, id: Uuid, keyring: Option<Arc<sealed::Keyring>>) -> Result<Option<Entry>> {
    let Some((secs, nanos)) = id.get_timestamp().map(|t| t.to_unix()) else { return Ok(None) };
    let made = SystemTime::UNIX_EPOCH + Duration::new(secs, nanos);
    let from = made.checked_sub(CLOCK_SLACK).unwrap_or(made);
    let start = consumer::DeliverPolicy::ByStartTime { start_time: from.into() };
    let mut parser = Parser::opening(keyring);
    let mut found = None;
    reading::read_back_awaiting(js, "", Reading::Gets, subject::ALL, start, async |seq, stored, msg| {
        if stored > made + LATE {
            return ControlFlow::Break(());
        }
        let entry = parser.read(seq, stored.into(), &msg.subject, &msg.payload).await;
        if let Some(entry) = entry.filter(|e| e.id == Some(id) && !e.forged) {
            found = Some(entry);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .await?;
    Ok(found)
}

/// The messages to or from `user` ([`own_subjects`]) stored in the last [`RECENT`] that have an id, each once, most
/// recent first.
pub async fn recent_messages(js: &jetstream::Context, user: &UserId) -> Result<Vec<Entry>> {
    let mut parser = Parser::default();
    let mut messages = Vec::new();
    for filter in own_subjects(user) {
        reading::read_back(js, "", Reading::Gets, &filter, window(RECENT), |seq, stored, subject, payload| {
            if let Some(entry) = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged) {
                messages.push(entry);
            }
            ControlFlow::Continue(())
        })
        .await?;
    }
    messages.sort_by_key(|e| std::cmp::Reverse(e.seq));
    let mut seen = HashSet::new();
    messages.retain(|e| e.id.is_some_and(|id| seen.insert(id)));
    Ok(messages)
}

/// The messages among `messages` whose id starts with `prefix`, each once.
fn matching<'a>(messages: &'a [Entry], prefix: &str) -> Vec<&'a Entry> {
    let mut seen = HashSet::new();
    messages
        .iter()
        .filter(|e| e.id.is_some_and(|id| id.hyphenated().to_string().starts_with(prefix) && seen.insert(id)))
        .collect()
}

/// The one message among `messages` (to or from `user`, most recent first) whose id starts with `prefix`; None when
/// none does. The error on several lists them.
pub(super) fn select(messages: &[Entry], prefix: &str, user: &UserId) -> Result<Option<Uuid>> {
    let found = matching(messages, prefix);
    match found.as_slice() {
        [] => Ok(None),
        [one] => Ok(one.id),
        _ => bail!(
            "{} messages to or from {user} over the last {} days start with {prefix}; give more of the id:\n{}",
            found.len(),
            RECENT.as_secs() / 86_400,
            listing(found.iter().copied())
        ),
    }
}

/// The one message among `messages` (any user's, most recent first) whose id starts with `prefix`, for a `user` it
/// was neither to nor from. The error on none lists `own`, the latest messages to or from `user`, and the one on
/// several lists those found.
pub(super) fn select_any(messages: &[Entry], own: &[Entry], prefix: &str, user: &UserId) -> Result<Uuid> {
    let found = matching(messages, prefix);
    let days = RECENT.as_secs() / 86_400;
    match found.as_slice() {
        [one] => one.id.ok_or_else(|| anyhow!("message without an id")),
        [] if own.is_empty() => {
            bail!("no message over the last {days} days starts with {prefix}: give the whole message id")
        }
        [] => bail!(
            "no message over the last {days} days starts with {prefix}; give the whole id of an older one, or pick one \
             of the latest to or from {user}:\n{}",
            listing(own.iter())
        ),
        _ => bail!(
            "{} messages over the last {days} days start with {prefix}, none of them to or from {user}; give more of \
             the id:\n{}",
            found.len(),
            listing(found.iter().copied())
        ),
    }
}

/// Messages as an error lists them, one per line: id, time, route and first line.
fn listing<'a>(messages: impl ExactSizeIterator<Item = &'a Entry>) -> String {
    let more = messages.len().saturating_sub(LISTED);
    let mut lines: Vec<String> = messages
        .take(LISTED)
        .filter_map(|e| {
            let id = e.id?;
            Some(format!("  {id}  {}  {}: {}", time(e.stored_at), e.route(), crate::text::excerpt(&e.text, 48)))
        })
        .collect();
    if more > 0 {
        lines.push(format!("  and {more} more"));
    }
    lines.join("\n")
}
