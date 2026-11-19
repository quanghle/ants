//! `ants thread search`: the messages stored on any thread that match every filter given (sender, recipient,
//! channel, kind, time and text), the latest `--limit` of them.
//!
//! Read back as `ants thread` reads ([`thread::scan`]): by message gets with a human's credentials, so nothing is
//! consumed, acked or marked read. The filters pick the subjects read (an inbox subject names its recipient and
//! sender, a channel subject its channel and sender, a notes subject its sender, a tool request its ant, a dead
//! letter its ant), one read of the stream per kind of subject, each starting at `--since` and stopping at
//! `--until`; what the subjects do not tell apart is filtered on the messages read.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{ChannelId, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clap::ValueEnum as _;

use crate::threads::thread::{self, Entry, Kind, Parser};

type Time = DateTime<Utc>;

/// How far before `--until` (or now) a search reads without `--since`.
pub const DEFAULT_SINCE: Duration = Duration::from_hours(24);
/// How long the line of text shown for a match may be.
const LINE: usize = 100;

/// A time bound as `--since` and `--until` take it: a duration back from now, or a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Ago(Duration),
    At(DateTime<Utc>),
}

impl When {
    /// The time this names, `now` being now; None for no bound (a duration of zero).
    pub fn at(self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Ago(ago) if ago.is_zero() => None,
            Self::Ago(ago) => chrono::Duration::from_std(ago).ok().and_then(|ago| now.checked_sub_signed(ago)),
            Self::At(at) => Some(at),
        }
    }
}

/// Parse a time bound: a duration back from now (`90m`, `2h`, `3d`, `0` for no bound), an RFC 3339 time
/// (`2026-11-14T09:30:00+09:00`), or a date or a date and time without an offset (`2026-11-14`,
/// `2026-11-14 09:30`, `2026-11-14T09:30:15`), taken in UTC as `ants thread` prints times.
pub fn parse_when(text: &str) -> Result<When, String> {
    let text = text.trim();
    if let Ok(ago) = ants_proto::duration::parse(text) {
        return Ok(When::Ago(ago));
    }
    if let Ok(at) = DateTime::parse_from_rfc3339(text) {
        return Ok(When::At(at.with_timezone(&Utc)));
    }
    for format in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"] {
        if let Ok(at) = NaiveDateTime::parse_from_str(text, format) {
            return Ok(When::At(at.and_utc()));
        }
    }
    if let Ok(day) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(When::At(day.and_hms_opt(0, 0, 0).expect("midnight").and_utc()));
    }
    Err(format!(
        "invalid time {text:?}: a duration back from now (such as 90m, 2h or 3d, 0 for no bound), an RFC 3339 time \
         (2026-11-14T09:30:00+09:00), or a date or date and time in UTC (2026-11-14, 2026-11-14 09:30)"
    ))
}

/// What `ants thread search` keeps: messages matching every filter set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Search {
    /// Only messages this ant or human sent (a human's decision on a tool request names no sender).
    pub from: Option<UserId>,
    /// Only messages to this ant or human: inbox messages, and the decisions and dead letters of this ant.
    pub to: Option<UserId>,
    /// Only posts to this channel and the mentions telling of them.
    pub channel: Option<ChannelId>,
    /// Only messages of these kinds; every kind when empty.
    pub kinds: BTreeSet<Kind>,
    /// Only messages whose text contains this, whatever the case.
    pub grep: Option<String>,
    /// Only messages stored at or after this.
    pub since: Option<DateTime<Utc>>,
    /// Only messages stored before this.
    pub until: Option<DateTime<Utc>>,
    /// Only the latest matches, this many.
    pub limit: usize,
}

impl Search {
    /// The kinds a match may be of: those asked for, less those the other filters rule out (only posts and mentions
    /// are on a channel; posts, notes and tool requests have no recipient).
    fn kinds(&self) -> BTreeSet<Kind> {
        let mut kinds: BTreeSet<Kind> =
            if self.kinds.is_empty() { Kind::value_variants().iter().copied().collect() } else { self.kinds.clone() };
        if self.channel.is_some() {
            kinds.retain(|k| matches!(k, Kind::Post | Kind::Mention));
        }
        if self.to.is_some() {
            kinds.retain(|k| !matches!(k, Kind::Post | Kind::Note | Kind::Approval));
        }
        kinds
    }

    /// The subjects read, each once: those of the kinds searched, narrowed to the sender, recipient and channel the
    /// subjects name. Tool requests come before decisions, which a request read places on its thread.
    fn filters(&self) -> Vec<String> {
        let kinds = self.kinds();
        let any = |name: Option<String>| name.unwrap_or_else(|| "*".to_owned());
        let from = any(self.from.as_ref().map(UserId::to_string));
        let to = any(self.to.as_ref().map(UserId::to_string));
        let channel = any(self.channel.as_ref().map(ChannelId::to_string));
        let mut filters = Vec::new();
        let mut add = |filter: String| {
            if !filters.contains(&filter) {
                filters.push(filter);
            }
        };
        if kinds.iter().any(|k| matches!(k, Kind::Message | Kind::Escalation | Kind::Mention | Kind::Decision)) {
            add(format!("ants.inbox.{to}.{from}"));
            add(format!("ants.inbox.{to}.{from}.{}", ants_proto::subject::WEBHOOK));
        }
        if kinds.contains(&Kind::Post) {
            add(format!("ants.chan.{channel}.{from}"));
            add(format!("ants.chan.{channel}.{from}.{}", ants_proto::subject::WEBHOOK));
        }
        if kinds.contains(&Kind::Note) {
            add(format!("ants.notes.*.{from}"));
        }
        if kinds.contains(&Kind::Approval) {
            add(format!("ants.approvals.{from}"));
        }
        // A human's decision names no sender: with --from only a chain ant's, an inbox message, is one.
        if kinds.contains(&Kind::Decision) && self.from.is_none() {
            add(format!("ants.approvals.{to}"));
            add(format!("ants.approvals.{to}.*"));
        }
        if kinds.contains(&Kind::Dead) {
            add(format!("ants.dead.{to}"));
        }
        filters
    }

    /// Whether `entry` matches every filter.
    fn keeps(&self, kinds: &BTreeSet<Kind>, entry: &Entry) -> bool {
        kinds.contains(&entry.kind)
            && self.from.as_ref().is_none_or(|from| entry.from.as_ref() == Some(from))
            && self.to.as_ref().is_none_or(|to| entry.to.as_ref() == Some(to))
            && self.channel.as_ref().is_none_or(|channel| entry.channel.as_ref() == Some(channel))
            && self.since.is_none_or(|since| entry.stored_at >= since)
            && self.until.is_none_or(|until| entry.stored_at < until)
            && self.grep.as_deref().is_none_or(|grep| entry.matchable() && matched(&entry.text, grep).is_some())
    }

    /// The latest [`Self::limit`] matches among the messages `js` reads back, oldest first, and how many earlier
    /// matches the limit left out. `parser` opens sealed messages and checks the claims of messages sent for a human
    /// ([`Parser::opening`], [`Parser::checking`]).
    pub async fn run(&self, js: &jetstream::Context, mut parser: Parser) -> Result<(Vec<Entry>, usize)> {
        let start = match self.since {
            Some(since) => consumer::DeliverPolicy::ByStartTime { start_time: SystemTime::from(since).into() },
            None => consumer::DeliverPolicy::All,
        };
        let kinds = self.kinds();
        let mut found: BTreeMap<u64, Entry> = BTreeMap::new();
        let mut omitted = 0;
        for filter in self.filters() {
            reading::read_back_awaiting(js, "", Reading::Gets, &filter, start, async |seq, stored, msg| {
                let stored = DateTime::<Utc>::from(stored);
                if self.until.is_some_and(|until| stored >= until) {
                    return ControlFlow::Break(());
                }
                let entry = parser.read(seq, stored, &msg.subject, &msg.payload).await;
                if let Some(entry) = entry.filter(|e| self.keeps(&kinds, e)) {
                    found.insert(seq, entry);
                    if found.len() > self.limit {
                        found.pop_first();
                        omitted += 1;
                    }
                }
                ControlFlow::Continue(())
            })
            .await?;
        }
        Ok((found.into_values().collect(), omitted))
    }
}

/// The first line of `text` containing `grep`, whatever the case.
pub fn matched<'a>(text: &'a str, grep: &str) -> Option<&'a str> {
    let grep = grep.to_lowercase();
    text.lines().find(|line| line.to_lowercase().contains(&grep))
}

/// The matches as `ants thread search` prints them, one per line: time, thread, kind, route, note, message id, and
/// the first line of text matching `grep`, else the first line.
pub fn render(entries: &[Entry], grep: Option<&str>) -> String {
    let mut out = String::new();
    for e in entries {
        let forged = if e.forged { " (forged)" } else { "" };
        let note = e.note.as_ref().map_or_default(|n| format!(" [{n}]"));
        let id = e.id.map_or_default(|id| format!(" (message {id})"));
        let line = grep.and_then(|grep| matched(&e.text, grep)).unwrap_or(&e.text);
        let _ = writeln!(
            out,
            "{}  {}  {:<10}  {}{forged}{note}{id}: {}",
            thread::time(e.stored_at),
            e.thread_id,
            e.kind.as_str(),
            e.route(),
            crate::text::excerpt(line.trim(), LINE)
        );
    }
    out
}

/// The times `--since` and `--until` name, `now` being now: by default [`DEFAULT_SINCE`] before the end.
pub fn bounds(since: Option<When>, until: Option<When>, now: DateTime<Utc>) -> Result<(Option<Time>, Option<Time>)> {
    let until = until.and_then(|until| until.at(now));
    let since = match since {
        Some(since) => since.at(now),
        None => until.unwrap_or(now).checked_sub_signed(chrono::Duration::from_std(DEFAULT_SINCE)?),
    };
    if let (Some(since), Some(until)) = (since, until)
        && since >= until
    {
        bail!("--since {} is not before --until {}", thread::time(since), thread::time(until));
    }
    Ok((since, until))
}

/// `ants thread search`: the latest messages matching `search`, its bounds being `since` and `until` from now.
pub async fn search(
    nats: &crate::connection::Nats<'_>,
    mut search: Search,
    since: Option<When>,
    until: Option<When>,
    json: bool,
) -> Result<()> {
    (search.since, search.until) = bounds(since, until, Utc::now())?;
    let js = thread::connect(nats).await?;
    let keyring = crate::people::sealed::Keyring::load(nats, &js).await.map(Arc::new);
    let behalf = Arc::new(crate::people::behalf::Checker::new(nats, &js));
    let (entries, omitted) = search.run(&js, Parser::opening(keyring).checking(Some(behalf))).await?;
    if json {
        for entry in &entries {
            println!("{}", serde_json::to_string(entry)?);
        }
    } else if entries.is_empty() {
        println!("no matching messages");
    } else {
        print!("{}", render(&entries, search.grep.as_deref()));
    }
    if omitted > 0 {
        eprintln!("{omitted} earlier matches left out: raise --limit or narrow the search");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_proto::{
        ApprovalRequest, ChannelPost, DeadLetter, Envelope, Escalation, EscalationReason, NotesPost, Role, ToolCall,
        Verdict,
    };

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn json<T: serde::Serialize>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    #[test]
    fn a_bound_is_a_duration_back_a_time_or_a_utc_date() {
        let now = DateTime::parse_from_rfc3339("2026-11-15T12:00:00Z").unwrap().to_utc();
        let at = |text: &str| parse_when(text).unwrap().at(now);
        let utc = |text: &str| Some(DateTime::parse_from_rfc3339(text).unwrap().to_utc());
        assert_eq!(at("2h"), utc("2026-11-15T10:00:00Z"));
        assert_eq!(at("3d"), utc("2026-11-12T12:00:00Z"));
        assert_eq!(at("0"), None, "no bound");
        assert_eq!(at("2026-11-14T09:30:00+09:00"), utc("2026-11-14T00:30:00Z"));
        assert_eq!(at("2026-11-14"), utc("2026-11-14T00:00:00Z"));
        assert_eq!(at("2026-11-14 09:30"), utc("2026-11-14T09:30:00Z"));
        assert_eq!(at("2026-11-14T09:30:15"), utc("2026-11-14T09:30:15Z"));
        for bad in ["", "2", "yesterday", "2026-13-01", "14/11/2026"] {
            assert!(parse_when(bad).unwrap_err().starts_with("invalid time"), "{bad}");
        }

        let when = |text: &str| Some(parse_when(text).unwrap());
        assert_eq!(bounds(None, None, now).unwrap(), (utc("2026-11-14T12:00:00Z"), None), "the last 24h");
        let day_before = (utc("2026-11-13T00:00:00Z"), utc("2026-11-14T00:00:00Z"));
        assert_eq!(bounds(None, when("2026-11-14"), now).unwrap(), day_before);
        assert_eq!(bounds(when("0"), when("1h"), now).unwrap(), (None, utc("2026-11-15T11:00:00Z")));
        let reversed = bounds(when("1h"), when("2h"), now).unwrap_err().to_string();
        assert_eq!(reversed, "--since 2026-11-15 11:00:00 is not before --until 2026-11-15 10:00:00");
    }

    #[test]
    fn the_filters_pick_the_subjects_read() {
        let filters = |search: Search| search.filters();
        let every = [
            "ants.inbox.*.*",
            "ants.inbox.*.*.webhook",
            "ants.chan.*.*",
            "ants.chan.*.*.webhook",
            "ants.notes.*.*",
            "ants.approvals.*",
            "ants.approvals.*.*",
            "ants.dead.*",
        ];
        assert_eq!(filters(Search::default()), every);
        let from = Search { from: Some(p("dev-a")), ..Default::default() };
        let from_dev_a = [
            "ants.inbox.*.dev-a",
            "ants.inbox.*.dev-a.webhook",
            "ants.chan.*.dev-a",
            "ants.chan.*.dev-a.webhook",
            "ants.notes.*.dev-a",
            "ants.approvals.dev-a",
            "ants.dead.*",
        ];
        assert_eq!(filters(from), from_dev_a, "a human's decision names no sender");
        let to = Search { to: Some(p("dev-a")), ..Default::default() };
        let to_dev_a = [
            "ants.inbox.dev-a.*",
            "ants.inbox.dev-a.*.webhook",
            "ants.approvals.dev-a",
            "ants.approvals.dev-a.*",
            "ants.dead.dev-a",
        ];
        assert_eq!(filters(to), to_dev_a);
        let channel = Search { channel: Some("ops".parse().unwrap()), ..Default::default() };
        let posts_and_mentions =
            ["ants.inbox.*.*", "ants.inbox.*.*.webhook", "ants.chan.ops.*", "ants.chan.ops.*.webhook"];
        assert_eq!(filters(channel.clone()), posts_and_mentions, "posts and mentions");
        let posts = Search { kinds: [Kind::Post].into(), ..channel };
        assert_eq!(filters(posts), ["ants.chan.ops.*", "ants.chan.ops.*.webhook"]);
        let none = Search { to: Some(p("dev-a")), kinds: [Kind::Note].into(), ..Default::default() };
        assert!(filters(none).is_empty(), "a note has no recipient");
    }

    #[tokio::test]
    async fn a_human_searches_every_thread_by_sender_recipient_channel_kind_time_and_text() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, dev_a, dev_b, lead) = (p("alice"), p("dev-a"), p("dev-b"), p("lead"));
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        let (_, js_a) = server.connect_as(&dev_a, Role::Ant).await;
        let (_, js_b) = server.connect_as(&dev_b, Role::Ant).await;
        let read = crate::messages::inbox::ensure_bucket(&js).await.unwrap();

        let ask = Envelope::new(alice.clone(), dev_a.clone(), "Deploy the API").with_owner(Some(alice.clone()));
        let thread = ask.thread_id.clone();
        let reply = ask.reply(dev_a.clone(), "deploy failed\nsee the logs", Role::Human).unwrap();
        let post = ChannelPost::new("ops".parse().unwrap(), dev_b.clone(), "the deploy is red");
        let note = NotesPost::new(thread.clone(), dev_a.clone(), "remember the deploy order");
        let escalation = Escalation::new(dev_a.clone(), thread.clone(), EscalationReason::TurnBudget);
        let up = Envelope::on_thread(thread.clone(), dev_a.clone(), lead, "stuck").with_escalation(Some(escalation));
        let request = ApprovalRequest::new(
            dev_a.clone(),
            thread.clone(),
            ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() },
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        let decision = request.decide(Verdict::AllowOnce);
        let dead = DeadLetter::of_envelope(dev_a.clone(), 3, 5, "harness died", ask.clone());
        let publish = |js: &jetstream::Context, subject: String, payload: Vec<u8>| {
            let js = js.clone();
            async move { js.publish(subject, payload.into()).await.unwrap().await.unwrap() }
        };
        publish(&js, ask.subject(), json(&ask)).await;
        publish(&js, reply.subject(), json(&reply)).await;
        publish(&js_b, post.subject(), json(&post)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mark = Utc::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        publish(&js_a, note.subject(), json(&note)).await;
        publish(&js, up.subject(), json(&up)).await;
        publish(&js_a, request.subject(), json(&request)).await;
        publish(&js, decision.subject(), json(&decision)).await;
        publish(&js_a, dead.subject(), json(&dead)).await;

        let run = |search: Search| {
            let js = js.clone();
            async move { search.run(&js, Parser::default()).await.unwrap() }
        };
        let kinds = |search: Search| {
            let run = run(Search { limit: 50, ..search });
            async move { run.await.0.into_iter().map(|e| e.kind).collect::<Vec<Kind>>() }
        };
        use Kind::{Approval, Dead, Decision, Escalation as Esc, Message, Note, Post};
        let all = [Message, Message, Post, Note, Esc, Approval, Decision, Dead];
        assert_eq!(kinds(Search::default()).await, all);
        let from = Search { from: Some(dev_a.clone()), ..Default::default() };
        assert_eq!(kinds(from.clone()).await, [Message, Note, Esc, Approval], "a dead letter is from its sender");
        let to = Search { to: Some(dev_a.clone()), ..Default::default() };
        assert_eq!(kinds(to).await, [Message, Decision, Dead]);
        let ops = Search { channel: Some("ops".parse().unwrap()), ..Default::default() };
        assert_eq!(kinds(ops.clone()).await, [Post]);
        let typed = Search { kinds: [Note, Approval].into(), ..Default::default() };
        assert_eq!(kinds(typed).await, [Note, Approval]);
        let grep = Search { grep: Some("DEPLOY".into()), ..Default::default() };
        assert_eq!(kinds(grep.clone()).await, [Message, Message, Post, Note, Dead], "whatever the case");

        // Filters compose.
        assert_eq!(kinds(Search { grep: Some("deploy".into()), ..from }).await, [Message, Note]);
        let to_alice = Search { to: Some(alice.clone()), kinds: [Message].into(), grep: Some("failed".into()), ..grep };
        let (found, _) = run(Search { limit: 50, ..to_alice }).await;
        assert_eq!(found.iter().map(|e| e.id).collect::<Vec<_>>(), [Some(reply.id)]);

        // Time bounds: --since inclusive, --until exclusive.
        assert_eq!(kinds(Search { until: Some(mark), ..Default::default() }).await, [Message, Message, Post]);
        assert_eq!(kinds(Search { since: Some(mark), ..Default::default() }).await, all[3..]);
        let ask_at = run(Search { limit: 1, until: Some(mark), kinds: [Post].into(), ..Default::default() }).await;
        let at = ask_at.0[0].stored_at;
        assert_eq!(kinds(Search { since: Some(at), until: Some(mark), ..Default::default() }).await, [Post]);
        assert!(kinds(Search { since: Some(at), until: Some(at), ..Default::default() }).await.is_empty());

        // Empty results and the limit, which keeps the latest.
        assert!(kinds(Search { from: Some(p("nobody")), ..Default::default() }).await.is_empty());
        assert!(kinds(Search { to: Some(alice.clone()), ..ops }).await.is_empty(), "a post has no recipient");
        let (latest, omitted) = run(Search { limit: 2, ..Default::default() }).await;
        assert_eq!((latest.iter().map(|e| e.kind).collect::<Vec<_>>(), omitted), (vec![Decision, Dead], 6));

        // What is printed names the thread, the kind, the message id, and the matching line.
        let (found, _) = run(Search { grep: Some("failed".into()), limit: 50, ..Default::default() }).await;
        let shown = render(&found, Some("failed"));
        let line = format!("  {thread}  message     dev-a -> alice (message {}): deploy failed\n", reply.id);
        assert!(shown.ends_with(&line), "{shown}");
        let json: serde_json::Value = serde_json::to_value(&found[0]).unwrap();
        assert_eq!(json["thread_id"], thread.as_str());
        assert_eq!((json["kind"].as_str(), json["id"].as_str()), (Some("message"), Some(&*reply.id.to_string())));
        assert!(json["stored_at"].is_string() && json["seq"].is_u64(), "{json}");

        // Nothing consumed, acked or marked read: no consumer, no message gone, no read marker.
        let stream = js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().clone();
        assert_eq!((stream.state.messages, stream.state.consumer_count), (8, 0));
        assert_eq!(read.status().await.unwrap().values(), 0, "no read marker");
    }
}
