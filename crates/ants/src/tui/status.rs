//! `ants status`: what waits on the human by kind, the ants working and their spend that UTC day, in one line for a
//! status bar (`--tmux`) or a script (`--json`). Reads as `ants chat` does, once, for at most [`WITHIN`]; what it
//! could not read in time shows as `?` (null in JSON).

use std::time::Duration;

use ants_daemon::limits::caps::Spent;
use ants_proto::{Activity, STREAM, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::home::{self, Why};
use super::model::Model;
use super::{SINCE, cards};
use crate::live::{self, State};
use crate::threads::thread::{Entry, Parser};
use crate::tools::Update;

/// How long `ants status` reads.
const WITHIN: Duration = Duration::from_secs(3);

/// What `ants status` tells; None for what was not read in time.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct Status {
    /// How many home items there are of each kind.
    pub home: Counts,
    /// The ants working, as the colony says.
    pub working: Option<Vec<UserId>>,
    /// What the ants spent that UTC day, in dollars.
    pub spend_today: Option<f64>,
}

#[derive(Debug, Default, PartialEq, Serialize)]
pub struct Counts {
    pub approve: Option<usize>,
    pub question: Option<usize>,
    pub stuck: Option<usize>,
    pub message: Option<usize>,
    pub mention: Option<usize>,
    pub finished: Option<usize>,
}

/// What was read in time, each part None when not.
#[derive(Default)]
struct Parts {
    threads: Option<(State, Vec<Entry>)>,
    marker: Option<ants_proto::ReadMarker>,
    humans: Option<ants_proto::Humans>,
    approvals: Option<Vec<ants_proto::ApprovalRequest>>,
    colony: Option<Vec<ants_proto::ColonyEntry>>,
    cards: Option<cards::Read>,
}

impl Status {
    /// What `parts` tell `me`, at `now`.
    fn of(parts: Parts, me: &UserId, now: DateTime<Utc>) -> Self {
        let (state, entries, threads) = match parts.threads {
            Some((state, entries)) => (state, entries, true),
            None => (State::new(me.clone(), SINCE), Vec::new(), false),
        };
        let mut model = Model::new(state, String::new());
        let (marker, approvals) = (parts.marker.is_some(), parts.approvals.is_some());
        model.data.humans = parts.humans;
        model.data.marker = parts.marker;
        model.stored(entries, now);
        for request in parts.approvals.unwrap_or_default() {
            model.data.live.approval(&Update::Pending(request));
        }
        let working = parts.colony.map(|colony| {
            colony.into_iter().filter(|e| e.activity == Activity::Working).map(|e| e.ant).collect::<Vec<_>>()
        });
        let (done, today) = parts.cards.as_ref().map_or((false, false), |read| (read.meta.is_ok(), read.today.is_ok()));
        if let Some(read) = parts.cards {
            model.data.cards.take(read);
        }
        let items = home::items(&model.data, now);
        let count = |why: Why, known: bool| known.then(|| items.iter().filter(|i| i.why == why).count());
        Self {
            home: Counts {
                approve: count(Why::Approval, approvals),
                question: count(Why::Question, threads),
                stuck: count(Why::Stuck, threads),
                message: count(Why::Message, threads && marker),
                mention: count(Why::Mention, threads && marker),
                finished: count(Why::Finished, threads && done && marker),
            },
            working,
            spend_today: today.then(|| model.data.cards.today().map_or(0.0, |s: Spent| s.cost)),
        }
    }

    /// How many home items there are, if all were read.
    fn waiting(&self) -> Option<usize> {
        let Counts { approve, question, stuck, message, mention, finished } = self.home;
        Some(approve? + question? + stuck? + message? + mention? + finished?)
    }

    /// One short line, for tmux's status-right: `⚑3 ◐2 $4.12`.
    pub fn tmux(&self) -> String {
        let spend = self.spend_today.map_or_else(|| "$?".to_owned(), super::work::dollars);
        format!("⚑{} ◐{} {spend}", or_unknown(self.waiting()), or_unknown(self.working.as_ref().map(Vec::len)))
    }

    /// One line: `1 approve · 0 question · 0 stuck · 1 message · 2 mention · 1 finished │ ◐ dev-a · ◐ scout │ $4.12 today`.
    pub fn line(&self) -> String {
        let Counts { approve, question, stuck, message, mention, finished } = self.home;
        let counts = [
            (approve, Why::Approval),
            (question, Why::Question),
            (stuck, Why::Stuck),
            (message, Why::Message),
            (mention, Why::Mention),
            (finished, Why::Finished),
        ]
        .map(|(n, why)| format!("{} {}", or_unknown(n), why.label()))
        .join(" · ");
        let working = match &self.working {
            None => "? working".to_owned(),
            Some(ants) if ants.is_empty() => "no ant working".to_owned(),
            Some(ants) => ants.iter().map(|a| format!("◐ {a}")).collect::<Vec<_>>().join(" · "),
        };
        let spend = self.spend_today.map_or_else(|| "$?".to_owned(), super::work::dollars);
        format!("{counts} │ {working} │ {spend} today")
    }
}

fn or_unknown(n: Option<usize>) -> String {
    n.map_or_else(|| "?".to_owned(), |n| n.to_string())
}

/// `ants status`: read for at most [`WITHIN`], then print what was read in time.
pub async fn run(nats: &crate::connection::Nats<'_>, tmux: bool, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; `ants status` tells what waits on a human");
    }
    let deadline = tokio::time::Instant::now() + WITHIN;
    let parts = match tokio::time::timeout_at(deadline, nats.connect_only("ants-status")).await {
        Ok(Ok((_, js))) => Box::pin(read(nats, &js, deadline)).await,
        Ok(Err(e)) => {
            eprintln!("reaching the server: {e:#}");
            Parts::default()
        }
        Err(_) => Parts::default(),
    };
    let status = Status::of(parts, me, Utc::now());
    let line = if json {
        serde_json::to_string(&status)?
    } else if tmux {
        status.tmux()
    } else {
        status.line()
    };
    println!("{line}");
    Ok(())
}

/// What `work` gives by `deadline`, if it succeeds by then.
async fn within<T>(deadline: tokio::time::Instant, work: impl Future<Output = Result<T>>) -> Option<T> {
    match tokio::time::timeout_at(deadline, work).await {
        Ok(Ok(read)) => Some(read),
        Ok(Err(e)) => {
            tracing::debug!("{e:#}");
            None
        }
        Err(_) => None,
    }
}

/// Read each part at once, keeping those read by `deadline`.
async fn read(nats: &crate::connection::Nats<'_>, js: &jetstream::Context, deadline: tokio::time::Instant) -> Parts {
    let me = nats.me.id.clone();
    let threads_and_cards = async {
        let threads = within(deadline, async {
            let state = live::state(nats, js, SINCE).await?;
            let (_, read) = live::tail(js.clone(), Parser::default(), None, SINCE, false).await;
            Ok((state, read?.0))
        })
        .await;
        let ids = threads.iter().flat_map(|(_, entries)| entries.iter().map(|e| e.thread_id.clone())).collect();
        let cards = Box::pin(within(deadline, async { Ok(cards::Source::open(nats, js).await.read(ids).await) })).await;
        (threads, cards)
    };
    let marker = within(deadline, async { Ok(crate::messages::inbox::Inbox::open(js, &me).await?.marker().await?.0) });
    let humans = within(deadline, crate::people::humans::read(js));
    let approvals = within(deadline, async {
        let stream = js.get_stream(STREAM).await?;
        crate::tools::pending(js, &stream, Utc::now()).await
    });
    let colony = within(deadline, crate::colony::list(js));
    let ((threads, cards), marker, humans, approvals, colony) =
        futures::join!(threads_and_cards, marker, humans, approvals, colony);
    Parts { threads, marker, humans, approvals, colony, cards }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::live::{at, message, user};

    #[test]
    fn what_was_not_read_in_time_shows_as_unknown() {
        let thread = ThreadId::generate();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        let entries = vec![opening, message(2, &thread, "dev-a", "alice", "deployed")];
        let mut scout = crate::test_support::colony::entry("scout", 60, 5, at(0));
        scout.activity = Activity::Working;
        let read = cards::Read {
            titles: Ok(Default::default()),
            meta: Ok(Default::default()),
            done_acting: Default::default(),
            spend: Ok(Default::default()),
            today: Err(anyhow::anyhow!("timed out")),
            reminders: Ok(Default::default()),
        };
        let parts = Parts {
            threads: Some((State::new(user("alice"), SINCE), entries)),
            approvals: Some(Vec::new()),
            colony: Some(vec![scout, crate::test_support::colony::entry("dev-a", 60, 5, at(0))]),
            cards: Some(read),
            ..Parts::default()
        };
        let status = Status::of(parts, &user("alice"), at(3));
        assert_eq!(
            status.line(),
            "0 approve · 0 question · 0 stuck · ? message · ? mention · ? finished │ ◐ scout │ $? today"
        );
        assert_eq!(status.tmux(), "⚑? ◐1 $?");
        assert_eq!(
            serde_json::to_string(&status).unwrap(),
            r#"{"home":{"approve":0,"question":0,"stuck":0,"message":null,"mention":null,"finished":null},"working":["scout"],"spend_today":null}"#
        );

        let nothing = Status::of(Parts::default(), &user("alice"), at(3));
        assert_eq!(
            nothing.line(),
            "? approve · ? question · ? stuck · ? message · ? mention · ? finished │ ? working │ $? today"
        );
        assert_eq!(nothing.tmux(), "⚑? ◐? $?");
    }
}
