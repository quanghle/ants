//! Following a run: its records in [`TRAIL_RUNS_BUCKET`] (header, session and journal) read again every [`POLL`], what
//! changed printed a line each, until the run ends or stops. A human reads the bucket only by direct gets, so each poll
//! reads the entries put since the last one.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, RunState, TRAIL_RUNS_BUCKET, TrailFunction, TrailRun, TrailSession, Uuid,
    subject, trail_run_key,
};
use anyhow::{Result, anyhow};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::Value;
use tokio::time::Instant;

use super::runs::{destination, entered, gist, header_json, parse};
use crate::machines::latest_after;
use crate::text::{after_colon, one_line};

/// How often the run's records are read again.
const POLL: Duration = Duration::from_millis(500);

/// What of a run was printed already, so that only what changed is.
#[derive(Debug, Default)]
pub(super) struct Follower {
    phase: Option<String>,
    state: Option<&'static str>,
    /// The calls sent, with their last message and the re-asks so far.
    sent: HashMap<u32, (Uuid, u32)>,
    queued: HashSet<u32>,
    answered: HashSet<u32>,
    /// The legs of the run's route seen.
    legs: usize,
}

impl Follower {
    /// The lines for the run's header: a new phase, or the run running again or waiting on humans.
    pub(super) fn header(&mut self, run: &TrailRun) -> Vec<String> {
        let mut lines = Vec::new();
        for leg in run.route.iter().skip(self.legs.max(1)) {
            lines.push(format!("went on to {}", entered(leg)));
        }
        self.legs = self.legs.max(run.route.len());
        if run.phase != self.phase {
            if let Some(phase) = &run.phase {
                lines.push(format!("phase: {}", one_line(phase, usize::MAX)));
            }
            self.phase.clone_from(&run.phase);
        }
        let state = run.state.name();
        if self.state != Some(state) && !run.finished() {
            match &run.state {
                RunState::Running if self.state.is_some() => lines.push("running".to_owned()),
                RunState::Waiting => lines.push("waiting on humans' answers".to_owned()),
                _ => {}
            }
            self.state = Some(state);
        }
        lines
    }

    /// The lines for the run's session: the calls queued, and the steps sent, asked again or sent anew.
    pub(super) fn session(&mut self, session: &TrailSession) -> Vec<String> {
        let mut lines = Vec::new();
        for call in &session.queued {
            if !self.answered.contains(&call.call)
                && !self.sent.contains_key(&call.call)
                && self.queued.insert(call.call)
            {
                lines.push(format!("step {} queued: {}", call.call, gist(call.function, &call.args)));
            }
        }
        for call in session.pending.iter().filter(|call| call.sent && !self.answered.contains(&call.call)) {
            match self.sent.insert(call.call, (call.message, call.retries)) {
                None => lines.push(sent(call)),
                Some((_, retries)) if retries < call.retries => lines.push(format!(
                    "step {} asked again of {} for an answer matching its schema ({} of 2)",
                    call.call, call.to, call.retries
                )),
                Some((message, _)) if message != call.message => {
                    lines.push(format!("step {} sent anew to {}", call.call, call.to));
                }
                Some(_) => {}
            }
        }
        lines
    }

    /// The line for an answered call, the first time.
    pub(super) fn entry(&mut self, entry: &JournalEntry) -> Option<String> {
        if !self.answered.insert(entry.call) {
            return None;
        }
        Some(match (entry.function, &entry.answer) {
            (TrailFunction::Goto, CallAnswer::Error(error)) => {
                format!("call {} to go to {} refused: {}", entry.call, destination(&entry.args), one_line(error, 200))
            }
            (_, CallAnswer::Ok(value)) => format!("step {} answered: {}", entry.call, one_line(&text(value), 200)),
            (_, CallAnswer::Error(error)) => format!("step {} failed: {}", entry.call, one_line(error, 200)),
        })
    }
}

fn sent(call: &PendingCall) -> String {
    match call.function {
        TrailFunction::Agent => format!("step {} sent to {}: {}", call.call, call.to, gist(call.function, &call.args)),
        TrailFunction::Human => format!("step {} asks {}: {}", call.call, call.to, gist(call.function, &call.args)),
        TrailFunction::Trail => match call.child() {
            Some(child) => format!("step {} runs {} as run {child}", call.call, gist(call.function, &call.args)),
            None => format!("step {} runs {}", call.call, gist(call.function, &call.args)),
        },
        TrailFunction::Goto => format!("call {} goes to {}", call.call, destination(&call.args)),
    }
}

/// A value as text: a string as is, anything else as JSON.
fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string()),
    }
}

/// What a run's records hold, as read since the last poll.
#[derive(Debug, Default)]
pub(super) struct Read {
    pub(super) header: Option<TrailRun>,
    pub(super) session: Option<TrailSession>,
    pub(super) journal: Vec<JournalEntry>,
}

/// Where a follow reads a run's records from.
pub(super) trait Records {
    /// What the records hold, as put since the last read.
    async fn read(&mut self) -> Result<Read>;
}

/// The run's records in [`TRAIL_RUNS_BUCKET`] under `key`, read past the sequences `seqs`, which move on past them.
struct Bucket<'a> {
    js: &'a jetstream::Context,
    key: String,
    seqs: (u64, u64),
}

impl Records for Bucket<'_> {
    async fn read(&mut self) -> Result<Read> {
        read(self.js, &self.key, &mut self.seqs).await
    }
}

async fn read(js: &jetstream::Context, key: &str, seqs: &mut (u64, u64)) -> Result<Read> {
    let header_filter = subject::kv(TRAIL_RUNS_BUCKET, key);
    let (headers, next) = latest_after(js, TRAIL_RUNS_BUCKET, &header_filter, seqs.0).await?;
    seqs.0 = next;
    let (entries, next) = latest_after(js, TRAIL_RUNS_BUCKET, &format!("{header_filter}.>"), seqs.1).await?;
    seqs.1 = next;
    let mut read = Read { header: headers.iter().rev().find_map(|(k, v)| parse(k, v)), ..Read::default() };
    let prefix = format!("{key}.");
    for (k, v) in &entries {
        match k.strip_prefix(&prefix) {
            Some("session") => read.session = parse(k, v).or(read.session.take()),
            Some(call) if call.starts_with("call.") => read.journal.extend(parse::<JournalEntry>(k, v)),
            _ => {}
        }
    }
    read.journal.sort_by_key(|entry| entry.call);
    Ok(read)
}

/// How long a follow bears with a run whose records stay unreadable or unchanged.
#[derive(Debug, Clone, Copy)]
pub(super) struct Patience {
    /// How often the run's records are read again.
    pub(super) poll: Duration,
    /// How long after the last read that held the follow gives up on reads that fail or get no answer.
    pub(super) unread: Duration,
    /// How long a run past its wall time goes unchanged before the follow takes it for one no daemon conducts any more:
    /// only its conductor's daemon fails it on its wall time.
    pub(super) quiet: Duration,
}

impl Patience {
    pub(super) const DEFAULT: Self =
        Self { poll: POLL, unread: Duration::from_secs(60), quiet: Duration::from_secs(120) };
}

/// The exit status of a follow that gave up as its run's records stayed unreadable, which the run's failing does not
/// share.
pub(crate) const UNREAD_STATUS: i32 = 3;

/// A follow given up as its run's records stayed unreadable: [`UNREAD_STATUS`].
#[derive(Debug)]
pub(crate) struct Unread(pub(crate) String);

impl std::fmt::Display for Unread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unread {}

/// Follow `run` until it ends: print what changes, a line each, then its result; with `json`, only its record once it
/// ends. Fails once it failed or stopped, or once it is past its wall time with no daemon conducting it.
pub(super) async fn follow(js: &jetstream::Context, run: TrailRun, json: bool) -> Result<()> {
    let records = Bucket { js, key: trail_run_key(&run.conductor, &run.run), seqs: (1, 1) };
    following(records, run, json, Patience::DEFAULT).await
}

/// [`follow`] the run `run` in `records`, bearing with them as `patience` says.
pub(super) async fn following(mut records: impl Records, run: TrailRun, json: bool, patience: Patience) -> Result<()> {
    if !json {
        let trail = run.first_trail().map_or_else(|| "an inline script".to_owned(), |t| format!("trail {t}"));
        println!(
            "following run {} of {trail} on {} (interrupting stops the following, not the run)",
            run.run, run.conductor
        );
    }
    let mut follower = Follower::default();
    let mut last = run;
    let (mut held, mut changed, mut failing) = (Instant::now(), Instant::now(), false);
    let result = loop {
        let left = patience.unread.saturating_sub(held.elapsed());
        let read = tokio::time::timeout(left, records.read()).await.unwrap_or_else(|_| Err(anyhow!("no answer")));
        let read = match read {
            Ok(read) => {
                (held, failing) = (Instant::now(), false);
                read
            }
            Err(e) if held.elapsed() < patience.unread => {
                if !failing {
                    eprintln!("reading run {} failed: {e:#}; trying again", last.run);
                    failing = true;
                }
                tokio::time::sleep(patience.poll).await;
                continue;
            }
            Err(e) => {
                let id = &last.run;
                return Err(Unread(format!(
                    "gave up following run {id}: its records could not be read for {}: {e:#}; \
                     `ants trail show {id}` shows it",
                    ants_proto::duration::format(patience.unread)
                ))
                .into());
            }
        };
        if read.header.is_some() || read.session.is_some() || !read.journal.is_empty() {
            changed = Instant::now();
        }
        if let Some(header) = read.header {
            last = header;
        }
        let mut lines = follower.header(&last);
        lines.extend(read.session.as_ref().map_or_default(|session| follower.session(session)));
        lines.extend(read.journal.iter().filter_map(|entry| follower.entry(entry)));
        for line in lines.iter().filter(|_| !json) {
            println!("{line}");
        }
        let id = &last.run;
        match &last.state {
            RunState::Done { result } => break Ok(text(result)),
            RunState::Failed { error, .. } => break Err(anyhow!("run {id} failed:{}", after_colon(error))),
            RunState::Stopped { by } => {
                break Err(anyhow!("run {id} was stopped by {by}: `ants trail resume {id}` resumes it"));
            }
            RunState::Running | RunState::Waiting if abandoned(&last, changed, patience.quiet) => {
                break Err(anyhow!(
                    "run {id} is past its wall time of {} and its record has not changed for {}: no daemon of {} \
                     conducts it any more",
                    ants_proto::duration::format(last.limits.wall_time()),
                    ants_proto::duration::format(patience.quiet),
                    last.conductor
                ));
            }
            RunState::Running | RunState::Waiting => tokio::time::sleep(patience.poll).await,
        }
    };
    if json {
        println!("{}", header_json(&last, &super::runs::Rated::default())?);
    } else if let Ok(result) = &result {
        println!("done:{}", after_colon(result));
    }
    result.map(drop)
}

/// Whether `run`, its record last changed at `changed`, is past its wall time and has gone unchanged for `quiet`.
fn abandoned(run: &TrailRun, changed: Instant, quiet: Duration) -> bool {
    let past = chrono::Duration::from_std(run.limits.wall_time()).is_ok_and(|wall| run.started_at + wall <= Utc::now());
    past && changed.elapsed() >= quiet
}
