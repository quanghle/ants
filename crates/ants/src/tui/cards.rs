//! What `ants chat` reads of the threads besides their messages, every while: each thread's shared title, done state
//! and TL;DR and the human's snoozes in ants_threads, and what the ants spent on it since a human's last message, and in the UTC day, in
//! ants_sessions.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::limits::caps::Spent;
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{Acting, Reminders, SESSIONS_BUCKET, ThreadDone, ThreadId, ThreadSnoozes, ThreadSummary, UserId};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::{FutureExt as _, StreamExt as _};

use crate::live::maybe;
use crate::thread_meta::{self, Meta};

/// How often the titles, done states, TL;DRs and spend are read.
const EVERY: Duration = Duration::from_secs(10);

/// What was read of the threads besides their messages.
#[derive(Debug, Default)]
pub struct Cards {
    /// Each thread's title a human set, opened where sealed, else `[sealed]`, as `ants thread list` shows it.
    pub titles: HashMap<ThreadId, String>,
    /// Each thread's done state, whether or not a message since reopened it.
    pub done: HashMap<ThreadId, ThreadDone>,
    /// What the `on_behalf_of` of each done state claiming one came to ([`crate::behalf::Checker`]).
    pub done_acting: HashMap<ThreadId, Acting>,
    pub tldr: HashMap<ThreadId, ThreadSummary>,
    /// Whether the TL;DRs were read once.
    pub tldr_read: bool,
    /// What each ant spent on each thread since a human's last message there.
    pub spend: HashMap<ThreadId, Vec<(UserId, Spent)>>,
    /// What each ant spent that UTC day, once read.
    pub today: Option<Vec<(UserId, Spent)>>,
    /// The threads the human snoozed on home.
    pub snoozed: ThreadSnoozes,
    /// The human's reminders.
    pub reminders: Reminders,
    /// Why the last read failed, to tell only once.
    pub failed: Vec<String>,
}

impl Cards {
    /// What the ants spent on `thread` since a human's last message there, if anything is stored.
    pub fn spent(&self, thread: &ThreadId) -> Option<Spent> {
        let by = self.spend.get(thread)?;
        let mut total = Spent::default();
        by.iter().for_each(|(_, s)| total += *s);
        Some(total)
    }

    /// What `ant` spent on `thread` since a human's last message there, if anything is stored.
    pub fn spent_by(&self, thread: &ThreadId, ant: &UserId) -> Option<Spent> {
        self.spend.get(thread)?.iter().find(|(a, _)| a == ant).map(|(_, s)| *s)
    }

    /// What the ants spent that UTC day, once read.
    pub fn today(&self) -> Option<Spent> {
        let mut total = Spent::default();
        self.today.as_ref()?.iter().for_each(|(_, s)| total += *s);
        Some(total)
    }

    /// Who marked a thread `done`: an assistant for its human as [`Acting::label`] names it, a claim not checked
    /// forged.
    pub fn done_by(&self, done: &ThreadDone) -> String {
        let acting = match &done.on_behalf_of {
            None => Acting::Own,
            Some(human) => {
                self.done_acting.get(&done.thread_id).cloned().unwrap_or_else(|| Acting::Forged(human.clone()))
            }
        };
        acting.label(done.by.as_str())
    }

    /// Take `read`, keeping what an unread part had; the reasons of the parts that failed, if they changed.
    pub fn take(&mut self, read: Read) -> Option<String> {
        let mut failed = Vec::new();
        match read.meta {
            Ok((done, tldr)) => {
                (self.done, self.tldr, self.tldr_read) = (done, tldr, true);
                self.done_acting = read.done_acting;
            }
            Err(e) => failed.push(format!("reading the done states and TL;DRs: {e:#}")),
        }
        match read.titles {
            Ok(titles) => self.titles = titles,
            Err(e) => failed.push(format!("reading the titles: {e:#}")),
        }
        match read.spend {
            Ok(spend) => self.spend = spend,
            Err(e) => failed.push(format!("reading the spend: {e:#}")),
        }
        match read.today {
            Ok(today) => self.today = Some(today),
            Err(e) => failed.push(format!("reading the spend: {e:#}")),
        }
        match read.reminders {
            Ok(mut reminders) => {
                self.snoozed = std::mem::take(&mut reminders.snoozed);
                self.reminders = reminders;
            }
            Err(e) => failed.push(format!("reading the reminders and snoozes: {e:#}")),
        }
        failed.dedup();
        if failed == self.failed {
            return None;
        }
        self.failed = failed;
        (!self.failed.is_empty()).then(|| self.failed.join("; "))
    }
}

/// One read: each part, or why it failed.
pub struct Read {
    pub titles: Result<HashMap<ThreadId, String>>,
    pub meta: Result<(HashMap<ThreadId, ThreadDone>, HashMap<ThreadId, ThreadSummary>)>,
    /// What the claims of the done states read came to.
    pub done_acting: HashMap<ThreadId, Acting>,
    pub spend: Result<HashMap<ThreadId, Vec<(UserId, Spent)>>>,
    pub today: Result<Vec<(UserId, Spent)>>,
    /// The human's due times, its snoozes taken into [`Cards::snoozed`].
    pub reminders: Result<Reminders>,
}

/// Where the reads go: the titles bucket's [`Meta`], and the sessions bucket.
#[derive(Clone)]
pub struct Source {
    js: jetstream::Context,
    /// Why there is none, when ants_threads cannot be read.
    meta: Result<Rc<Meta>, String>,
    sessions: Result<DirectKv, String>,
    /// What checks the claims of the done states, marks and snoozes an assistant set for its human.
    behalf: Arc<crate::behalf::Checker>,
}

impl Source {
    pub async fn open(nats: &crate::connection::Nats<'_>, js: &jetstream::Context) -> Self {
        let meta = thread_meta::open(nats, js).await.map(Rc::new).map_err(|e| format!("{e:#}"));
        let sessions = DirectKv::new(js, None, SESSIONS_BUCKET).await.map_err(|e| format!("{e:#}"));
        let behalf = Arc::new(crate::behalf::Checker::new(nats, js));
        Self { js: js.clone(), meta, sessions, behalf }
    }

    /// The source, checking claims with `behalf` rather than a checker of its own.
    pub fn checking(self, behalf: Option<Arc<crate::behalf::Checker>>) -> Self {
        let behalf = behalf.unwrap_or(self.behalf);
        let meta =
            self.meta.map(|m| Rc::try_unwrap(m).map_or_else(|m| m, |m| Rc::new(m.checking(Some(behalf.clone())))));
        Self { behalf, meta, ..self }
    }

    /// Read what there is of `threads`: their titles, done states and TL;DRs, what was spent on them, that day's spend, and the
    /// human's snoozes and reminders.
    pub async fn read(self, threads: HashSet<ThreadId>) -> Read {
        let titles = async {
            let titles = self.meta.as_ref().map_err(|e| anyhow!("{e}"))?.titles(&threads).await?;
            Ok(titles.into_iter().map(|(thread, title)| (thread, title.title)).collect())
        };
        let meta = async {
            let meta = self.meta.as_ref().map_err(|e| anyhow!("{e}"))?;
            let done = meta.dones(&threads).await?;
            let reads = futures::stream::iter(&threads).map(async |t| meta.summary(t).await).buffer_unordered(16);
            let read: Vec<Result<Option<ThreadSummary>>> = reads.collect().await;
            let mut tldr = HashMap::new();
            for summary in read {
                if let Some(summary) = summary? {
                    tldr.insert(summary.thread_id.clone(), summary);
                }
            }
            Ok((done, tldr))
        };
        let spend = async {
            let kv = self.sessions.as_ref().map_err(|e| anyhow!("{e}"))?;
            crate::spend::on_threads(&self.js, kv, &threads).await.context("per thread")
        };
        let today = async {
            let kv = self.sessions.as_ref().map_err(|e| anyhow!("{e}"))?;
            crate::spend::today(&self.js, kv).await.context("of the day")
        };
        let reminders = async { self.meta.as_ref().map_err(|e| anyhow!("{e}"))?.reminders().await };
        let (titles, meta, spend, today, reminders) = futures::join!(titles, meta, spend, today, reminders);
        let mut done_acting = HashMap::new();
        for done in meta.iter().flat_map(|(done, _)| done.values()).filter(|d| d.on_behalf_of.is_some()) {
            let acting = self.behalf.check(&done.by, done.on_behalf_of.as_ref()).await;
            done_acting.insert(done.thread_id.clone(), acting);
        }
        Read { titles, meta, done_acting, spend, today, reminders }
    }
}

/// Reads [`Read`] every while, and as soon as a thread not read yet is listed.
pub struct Io {
    source: Source,
    /// The threads to read.
    threads: HashSet<ThreadId>,
    /// The threads the last read started with.
    read_threads: HashSet<ThreadId>,
    every: tokio::time::Interval,
    wanted: bool,
    read: Option<LocalBoxFuture<'static, Read>>,
}

impl Io {
    pub fn new(source: Source) -> Self {
        Self {
            source,
            threads: HashSet::new(),
            read_threads: HashSet::new(),
            every: tokio::time::interval(EVERY),
            wanted: false,
            read: None,
        }
    }

    /// Read `threads` from now on, at once if any was not read yet.
    pub fn threads<'a>(&mut self, threads: impl IntoIterator<Item = &'a ThreadId>) {
        let threads: HashSet<ThreadId> = threads.into_iter().cloned().collect();
        if threads != self.threads {
            self.wanted |= !threads.is_subset(&self.read_threads);
            self.threads = threads;
        }
    }

    /// Read again at once.
    pub fn refresh(&mut self) {
        self.wanted = true;
    }

    /// The next read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Read {
        loop {
            if self.wanted && self.read.is_none() {
                self.start();
            }
            tokio::select! {
                _ = self.every.tick() => self.wanted = true,
                read = maybe(&mut self.read) => {
                    self.read = None;
                    return read;
                }
            }
        }
    }

    fn start(&mut self) {
        self.wanted = false;
        self.read_threads.clone_from(&self.threads);
        self.read = Some(self.source.clone().read(self.threads.clone()).boxed_local());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::user;

    #[test]
    fn a_failed_read_keeps_what_was_read_and_tells_once() {
        let thread = ThreadId::generate();
        let mut cards = Cards::default();
        let spent = Spent { tokens: 10, cost: 0.25 };
        let ok = || Read {
            titles: Ok(HashMap::new()),
            meta: Ok((HashMap::new(), HashMap::new())),
            done_acting: HashMap::new(),
            spend: Ok([(thread.clone(), vec![(user("dev-a"), spent), (user("dev-b"), spent)])].into()),
            today: Ok(vec![(user("dev-a"), spent)]),
            reminders: Ok(Reminders::default()),
        };
        assert_eq!(cards.take(ok()), None);
        assert_eq!(cards.spent(&thread), Some(Spent { tokens: 20, cost: 0.5 }));
        assert_eq!(cards.spent_by(&thread, &user("dev-b")), Some(spent));
        assert_eq!(cards.today().map(|s| s.tokens), Some(10));
        let failing = || Read { spend: Err(anyhow!("no bucket")), ..ok() };
        assert_eq!(cards.take(failing()).as_deref(), Some("reading the spend: no bucket"));
        assert_eq!(cards.take(failing()), None, "told once");
        assert_eq!(cards.spent(&thread).map(|s| s.tokens), Some(20), "kept");
        assert_eq!(cards.take(ok()), None);
        assert!(cards.failed.is_empty());
    }
}
