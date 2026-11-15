//! What humans make of threads, in [`THREADS_BUCKET`]: a title, a TL;DR ([`ThreadSummary`]) and a done state per
//! thread, each shared by every human, and each human's pinned and archived threads ([`ThreadMarks`]), shared by its devices. Read and written by
//! direct gets and plain publishes ([`DirectKv`]); the marks, one entry per human, with compare-and-set, so devices
//! marking at once lose nothing.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Duration;

use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_proto::{
    JsDomain, Mark, STREAM, THREADS_BUCKET, ThreadDone, ThreadId, ThreadMarks, ThreadSummary, ThreadTitle, UserId,
    done_key, marks_key, summary_key, title_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;

use crate::sealed::{self, Keyring, Sealer};
use crate::thread::{self, Entry, Pick};

/// Why titles and marks cannot be had when the hub has no bucket for them.
pub const MISSING: &str = "there is no bucket ants_threads: `ants init` as a human creates it";
/// How many times a change of the marks is tried again when another device changed them meanwhile.
const CONFLICTS: usize = 16;
/// How many titles are read at once.
const CONCURRENT: usize = 16;
/// How long a title or mark stays at least, even on a thread with no message in the stream: a message may reach the
/// stream that late, out of a leaf's outbox, and a title or mark may come before it.
pub const KEPT: Duration = ants_daemon::bus::RETENTION;
/// How long the delete marker of a pruned title or a cleared done state lasts: the shortest per-message TTL the stream
/// takes, as nothing needs it.
const PRUNED_MARKER: Duration = Duration::from_secs(1);

/// The direct get subject of `human`'s marks, which only `human`'s credentials may publish.
fn marks_right(human: &UserId) -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.{}", marks_key(human))
}

/// The direct get subject of every title.
pub fn titles_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.title.*")
}

/// The direct get subject of every done state, which credentials issued before permissions version 2 lack.
fn done_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.done.*")
}

/// The direct get subject of every TL;DR, which credentials issued before permissions version 2 lack.
fn summaries_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.summary.*")
}

/// The titles, TL;DRs, done states and the marks of the CLI's human, unless its credentials are no human's: a request
/// they deny would only time out. Credentials that may not read done states or TL;DRs leave them out ([`Meta::done`],
/// [`Meta::summary`]).
pub async fn open(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Result<Meta> {
    let me = &nats.me.id;
    if !nats.rights.may_publish(&marks_right(me)) {
        bail!(
            "the credentials of {me} may not read thread titles, pins and archives: those of a human may, from \
             `ants auth user NAME --human`"
        );
    }
    let mut meta = Meta::open(js, nats.domain, me).await?;
    meta.keyring = Keyring::lazy(nats.creds, ants_daemon::keystore::os(), js).map(Arc::new);
    meta.done_right = nats.rights.may_publish(&done_right());
    meta.summaries_right = nats.rights.may_publish(&summaries_right());
    Ok(meta)
}

/// What humans made of a thread ([`of_or_none`]).
#[derive(Debug, Default)]
pub struct About {
    pub title: Option<ThreadTitle>,
    /// How the CLI's human marked it.
    pub mark: Option<Mark>,
    /// Its done state, while it holds.
    pub done: Option<ThreadDone>,
}

/// The title of `thread`, whose entries read are `entries`, how the CLI's human marked it, idle for `auto_archive`
/// included, and its done state while it holds; none on any error, warned of. A thread that never had a title gets
/// the one an ant `suggested`, if any ([`Meta::adopt_title`]).
pub async fn of_or_none(
    nats: &crate::Nats<'_>,
    js: &jetstream::Context,
    thread: &ThreadId,
    entries: &[Entry],
    suggested: Option<(&UserId, &str)>,
    auto_archive: Option<Duration>,
) -> About {
    let read = async {
        let meta = open(nats, js).await?;
        let mut title = meta.title(thread).await?;
        if let (None, Some((by, text))) = (&title, suggested) {
            title = meta.adopt_title(thread, text, by).await.unwrap_or_else(|e| {
                tracing::warn!("not storing the title {by} suggested: {e:#}");
                None
            });
        }
        let marks = meta.marks().await?;
        let mark = match entries.last() {
            Some(e) => marks.mark_at(thread, e.seq, e.stored_at, thread::idle_before(auto_archive, Utc::now())),
            None => marks.mark(thread, 0),
        };
        let done = meta.done(thread).await?.filter(|d| d.holds(thread::last_reopening(entries)));
        anyhow::Ok(About { title, mark, done })
    };
    read.await.unwrap_or_else(|e| {
        tracing::warn!("not showing the title, pinned, archived or done: {e:#}");
        About::default()
    })
}

/// `ants thread title THREAD [TEXT...]`: title the thread `pick` picks `text`, words joined by spaces, clear its
/// title with `clear`, or with neither print it.
pub async fn title(nats: &crate::Nats<'_>, pick: Pick, text: &[String], clear: bool) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    let text = text.join(" ");
    if !clear && text.trim().is_empty() {
        match meta.title(thread).await? {
            Some(title) => println!("{}", title.title),
            None => println!("thread {thread} has no title"),
        }
        return Ok(());
    }
    let had = if clear {
        meta.clear_title(thread).await?
    } else {
        let mut title = ThreadTitle::new(thread.clone(), &text, nats.me.id.clone())?;
        if let Some(sealer) = title_sealer(nats, &js, thread).await? {
            sealer.seal_title(&mut title)?;
        }
        meta.put_title(&title).await?
    };
    println!("{}", titled_line(thread, &text, clear, had));
    Ok(())
}

/// Title `thread` `text` as `me`, the human of `meta`, or with `clear` clear its title; what `ants thread title`
/// prints. Refused for a thread between two humans whose messages are sealed, whose title only `ants thread title`
/// seals.
pub async fn retitle(
    js: &jetstream::Context,
    meta: &Meta,
    me: &UserId,
    thread: &ThreadId,
    text: &str,
    clear: bool,
) -> Result<String> {
    let had = if clear {
        meta.clear_title(thread).await?
    } else {
        if sealed_with(js, me, thread).await?.is_some() {
            bail!("thread {thread} holds sealed messages: `ants thread title` seals its title");
        }
        meta.put_title(&ThreadTitle::new(thread.clone(), text, me.clone())?).await?
    };
    Ok(titled_line(thread, text, clear, had))
}

/// What titling `thread` `text`, or with `clear` clearing its title, prints, given the title it `had`.
fn titled_line(thread: &ThreadId, text: &str, clear: bool, had: Option<ThreadTitle>) -> String {
    let was = had.map_or_default(|t| format!(" (was: {})", t.title));
    match clear {
        true if was.is_empty() => format!("thread {thread} has no title"),
        true => format!("cleared the title of thread {thread}{was}"),
        false => format!("titled thread {thread}: {}{was}", text.trim()),
    }
}

/// What seals the title of `thread` if it is a thread between two humans whose messages are sealed: for the devices
/// of both, refused unless the CLI's human is one of them.
async fn title_sealer(nats: &crate::Nats<'_>, js: &jetstream::Context, thread: &ThreadId) -> Result<Option<Sealer>> {
    let Some(other) = sealed_with(js, &nats.me.id, thread).await? else { return Ok(None) };
    Sealer::new(nats, js, &other, "the title of such a thread is never stored in the clear").await.map(Some)
}

/// The other human of `thread` if it is a thread between two humans whose messages are sealed, refused unless `me`
/// is one of them.
async fn sealed_with(js: &jetstream::Context, me: &UserId, thread: &ThreadId) -> Result<Option<UserId>> {
    let entries = thread::history(js, thread, None, false, None).await?;
    let sealed = entries.into_iter().find(|e| e.sealed.is_some() && !e.forged);
    let Some((from, to)) = sealed.and_then(|e| Some((e.from?, e.to?))) else { return Ok(None) };
    match (me == &from, me == &to) {
        (true, _) => Ok(Some(to)),
        (_, true) => Ok(Some(from)),
        _ => bail!("thread {thread} holds sealed messages between {from} and {to}: only they title it"),
    }
}

/// What `ants thread pin`, `unpin`, `archive` and `unarchive` do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marking {
    Pin,
    Unpin,
    Archive,
    Unarchive,
}

/// `ants thread pin|unpin|archive|unarchive THREAD`: mark the thread `pick` picks so, for every device of the CLI's
/// human. Unarchiving a thread archived for having been idle keeps it ([`ThreadMarks::keep`]).
pub async fn mark(nats: &crate::Nats<'_>, pick: Pick, marking: Marking) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    println!("{}", marked(&js, &meta, thread, marking).await?);
    Ok(())
}

/// Mark `thread` so as the human of `meta`, through `js`; what `ants thread pin|unpin|archive|unarchive` prints.
pub async fn marked(js: &jetstream::Context, meta: &Meta, thread: &ThreadId, marking: Marking) -> Result<String> {
    let auto_archive = match marking {
        Marking::Unarchive => crate::defaults::auto_archive()?,
        _ => None,
    };
    let after = match marking {
        Marking::Archive => last_sequence(js).await?,
        _ => 0,
    };
    let last_at = match auto_archive {
        Some(_) => thread::last_activity(js, thread).await?,
        None => None,
    };
    let idle_before = thread::idle_before(auto_archive, Utc::now());
    let kept = Cell::new(false);
    let changed = meta
        .change_marks(|marks| match marking {
            Marking::Pin => marks.pin(thread),
            Marking::Unpin => marks.unpin(thread),
            Marking::Archive => marks.archive(thread, after),
            Marking::Unarchive => {
                let unarchived = marks.unarchive(thread);
                let idle = last_at.zip(idle_before).is_some_and(|(at, before)| marks.idle(thread, at, before));
                kept.set(idle && marks.keep(thread));
                unarchived || kept.get()
            }
        })
        .await?;
    if changed && kept.get() {
        let idle = ants_proto::duration::format(auto_archive.unwrap_or_default());
        return Ok(format!("thread {thread} unarchived; archived again automatically after {idle} without a message"));
    }
    let line = match (marking, changed) {
        (Marking::Pin, true) => "pinned",
        (Marking::Pin, false) => "already pinned",
        (Marking::Unpin, true) => "unpinned",
        (Marking::Unpin, false) => "not pinned",
        (Marking::Archive, true) => "archived; its messages and its work stay as they are",
        (Marking::Archive, false) => "already archived",
        (Marking::Unarchive, true) => "unarchived",
        (Marking::Unarchive, false) => "not archived",
    };
    Ok(format!("thread {thread} {line}"))
}

/// `ants thread done|undone THREAD`: mark the thread `pick` picks done, or not, for every human, as of the last
/// message, channel post or note stored on it.
pub async fn done(nats: &crate::Nats<'_>, pick: Pick, done: bool) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    println!("{}", marked_done(&js, &meta, thread, done).await?);
    Ok(())
}

/// Mark `thread` done, or not, as the human of `meta`, through `js`; what `ants thread done|undone` prints.
pub async fn marked_done(js: &jetstream::Context, meta: &Meta, thread: &ThreadId, done: bool) -> Result<String> {
    let last = last_reopening(js, thread).await?;
    let line = match (done, mark_done(meta, thread, done, last).await?) {
        (true, true) => "done",
        (true, false) => "already done",
        (false, true) => "no longer done",
        (false, false) => "not done",
    };
    Ok(format!("thread {thread} {line}"))
}

/// Mark `thread`, the last message, channel post or note stored on which has sequence `last`, done as the human of
/// `meta`, or with `done` false clear its done state. Whether that changed anything: marking a thread done while it is
/// does not, nor does clearing the state of a thread not done, or reopened by a message since.
pub async fn mark_done(meta: &Meta, thread: &ThreadId, done: bool, last: u64) -> Result<bool> {
    if done {
        meta.set_done(thread, last).await
    } else {
        Ok(meta.clear_done(thread).await?.is_some_and(|had| had.holds(last)))
    }
}

/// The sequence of the last message, channel post or note stored on `thread` ([`thread::Kind::reopens`]), reading from
/// the thread's start when its id is time-ordered, else from all the stream keeps; 0 for none.
pub async fn last_reopening(js: &jetstream::Context, thread: &ThreadId) -> Result<u64> {
    let mut last = 0;
    thread::scan(js, thread::show_start(thread, None), |entry| {
        if entry.thread_id == *thread && entry.kind.reopens() {
            last = last.max(entry.seq);
        }
    })
    .await?;
    Ok(last)
}

/// The sequence of the last message the stream stored: a thread archived now has new messages once it has one stored
/// after it.
pub async fn last_sequence(js: &jetstream::Context) -> Result<u64> {
    let stream = js.get_stream(STREAM).await.context("reading the ANTS stream")?;
    Ok(stream.cached_info().state.last_sequence)
}

/// The titles, the TL;DRs, the done states and the marks of one human.
pub struct Meta {
    kv: DirectKv,
    human: UserId,
    /// Whether its credentials may read and write done states.
    done_right: bool,
    /// Whether its credentials may read and write TL;DRs.
    summaries_right: bool,
    /// What opens sealed titles; without one, they read `[sealed]`.
    keyring: Option<Arc<Keyring>>,
}

impl Meta {
    /// Those of `human`, through `js`, made for JetStream domain `js_domain`.
    pub async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>, human: &UserId) -> Result<Self> {
        let kv = DirectKv::new(js, js_domain, THREADS_BUCKET).await?;
        Ok(Self { kv, human: human.clone(), done_right: true, summaries_right: true, keyring: None })
    }

    /// Opening sealed titles with `keyring`.
    pub fn opening(self, keyring: Option<Arc<Keyring>>) -> Self {
        Self { keyring, ..self }
    }

    /// Whether the credentials may read and write TL;DRs; warns once per process when they may not.
    fn may_summarise(&self) -> bool {
        static WARNED: Once = Once::new();
        if !self.summaries_right {
            WARNED.call_once(|| {
                tracing::warn!(
                    "the credentials of {} predate TL;DRs of threads (permissions version 2): going on without \
                     keeping them; `ants renew` renews them",
                    self.human
                );
            });
        }
        self.summaries_right
    }

    /// The TL;DR of `thread`, if it has one and the credentials may read it. An entry naming another thread than its
    /// key is none.
    pub async fn summary(&self, thread: &ThreadId) -> Result<Option<ThreadSummary>> {
        if !self.may_summarise() {
            return Ok(None);
        }
        match self.kv.lookup(summary_key(thread)).await.context("reading the TL;DR")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => Ok(None),
            Lookup::Value(value) => {
                Ok(serde_json::from_slice::<ThreadSummary>(&value).ok().filter(|t| t.thread_id == *thread))
            }
        }
    }

    /// Store `summary` as its thread's TL;DR for every human, in place of any it had, if the credentials may. Whether
    /// it did.
    pub async fn put_summary(&self, summary: &ThreadSummary) -> Result<bool> {
        if !self.may_summarise() {
            return Ok(false);
        }
        let key = summary_key(&summary.thread_id);
        self.kv.put(key, serde_json::to_vec(summary)?).await.context("storing the TL;DR")?;
        Ok(true)
    }

    /// The title of `thread`, if it has one, opened if sealed ([`sealed::read_title`]). An entry naming another
    /// thread than its key is no title.
    pub async fn title(&self, thread: &ThreadId) -> Result<Option<ThreadTitle>> {
        let title = match self.kv.lookup(title_key(thread)).await.context("reading the title")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => return Ok(None),
            Lookup::Value(value) => {
                serde_json::from_slice::<ThreadTitle>(&value).ok().filter(|t| t.thread_id == *thread)
            }
        };
        match title {
            Some(title) => Ok(Some(sealed::read_title(title, self.keyring.as_deref()).await)),
            None => Ok(None),
        }
    }

    /// The titles of those of `threads` that have one.
    pub async fn titles<'a>(
        &self,
        threads: impl IntoIterator<Item = &'a ThreadId>,
    ) -> Result<HashMap<ThreadId, ThreadTitle>> {
        let reads =
            futures::stream::iter(threads).map(async |thread| self.title(thread).await).buffer_unordered(CONCURRENT);
        let read: Vec<Result<Option<ThreadTitle>>> = reads.collect().await;
        let mut titles = HashMap::new();
        for title in read {
            if let Some(title) = title? {
                titles.insert(title.thread_id.clone(), title);
            }
        }
        Ok(titles)
    }

    /// Title `thread` `text` as the human, whatever title it had. The title it had, if any.
    #[cfg(test)]
    pub async fn set_title(&self, thread: &ThreadId, text: &str) -> Result<Option<ThreadTitle>> {
        self.put_title(&ThreadTitle::new(thread.clone(), text, self.human.clone())?).await
    }

    /// Store `title`, sealed or not, whatever title its thread had. The title it had, if any.
    pub async fn put_title(&self, title: &ThreadTitle) -> Result<Option<ThreadTitle>> {
        let had = self.title(&title.thread_id).await?;
        self.kv.put(title_key(&title.thread_id), serde_json::to_vec(title)?).await.context("storing the title")?;
        Ok(had)
    }

    /// Clear the title of `thread`. The title it had, if any.
    pub async fn clear_title(&self, thread: &ThreadId) -> Result<Option<ThreadTitle>> {
        let had = self.title(thread).await?;
        if had.is_some() {
            self.kv.delete(title_key(thread)).await.context("clearing the title")?;
        }
        Ok(had)
    }

    /// Title `thread` `text` as `by`, the ant that suggested it ([`ants_proto::Envelope::title`]), only if the thread
    /// never had a title: one a human set, renamed or cleared stays, compare-and-set keeping it from one set
    /// meanwhile. A cleared title's delete marker counts until [`Meta::prune`] drops it with its thread. The title
    /// stored, if any.
    pub async fn adopt_title(&self, thread: &ThreadId, text: &str, by: &UserId) -> Result<Option<ThreadTitle>> {
        match self.kv.lookup_last(title_key(thread)).await.context("reading the title")? {
            (Lookup::NoBucket, _) => bail!(MISSING),
            (Lookup::NoValue, 0) => {}
            _ => return Ok(None),
        }
        let title = ThreadTitle::new(thread.clone(), text, by.clone())?;
        let value = serde_json::to_vec(&title)?;
        let stored = self.kv.update(title_key(thread), value, 0).await.context("storing the suggested title")?;
        Ok(stored.then_some(title))
    }

    /// Whether the credentials may read and write done states; warns once per process when they may not.
    fn may_done(&self) -> bool {
        static WARNED: Once = Once::new();
        if !self.done_right {
            WARNED.call_once(|| {
                tracing::warn!(
                    "the credentials of {} predate done states (permissions version 2): not showing threads done; \
                     `ants renew` renews them",
                    self.human
                );
            });
        }
        self.done_right
    }

    /// The done state of `thread`, if any, whether or not a message since reopened it ([`ThreadDone::holds`]); none
    /// when the credentials may not read it. An entry naming another thread than its key is none.
    pub async fn done(&self, thread: &ThreadId) -> Result<Option<ThreadDone>> {
        if !self.may_done() {
            return Ok(None);
        }
        match self.kv.lookup(done_key(thread)).await.context("reading the done state")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => Ok(None),
            Lookup::Value(value) => {
                Ok(serde_json::from_slice::<ThreadDone>(&value).ok().filter(|d| d.thread_id == *thread))
            }
        }
    }

    /// The done states of those of `threads` that have one, as [`Meta::done`] reads them.
    pub async fn dones<'a>(
        &self,
        threads: impl IntoIterator<Item = &'a ThreadId>,
    ) -> Result<HashMap<ThreadId, ThreadDone>> {
        if !self.may_done() {
            return Ok(HashMap::new());
        }
        let reads =
            futures::stream::iter(threads).map(async |thread| self.done(thread).await).buffer_unordered(CONCURRENT);
        let read: Vec<Result<Option<ThreadDone>>> = reads.collect().await;
        let mut dones = HashMap::new();
        for done in read {
            if let Some(done) = done? {
                dones.insert(done.thread_id.clone(), done);
            }
        }
        Ok(dones)
    }

    /// Mark `thread` done as the human, as of `after`, the sequence of the last message, channel post or note stored
    /// on it the human saw. Whether that changed anything: a thread done as of `after` or later stays as it is.
    pub async fn set_done(&self, thread: &ThreadId, after: u64) -> Result<bool> {
        self.require_done()?;
        if self.done(thread).await?.is_some_and(|had| had.holds(after)) {
            return Ok(false);
        }
        let done = ThreadDone::new(thread.clone(), self.human.clone(), after);
        self.kv.put(done_key(thread), serde_json::to_vec(&done)?).await.context("storing the done state")?;
        Ok(true)
    }

    /// Clear the done state of `thread`, leaving a delete marker lasting [`PRUNED_MARKER`], as no reader needs it: a
    /// state set again meanwhile stays. The state it had, if any, whether or not it still held.
    pub async fn clear_done(&self, thread: &ThreadId) -> Result<Option<ThreadDone>> {
        self.require_done()?;
        let key = done_key(thread);
        let (found, last) = self.kv.lookup_last(&key).await.context("reading the done state")?;
        let value = match found {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => return Ok(None),
            Lookup::Value(value) => value,
        };
        self.kv.delete_expecting(&key, last, Some(PRUNED_MARKER)).await.context("clearing the done state")?;
        Ok(serde_json::from_slice::<ThreadDone>(&value).ok().filter(|d| d.thread_id == *thread))
    }

    fn require_done(&self) -> Result<()> {
        if !self.done_right {
            bail!(
                "the credentials of {} predate done states (permissions version 2): `ants renew` renews them",
                self.human
            );
        }
        Ok(())
    }

    /// The human's marks, and the stream sequence of their entry (0 for none).
    async fn marks_last(&self) -> Result<(ThreadMarks, u64)> {
        match self.kv.lookup_last(marks_key(&self.human)).await.context("reading the pinned and archived threads")? {
            (Lookup::NoBucket, _) => bail!(MISSING),
            (Lookup::NoValue, last) => Ok((ThreadMarks::default(), last)),
            (Lookup::Value(value), last) => {
                let marks = serde_json::from_slice(&value).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "malformed pinned and archived threads; starting over");
                    ThreadMarks::default()
                });
                Ok((marks, last))
            }
        }
    }

    /// The human's pinned and archived threads.
    pub async fn marks(&self) -> Result<ThreadMarks> {
        Ok(self.marks_last().await?.0)
    }

    /// Forget what was said of the threads `gone` says have no message left in the stream more than [`KEPT`] ago:
    /// the human's marks, which `marks` are as last read, and with `titles`, every title, TL;DR and done state; and
    /// the keeps of threads `active` says had a message stored at or after the given time ([`ThreadMarks::prune`]). A
    /// mark, title, TL;DR or done state set again meanwhile stays: marks change by compare-and-set and the others are
    /// deleted only if unchanged since read. How many marks, and titles, TL;DRs and done states, it forgot.
    pub async fn prune(
        &self,
        marks: &ThreadMarks,
        titles: bool,
        gone: impl Fn(&ThreadId) -> bool,
        active: impl Fn(&ThreadId, DateTime<Utc>) -> bool,
    ) -> Result<(usize, usize)> {
        let before = Utc::now() - KEPT;
        let mut marked = 0;
        if marks.clone().prune(before, &gone, &active) > 0 {
            let forgot = Cell::new(0);
            self.change_marks(|marks| {
                forgot.set(marks.prune(before, &gone, &active));
                forgot.get() > 0
            })
            .await?;
            marked = forgot.get();
        }
        let mut shared = 0;
        if titles {
            shared += self.prune_titles(before, &gone).await?;
            if self.done_right {
                shared += self.prune_dones(before, &gone).await?;
            }
            if self.summaries_right {
                shared += self.prune_summaries(before, &gone).await?;
            }
        }
        Ok((marked, shared))
    }

    async fn prune_titles(&self, before: DateTime<Utc>, gone: &impl Fn(&ThreadId) -> bool) -> Result<usize> {
        self.prune_shared("title", before, gone, |v| Some(serde_json::from_slice::<ThreadTitle>(v).ok()?.at)).await
    }

    async fn prune_dones(&self, before: DateTime<Utc>, gone: &impl Fn(&ThreadId) -> bool) -> Result<usize> {
        self.prune_shared("done", before, gone, |v| Some(serde_json::from_slice::<ThreadDone>(v).ok()?.at)).await
    }

    async fn prune_summaries(&self, before: DateTime<Utc>, gone: &impl Fn(&ThreadId) -> bool) -> Result<usize> {
        self.prune_shared("summary", before, gone, |v| Some(serde_json::from_slice::<ThreadSummary>(v).ok()?.at)).await
    }

    /// Drop, of the threads `gone` says have no message left, the entries under `prefix` (titles, TL;DRs or done
    /// states) set before `before`, as `at` reads the time an entry was set, and the delete markers of those cleared
    /// before then, each replaced by a delete marker lasting [`PRUNED_MARKER`] if unchanged since read: a cleared
    /// title's marker keeps an ant's suggestion from replacing it ([`Meta::adopt_title`]) only while its thread has
    /// messages. How many it dropped.
    async fn prune_shared(
        &self,
        prefix: &str,
        before: DateTime<Utc>,
        gone: &impl Fn(&ThreadId) -> bool,
        at: impl Fn(&[u8]) -> Option<DateTime<Utc>>,
    ) -> Result<usize> {
        let keys = self.kv.keys(&format!("{prefix}.*")).await.with_context(|| format!("listing the {prefix} keys"))?;
        let threads =
            keys.iter().filter_map(|key| key.strip_prefix(prefix)?.strip_prefix('.')?.parse::<ThreadId>().ok());
        let deletes = futures::stream::iter(threads.filter(|t| gone(t)))
            .map(async |thread| {
                let key = format!("{prefix}.{thread}");
                let set = match self.kv.lookup_stored(&key).await? {
                    (Lookup::Value(value), last, _) => at(&value).map(|at| (at, last)),
                    (Lookup::NoValue, last, Some(cleared)) => Some((cleared.into(), last)),
                    _ => None,
                };
                match set {
                    Some((at, last)) if at < before => self.kv.delete_expecting(&key, last, Some(PRUNED_MARKER)).await,
                    _ => Ok(false),
                }
            })
            .buffer_unordered(CONCURRENT);
        let deleted: Vec<Result<bool>> = deletes.collect().await;
        let mut count = 0;
        for deleted in deleted {
            count += usize::from(deleted.with_context(|| format!("deleting a {prefix} key"))?);
        }
        Ok(count)
    }

    /// Change the human's marks with `change`, on top of whatever its other devices changed meanwhile, storing them
    /// only if `change` says it changed them. What `change` said.
    pub async fn change_marks(&self, change: impl Fn(&mut ThreadMarks) -> bool) -> Result<bool> {
        for _ in 0..CONFLICTS {
            let (mut marks, last) = self.marks_last().await?;
            if !change(&mut marks) {
                return Ok(false);
            }
            let value = serde_json::to_vec(&marks)?;
            if self.kv.update(marks_key(&self.human), value, last).await.context("storing the marks")? {
                return Ok(true);
            }
        }
        bail!("storing the pinned and archived threads: they kept changing")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use ants_proto::{Envelope, Role, Uuid};
    use async_nats::jetstream::consumer::DeliverPolicy;

    use super::*;
    use crate::thread::{Select, Threads, prune, scan};

    /// A time-ordered thread id made `days` ago.
    fn made_ago(days: i64) -> ThreadId {
        let ms = (Utc::now() - chrono::Duration::days(days)).timestamp_millis() as u64;
        let random = Uuid::now_v7().as_u128() as u64 & 0xffff_ffff_ffff;
        format!("{:08x}-{:04x}-7000-8000-{random:012x}", ms >> 16, ms & 0xffff).parse().unwrap()
    }

    #[tokio::test]
    async fn an_archived_thread_comes_back_flagged_once_a_message_is_stored_on_it() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        let meta = Meta::open(&js, None, &alice).await.unwrap();
        let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it");
        let thread = &ask.thread_id;
        let store = async |envelope: &Envelope| {
            let payload = serde_json::to_vec(envelope).unwrap();
            js.publish(envelope.subject(), payload.into()).await.unwrap().await.unwrap();
        };
        store(&ask).await;
        let listed = async |select: &Select| {
            let mut threads = Threads::new(None);
            let start = async_nats::jetstream::consumer::DeliverPolicy::All;
            scan(&js, start, |e| threads.add(&e)).await.unwrap();
            threads.finish(select, &meta.marks().await.unwrap()).0
        };

        let after = last_sequence(&js).await.unwrap();
        assert!(meta.change_marks(|m| m.archive(thread, after)).await.unwrap());
        assert!(!meta.change_marks(|m| m.archive(thread, after)).await.unwrap(), "archived already");
        assert!(listed(&Select::default()).await.is_empty(), "hidden by default");
        let archived = listed(&Select { archived: true, ..Default::default() }).await;
        assert!(archived[0].archived && !archived[0].new_activity);

        store(&ask.reply(dev_a, "built", Role::Human).unwrap()).await;
        let back = listed(&Select::default()).await;
        assert_eq!(back.len(), 1);
        assert!(back[0].archived && back[0].new_activity, "still archived, but flagged");
        assert_eq!(js.get_stream(STREAM).await.unwrap().cached_info().state.messages, 2, "nothing deleted");

        let after = last_sequence(&js).await.unwrap();
        assert!(meta.change_marks(|m| m.archive(thread, after)).await.unwrap(), "archiving again moves on");
        assert!(listed(&Select::default()).await.is_empty());
        assert!(meta.change_marks(|m| m.pin(thread)).await.unwrap());
        let pinned = listed(&Select { pinned: true, ..Default::default() }).await;
        assert!(pinned[0].pinned && !pinned[0].archived, "pinning unarchives");

        assert!(meta.change_marks(|m| m.unpin(thread)).await.unwrap());
        let last_at = thread::last_activity(&js, thread).await.unwrap().unwrap();
        let idle = Select { idle_before: Some(last_at), ..Default::default() };
        assert!(listed(&idle).await.is_empty(), "idle since its last message");
        assert!(listed(&Select { archived: true, ..idle.clone() }).await[0].auto_archived);
        assert!(meta.change_marks(|m| m.keep(thread)).await.unwrap());
        assert!(!listed(&idle).await[0].archived, "kept");
        assert_eq!(thread::last_activity(&js, &ThreadId::generate()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_thread_done_for_every_human_reopens_once_a_message_is_stored_on_it() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, bob, dev_a) =
            ("alice".parse::<UserId>().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        let (_, b_js) = server.connect_as(&bob, Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        let (meta, bob_meta) =
            (Meta::open(&js, None, &alice).await.unwrap(), Meta::open(&b_js, None, &bob).await.unwrap());
        let ask = Envelope::new(alice.clone(), dev_a, "build it");
        let thread = &ask.thread_id;
        let store = async |envelope: &Envelope| {
            let payload = serde_json::to_vec(envelope).unwrap();
            js.publish(envelope.subject(), payload.into()).await.unwrap().await.unwrap();
        };
        store(&ask).await;

        let last = last_reopening(&js, thread).await.unwrap();
        assert!(last > 0);
        assert!(mark_done(&meta, thread, true, last).await.unwrap());
        assert!(!mark_done(&bob_meta, thread, true, last).await.unwrap(), "done already, for bob too");
        let done = bob_meta.done(thread).await.unwrap().unwrap();
        assert_eq!((&done.by, done.after), (&alice, last));
        assert!(meta.dones([thread, &ThreadId::generate()]).await.unwrap().contains_key(thread));

        store(&ask.reply(ask.to.clone(), "built", Role::Human).unwrap()).await;
        let later = last_reopening(&js, thread).await.unwrap();
        assert!(!meta.done(thread).await.unwrap().unwrap().holds(later), "a reply reopens it");
        assert!(!mark_done(&meta, thread, false, later).await.unwrap(), "reopened: nothing to clear");
        assert_eq!(meta.done(thread).await.unwrap(), None, "the stale state is gone though");
        assert!(mark_done(&bob_meta, thread, true, later).await.unwrap());
        assert!(mark_done(&meta, thread, false, later).await.unwrap(), "alice clears bob's");

        let mut old = Meta::open(&js, None, &alice).await.unwrap();
        old.done_right = false;
        assert!(mark_done(&bob_meta, thread, true, later).await.unwrap());
        assert_eq!(old.done(thread).await.unwrap(), None, "credentials without the right see no done state");
        assert!(old.dones([thread]).await.unwrap().is_empty());
        assert!(old.set_done(thread, later).await.is_err() && old.clear_done(thread).await.is_err());
    }

    #[tokio::test]
    async fn marks_and_titles_of_threads_with_no_message_left_are_forgotten_once_old() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        let meta = Meta::open(&js, None, &alice).await.unwrap();
        let (gone, alive, fresh, renamed) = (made_ago(40), made_ago(40), made_ago(40), made_ago(40));
        let unordered: ThreadId = "deploy-1".parse().unwrap();
        let ask = Envelope { thread_id: alive.clone(), ..Envelope::new(alice.clone(), dev_a, "still here") };
        js.publish(ask.subject(), serde_json::to_vec(&ask).unwrap().into()).await.unwrap().await.unwrap();
        let long_ago = Utc::now() - KEPT - chrono::Duration::days(1);
        let old_title = |thread: &ThreadId| {
            let title = ThreadTitle { at: long_ago, ..ThreadTitle::new(thread.clone(), "old", alice.clone()).unwrap() };
            serde_json::to_vec(&title).unwrap()
        };
        for thread in [&gone, &alive, &renamed, &unordered] {
            meta.kv.put(title_key(thread), old_title(thread)).await.unwrap();
        }
        for thread in [&gone, &alive] {
            let done = ThreadDone { at: long_ago, ..ThreadDone::new(thread.clone(), alice.clone(), 0) };
            meta.kv.put(done_key(thread), serde_json::to_vec(&done).unwrap()).await.unwrap();
        }
        meta.set_title(&fresh, "new").await.unwrap();
        meta.change_marks(|m| {
            for thread in [&gone, &alive, &unordered] {
                m.pinned.insert(thread.clone(), long_ago);
            }
            m.archived.insert(renamed.clone(), ants_proto::Archived { at: long_ago, after: 0 });
            m.kept.insert(alive.clone(), Utc::now() - chrono::Duration::days(1));
            m.kept.insert(fresh.clone(), Utc::now());
            m.kept.insert(gone.clone(), long_ago);
            m.pin(&fresh)
        })
        .await
        .unwrap();
        let present = async |start: DeliverPolicy| {
            let mut threads = Threads::new(None);
            scan(&js, start, |e| threads.add(&e)).await.unwrap();
            threads.summaries().map(|t| (t.thread_id.clone(), t.last_at)).collect::<HashMap<_, _>>()
        };
        let (last, revision) = meta.kv.lookup_last(title_key(&renamed)).await.unwrap();
        assert!(matches!(last, Lookup::Value(_)));
        meta.set_title(&renamed, "renamed meanwhile").await.unwrap();
        let deleted = meta.kv.delete_expecting(title_key(&renamed), revision, None).await.unwrap();
        assert!(!deleted, "a title set again stays");

        let start = crate::thread::window(Duration::from_hours(50 * 24));
        prune(&meta, &meta.marks().await.unwrap(), &start, &present(start).await).await;
        let marks = meta.marks().await.unwrap();
        let pinned: HashSet<&ThreadId> = marks.pinned.keys().collect();
        assert_eq!(pinned, HashSet::from([&alive, &fresh, &unordered]), "the old pin of a thread gone goes");
        assert!(marks.archived.is_empty(), "so does an old archive");
        assert!(marks.kept.is_empty(), "and keeps of threads gone or with a message since");
        assert_eq!(meta.title(&gone).await.unwrap(), None, "and an old title");
        assert_eq!(meta.done(&gone).await.unwrap(), None, "and an old done state");
        assert!(meta.done(&alive).await.unwrap().is_some(), "a thread with a message keeps its done state");
        assert_eq!(meta.title(&alive).await.unwrap().unwrap().title, "old", "a thread with a message keeps its title");
        assert_eq!(meta.title(&fresh).await.unwrap().unwrap().title, "new", "a recent title stays");
        assert_eq!(meta.title(&renamed).await.unwrap().unwrap().title, "renamed meanwhile");
        assert!(meta.title(&unordered).await.unwrap().is_some(), "a read from a time leaves an unordered id be");

        let start = crate::thread::window(Duration::from_hours(24));
        prune(&meta, &meta.marks().await.unwrap(), &start, &present(start).await).await;
        assert!(meta.marks().await.unwrap().pinned.contains_key(&unordered), "nor does a read of one day");

        prune(&meta, &meta.marks().await.unwrap(), &DeliverPolicy::All, &present(DeliverPolicy::All).await).await;
        assert_eq!(meta.title(&unordered).await.unwrap(), None, "a read of everything kept does");
        let pinned: Vec<ThreadId> = meta.marks().await.unwrap().pinned.into_keys().collect();
        assert_eq!(HashSet::from_iter(pinned), HashSet::from([alive, fresh]));
    }

    /// A cleared title's delete marker keeps out an ant's suggestion while its thread has messages, and goes with the
    /// thread, as a pruned title's does, leaving no key behind.
    #[tokio::test]
    async fn the_markers_of_cleared_and_pruned_titles_go_with_their_threads() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
        let meta = Meta::open(&js, None, &alice).await.unwrap();
        let (cleared, titled, alive) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        for thread in [&cleared, &titled, &alive] {
            meta.set_title(thread, "Release 1.2").await.unwrap();
        }
        meta.clear_title(&cleared).await.unwrap();
        meta.clear_title(&alive).await.unwrap();
        assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None, "the clear wins");

        let gone = |thread: &ThreadId| *thread != alive;
        assert_eq!(meta.prune_titles(Utc::now() - KEPT, &gone).await.unwrap(), 0, "recent ones stay");
        let later = Utc::now() + chrono::Duration::seconds(1);
        assert_eq!(meta.prune_titles(later, &gone).await.unwrap(), 2);
        assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None, "a marker for now");
        let left = [title_key(&alive)];
        for _ in 0..100 {
            if meta.kv.keys("title.*").await.unwrap() == left {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(meta.kv.keys("title.*").await.unwrap(), left, "the markers of threads gone expire");
        assert!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap().is_some());
        assert_eq!(
            meta.adopt_title(&alive, "Fix the build", &dev_a).await.unwrap(),
            None,
            "a live thread's clear wins"
        );
    }

    /// The title of a thread between two humans whose messages are sealed is sealed for both: each opens it, a
    /// third human reads `[sealed]` and may not title it, and the thread keeps no ant's suggestion.
    #[tokio::test]
    async fn the_title_of_a_sealed_thread_is_sealed_for_both_humans() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let humans: Vec<UserId> = ["alice", "bob", "carol"].iter().map(|h| h.parse().unwrap()).collect();
        let creds: Vec<_> = humans.iter().map(|h| server.creds_as(h, Role::Human)).collect();
        let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let mut js = Vec::new();
        for n in &nats {
            js.push(n.connect("test").await.unwrap().1);
        }
        let _ = js[0].create_stream(ants_daemon::hub::threads_config()).await;
        let mut ask = Envelope::new(humans[0].clone(), humans[1].clone(), "lunch?");
        Sealer::new(&nats[0], &js[0], &humans[1], "").await.unwrap().seal(&mut ask, &mut []).unwrap();
        ants_daemon::bus::publish(&js[0], &ask).await.unwrap();
        let thread = &ask.thread_id;
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, ant_js) = server.connect_as(&dev_a, Role::Ant).await;
        let mut noted = Envelope::on_thread(thread.clone(), dev_a, humans[0].clone(), "noted");
        noted.title = Some("Lunch order".to_owned());
        ants_daemon::bus::publish(&ant_js, &noted).await.unwrap();

        let sealer = title_sealer(&nats[0], &js[0], thread).await.unwrap().expect("a sealed thread");
        let mut title = ThreadTitle::new(thread.clone(), "Team lunch", humans[0].clone()).unwrap();
        sealer.seal_title(&mut title).unwrap();
        assert_eq!(open(&nats[0], &js[0]).await.unwrap().put_title(&title).await.unwrap(), None);
        let meta = open(&nats[0], &js[0]).await.unwrap();
        let Lookup::Value(stored) = meta.kv.lookup(title_key(thread)).await.unwrap() else { panic!("no title") };
        assert!(!String::from_utf8_lossy(&stored).contains("lunch"), "stored sealed");
        for i in 0..2 {
            let title = open(&nats[i], &js[i]).await.unwrap().title(thread).await.unwrap().unwrap();
            assert_eq!((title.title.as_str(), &title.by, title.sealed), ("Team lunch", &humans[0], None));
        }
        let unopened = |meta: Meta| async move { meta.title(thread).await.unwrap().unwrap().title };
        assert_eq!(unopened(open(&nats[2], &js[2]).await.unwrap()).await, "[sealed]");
        assert_eq!(unopened(Meta::open(&js[1], None, &humans[1]).await.unwrap()).await, "[sealed]");
        let refused = title_sealer(&nats[2], &js[2], thread).await.err().unwrap().to_string();
        assert!(refused.contains("only they title it"), "{refused}");
        assert!(title_sealer(&nats[0], &js[0], &ThreadId::generate()).await.unwrap().is_none());

        let mut threads = Threads::new(None);
        scan(&js[0], async_nats::jetstream::consumer::DeliverPolicy::All, |e| threads.add(&e)).await.unwrap();
        let summary = threads.summaries().find(|t| t.thread_id == *thread).unwrap();
        assert!(summary.sealed && summary.suggested.is_some(), "an ant's suggestion stays unstored");
    }
}
