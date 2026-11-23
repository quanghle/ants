//! What humans make of threads, in [`THREADS_BUCKET`]: a title, a TL;DR ([`ThreadSummary`]) and a done state per
//! thread, each shared by every human, and each human's pinned and archived threads ([`ThreadMarks`]) and its due
//! times, its reminders and the threads it snoozed on home ([`Reminders`]), shared by its devices. Read and written by
//! direct gets and plain publishes ([`DirectKv`]); the marks and due times, one entry each per human, with
//! compare-and-set, so devices marking at once lose nothing. The human's assistant sets marks and snoozes for it in
//! entries of its own ([`ants_proto::assisted_marks_key`], [`ants_proto::assisted_reminders_key`]): they count only
//! while the human's grant names it ([`crate::people::behalf::Checker`]), and only on threads the human has no mark or snooze
//! of its own on.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ants_client::entry::Entry;
use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_proto::{
    JsDomain, Mark, Reminders, STREAM, THREAD_UNDONE, THREADS_BUCKET, ThreadDone, ThreadId, ThreadMarks, ThreadSnoozes,
    ThreadSummary, ThreadTitle, UserId, assisted_marks_key, assisted_reminders_key, done_key, marks_key, reminders_key,
    summary_key, title_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::{Serialize, de::DeserializeOwned};

use crate::people::sealed::{self, Keyring, Sealer, Sealing};
use crate::threads::thread::{self, Pick};

/// Why titles and marks cannot be had when the hub has no bucket for them.
pub const MISSING: &str = "there is no bucket ants_threads: `ants init` as a human creates it";
/// How many times a change of the marks is tried again when another device changed them meanwhile.
const CONFLICTS: usize = 16;
/// How many titles are read at once.
const CONCURRENT: usize = 16;
/// How long a title or mark stays at least, even on a thread with no message in the stream: a message may reach the
/// stream that late, out of a leaf's outbox, and a title or mark may come before it.
pub const KEPT: Duration = ants_daemon::bus::RETENTION;
/// How long a pruning delete marker lasts: the shortest per-message TTL the stream takes, as no reader needs it.
const PRUNED_MARKER: Duration = Duration::from_secs(1);

/// The direct get subject of every key naming `human` second, which only `human`'s credentials may publish.
fn own_right(human: &UserId) -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.*.{human}")
}

/// The direct get subject of every title.
pub fn titles_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.title.*")
}

/// The titles, TL;DRs, done states and the marks of the CLI's human, unless its credentials are no human's: a request
/// they deny would only time out.
pub async fn open(nats: &crate::connection::Nats<'_>, js: &jetstream::Context) -> Result<Meta> {
    let me = &nats.me.id;
    if !nats.rights.may_publish(&own_right(me)) {
        bail!(
            "the credentials of {me} may not read thread titles, pins and archives: those of a human may, from \
             `ants auth user NAME --human`"
        );
    }
    let mut meta =
        Meta::open(js, nats.domain, me).await?.checking(Some(Arc::new(crate::people::behalf::Checker::new(nats, js))));
    meta.follows = true;
    meta.keyring = Keyring::lazy(nats.creds, ants_daemon::secrets::keystore::os(), js).map(Arc::new);
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
    nats: &crate::connection::Nats<'_>,
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
        let done = meta.done(thread).await?.filter(|d| d.holds(ants_client::entry::last_reopening(entries)));
        anyhow::Ok(About { title, mark, done })
    };
    read.await.unwrap_or_else(|e| {
        tracing::warn!("not showing the title, pinned, archived or done: {e:#}");
        About::default()
    })
}

/// `ants thread title THREAD [TEXT...]`: title the thread `pick` picks `text`, words joined by spaces, clear its
/// title with `clear`, or with neither print it.
pub async fn title(nats: &crate::connection::Nats<'_>, pick: Pick, text: &[String], clear: bool) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(nats, &js).await?;
    let meta = open(nats, &js).await?;
    let text = text.join(" ");
    if !clear && text.trim().is_empty() {
        match meta.title(thread).await? {
            Some(title) => println!("{}", title.title),
            None => println!("thread {thread} has no title"),
        }
        return Ok(());
    }
    println!("{}", retitle(&js, &meta, &Sealing::of(nats), thread, &text, clear).await?);
    Ok(())
}

/// Title `thread` `text` as the human of `sealing`, that of `meta`, or with `clear` clear its title; what `ants thread
/// title` prints. On a thread between two humans whose messages are sealed, the title is sealed for both
/// ([`title_sealer`]).
pub async fn retitle(
    js: &jetstream::Context,
    meta: &Meta,
    sealing: &Sealing,
    thread: &ThreadId,
    text: &str,
    clear: bool,
) -> Result<String> {
    let had = if clear {
        meta.clear_title(thread).await?
    } else {
        let mut title = ThreadTitle::new(thread.clone(), text, sealing.me().clone())?;
        if let Some(sealer) = title_sealer(sealing, js, thread).await? {
            sealer.seal_title(&mut title)?;
        }
        meta.put_title(&title).await?
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
async fn title_sealer(sealing: &Sealing, js: &jetstream::Context, thread: &ThreadId) -> Result<Option<Sealer>> {
    let Some(other) = sealed_with(js, sealing.me(), thread).await? else { return Ok(None) };
    sealing.sealer(js, &other, "the title of such a thread is never stored in the clear").await.map(Some)
}

/// The other human of `thread` if it is a thread between two humans whose messages are sealed, refused unless `me`
/// is one of them.
async fn sealed_with(js: &jetstream::Context, me: &UserId, thread: &ThreadId) -> Result<Option<UserId>> {
    let entries = thread::history(js, thread, None, thread::Activity::None, None).await?;
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
pub async fn mark(nats: &crate::connection::Nats<'_>, pick: Pick, marking: Marking) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(nats, &js).await?;
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
        .change_thread_marks(thread, |marks| match marking {
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
pub async fn done(nats: &crate::connection::Nats<'_>, pick: Pick, done: bool) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(nats, &js).await?;
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

/// The sequence of the last message, channel post or note stored on `thread`
/// ([`ants_client::entry::Kind::reopens`]), reading from the thread's start when its id is time-ordered, else from all
/// the stream keeps; 0 for none.
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
    /// What opens sealed titles; without one, they read `[sealed]`.
    keyring: Option<Arc<Keyring>>,
    /// What checks the marks and snoozes set by an assistant; without one, none counts.
    behalf: Option<Arc<crate::people::behalf::Checker>>,
    /// Whether `behalf` is its own, which it keeps fresh itself ([`crate::people::behalf::Checker::follow`]) once it read
    /// whether the human has an assistant, rather than one a feed keeps fresh.
    follows: bool,
    following: OnceLock<crate::people::behalf::Following>,
}

impl Meta {
    /// Those of `human`, through `js`, made for JetStream domain `js_domain`.
    pub async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>, human: &UserId) -> Result<Self> {
        let kv = DirectKv::new(js, js_domain, THREADS_BUCKET).await?;
        Ok(Self { kv, human: human.clone(), keyring: None, behalf: None, follows: false, following: OnceLock::new() })
    }

    /// Checking the marks and snoozes set by an assistant with `behalf`, if any, rather than the checker it had: one
    /// kept fresh by its giver.
    pub fn checking(self, behalf: Option<Arc<crate::people::behalf::Checker>>) -> Self {
        match behalf {
            Some(behalf) => Self { behalf: Some(behalf), follows: false, ..self },
            None => self,
        }
    }

    /// The assistant acting for the human now, whose marks and snoozes count, if any.
    async fn assistant(&self) -> Option<UserId> {
        let behalf = self.behalf.as_ref()?;
        if self.follows {
            self.following.get_or_init(|| behalf.follow());
        }
        behalf.assistant(&self.human).await
    }

    /// Opening sealed titles with `keyring`.
    pub fn opening(self, keyring: Option<Arc<Keyring>>) -> Self {
        Self { keyring, ..self }
    }

    /// The TL;DR of `thread`, if it has one. An entry naming another thread than its key is none.
    pub async fn summary(&self, thread: &ThreadId) -> Result<Option<ThreadSummary>> {
        match self.kv.lookup(summary_key(thread)).await.context("reading the TL;DR")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => Ok(None),
            Lookup::Value(value) => {
                Ok(serde_json::from_slice::<ThreadSummary>(&value).ok().filter(|t| t.thread_id == *thread))
            }
        }
    }

    /// Store `summary` as its thread's TL;DR for every human, in place of any it had.
    pub async fn put_summary(&self, summary: &ThreadSummary) -> Result<()> {
        let key = summary_key(&summary.thread_id);
        self.kv.put(key, serde_json::to_vec(summary)?).await.context("storing the TL;DR")?;
        Ok(())
    }

    /// The title of `thread`, if it has one, opened if sealed ([`sealed::read_title`]), as one line without control
    /// characters or those reordering text, which an entry put past `ants thread title` may hold. An entry naming
    /// another thread than its key is no title, nor one left empty.
    pub async fn title(&self, thread: &ThreadId) -> Result<Option<ThreadTitle>> {
        let title = match self.kv.lookup(title_key(thread)).await.context("reading the title")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => return Ok(None),
            Lookup::Value(value) => {
                serde_json::from_slice::<ThreadTitle>(&value).ok().filter(|t| t.thread_id == *thread)
            }
        };
        match title {
            Some(title) => {
                let mut title = sealed::read_title(title, self.keyring.as_deref()).await;
                title.title = ants_proto::one_line(&title.title, ants_proto::MAX_TITLE);
                Ok(Some(title).filter(|t| !t.title.is_empty()))
            }
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

    /// The done state of `thread`, if any, whether or not a message since reopened it ([`ThreadDone::holds`]). An entry
    /// naming another thread than its key is none.
    pub async fn done(&self, thread: &ThreadId) -> Result<Option<ThreadDone>> {
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
        if self.done(thread).await?.is_some_and(|had| had.holds(after)) {
            return Ok(false);
        }
        let done = ThreadDone::new(thread.clone(), self.human.clone(), after);
        ants_daemon::provider::events::done::mark(&self.kv, &done).await?;
        Ok(true)
    }

    /// Clear the done state of `thread`, leaving a marker for review packets for [`KEPT`]: a state set again
    /// meanwhile stays. The state it had, if any, whether or not it still held.
    pub async fn clear_done(&self, thread: &ThreadId) -> Result<Option<ThreadDone>> {
        let key = done_key(thread);
        let (found, last) = self.kv.lookup_last(&key).await.context("reading the done state")?;
        let value = match found {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => return Ok(None),
            Lookup::Value(value) => value,
        };
        let cleared = self
            .kv
            .delete_with_marker(&key, last, Some(KEPT), THREAD_UNDONE)
            .await
            .context("clearing the done state")?;
        if !cleared {
            return Ok(None);
        }
        Ok(serde_json::from_slice::<ThreadDone>(&value).ok().filter(|d| d.thread_id == *thread))
    }

    /// The entry under `key`, `what` it holds, and its stream sequence (0 for none).
    async fn entry_last<T: DeserializeOwned + Default>(&self, key: &str, what: &str) -> Result<(T, u64)> {
        match self.kv.lookup_last(key).await.with_context(|| format!("reading the {what}"))? {
            (Lookup::NoBucket, _) => bail!(MISSING),
            (Lookup::NoValue, last) => Ok((T::default(), last)),
            (Lookup::Value(value), last) => {
                let entry = serde_json::from_slice(&value).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "malformed {what}; starting over");
                    T::default()
                });
                Ok((entry, last))
            }
        }
    }

    /// Change the entry under `key`, holding `what`, with `change`, on top of whatever was changed meanwhile, storing it
    /// only if `change` says it changed it. What `change` said.
    async fn change_entry<T: Serialize + DeserializeOwned + Default>(
        &self,
        key: &str,
        what: &str,
        change: impl Fn(&mut T) -> bool,
    ) -> Result<bool> {
        for _ in 0..CONFLICTS {
            let (mut entry, last) = self.entry_last::<T>(key, what).await?;
            if !change(&mut entry) {
                return Ok(false);
            }
            let value = serde_json::to_vec(&entry)?;
            if self.kv.update(key, value, last).await.with_context(|| format!("storing the {what}"))? {
                return Ok(true);
            }
        }
        bail!("storing the {what}: they kept changing")
    }

    /// The human's marks, and the stream sequence of their entry (0 for none).
    async fn marks_last(&self) -> Result<(ThreadMarks, u64)> {
        self.entry_last(&marks_key(&self.human), MARKS).await
    }

    /// The human's pinned and archived threads, with those its assistant marked for it while it acts for it on the
    /// threads it marked none of itself ([`ThreadMarks::with_assisted`]).
    pub async fn marks(&self) -> Result<ThreadMarks> {
        let marks = self.marks_last().await?.0;
        let Some(assistant) = self.assistant().await else { return Ok(marks) };
        let assisted = self.entry_last(&assisted_marks_key(&self.human, &assistant), MARKS).await?.0;
        Ok(marks.with_assisted(&assisted))
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
            let prune = |marks: &mut ThreadMarks| {
                forgot.set(marks.prune(before, &gone, &active));
                forgot.get() > 0
            };
            self.change_marks(&prune).await?;
            marked = forgot.get();
            if let Some(assistant) = self.assistant().await {
                self.change_entry(&assisted_marks_key(&self.human, &assistant), MARKS, &prune).await?;
                marked += forgot.get();
            }
        }
        let mut shared = 0;
        if titles {
            shared += self.prune_titles(before, &gone).await?;
            shared += self.prune_dones(before, &gone).await?;
            shared += self.prune_summaries(before, &gone).await?;
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

    /// The human's due times: its reminders and the threads it snoozed on home, with those its assistant snoozed for it
    /// while it acts for it on the threads it has no snooze of its own in force on ([`ThreadSnoozes::with_assisted`]).
    pub async fn reminders(&self) -> Result<Reminders> {
        let mut reminders: Reminders = self.entry_last(&reminders_key(&self.human), REMINDERS).await?.0;
        let Some(assistant) = self.assistant().await else { return Ok(reminders) };
        let key = assisted_reminders_key(&self.human, &assistant);
        let assisted: Reminders = self.entry_last(&key, REMINDERS).await?.0;
        reminders.snoozed = std::mem::take(&mut reminders.snoozed).with_assisted(&assisted.snoozed, Utc::now());
        Ok(reminders)
    }

    /// Change the human's due times with `change`, on top of whatever its other devices or notifiers changed
    /// meanwhile, storing them only if `change` says it changed them. What `change` said.
    pub async fn change_reminders(&self, change: impl Fn(&mut Reminders) -> bool) -> Result<bool> {
        self.change_entry(&reminders_key(&self.human), REMINDERS, change).await
    }

    /// Change the human's snoozes with `change`, as [`Meta::change_reminders`] does.
    pub async fn change_snoozes(&self, change: impl Fn(&mut ThreadSnoozes) -> bool) -> Result<bool> {
        self.change_reminders(|r| change(&mut r.snoozed)).await
    }

    /// Change the human's snooze of `thread` with `change`, as [`Meta::change_snoozes`] does, ending any snooze its
    /// assistant set on it: what the human does with a thread takes over from what its assistant did. Whether either
    /// changed.
    pub async fn change_thread_snoozes(
        &self,
        thread: &ThreadId,
        change: impl Fn(&mut ThreadSnoozes) -> bool,
    ) -> Result<bool> {
        let changed = self.change_snoozes(change).await?;
        let Some(assistant) = self.assistant().await else { return Ok(changed) };
        let key = assisted_reminders_key(&self.human, &assistant);
        let woke = self.change_entry(&key, REMINDERS, |r: &mut Reminders| r.snoozed.wake(thread, Utc::now())).await?;
        Ok(changed || woke)
    }

    /// Change the human's marks with `change`, on top of whatever its other devices changed meanwhile, storing them
    /// only if `change` says it changed them. What `change` said.
    pub async fn change_marks(&self, change: impl Fn(&mut ThreadMarks) -> bool) -> Result<bool> {
        self.change_entry(&marks_key(&self.human), MARKS, change).await
    }

    /// Change the human's marks of `thread` with `change`, as [`Meta::change_marks`] does, dropping any pin or archive
    /// its assistant set on it: what the human does with a thread takes over from what its assistant did. Whether
    /// either changed.
    pub async fn change_thread_marks(
        &self,
        thread: &ThreadId,
        change: impl Fn(&mut ThreadMarks) -> bool,
    ) -> Result<bool> {
        let changed = self.change_marks(change).await?;
        let Some(assistant) = self.assistant().await else { return Ok(changed) };
        let key = assisted_marks_key(&self.human, &assistant);
        let dropped =
            self.change_entry(&key, MARKS, |m: &mut ThreadMarks| m.unpin(thread) | m.unarchive(thread)).await?;
        Ok(changed || dropped)
    }
}

/// What the marks and due times entries hold, as errors name them.
const MARKS: &str = "pinned and archived threads";
const REMINDERS: &str = "reminders and snoozed threads";

#[cfg(test)]
mod tests;
