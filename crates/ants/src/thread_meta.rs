//! What humans make of threads, in [`THREADS_BUCKET`]: a title and a TL;DR ([`ThreadSummary`]) per thread, shared by
//! every human, and each human's pinned and archived threads ([`ThreadMarks`]), shared by its devices. Read and written by direct gets and plain
//! publishes ([`DirectKv`]); the marks, one entry per human, with compare-and-set, so devices marking at once lose
//! nothing.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_proto::{
    JsDomain, Mark, STREAM, THREADS_BUCKET, ThreadId, ThreadMarks, ThreadSummary, ThreadTitle, UserId, marks_key,
    summary_key, title_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::de::DeserializeOwned;

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

/// The direct get subject of `human`'s marks, which only `human`'s credentials may publish.
fn marks_right(human: &UserId) -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.{}", marks_key(human))
}

/// The direct get subject of every title.
pub fn titles_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.title.*")
}

/// The direct get subject of every TL;DR, which credentials older than permissions version 2 lack.
fn summaries_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.summary.*")
}

/// Whether the lack of [`summaries_right`] was warned of.
static NO_SUMMARIES: AtomicBool = AtomicBool::new(false);

/// The titles and the marks of the CLI's human, unless its credentials are no human's: a request they deny would only
/// time out. With credentials lacking [`summaries_right`], TL;DRs are neither read nor stored ([`Meta::summary`]).
pub async fn open(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Result<Meta> {
    let me = &nats.me.id;
    if !nats.rights.may_publish(&marks_right(me)) {
        bail!(
            "the credentials of {me} may not read thread titles, pins and archives: those of a human may, from \
             `ants auth user NAME --human`"
        );
    }
    let mut meta = Meta::open(js, nats.domain, me).await?;
    meta.summaries = nats.rights.may_publish(&summaries_right());
    Ok(meta)
}

/// The title of `thread`, whose last message read is `last`, and how the CLI's human marked it, idle for
/// `auto_archive` included; none on any error, warned of. A thread that never had a title gets the one an ant
/// `suggested`, if any ([`Meta::adopt_title`]).
pub async fn of_or_none(
    nats: &crate::Nats<'_>,
    js: &jetstream::Context,
    thread: &ThreadId,
    last: Option<&Entry>,
    suggested: Option<(&UserId, &str)>,
    auto_archive: Option<Duration>,
) -> (Option<ThreadTitle>, Option<Mark>) {
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
        let mark = match last {
            Some(e) => marks.mark_at(thread, e.seq, e.stored_at, thread::idle_before(auto_archive, Utc::now())),
            None => marks.mark(thread, 0),
        };
        anyhow::Ok((title, mark))
    };
    read.await.unwrap_or_else(|e| {
        tracing::warn!("not showing the title, pinned or archived: {e:#}");
        (None, None)
    })
}

/// `ants thread title THREAD [TEXT...]`: title the thread `pick` picks `text`, words joined by spaces, clear its
/// title with `clear`, or with neither print it.
pub async fn title(nats: &crate::Nats<'_>, pick: Pick, text: &[String], clear: bool) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    let text = text.join(" ");
    let had = if clear {
        meta.clear_title(thread).await?
    } else if text.trim().is_empty() {
        match meta.title(thread).await? {
            Some(title) => println!("{}", title.title),
            None => println!("thread {thread} has no title"),
        }
        return Ok(());
    } else {
        meta.set_title(thread, &text).await?
    };
    let was = had.map_or_default(|t| format!(" (was: {})", t.title));
    match clear {
        true if was.is_empty() => println!("thread {thread} has no title"),
        true => println!("cleared the title of thread {thread}{was}"),
        false => println!("titled thread {thread}: {}{was}", text.trim()),
    }
    Ok(())
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
    let auto_archive = match marking {
        Marking::Unarchive => crate::defaults::auto_archive()?,
        _ => None,
    };
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    let after = match marking {
        Marking::Archive => last_sequence(&js).await?,
        _ => 0,
    };
    let last_at = match auto_archive {
        Some(_) => thread::last_activity(&js, thread).await?,
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
        println!("thread {thread} unarchived; archived again automatically after {idle} without a message");
        return Ok(());
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
    println!("thread {thread} {line}");
    Ok(())
}

/// The sequence of the last message the stream stored: a thread archived now has new messages once it has one stored
/// after it.
pub async fn last_sequence(js: &jetstream::Context) -> Result<u64> {
    let stream = js.get_stream(STREAM).await.context("reading the ANTS stream")?;
    Ok(stream.cached_info().state.last_sequence)
}

/// The titles, the TL;DRs and the marks of one human.
pub struct Meta {
    kv: DirectKv,
    human: UserId,
    /// Whether the human's credentials may read and store TL;DRs.
    summaries: bool,
}

impl Meta {
    /// Those of `human`, through `js`, made for JetStream domain `js_domain`.
    pub async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>, human: &UserId) -> Result<Self> {
        Ok(Self { kv: DirectKv::new(js, js_domain, THREADS_BUCKET).await?, human: human.clone(), summaries: true })
    }

    /// Whether TL;DRs are read and stored: the human's credentials may. Warns once when not.
    fn keeps_summaries(&self) -> bool {
        if !self.summaries && !NO_SUMMARIES.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "the credentials of {} predate permissions version 2 and may not keep TL;DRs of threads: going on \
                 without them until renewed (`ants renew`)",
                self.human
            );
        }
        self.summaries
    }

    /// The TL;DR of `thread`, if it has one and TL;DRs are kept. An entry naming another thread than its key is none.
    pub async fn summary(&self, thread: &ThreadId) -> Result<Option<ThreadSummary>> {
        if !self.keeps_summaries() {
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

    /// Store `summary` as its thread's TL;DR for every human, in place of any it had, if TL;DRs are kept. Whether it
    /// did.
    pub async fn put_summary(&self, summary: &ThreadSummary) -> Result<bool> {
        if !self.keeps_summaries() {
            return Ok(false);
        }
        let key = summary_key(&summary.thread_id);
        self.kv.put(key, serde_json::to_vec(summary)?).await.context("storing the TL;DR")?;
        Ok(true)
    }

    /// The title of `thread`, if it has one. An entry naming another thread than its key is no title.
    pub async fn title(&self, thread: &ThreadId) -> Result<Option<ThreadTitle>> {
        match self.kv.lookup(title_key(thread)).await.context("reading the title")? {
            Lookup::NoBucket => bail!(MISSING),
            Lookup::NoValue => Ok(None),
            Lookup::Value(value) => {
                Ok(serde_json::from_slice::<ThreadTitle>(&value).ok().filter(|t| t.thread_id == *thread))
            }
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
    pub async fn set_title(&self, thread: &ThreadId, text: &str) -> Result<Option<ThreadTitle>> {
        let title = ThreadTitle::new(thread.clone(), text, self.human.clone())?;
        let had = self.title(thread).await?;
        self.kv.put(title_key(thread), serde_json::to_vec(&title)?).await.context("storing the title")?;
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
    /// meanwhile. The title stored, if any.
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
    /// the human's marks, which `marks` are as last read, and with `titles`, every title and TL;DR; and the keeps of
    /// threads `active` says had a message stored at or after the given time ([`ThreadMarks::prune`]). A mark, title
    /// or TL;DR set again meanwhile stays: marks change by compare-and-set and a title or TL;DR is deleted only if
    /// unchanged since read. How many marks, and titles and TL;DRs, it forgot.
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
        let mut titled = 0;
        if titles {
            titled += self.prune_set::<ThreadTitle>("title", before, &gone, |t| t.at).await?;
            if self.summaries {
                titled += self.prune_set::<ThreadSummary>("summary", before, &gone, |t| t.at).await?;
            }
        }
        Ok((marked, titled))
    }

    /// Delete the entries under `kind.THREAD` (titles or TL;DRs) set (`at`) before `before` of the threads `gone` says
    /// have no message left. How many it deleted.
    async fn prune_set<T: DeserializeOwned>(
        &self,
        kind: &str,
        before: DateTime<Utc>,
        gone: &impl Fn(&ThreadId) -> bool,
        at: impl Fn(&T) -> DateTime<Utc>,
    ) -> Result<usize> {
        let keys = self.kv.keys(&format!("{kind}.*")).await.with_context(|| format!("listing the {kind} entries"))?;
        let threads = keys.iter().filter_map(|key| key.strip_prefix(kind)?.strip_prefix('.')?.parse::<ThreadId>().ok());
        let deletes = futures::stream::iter(threads.filter(|t| gone(t)))
            .map(async |thread| {
                let key = format!("{kind}.{thread}");
                let (Lookup::Value(value), last) = self.kv.lookup_last(&key).await? else { return Ok(false) };
                match serde_json::from_slice::<T>(&value) {
                    Ok(set) if at(&set) < before => self.kv.delete_expecting(&key, last).await,
                    _ => Ok(false),
                }
            })
            .buffer_unordered(CONCURRENT);
        let deleted: Vec<Result<bool>> = deletes.collect().await;
        let mut count = 0;
        for deleted in deleted {
            count += usize::from(deleted.with_context(|| format!("deleting a {kind} entry"))?);
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
        js.create_key_value(ants_daemon::hub::threads_config()).await.unwrap();
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
    async fn marks_and_titles_of_threads_with_no_message_left_are_forgotten_once_old() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        js.create_key_value(ants_daemon::hub::threads_config()).await.unwrap();
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
        assert!(!meta.kv.delete_expecting(title_key(&renamed), revision).await.unwrap(), "a title set again stays");

        let start = crate::thread::window(Duration::from_hours(50 * 24));
        prune(&meta, &meta.marks().await.unwrap(), &start, &present(start).await).await;
        let marks = meta.marks().await.unwrap();
        let pinned: HashSet<&ThreadId> = marks.pinned.keys().collect();
        assert_eq!(pinned, HashSet::from([&alive, &fresh, &unordered]), "the old pin of a thread gone goes");
        assert!(marks.archived.is_empty(), "so does an old archive");
        assert!(marks.kept.is_empty(), "and keeps of threads gone or with a message since");
        assert_eq!(meta.title(&gone).await.unwrap(), None, "and an old title");
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
}
