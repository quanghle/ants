//! What humans make of threads, in [`THREADS_BUCKET`]: a title per thread, shared by every human, and each human's
//! pinned and archived threads ([`ThreadMarks`]), shared by its devices. Read and written by direct gets and plain
//! publishes ([`DirectKv`]); the marks, one entry per human, with compare-and-set, so devices marking at once lose
//! nothing.

use std::collections::HashMap;

use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_proto::{
    JsDomain, Mark, STREAM, THREADS_BUCKET, ThreadId, ThreadMarks, ThreadTitle, UserId, marks_key, title_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use futures::StreamExt as _;

use crate::thread::{self, Entry, Pick};

/// Why titles and marks cannot be had when the hub has no bucket for them.
pub const MISSING: &str = "there is no bucket ants_threads (a hub set up by an older ants): `ants init` as a human \
                           creates it";
/// How many times a change of the marks is tried again when another device changed them meanwhile.
const CONFLICTS: usize = 16;
/// How many titles are read at once.
const CONCURRENT: usize = 16;

/// The direct get subject of `human`'s marks: credentials that may not publish it predate titles, pins and archives.
fn marks_right(human: &UserId) -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.{}", marks_key(human))
}

/// The direct get subject of every title.
pub fn titles_right() -> String {
    format!("$JS.API.DIRECT.GET.KV_{THREADS_BUCKET}.$KV.{THREADS_BUCKET}.title.*")
}

/// The titles and the marks of the CLI's human, unless its credentials predate them: a request they deny would only
/// time out.
pub async fn open(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Result<Meta> {
    let me = &nats.me.id;
    if !nats.rights.may_publish(&marks_right(me)) {
        bail!(
            "the credentials of {me} predate thread titles, pins and archives: renew them with `ants renew FILE`, or \
             issue new ones with `ants auth user {me} --human`"
        );
    }
    Meta::open(js, nats.domain, me).await
}

/// The title of `thread`, whose last message read is `last`, and how the CLI's human marked it; none on any error,
/// warned of.
pub async fn of_or_none(
    nats: &crate::Nats<'_>,
    js: &jetstream::Context,
    thread: &ThreadId,
    last: Option<&Entry>,
) -> (Option<ThreadTitle>, Option<Mark>) {
    let read = async {
        let meta = open(nats, js).await?;
        let title = meta.title(thread).await?;
        let mark = meta.marks().await?.mark(thread, last.map_or(0, |e| e.seq));
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
    let was = had.map(|t| format!(" (was: {})", t.title)).unwrap_or_default();
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
/// human.
pub async fn mark(nats: &crate::Nats<'_>, pick: Pick, marking: Marking) -> Result<()> {
    let js = thread::connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let meta = open(nats, &js).await?;
    let after = match marking {
        Marking::Archive => last_sequence(&js).await?,
        _ => 0,
    };
    let changed = meta
        .change_marks(|marks| match marking {
            Marking::Pin => marks.pin(thread),
            Marking::Unpin => marks.unpin(thread),
            Marking::Archive => marks.archive(thread, after),
            Marking::Unarchive => marks.unarchive(thread),
        })
        .await?;
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

/// The titles and the marks of one human.
pub struct Meta {
    kv: DirectKv,
    human: UserId,
}

impl Meta {
    /// Those of `human`, through `js`, made for JetStream domain `js_domain`.
    pub async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>, human: &UserId) -> Result<Self> {
        Ok(Self { kv: DirectKv::new(js, js_domain, THREADS_BUCKET).await?, human: human.clone() })
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
    use ants_proto::{Envelope, Role};

    use super::*;
    use crate::thread::{Select, Threads, scan};

    #[tokio::test]
    async fn an_archived_thread_comes_back_flagged_once_a_message_is_stored_on_it() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        ants_daemon::bus::init_stream(&js).await.unwrap();
        ants_daemon::hub::create_bucket(&js, ants_daemon::hub::threads_config()).await.unwrap();
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
    }
}
