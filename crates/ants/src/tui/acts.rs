//! What the thread view changes and reads of the open thread besides its messages: pinning, muting, archiving,
//! snoozing on home and marking it done for the human, as `ants thread pin|mute|archive|done` do, and reading every while what the side
//! panel shows of it: those marks, and what its ants spent.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{JsDomain, Reminder, SESSIONS_BUCKET, ThreadDone, ThreadId, ThreadMarks, UserId};
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;

use crate::live::maybe;
use crate::people::quiet;
use crate::spend;
use crate::spend::ThreadReport;
use crate::threads::thread_meta::{self, Meta};

/// How often the open thread's marks and spend are read.
const ABOUT_EVERY: Duration = Duration::from_secs(10);

/// A change to how the human holds a thread: on, or off again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Pin(bool),
    /// Notices of the thread held back for [`quiet::MUTE_FOR`].
    Mute(bool),
    Archive(bool),
    /// Done for every human, as of the last message, post or note.
    Done(bool),
    /// Off the human's home until then, or until an item for it arrives; None ends the snooze.
    Snooze(Option<DateTime<Utc>>),
    /// A reminder of the thread for the human, in place of any other; None clears it, done or cancelled.
    Remind(Option<Reminder>),
}

impl Change {
    /// What it did, after the thread's name: `pinned`, `no longer done`.
    pub fn done_line(&self, changed: bool) -> &'static str {
        match (self, changed) {
            (Self::Pin(true), true) => "pinned",
            (Self::Pin(true), false) => "already pinned",
            (Self::Pin(false), true) => "unpinned",
            (Self::Pin(false), false) => "not pinned",
            (Self::Mute(true), _) => "muted: no notice of it reaches this human's devices",
            (Self::Mute(false), true) => "unmuted",
            (Self::Mute(false), false) => "not muted",
            (Self::Archive(true), true) => "archived; its messages and its work stay as they are",
            (Self::Archive(true), false) => "already archived",
            (Self::Archive(false), true) => "unarchived",
            (Self::Archive(false), false) => "not archived",
            (Self::Done(true), true) => "done",
            (Self::Done(true), false) => "already done",
            (Self::Done(false), true) => "no longer done",
            (Self::Done(false), false) => "not done",
            (Self::Snooze(Some(_)), _) => "snoozed: off home until then, or until something for this human arrives",
            (Self::Snooze(None), true) => "no longer snoozed",
            (Self::Snooze(None), false) => "not snoozed",
            (Self::Remind(Some(_)), _) => "to remind of: on home and pushed once due",
            (Self::Remind(None), true) => "reminder cleared",
            (Self::Remind(None), false) => "no reminder to clear",
        }
    }
}

/// What the side panel shows of a thread besides its messages.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct About {
    pub marks: ThreadMarks,
    /// Until when its notices are held back.
    pub muted: Option<DateTime<Utc>>,
    pub done: Option<ThreadDone>,
    /// What each ant on it spent since a human's last message there; None when that could not be read.
    pub spend: Option<Vec<ThreadReport>>,
}

/// Something done or read.
pub enum Acted {
    About(ThreadId, Result<About>),
    Changed(ThreadId, Change, Result<bool>),
}

/// A change to make: the thread, the change, and the sequence of its last message, post or note.
type Pending = (ThreadId, Change, u64);
/// A change made, and whether it changed anything.
type Changing = (ThreadId, Change, Result<bool>);

pub struct Acts {
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    /// The human's titles, marks and done states; None when its credentials may not reach them, as said once.
    meta: Result<Rc<Meta>, String>,
    /// The thread whose marks and spend are read every while.
    watched: Option<ThreadId>,
    every: tokio::time::Interval,
    about: Option<LocalBoxFuture<'static, (ThreadId, Result<About>)>>,
    to_change: VecDeque<Pending>,
    changing: Option<LocalBoxFuture<'static, Changing>>,
}

impl Acts {
    pub fn new(js: jetstream::Context, domain: Option<JsDomain>, me: UserId, meta: Result<Meta>) -> Self {
        Self {
            js,
            domain,
            me,
            meta: meta.map(Rc::new).map_err(|e| format!("{e:#}")),
            watched: None,
            every: tokio::time::interval(ABOUT_EVERY),
            about: None,
            to_change: VecDeque::new(),
            changing: None,
        }
    }

    /// The next thing done or read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Acted {
        loop {
            if self.changing.is_none()
                && let Some((thread, change, last)) = self.to_change.pop_front()
            {
                self.changing = Some(self.start(thread, change, last));
            }
            tokio::select! {
                _ = self.every.tick() => {
                    if self.about.is_none() {
                        self.read_about();
                    }
                }
                (thread, about) = maybe(&mut self.about) => {
                    self.about = None;
                    if self.watched.as_ref() == Some(&thread) {
                        return Acted::About(thread, about);
                    }
                }
                (thread, change, changed) = maybe(&mut self.changing) => {
                    self.changing = None;
                    // What the panel shows changed: show it at once.
                    self.read_about();
                    return Acted::Changed(thread, change, changed);
                }
            }
        }
    }

    /// Read the marks and spend of `thread` now and every while, instead of those of the thread watched so far; with
    /// None, of none.
    pub fn watch(&mut self, thread: Option<ThreadId>) {
        if self.watched != thread {
            self.watched = thread;
            self.read_about();
        }
    }

    /// Make `change` to `thread`, whose last message, post or note has sequence `last`, after those asked for before.
    pub fn change(&mut self, thread: ThreadId, change: Change, last: u64) {
        self.to_change.push_back((thread, change, last));
    }

    /// Read everything again now.
    pub fn refresh(&mut self) {
        self.read_about();
    }

    fn read_about(&mut self) {
        let Some(thread) = self.watched.clone() else {
            self.about = None;
            return;
        };
        let (js, domain, me, meta) = (self.js.clone(), self.domain.clone(), self.me.clone(), self.meta.clone());
        self.about = Some(
            async move {
                let about = async {
                    let meta = meta.map_err(anyhow::Error::msg)?;
                    let (marks, done) = (meta.marks().await?, meta.done(&thread).await?);
                    let muted = quiet::prefs(&js, &me).await?.muted.get(&thread).copied();
                    let spend = async {
                        let kv = DirectKv::new(&js, domain.as_ref(), SESSIONS_BUCKET).await?;
                        spend::thread(&js, &kv, None, &thread, &BTreeMap::new()).await
                    };
                    let spend = spend.await.inspect_err(|e| tracing::debug!("reading the spend: {e:#}")).ok();
                    Ok(About { marks, muted, done, spend })
                };
                let about = about.await;
                (thread, about)
            }
            .boxed_local(),
        );
    }

    fn start(&self, thread: ThreadId, change: Change, last: u64) -> LocalBoxFuture<'static, Changing> {
        let (js, me, meta) = (self.js.clone(), self.me.clone(), self.meta.clone());
        async move {
            let changed = changing(&js, &me, meta, &thread, change.clone(), last).await;
            (thread, change, changed)
        }
        .boxed_local()
    }
}

/// Make `change` to `thread`: whether that changed anything.
async fn changing(
    js: &jetstream::Context,
    me: &UserId,
    meta: Result<Rc<Meta>, String>,
    thread: &ThreadId,
    change: Change,
    last: u64,
) -> Result<bool> {
    let meta = || meta.clone().map_err(anyhow::Error::msg);
    match change {
        Change::Mute(on) => {
            let was = std::cell::Cell::new(false);
            quiet::update(js, me, |prefs, now| {
                was.set(prefs.muted.get(thread).is_some_and(|until| now < *until));
                match quiet::until(now, quiet::MUTE_FOR) {
                    Ok(until) if on => _ = prefs.muted.insert(thread.clone(), until),
                    _ => _ = prefs.muted.remove(thread),
                }
            })
            .await?;
            Ok(on || was.get())
        }
        Change::Pin(on) => {
            meta()?.change_thread_marks(thread, |marks| if on { marks.pin(thread) } else { marks.unpin(thread) }).await
        }
        Change::Archive(true) => {
            let after = thread_meta::last_sequence(js).await?;
            meta()?.change_thread_marks(thread, |marks| marks.archive(thread, after)).await
        }
        // Keeping it too unarchives one archived for having been idle.
        Change::Archive(false) => {
            meta()?.change_thread_marks(thread, |marks| marks.unarchive(thread) | marks.keep(thread)).await
        }
        Change::Done(on) => thread_meta::mark_done(&*meta()?, thread, on, last).await,
        Change::Snooze(until) => {
            let now = Utc::now();
            meta()?
                .change_thread_snoozes(thread, |snoozes| match until {
                    Some(until) => {
                        snoozes.snooze(thread, now, until);
                        true
                    }
                    None => snoozes.wake(thread, now),
                })
                .await
        }
        Change::Remind(reminder) => {
            meta()?
                .change_reminders(|reminders| match &reminder {
                    Some(reminder) => {
                        reminders.remind(thread, reminder.clone());
                        true
                    }
                    None => reminders.clear(thread),
                })
                .await
        }
    }
}
