//! The threads' shared done states, read from ants_threads every while and as soon as a thread not read yet is listed,
//! as `ants chat` reads them.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use ants_proto::{ThreadDone, ThreadId};
use anyhow::{Result, anyhow};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;

use crate::live::maybe;
use crate::thread_meta::Meta;

/// How often the done states are read.
const EVERY: Duration = Duration::from_secs(10);

pub struct Dones {
    /// Why there is none, when ants_threads cannot be read.
    meta: Result<Rc<Meta>, String>,
    /// The threads to read.
    threads: HashSet<ThreadId>,
    /// The threads the last read started with.
    read_threads: HashSet<ThreadId>,
    every: tokio::time::Interval,
    wanted: bool,
    read: Option<LocalBoxFuture<'static, Result<HashMap<ThreadId, ThreadDone>>>>,
}

impl Dones {
    pub fn new(meta: Result<Meta, String>) -> Self {
        Self {
            meta: meta.map(Rc::new),
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
    pub async fn next(&mut self) -> Result<HashMap<ThreadId, ThreadDone>> {
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
        let (meta, threads) = (self.meta.clone(), self.threads.clone());
        self.read = Some(
            async move {
                let meta = meta.map_err(|e| anyhow!("{e}"))?;
                meta.dones(&threads).await
            }
            .boxed_local(),
        );
    }
}
