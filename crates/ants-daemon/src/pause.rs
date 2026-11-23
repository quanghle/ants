//! Pausing an ant: `paused = true` in its `[[ant]]` table, which an admin's pause order sets ([`crate::admin`]). The
//! daemon serving a paused ant stops pulling from its consumers (`pausable`), so the messages to it wait queued in
//! the stream, neither drained nor handed back, and starts no new prompt; the prompts running finish. Pausing or
//! resuming changes the running ant in place ([`Pause::replace`]), restarting no harness.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::{Paused, UserId};
use anyhow::{Result, anyhow};
use chrono::Utc;
use futures::{FutureExt as _, Stream, StreamExt as _};
use tokio::sync::watch;

use crate::sync::MutexExt as _;

/// How long after failing to open the consumers [`pausable`] tries again.
const REOPEN_DELAY: Duration = Duration::from_secs(5);

/// Whether an ant is paused, and since when and by whom; a clone shares it, so the running ant sees it change. Equal to
/// any other, so that a pause or resume alone does not restart the ant.
#[derive(Debug, Clone)]
pub struct Pause(Arc<watch::Sender<Option<Paused>>>);

impl Pause {
    /// Paused from now, by no admin known, or not paused.
    pub fn new(paused: bool) -> Self {
        Self(Arc::new(watch::Sender::new(paused.then(|| Paused { since: Utc::now(), by: None }))))
    }

    pub fn get(&self) -> Option<Paused> {
        self.0.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<Paused>> {
        self.0.subscribe()
    }

    /// Credit `by`, the admin whose order paused the ant, when paused and no one is credited yet.
    pub fn credit(&self, by: Option<UserId>) {
        let Some(by) = by else { return };
        self.0.send_if_modified(|paused| match paused {
            Some(paused) if paused.by.is_none() => {
                paused.by = Some(by);
                true
            }
            _ => false,
        });
    }

    /// Take the state of `listed`, the ant's config as listed anew: paused or not, a pause already on keeping when
    /// and by whom it started.
    pub fn replace(&self, listed: &Self) {
        let listed = listed.get();
        self.0.send_if_modified(|paused| {
            if paused.is_some() == listed.is_some() {
                return false;
            }
            *paused = listed;
            true
        });
    }

    /// Keep when and by whom `serving`, the config the ant was served with, was paused, when this one is paused too.
    pub fn carry(&self, serving: &Self) {
        if let (Some(_), Some(serving)) = (self.get(), serving.get()) {
            self.0.send_replace(Some(serving));
        }
    }
}

impl PartialEq for Pause {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// The admins whose pause orders a daemon applied, by ant, until the config reloaded names the ant paused
/// ([`Pause::credit`]).
#[derive(Debug, Clone, Default)]
pub struct Pausers(Arc<Mutex<HashMap<UserId, UserId>>>);

impl Pausers {
    pub fn paused(&self, ant: &UserId, by: &UserId) {
        self.0.locked().insert(ant.clone(), by.clone());
    }

    /// The admin who paused `ant` last, forgotten from then on.
    pub fn take(&self, ant: &UserId) -> Option<UserId> {
        self.0.locked().remove(ant)
    }
}

/// The items of the stream `open` opens, while `paused` holds none: `current` first, if open already. On a pause the
/// items already delivered still come, then the stream is dropped, so the consumers it pulls from deliver no more; on
/// resuming, `open` opens it again. A stream that fails to open is tried again after [`REOPEN_DELAY`].
pub(crate) fn pausable<S, T, E>(
    paused: watch::Receiver<Option<Paused>>,
    current: Option<S>,
    open: impl AsyncFnMut() -> Result<S>,
) -> impl Stream<Item = Result<T, anyhow::Error>>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    E: Display,
{
    let item = |item: Result<T, E>| item.map_err(|e| anyhow!("{e}"));
    futures::stream::unfold((paused, current, open), move |(mut paused, mut current, mut open)| async move {
        loop {
            if paused.borrow_and_update().is_some() {
                if let Some(stream) = current.as_mut() {
                    match stream.next().now_or_never() {
                        Some(Some(next)) => return Some((item(next), (paused, current, open))),
                        Some(None) => return None,
                        None => {
                            tracing::info!("paused: taking no more messages");
                            current = None;
                        }
                    }
                }
                changed(&mut paused).await;
                continue;
            }
            let stream = match current.as_mut() {
                Some(stream) => stream,
                None => match open().await {
                    Ok(stream) => current.insert(stream),
                    Err(e) => {
                        tracing::warn!(error = format!("{e:#}"), retry_in = ?REOPEN_DELAY, "opening the consumers");
                        tokio::select! {
                            () = tokio::time::sleep(REOPEN_DELAY) => {}
                            () = changed(&mut paused) => {}
                        }
                        continue;
                    }
                },
            };
            let next = tokio::select! {
                next = stream.next() => Some(next),
                () = changed(&mut paused) => None,
            };
            match next {
                Some(Some(next)) => return Some((item(next), (paused, current, open))),
                Some(None) => return None,
                None => {}
            }
        }
    })
}

/// Until `paused` changes; never once its sender is gone.
async fn changed(paused: &mut watch::Receiver<Option<Paused>>) {
    if paused.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use futures::channel::mpsc;

    use super::*;
    use ants_test_support::user as p;

    #[test]
    fn a_pause_keeps_its_start_and_its_admin_until_resumed() {
        let serving = Pause::new(false);
        assert_eq!(serving.get(), None);
        let listed = Pause::new(true);
        listed.credit(Some(p("alice")));
        serving.replace(&listed);
        let paused = serving.get().unwrap();
        assert_eq!(paused.by, Some(p("alice")));
        let again = Pause::new(true);
        again.credit(Some(p("bob")));
        serving.replace(&again);
        assert_eq!(serving.get(), Some(paused.clone()), "already paused");
        let restarted = Pause::new(true);
        restarted.carry(&serving);
        assert_eq!(restarted.get(), Some(paused));
        serving.replace(&Pause::new(false));
        assert_eq!(serving.get(), None);
        let resumed = Pause::new(false);
        resumed.carry(&restarted);
        assert_eq!(resumed.get(), None);
        assert_eq!(Pause::new(true), Pause::new(false), "a pause alone restarts nothing");

        let pausers = Pausers::default();
        pausers.paused(&p("dev-a"), &p("alice"));
        assert_eq!(pausers.take(&p("dev-b")), None);
        assert_eq!(pausers.take(&p("dev-a")), Some(p("alice")));
        assert_eq!(pausers.take(&p("dev-a")), None);
    }

    /// The next item within a moment, if any.
    async fn next<S: Stream + Unpin>(items: &mut S) -> Option<S::Item> {
        tokio::time::timeout(Duration::from_millis(100), items.next()).await.ok().flatten()
    }

    #[tokio::test]
    async fn a_paused_stream_drops_its_source_after_what_it_delivered_and_reopens_on_resuming() {
        let pause = Pause::new(true);
        let opened = Cell::new(0);
        let (tx, rx) = mpsc::unbounded::<Result<u32, String>>();
        let (tx_again, rx_again) = mpsc::unbounded();
        let mut sources = vec![rx_again, rx];
        let mut items = std::pin::pin!(pausable(pause.subscribe(), None, async || {
            opened.set(opened.get() + 1);
            sources.pop().ok_or_else(|| anyhow!("no more"))
        }));
        assert!(next(&mut items).await.is_none());
        assert_eq!(opened.get(), 0, "not opened while paused");

        pause.replace(&Pause::new(false));
        tx.unbounded_send(Ok(1)).unwrap();
        assert_eq!(next(&mut items).await.unwrap().unwrap(), 1);
        assert_eq!(opened.get(), 1);

        tx.unbounded_send(Ok(2)).unwrap();
        tx.unbounded_send(Err("lost".to_owned())).unwrap();
        pause.replace(&Pause::new(true));
        assert_eq!(next(&mut items).await.unwrap().unwrap(), 2, "delivered before the pause");
        assert_eq!(next(&mut items).await.unwrap().unwrap_err().to_string(), "lost");
        assert!(next(&mut items).await.is_none());
        assert!(tx.unbounded_send(Ok(3)).is_err(), "dropped while paused");

        pause.replace(&Pause::new(false));
        tx_again.unbounded_send(Ok(4)).unwrap();
        assert_eq!(next(&mut items).await.unwrap().unwrap(), 4);
        assert_eq!(opened.get(), 2, "opened again on resuming");
    }
}
