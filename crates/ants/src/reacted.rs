//! Reactions added and removed live while `ants inbox --follow`, `ants ask --follow` and `ants post --follow` run, to
//! the messages they showed. A reaction ends no wait and marks nothing read; credentials that cannot read the
//! reactions follow without them.

use std::cell::RefCell;
use std::collections::HashMap;

use ants_daemon::reactions::{self, Change};
use ants_proto::{REACTIONS_BUCKET, UserId, Uuid};
use anyhow::Result;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _};

use crate::Nats;

/// How many characters of a message's first line name it beside a reaction.
const OPENING: usize = 40;

/// Every reaction added or removed from now on; none, quietly, with credentials that may not read them all, as an
/// ant's, and none, with a warning, when the bucket cannot be watched.
pub async fn changes(nats: &Nats<'_>, js: &async_nats::jetstream::Context) -> BoxStream<'static, Change> {
    let none = || futures::stream::pending().boxed();
    if !nats.rights.may_publish(&format!("$JS.API.CONSUMER.CREATE.KV_{REACTIONS_BUCKET}")) {
        return none();
    }
    match reactions::watch(js).await {
        Ok(changes) => changes.chain(futures::stream::pending()).boxed(),
        Err(e) => {
            tracing::warn!("not showing reactions: {e:#}");
            none()
        }
    }
}

/// The messages a follow showed, each with how a reaction line names it: its sender and the start of its text.
#[derive(Default)]
pub struct Shown(RefCell<HashMap<Uuid, String>>);

impl Shown {
    pub fn add(&self, id: Uuid, from: &UserId, text: &str) {
        self.0.borrow_mut().insert(id, format!("{from}: {}", crate::excerpt(text.trim(), OPENING)));
    }

    /// Hand each change to a message shown to `print` with the message's name, until printing fails.
    pub async fn print(
        &self,
        changes: impl Stream<Item = Change>,
        mut print: impl FnMut(&Change, &str) -> Result<()>,
    ) -> Result<()> {
        tokio::pin!(changes);
        while let Some(change) = changes.next().await {
            let name = self.0.borrow().get(&change.message).cloned();
            if let Some(name) = name {
                print(&change, &name)?;
            }
        }
        Ok(())
    }

    /// Print each change to a message shown, as a line or with `json` as an object of its message, by, reaction and
    /// removed, until printing fails.
    pub async fn show(&self, changes: impl Stream<Item = Change>, json: bool) -> Result<()> {
        self.print(changes, |change, name| {
            crate::progress::clear();
            if json {
                println!("{}", serde_json::to_string(change)?);
            } else {
                println!("{}", line(change, name));
            }
            Ok(())
        })
        .await
    }
}

/// A change as a follow prints it: as `ants watch` does, with the message's sender and opening.
fn line(change: &Change, name: &str) -> String {
    format!("{} ({name})", crate::cmd::watch::reaction_line(change))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::Role;

    use super::*;
    use crate::auth::tests::Server;

    /// A human's credentials follow every reaction; an ant's, which read none, follow none and fail nothing.
    #[tokio::test]
    async fn only_credentials_reading_every_reaction_follow_them() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (UserId::human(), "dev-a".parse::<UserId>().unwrap());
        let (_h, h_js) = server.connect_as(&human, Role::Human).await;
        h_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
        let (_a, a_js) = server.connect_as(&dev_a, Role::Ant).await;
        let follow = async |user: &UserId, role, js| {
            let creds = server.creds_as(user, role);
            let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
            changes(&nats, js).await
        };
        let mut as_human = follow(&human, Role::Human, &h_js).await;
        let mut as_ant = follow(&dev_a, Role::Ant, &a_js).await;
        let message = Uuid::now_v7();
        assert!(reactions::add(&h_js, None, &human, &message, &"eyes".parse().unwrap()).await.unwrap());
        let wait = Duration::from_secs(5);
        let change = tokio::time::timeout(wait, as_human.next()).await.unwrap().unwrap();
        assert_eq!((change.message, &change.by, change.removed), (message, &human, false));
        let none = tokio::time::timeout(Duration::from_millis(300), as_ant.next()).await;
        assert!(none.is_err(), "an ant follows no reactions");
    }

    #[tokio::test]
    async fn only_changes_to_messages_shown_are_printed_with_their_sender_and_opening() {
        let (asked, reply, other) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let shown = Shown::default();
        shown.add(asked, &"alice".parse().unwrap(), "  check the build\nand the docs");
        let change = |message, by: &str, removed| Change {
            message,
            by: by.parse().unwrap(),
            reaction: "eyes".parse().unwrap(),
            removed,
        };
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tx.unbounded_send(change(asked, "dev-a", false)).unwrap();
        tx.unbounded_send(change(other, "dev-b", false)).unwrap();
        let mut printed = Vec::new();
        let printing = shown.print(rx, |c, name| {
            printed.push(line(c, name));
            // A message shown meanwhile gets its changes printed too.
            if printed.len() == 1 {
                shown.add(reply, &"dev-a".parse().unwrap(), &"x".repeat(50));
                tx.unbounded_send(change(reply, "bob", false)).unwrap();
                tx.unbounded_send(change(asked, "dev-a", true)).unwrap();
                tx.close_channel();
            }
            Ok(())
        });
        printing.await.unwrap();
        let long = format!("{}…", "x".repeat(40));
        assert_eq!(
            printed,
            [
                format!("dev-a reacted eyes to message {asked} (alice: check the build…)"),
                format!("bob reacted eyes to message {reply} (dev-a: {long})"),
                format!("dev-a removed the reaction eyes to message {asked} (alice: check the build…)"),
            ]
        );
    }
}
