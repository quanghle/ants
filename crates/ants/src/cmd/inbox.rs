//! `ants inbox`: the messages to the CLI's human.

use ants_daemon::{dead, reactions};
use ants_proto::{Envelope, Following, Uuid};
use anyhow::{Result, bail};

use crate::{Nats, inbox, presence, thread, tools};

/// Why credentials of a human from before the read marker cannot mark replies read.
pub(crate) const REISSUE_HUMAN: &str = "these credentials predate the read marker every device of a human shares: issue new ones \
                             with `ants auth user NAME --human` (and a leaf's with `ants auth leaf`)";

pub(crate) async fn show(
    nats: &Nats<'_>,
    mut select: inbox::Select,
    thread: Option<thread::Pick>,
    follow: bool,
    json: bool,
) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's, whose inbox its daemon reads; `ants inbox` reads a human's");
    }
    if !nats.device.marks_read {
        bail!("{REISSUE_HUMAN}");
    }
    let (client, js) = nats.connect("ants-inbox").await?;
    select.thread = thread::resolve(thread, &js, &nats.me).await?;
    let select = &select;
    let mut inbox = inbox::Inbox::open(&js, me).await?;
    let show_with = |e: &Envelope, reactions: &[reactions::Reacted]| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(&inbox::Shown { envelope: e, reactions })?);
        } else {
            println!("{}", inbox::render(e, reactions));
        }
        Ok(())
    };
    let show = |e: &Envelope| show_with(e, &[]);
    let unread = inbox.unread(select).await?;
    if unread.is_empty() && !follow && !json {
        println!("no unread replies");
    }
    let ids: Vec<Uuid> = unread.iter().map(|e| e.id).collect();
    let reactions = inbox::reactions_or_none(&js, &ids).await;
    unread.iter().try_for_each(|e| show_with(e, reactions.get(&e.id).map_or(&[], Vec::as_slice)))?;
    if follow {
        // A dead letter stored here tells the human through the inbox, like one the daemon stores.
        let watch = async {
            let dead = async {
                if let Err(e) = dead::watch(&client, &js, me, |_, _| {}).await {
                    tracing::warn!("not watching for messages JetStream stopped delivering: {e:#}");
                }
            };
            // Following every thread as text tells of the messages sent that keep waiting for an absent ant.
            let unserved = async {
                if select.thread.is_none() && !json {
                    Box::pin(inbox::tell_unserved(nats, &js)).await;
                }
            };
            tokio::join!(dead, unserved);
            std::future::pending::<()>().await;
        };
        let followed = inbox.follow(select, show, async { _ = tokio::signal::ctrl_c().await });
        // Following one thread shows no escalation on the others, so only following them all counts as reading.
        let followed = async {
            match select.thread {
                None => {
                    let device = nats.device.id;
                    Box::pin(presence::while_following(&js, nats.domain, me, device, Following::Inbox, followed)).await
                }
                Some(_) => followed.await,
            }
        };
        // Following every thread as text shows the tool requests too, until the replies end or showing them fails.
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let followed = async {
            let followed = followed.await;
            drop(stop);
            followed
        };
        let requests = async {
            if select.thread.is_some() || json {
                return;
            }
            let report = |update: &tools::Update| {
                println!("{}", tools::render(update, chrono::Utc::now()));
                Ok(())
            };
            let shown = async {
                tokio::select! {
                    shown = Box::pin(tools::follow(&client, &js, report)) => {
                        if let Err(e) = shown {
                            tracing::warn!("not showing tool requests: {e:#}");
                        }
                    }
                    _ = stopped => {}
                }
            };
            let device = nats.device.id;
            Box::pin(presence::while_following(&js, nats.domain, me, device, Following::Approvals, shown)).await;
        };
        let followed = async { tokio::join!(followed, requests).0 };
        tokio::select! {
            followed = followed => followed?,
            () = watch => {}
        }
    }
    Ok(())
}
