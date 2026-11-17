//! `ants inbox`: the messages to the CLI's human.

use ants_daemon::{dead, reactions};
use std::collections::BTreeMap;

use ants_proto::{Envelope, Following, ThreadId, Uuid};
use anyhow::{Result, bail};
use async_nats::jetstream::consumer::DeliverPolicy;
use futures::StreamExt as _;

use crate::avatars::Faces;
use crate::behalf::Checker;
use crate::connection::{Listing, Nats};
use crate::{inbox, presence, reacted, sealed, thread, tools};

/// Prints a message of the inbox, opened as `state` says, from `acting` for its human or not.
fn print(
    e: &Envelope,
    state: Option<sealed::State>,
    acting: ants_proto::Acting,
    reactions: &[reactions::Reacted],
    json: bool,
    faces: &Faces,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(&inbox::Shown::new(e, reactions).sealed(state).acting(acting))?);
    } else {
        println!("{}", inbox::render(e, &acting, reactions, Some(faces)));
    }
    Ok(())
}

pub(crate) async fn show(
    nats: &Nats<'_>,
    mut select: inbox::Select,
    thread: Option<thread::Pick>,
    listing: Listing,
) -> Result<()> {
    let Listing { follow, json } = listing;
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's, whose inbox its daemon reads; `ants inbox` reads a human's");
    }
    let (client, js) = nats.connect("ants-inbox").await?;
    select.thread = thread::resolve(thread, nats, &js).await?;
    let select = &select;
    let mut inbox = inbox::Inbox::open(&js, me).await?;
    let keyring = sealed::Keyring::load(nats, &js).await;
    let behalf = std::sync::Arc::new(Checker::new(nats, &js));
    let _following = follow.then(|| behalf.follow());
    let shown = reacted::Shown::default();
    let faces = Faces::stdout(crate::avatars::read_or_none(&js).await);
    let show_with = |e: &Envelope, reactions: &[reactions::Reacted]| -> Result<()> {
        let (e, state) = sealed::read(e.clone(), keyring.as_ref());
        shown.add(e.id, &e.from, &e.text);
        print(&e, state, behalf.acting(&e.from, e.on_behalf_of.as_ref()), reactions, json, &faces)
    };
    // Watched before reading, so no reaction to a message shown is missed.
    let changes = if follow { reacted::changes(nats, &js, &behalf).await } else { futures::stream::pending().boxed() };
    let show = |e: &Envelope| show_with(e, &[]);
    let unread = inbox.unread(select).await?;
    let reports = inbox.reports();
    if unread.is_empty() && reports.is_empty() && !follow && !json {
        println!("no unread replies");
    }
    let ids: Vec<Uuid> = unread.iter().map(|e| e.id).collect();
    let mut reactions = inbox::reactions_or_none(&js, &ids).await;
    behalf.reactions(&mut reactions).await;
    for e in &unread {
        if let Some(keyring) = &keyring {
            keyring.learn(e).await;
        }
        behalf.learn(e).await;
        show_with(e, reactions.get(&e.id).map_or(&[], Vec::as_slice))?;
    }
    let reported = |reports: BTreeMap<ThreadId, usize>| {
        if !json {
            reports.iter().for_each(|(thread, n)| println!("{}", report_line(thread, *n)));
        }
        Ok(())
    };
    reported(reports)?;
    if follow {
        let watch = watch_stored(nats, &client, &js, select.thread.is_none() && !json);
        let ctrl_c = async { _ = tokio::signal::ctrl_c().await };
        let followed = Box::pin(inbox.follow(select, keyring.as_ref(), Some(&behalf), show, reported, ctrl_c));
        // Following one thread shows no escalation on the others, so only following them all counts as reading.
        let followed = async {
            match select.thread {
                None => {
                    let device = nats.device;
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
            if select.thread.is_none() && !json {
                Box::pin(show_requests(nats, &client, &js, stopped)).await;
            }
        };
        let followed = async { tokio::join!(followed, requests).0 };
        // Ends only when printing fails.
        let reactions = shown.show(changes, json);
        tokio::select! {
            followed = followed => followed?,
            () = watch => {}
            printed = reactions => printed?,
        }
    }
    Ok(())
}

/// The line summing up `n` reports of an assistant's acts left out on the activity `thread`.
fn report_line(thread: &ThreadId, n: usize) -> String {
    let reports =
        if n == 1 { "1 assistant activity report".to_owned() } else { format!("{n} assistant activity reports") };
    format!("{reports} on thread {thread} (`ants thread show {thread}` shows them)")
}

/// `ants inbox --unread TARGET`: mark unread again on all the human's devices the message TARGET names to the human and
/// the later ones on its thread, else the latest message to the human on the thread it names.
pub(crate) async fn mark_unread(nats: &Nats<'_>, target: &str) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's, whose inbox its daemon reads; `ants inbox` reads a human's");
    }
    let (_client, js) = nats.connect("ants-inbox").await?;
    let inbox = inbox::Inbox::open(&js, me).await?;
    let message = match target.parse::<thread::MessagePick>() {
        Ok(pick) => match pick.resolve(&js, &nats.me).await {
            Ok(id) => thread::find_message(&js, id, None).await?,
            Err(e) => {
                tracing::debug!("{target} as a message: {e:#}");
                None
            }
        },
        Err(_) => None,
    };
    let (thread, seqs) = match message {
        Some(entry) => {
            let start = DeliverPolicy::ByStartSequence { start_sequence: entry.seq };
            let seqs = inbox.on_thread(&entry.thread_id, start).await?;
            if seqs.is_empty() {
                bail!("no message to {me} on thread {} from message {target} on", entry.thread_id);
            }
            (entry.thread_id, seqs)
        }
        None => {
            let thread = target.parse::<thread::Pick>()?.resolve(nats, &js).await?;
            let seqs = inbox.on_thread(&thread, thread::show_start(&thread, None)).await?;
            let Some(last) = seqs.last() else { bail!("no message to {me} on thread {thread}") };
            (thread, vec![*last])
        }
    };
    inbox.unmark(&seqs).await?;
    let messages = if seqs.len() == 1 { "1 message".to_owned() } else { format!("{} messages", seqs.len()) };
    println!("marked {messages} to {me} on thread {thread} unread");
    Ok(())
}

/// Watch for what the inbox tells the human besides the replies, never ending: a dead letter stored here, like one the
/// daemon stores, and with `unserved`, when following every thread as text, the messages sent that keep waiting for an
/// absent ant.
async fn watch_stored(
    nats: &Nats<'_>,
    client: &async_nats::Client,
    js: &async_nats::jetstream::Context,
    unserved: bool,
) {
    let dead = async {
        if let Err(e) = Box::pin(dead::watch(client, js, &nats.me.id, |_, _| {})).await {
            tracing::warn!("not watching for messages JetStream stopped delivering: {e:#}");
        }
    };
    let unserved = async {
        if unserved {
            Box::pin(inbox::tell_unserved(nats, js)).await;
        }
    };
    tokio::join!(dead, unserved);
    std::future::pending::<()>().await;
}

/// Show the tool requests, as following every thread as text does, until `stopped` or showing them fails.
async fn show_requests(
    nats: &Nats<'_>,
    client: &async_nats::Client,
    js: &async_nats::jetstream::Context,
    stopped: tokio::sync::oneshot::Receiver<()>,
) {
    let report = |update: &tools::Update| {
        println!("{}", tools::render(update, chrono::Utc::now()));
        Ok(())
    };
    let shown = async {
        tokio::select! {
            shown = Box::pin(tools::follow(client, js, report)) => {
                if let Err(e) = shown {
                    tracing::warn!("not showing tool requests: {e:#}");
                }
            }
            _ = stopped => {}
        }
    };
    let (me, device) = (&nats.me.id, nats.device);
    Box::pin(presence::while_following(js, nats.domain, me, device, Following::Approvals, shown)).await;
}
