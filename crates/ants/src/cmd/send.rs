//! `ants ask`, `ants post`, `ants notes` and `ants react`: what the CLI's human sends.

use ants_daemon::reading::Reading;
use ants_daemon::{bus, notes, owners, reactions};
use ants_proto::{ChannelId, ChannelPost, Envelope, NotesPost, Reaction, ThreadId, UserId, subject};
use anyhow::Result;
use futures::StreamExt as _;

use crate::cli::Replies;
use crate::cmd::inbox::REISSUE_HUMAN;
use crate::{Nats, channel, compose, humans, inbox, progress, roster, thread};

/// Split a leading `@ant` off the words; the message is the rest, or what `input` reads.
pub(crate) fn recipient(mut words: Vec<String>, input: &compose::Input) -> Result<(UserId, String)> {
    let to = match words.first().and_then(|w| w.strip_prefix('@')) {
        Some(name) => {
            let to = name.parse()?;
            words.remove(0);
            to
        }
        None => UserId::messenger(),
    };
    Ok((to, input.text(words)?))
}

pub(crate) async fn ask(
    nats: &Nats<'_>,
    thread: Option<thread::Pick>,
    replies: &Replies,
    input: &compose::Input,
    words: Vec<String>,
) -> Result<()> {
    // Refuse a message with nothing to send before connecting.
    let (to, text) = recipient(words, input)?;
    let (client, js) = nats.connect("ants-cli").await?;
    let thread = thread::resolve(thread, &js, &nats.me).await?;
    let envelope = ask_envelope(&nats.me, thread, to, text);
    // Subscribe first so a fast reply is not missed. A waiter also reads back what the hub stored while out of its
    // reach.
    let inbox = if replies.wait || replies.follow {
        bus::caught_up_inbox(&client, &js, &nats.me.id, nats.me.role).await?
    } else {
        bus::live_inbox(&client, &nats.me.id).await?
    };
    // Warned rather than refused: a daemon may start later and take the message, which waits in the stream.
    let warnings = roster::unserved_warnings(&js, &[&envelope.to]).await;
    let to_human = !warnings.is_empty() && humans::load(&js, &nats.me).await.contains(&envelope.to);
    let unserved = !warnings.is_empty() && !to_human;
    if unserved {
        for warning in warnings {
            tracing::warn!("{warning}");
        }
        if replies.wait || replies.follow {
            let flag = if replies.wait { "--wait" } else { "--follow" };
            tracing::warn!("a reply waits for a daemon to serve {}: {flag} may not end until one does", envelope.to);
        }
    }
    let status = if (replies.wait || replies.follow) && !to_human && progress::wanted(nats.me.role.is_human()) {
        client.subscribe(subject::status(&envelope.to, &envelope.thread_id)).await.ok()
    } else {
        None
    };
    if let Some(owner) = &envelope.owner {
        owners::recording(&js, nats.domain, &envelope.thread_id, &envelope.to, owner).await;
    }
    if bus::publish(&js, &envelope).await? == bus::Stored::Outbox {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    println!("thread {}", envelope.thread_id);
    println!("message {}", envelope.id);
    if !replies.wait && !replies.follow {
        return Ok(());
    }
    let _shown = status.map(|sub| {
        let stand = if unserved { progress::Stand::Unserved } else { progress::Stand::Waiting };
        let status = progress::Status::new(vec![envelope.id]).with(envelope.to.clone(), stand);
        progress::start(sub, status, replies.wait)
    });
    let thread = envelope.thread_id.clone();
    let on_thread = inbox.filter_map(move |msg| {
        let reply = match serde_json::from_slice::<Envelope>(&msg.payload) {
            Ok(reply) => match reply.check_subject(&msg.subject) {
                Ok(()) => (reply.thread_id == thread).then_some(reply),
                Err(e) => {
                    tracing::warn!("ignoring a reply with a forged sender: {e}");
                    None
                }
            },
            Err(_) => None,
        };
        std::future::ready(reply)
    });
    replies
        .show(&envelope.thread_id, on_thread, async |reply: Envelope| {
            progress::clear();
            println!("{}: {}", reply.from, reply.text);
            // Seen here, so `ants inbox` need not show it again. The other replies stay unread; an ant's inbox
            // consumer is its daemon's.
            if nats.me.role.is_human() && !nats.device.marks_read {
                tracing::warn!("{REISSUE_HUMAN}; the reply stays unread");
            } else if nats.me.role.is_human() {
                match inbox::mark_read(&js, &nats.me.id, &reply).await {
                    Ok(true) => {}
                    Ok(false) => tracing::debug!("reply already read"),
                    Err(e) => tracing::warn!("could not mark the reply read: {e:#}"),
                }
            }
            Ok(())
        })
        .await
}

/// What `ants ask` sends as `me`: a thread `me` owns, as it starts it or continues it, when `me` is a human. A thread
/// an ant starts has no owner: only a human gets a thread's escalations, with a fresh turn budget. A human's owner is
/// recorded for the thread and the recipient before the message goes out ([`owners::record`]); the first record of a
/// thread to an ant stays.
pub(crate) fn ask_envelope(me: &humans::Me, thread: Option<ThreadId>, to: UserId, text: String) -> Envelope {
    let envelope = match thread {
        Some(thread) => Envelope::on_thread(thread, me.id.clone(), to, text),
        None => Envelope::new(me.id.clone(), to, text),
    };
    envelope.with_owner(me.role.is_human().then(|| me.id.clone()))
}

pub(crate) async fn post(
    nats: &Nats<'_>,
    channel: ChannelId,
    thread: Option<thread::Pick>,
    replies: &Replies,
    input: &compose::Input,
    words: Vec<String>,
) -> Result<()> {
    let text = input.text(words)?;
    let (client, js) = nats.connect("ants-cli").await?;
    let thread = thread::resolve(thread, &js, &nats.me).await?;
    let humans = humans::load(&js, &nats.me).await;
    let post = channel::compose(&js, &nats.me, &humans, channel, thread, text).await;
    let entry = channel::get(&channel::open(&js).await?, &post.channel).await?;
    if entry.is_none() {
        tracing::warn!("no channel #{0} (`ants channel create {0}` makes one): the post wakes nobody", post.channel);
    }
    for user in channel::idle_mentions(&post, entry.as_ref()) {
        tracing::warn!("@{user} is no member of #{}: the post neither wakes nor notifies {user}", post.channel);
    }
    // Subscribe first so a fast reply is not missed.
    let room = if replies.wait || replies.follow {
        Some(client.subscribe(subject::chan_room(&post.channel)).await?)
    } else {
        None
    };
    let status = if room.is_some() && progress::wanted(nats.me.role.is_human()) {
        client.subscribe(subject::status_on(&post.thread_id)).await.ok()
    } else {
        None
    };
    let stored = bus::store_acked(&js, post.subject(), &post, &post.id.to_string()).await?.map(|ack| ack.sequence);
    if stored.is_none() {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    // An ant's daemon notifies the humans its posts mention.
    if nats.me.role.is_human() {
        let notified = channel::notify_mentioned(&js, &post, entry.as_ref(), &humans).await;
        if !notified.is_empty() {
            let names: Vec<&str> = notified.iter().map(UserId::as_str).collect();
            println!("notified {}", names.join(", "));
        }
    }
    println!("thread {}", post.thread_id);
    println!("message {}", post.id);
    let Some(room) = room else { return Ok(()) };
    let _shown = status.map(|sub| progress::start(sub, progress::Status::new(vec![post.id]), replies.wait));
    // Waiting is for an answer, so the poster's own posts from elsewhere do not count; following shows them all. What
    // never comes live, as while the leaf's uplink is down, is read back from the hub's stream.
    let posts = channel::caught_up_posts(room, &js, &nats.me, &post, stored);
    let on_thread = channel::thread_posts(posts, &post, replies.wait);
    replies
        .show(&post.thread_id, on_thread, async |reply: ChannelPost| {
            progress::clear();
            println!("{}: {}", reply.from, reply.text);
            Ok(())
        })
        .await
}

pub(crate) async fn thread_notes(nats: &Nats<'_>, thread: thread::Pick, text: Vec<String>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let thread = thread.resolve(&js, &nats.me).await?;
    if !text.is_empty() {
        let post = NotesPost::new(thread, nats.me.id.clone(), text.join(" "));
        notes::publish(&js, &post, None).await?;
        println!("posted to the notes of thread {} ({} turns left)", post.thread_id, post.turn_budget);
        return Ok(());
    }
    let read = notes::read(&js, &nats.me.id, Reading::cli(nats.me.role), &thread).await?;
    if read.posts.is_empty() {
        println!("thread {thread} has no notes");
    }
    for post in &read.posts {
        let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
        println!("{time} {} ({} turns left): {}", post.from, post.turn_budget, post.text);
    }
    Ok(())
}

pub(crate) async fn react(
    nats: &Nats<'_>,
    message: thread::MessagePick,
    reaction: Reaction,
    remove: bool,
) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let message = message.resolve(&js, &nats.me).await?;
    let me = &nats.me.id;
    let line = if remove {
        if reactions::remove(&js, nats.domain, me, &message, &reaction).await? {
            format!("removed {reaction} from message {message}")
        } else {
            format!("no {reaction} reaction to message {message} to remove")
        }
    } else if reactions::add(&js, nats.domain, me, &message, &reaction).await? {
        format!("reacted {reaction} to message {message}")
    } else {
        format!("already reacted {reaction} to message {message}")
    };
    println!("{line}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn leading_at_picks_the_recipient() {
        let (to, text) = recipient(words("@dev-b check the build"), &compose::Input::default()).unwrap();
        assert_eq!((to.as_str(), text.as_str()), ("dev-b", "check the build"));
    }

    #[test]
    fn messenger_is_the_default_recipient() {
        let (to, text) = recipient(words("who owns billing?"), &compose::Input::default()).unwrap();
        assert_eq!((to, text.as_str()), (UserId::messenger(), "who owns billing?"));
    }

    #[test]
    fn an_ask_comes_from_and_belongs_to_the_cli_human() {
        let alice: UserId = "alice".parse().unwrap();
        let me = humans::Me { id: alice.clone(), role: ants_proto::Role::Human };
        let ask = ask_envelope(&me, None, "dev-b".parse().unwrap(), "check the build".into());
        assert_eq!((&ask.from, ask.to.as_str(), ask.owner.as_ref()), (&alice, "dev-b", Some(&alice)));
        let thread: ThreadId = "t1".parse().unwrap();
        let follow_up = ask_envelope(&me, Some(thread.clone()), "dev-b".parse().unwrap(), "and the docs".into());
        assert_eq!((follow_up.thread_id, follow_up.owner), (thread, Some(alice)));
    }

    #[test]
    fn an_ask_with_an_ant_s_credentials_has_no_owner() {
        let me = humans::Me { id: "dev-a".parse().unwrap(), role: ants_proto::Role::Ant };
        let ask = ask_envelope(&me, None, "dev-b".parse().unwrap(), "check the build".into());
        assert_eq!((&ask.from, ask.owner), (&me.id, None));
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        let input = compose::Input::default();
        assert!(recipient(words("@dev-b"), &input).is_err());
        assert!(recipient(words("@Bad.Name hi"), &input).is_err());
        let file = compose::Input { file: Some("-".into()), edit: false };
        assert!(recipient(words("@dev-b more"), &file).unwrap_err().to_string().contains("not both"));
    }
}
