//! `ants ask`, `ants post`, `ants notes`, `ants react` and `ants answer`: what the CLI's human sends.

use ants_client::attachments::listed_on;
use ants_client::inbox::ParseError;
use ants_client::sending::Addressed;
use ants_daemon::bus;
use ants_daemon::bus::relayed;
use ants_daemon::records::reactions;
use ants_daemon::stream::notes;
use ants_proto::{
    ChannelId, ChannelPost, Envelope, Quote, Reaction, ReactionChange, ReactionsOutcome, ReactionsRequest, Role,
    ThreadId, UserId, Uuid, subject,
};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream;
use futures::StreamExt as _;
use futures::stream::BoxStream;

use clap_complete::engine::{ArgValueCandidates, ArgValueCompleter};

use crate::cli::{Replies, Sending};
use crate::connection::Nats;
use crate::messages::send::{
    OUTBOX, Picks, Posting, Quoted, Recipient, answering, ask_envelope, check_fits, deliver, notes_posted, post_notes,
    reacted_line, unaddressed,
};
use crate::messages::{attachments, compose, forward, inbox, progress, reacted};
use crate::people::{humans, sealed};
use crate::threads::thread;
use crate::{ants::colony, channel, completions};

/// `ants ask`.
#[derive(clap::Args, Debug, Default)]
pub(crate) struct AskArgs {
    /// Continue an existing thread: its id, a unique start of one, or `last`. The message still prompts only
    /// its recipient (@ant, else the human's assistant or the messenger), not the thread's other ants.
    /// A human's message steers a running prompt on that thread, joining it where the harness takes steers,
    /// else interrupting it and prompting the same session next, unless sent with --queue.
    #[arg(long, value_name = "THREAD")]
    #[arg(add = ArgValueCandidates::new(completions::threads))]
    pub(crate) thread: Option<thread::Pick>,
    /// Reply to this message or post, quoting its author and an excerpt of it: its id, or with a human's
    /// credentials a unique start of one, as `ants react` takes. The message goes on that message's thread and,
    /// without @ant, to its author unless the CLI's human wrote it.
    #[arg(long, value_name = "MESSAGE", add = ArgValueCompleter::new(completions::messages))]
    pub(crate) reply_to: Option<thread::MessagePick>,
    /// On an existing thread (--thread or --reply-to), wait behind the prompt running there rather than
    /// steering it, as a human's message does by default.
    #[arg(long)]
    pub(crate) queue: bool,
    /// Hand this message or post on to @ant (else the human's assistant or the messenger) on a new thread: its
    /// id, or a unique start of one, as --reply-to takes. The new message quotes it with its author and the
    /// thread it is on, carries its attachments, and the words, if any, as a note before it; the new thread
    /// links to the source thread, as a spin-off does. Needs human credentials.
    #[arg(long, value_name = "MESSAGE", conflicts_with_all = ["thread", "reply_to"])]
    #[arg(add = ArgValueCompleter::new(completions::messages))]
    pub(crate) forward: Option<thread::MessagePick>,
    /// Hand this thread on as --forward hands a message: its TL;DR kept (`ants thread tldr` keeps one), quoted
    /// with who wrote it; its id, a unique start of one, or `last`, as --thread takes.
    #[arg(long, value_name = "THREAD", conflicts_with_all = ["thread", "reply_to", "forward"])]
    #[arg(add = ArgValueCandidates::new(completions::threads))]
    pub(crate) forward_thread: Option<thread::Pick>,
    #[command(flatten)]
    pub(crate) sending: Sending,
    #[command(flatten)]
    pub(crate) replies: Replies,
    #[command(flatten)]
    pub(crate) input: compose::Input,
    #[command(flatten)]
    pub(crate) attach: attachments::Attach,
    /// `@ant` (optional), then the message, unless --file or --edit gives it.
    #[arg(num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
    pub(crate) words: Vec<String>,
}

/// `ants post`.
#[derive(clap::Args, Debug)]
pub(crate) struct PostArgs {
    /// The channel: `ops` or `'#ops'` (quoted, as shells take an unquoted `#` for a comment).
    #[arg(value_parser = channel::parse_channel)]
    #[arg(add = ArgValueCandidates::new(completions::channels))]
    pub(crate) channel: ChannelId,
    /// Post on an existing thread of the channel instead of starting one (its id, a unique start of one, or
    /// `last`); the thread keeps its owner.
    #[arg(long, value_name = "THREAD")]
    #[arg(add = ArgValueCandidates::new(completions::threads))]
    pub(crate) thread: Option<thread::Pick>,
    /// Reply to this post or message on a thread of the channel, quoting its author and an excerpt of it: its id,
    /// or with a human's credentials a unique start of one, as `ants react` takes. The post goes on that thread;
    /// an @mention in the text still picks whom it wakes.
    #[arg(long, value_name = "MESSAGE", add = ArgValueCompleter::new(completions::messages))]
    pub(crate) reply_to: Option<thread::MessagePick>,
    /// On an existing thread (--thread or --reply-to), wait behind the prompt running there rather than
    /// steering it, as a human's post does by default.
    #[arg(long)]
    pub(crate) queue: bool,
    #[command(flatten)]
    pub(crate) replies: Replies,
    #[command(flatten)]
    pub(crate) input: compose::Input,
    #[command(flatten)]
    pub(crate) attach: attachments::Attach,
    /// The post, unless --file or --edit gives it; each @ant mention wakes that member.
    #[arg(num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
    pub(crate) words: Vec<String>,
}

/// Split a leading `@ant` off the words, none when they name no one ([`unaddressed`]); the message is the rest, or
/// what `input` reads.
pub(crate) fn recipient(mut words: Vec<String>, input: &compose::Input) -> Result<(Option<UserId>, String)> {
    let to = match words.first().and_then(|w| w.strip_prefix('@')) {
        Some(name) => {
            let to = name.parse()?;
            words.remove(0);
            Some(to)
        }
        None => None,
    };
    Ok((to, input.text(words)?))
}

/// Split a leading `@ant` off the words as [`recipient`] does; the note forwarded with a message is the rest, or what
/// `input` reads, and may be empty.
fn note(words: Vec<String>, input: &compose::Input) -> Result<(Option<UserId>, String)> {
    let bare = input.file.is_none() && !input.edit;
    match words.as_slice() {
        [] if bare => Ok((None, String::new())),
        [only] if bare && only.starts_with('@') => Ok((Some(only[1..].parse()?), String::new())),
        _ => recipient(words, input),
    }
}

/// `to`, else who gets what names no one ([`unaddressed`]), which `ants ask` then tells.
async fn addressee(js: &jetstream::Context, nats: &Nats<'_>, to: Option<UserId>) -> (UserId, Option<Recipient>) {
    match to {
        Some(to) => (to, None),
        None => {
            let recipient = unaddressed(js, &nats.me, nats.account.as_deref()).await;
            (recipient.to.clone(), Some(recipient))
        }
    }
}

/// Where a message to `to` goes: to the lead of the team `to` names, carrying the team
/// ([`ants_client::sending::addressed`]), refused for a team with no lead, nor any above it; to `to` itself when a
/// human, on a bus without teams, or, with a warning, when the team directory cannot be read.
async fn to_team(js: &jetstream::Context, nats: &Nats<'_>, to: UserId) -> Result<Addressed> {
    match crate::teams::read_of(js, nats.domain).await {
        Ok(Some(directory)) if directory.is_team(&to) => {
            let humans = humans::load(js, &nats.me).await;
            Ok(ants_client::sending::addressed(&directory, |u| humans.contains(u), to)?)
        }
        Ok(Some(_)) => Ok(Addressed { to, team: None }),
        Ok(None) => Ok(Addressed { to, team: None }),
        Err(e) => {
            tracing::warn!("not checking whether {to} names a team: {e:#}");
            Ok(Addressed { to, team: None })
        }
    }
}

/// Where `ants ask` or `ants post` sends: on the thread `thread` picks, else a new one, or replying to the message
/// `reply_to` picks; for `ants ask`, on a new thread forwarding what `forward` picks ([`forward`]).
#[derive(Default)]
pub(crate) struct Target {
    pub thread: Option<thread::Pick>,
    pub reply_to: Option<thread::MessagePick>,
    pub forward: Option<Forwarding>,
    /// Wait behind the prompt running on the thread rather than steering it ([`Envelope::queue`]).
    pub queue: bool,
}

/// What `ants ask` forwards: a message, or a thread by its kept TL;DR.
pub(crate) enum Forwarding {
    Message(thread::MessagePick),
    Thread(thread::Pick),
}

impl Forwarding {
    /// The source picked, read as the CLI's human reads it, with `keyring` opening a sealed message.
    async fn source(
        self,
        nats: &Nats<'_>,
        js: &jetstream::Context,
        keyring: Option<std::sync::Arc<sealed::Keyring>>,
    ) -> Result<forward::Source> {
        match self {
            Self::Message(pick) => {
                Ok(forward::Source::Message(Box::new(message(nats, js, pick, keyring, "forward").await?)))
            }
            Self::Thread(pick) => {
                let thread = pick.resolve(nats, js).await?;
                let tldr = crate::threads::thread_meta::open(nats, js).await?.summary(&thread).await?;
                let tldr = tldr.ok_or_else(|| {
                    anyhow!("no TL;DR of thread {thread} is kept: `ants thread tldr {thread}` asks for one")
                })?;
                Ok(forward::Source::Thread(Box::new(tldr)))
            }
        }
    }
}

impl Target {
    /// Refuse `--queue` without an existing thread to wait on.
    fn check_queue(&self) -> Result<()> {
        if self.queue && self.thread.is_none() && self.reply_to.is_none() {
            bail!("--queue waits behind a prompt on an existing thread: name one with --thread or --reply-to");
        }
        Ok(())
    }

    /// The thread to send on, none for a new one, and the message replied to if any: the thread is that message's,
    /// which --thread, if given, must name.
    async fn resolve(self, nats: &Nats<'_>, js: &jetstream::Context) -> Result<(Option<ThreadId>, Option<Replied>)> {
        let thread = thread::resolve(self.thread, nats, js).await?;
        let Some(pick) = self.reply_to else { return Ok((thread, None)) };
        let message = message(nats, js, pick, None, "reply to").await?;
        let id = message.id.unwrap_or_default();
        if message.sealed.is_some_and(|s| s != ants_client::sealed::State::Opened) {
            bail!("message {id} is sealed and does not open on this device: there is nothing to quote");
        }
        let from = message.from.clone().ok_or_else(|| anyhow!("message {id} names no sender to reply to"))?;
        if let Some(thread) = thread.filter(|t| *t != message.thread_id) {
            bail!("message {id} is on thread {}, not {thread}", message.thread_id);
        }
        let quote = Quote::new(from, &message.text);
        let quoted = Quoted { id, quote, sealed: message.sealed.is_some() };
        let replied = Replied { quoted, channel: message.channel };
        Ok((Some(message.thread_id), Some(replied)))
    }
}

/// The message or post `pick` picks, opened with `keyring`, else with one loaded, as the CLI's human reads it back to
/// `what`: only with a human's credentials.
async fn message(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    pick: thread::MessagePick,
    keyring: Option<std::sync::Arc<sealed::Keyring>>,
    what: &str,
) -> Result<ants_client::entry::Entry> {
    let me = &nats.me;
    if !me.role.is_human() {
        bail!("the credentials of {} are an ant's, which read no message back to {what}", me.id);
    }
    let id = pick.resolve(js, me).await?;
    let keyring = match keyring {
        Some(keyring) => Some(keyring),
        None => sealed::Keyring::load(nats, js).await.map(std::sync::Arc::new),
    };
    thread::find_message(js, id, keyring).await?.ok_or_else(|| anyhow!("no message {id} is kept"))
}

/// The message a send replies to ([`Target::reply_to`]), and the channel of a post.
struct Replied {
    quoted: Quoted,
    channel: Option<ChannelId>,
}

pub(crate) async fn ask(nats: &Nats<'_>, args: AskArgs) -> Result<()> {
    let AskArgs { thread, reply_to, queue, forward, forward_thread, sending, replies, input, attach, words } = args;
    let Sending { plaintext } = sending;
    let (replies, input, attach) = (&replies, &input, &attach);
    let forward = forward.map(Forwarding::Message).or_else(|| forward_thread.map(Forwarding::Thread));
    let mut target = Target { thread, reply_to, queue, forward };
    // Refuse a message with nothing to send, or a file that cannot be attached, before connecting.
    target.check_queue()?;
    let forward = target.forward.take();
    let (to, text) = if forward.is_some() { note(words, input)? } else { recipient(words, input)? };
    let mut files = attach.read()?;
    let (client, js) = nats.connect("ants-cli").await?;
    let queue = target.queue;
    let forwarded = match forward {
        Some(forwarding) => {
            let keyring = sealed::Keyring::load(nats, &js).await.map(std::sync::Arc::new);
            let source = forwarding.source(nats, &js, keyring.clone()).await?;
            source.check()?;
            files = source.files(&js, keyring.as_deref(), files).await?;
            Some(source)
        }
        None => None,
    };
    let (thread, replied) = target.resolve(nats, &js).await?;
    let author = replied.as_ref().map(|r| &r.quoted.quote.from).filter(|from| **from != nats.me.id);
    let (to, unaddressed) = addressee(&js, nats, to.or_else(|| author.cloned())).await;
    let addressed = match unaddressed {
        Some(_) => Addressed { to, team: None },
        None => to_team(&js, nats, to).await?,
    };
    ants_client::sending::one_recipient(&nats.me.id, &addressed.to, &text)?;
    let mut envelope = match &forwarded {
        Some(source) => source.envelope(&nats.me, addressed.to.clone(), &text),
        None => ask_envelope(&nats.me, thread, addressed.to.clone(), text),
    };
    addressed.apply(&mut envelope);
    if let Some(r) = &replied {
        envelope = envelope.quoting(r.quoted.id, r.quoted.quote.clone());
    }
    envelope.attachments = attachments::references(&files);
    envelope.queue = queue;
    let to_human = nats.me.role.is_human() && humans::load(&js, &nats.me).await.contains(&envelope.to);
    let text = envelope.text.clone();
    let sealer = sealing(nats, &js, &mut envelope, &mut files, to_human && !plaintext).await?;
    // Sealing took the quote into the sealed body; one left is in the clear.
    if let Some(r) = replied.filter(|_| envelope.quote.is_some()) {
        envelope.quote = Some(r.quoted.quote(false));
    }
    let (envelope, files) = (envelope, files);
    check_fits(&js, &envelope, ATTACH)?;
    // Subscribe first so a fast reply is not missed. A waiter also reads back what the hub stored while out of its
    // reach.
    let inbox = if replies.wait || replies.follow {
        bus::caught_up_inbox(&client, &js, &nats.me.id, nats.me.reading()).await?
    } else {
        bus::live_inbox(&client, &nats.me.id).await?
    };
    let (to_human, unserved) = warn_unserved(&js, &nats.me, &envelope.to, to_human, replies).await;
    let changes = followed_reactions(nats, &js, replies).await;
    let status = if (replies.wait || replies.follow) && !to_human && progress::wanted(nats.me.role.is_human()) {
        relayed::subscribe(&client, subject::status(&envelope.to, &envelope.thread_id))
            .await
            .inspect_err(|e| tracing::debug!("showing no progress: {e}"))
            .ok()
    } else {
        None
    };
    attachments::store(&js, &files, |id| envelope.attachment_handle(id)).await?;
    let stored = deliver(&js, nats.domain, &envelope).await?;
    print_sent(stored, &envelope, unaddressed.as_ref(), &files);
    if !replies.wait && !replies.follow {
        return Ok(());
    }
    let _shown = status.map(|sub| {
        let stand = if unserved { progress::Stand::Unserved } else { progress::Stand::Waiting };
        let status = progress::Status::new(vec![envelope.id]).with(envelope.to.clone(), stand);
        progress::start(sub, status, replies.wait)
    });
    let shown = reacted::Shown::default();
    shown.add(envelope.id, &envelope.from, &text);
    let keyring = match sealer {
        Some(sealer) => Some(sealer.keyring),
        None if to_human => sealed::Keyring::load(nats, &js).await,
        None => None,
    };
    let on_thread = replies_on(inbox, envelope.thread_id.clone());
    let shown_replies = replies.show(&envelope.thread_id, on_thread, async |reply: Envelope| {
        show_reply(nats, &js, keyring.as_ref(), &shown, reply).await;
        Ok(())
    });
    Box::pin(with_reactions(shown_replies, &shown, changes)).await
}

/// Warn when no daemon serves `to` now, unless a human, and when the replies `replies` waits for may then not come.
/// Warned rather than refused: a daemon may start later and take the message, which waits in the stream. Returns
/// whether `to` is a human, `to_human` as `me` knew it or as the colony tells for an ant, and whether `to` is unserved.
async fn warn_unserved(
    js: &jetstream::Context,
    me: &humans::Me,
    to: &UserId,
    to_human: bool,
    replies: &Replies,
) -> (bool, bool) {
    let warnings = colony::unserved_warnings(js, &[to]).await;
    let to_human = to_human || !me.role.is_human() && !warnings.is_empty() && humans::load(js, me).await.contains(to);
    let unserved = !warnings.is_empty() && !to_human;
    if unserved {
        for warning in warnings {
            tracing::warn!("{warning}");
        }
        if replies.wait || replies.follow {
            let flag = if replies.wait { "--wait" } else { "--follow" };
            tracing::warn!("a reply waits for a daemon to serve {to}: {flag} may not end until one does");
        }
    }
    (to_human, unserved)
}

/// Print where `envelope` went, `stored` as it was, to whom when it named no one, and the `files` it carries.
fn print_sent(stored: bus::Stored, envelope: &Envelope, unaddressed: Option<&Recipient>, files: &[attachments::File]) {
    if stored == bus::Stored::Outbox {
        println!("{OUTBOX}");
    }
    println!("thread {}", envelope.thread_id);
    println!("message {}", envelope.id);
    if let Some(recipient) = unaddressed {
        println!("to {recipient}");
    }
    if let Some(team) = &envelope.team {
        println!("to {} (lead of team {team})", envelope.to);
    }
    print_attached(files, |id| envelope.attachment_handle(id));
}

/// The replies in `inbox` on `thread`, those with a forged sender skipped.
fn replies_on(inbox: bus::LiveInbox, thread: ThreadId) -> impl futures::Stream<Item = Envelope> {
    inbox.filter_map(move |msg| {
        let reply = match ants_client::inbox::parse(&msg.subject, &msg.payload) {
            Ok(reply) => (reply.thread_id == thread).then_some(reply),
            Err(ParseError::Forged(e)) => {
                tracing::warn!("ignoring a reply with a forged sender: {e}");
                None
            }
            Err(ParseError::Malformed(_)) => None,
        };
        std::future::ready(reply)
    })
}

/// Print `reply`, opened with `keyring` when sealed, and mark it read for a human.
async fn show_reply(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    keyring: Option<&sealed::Keyring>,
    shown: &reacted::Shown,
    reply: Envelope,
) {
    progress::clear();
    if let Some(keyring) = keyring {
        keyring.learn(&reply).await;
    }
    let (opened, _) = sealed::read(reply.clone(), keyring);
    shown.add(opened.id, &opened.from, &opened.text);
    println!("{}", said(opened.from.as_str(), &opened.text));
    print!("{}", attachments::render(&listed_on(&opened.subject(), &opened.attachments), "  "));
    // Seen here, so `ants inbox` need not show it again. The other replies stay unread; an ant's inbox consumer is its
    // daemon's.
    if nats.me.role.is_human() {
        match inbox::mark_read(js, &nats.me.id, &reply).await {
            Ok(true) => {}
            Ok(false) => tracing::debug!("reply already read"),
            Err(e) => tracing::warn!("could not mark the reply read: {e:#}"),
        }
    }
}

/// Seal `envelope` to another human if `seal`, with `files`, those it carries, which become what to store, returning
/// what sealed it, which opens the replies too.
async fn sealing(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    envelope: &mut Envelope,
    files: &mut [attachments::File],
    seal: bool,
) -> Result<Option<sealed::Sealer>> {
    if !seal {
        return Ok(None);
    }
    let sealer = sealed::Sealer::new(nats, js, &envelope.to, sealed::PLAINTEXT).await?;
    sealer.seal(envelope, files)?;
    Ok(Some(sealer))
}

pub(crate) async fn post(nats: &Nats<'_>, args: PostArgs) -> Result<()> {
    let PostArgs { channel, thread, reply_to, queue, replies, input, attach, words } = args;
    let (replies, input, attach) = (&replies, &input, &attach);
    let target = Target { thread, reply_to, queue, forward: None };
    target.check_queue()?;
    let text = input.text(words)?;
    let files = attach.read()?;
    let (client, js) = nats.connect("ants-cli").await?;
    let queue = target.queue;
    let (thread, replied) = target.resolve(nats, &js).await?;
    if let Some(other) = replied.as_ref().and_then(|r| r.channel.as_ref()).filter(|c| **c != channel) {
        bail!("the message replied to is a post to #{other}, not #{channel}");
    }
    let mut posting = Posting::compose(&js, nats.domain, &nats.me, channel, thread, text).await?;
    if let Some(r) = replied {
        posting.post = posting.post.quoting(r.quoted.id, r.quoted.quote(false));
    }
    posting.post.attachments = attachments::references(&files);
    posting.post.queue = queue;
    for warning in &posting.warnings {
        tracing::warn!("{warning}");
    }
    let post = &posting.post;
    check_fits(&js, post, ATTACH)?;
    // Subscribe first so a fast reply is not missed.
    let room = if replies.wait || replies.follow {
        Some(relayed::subscribe(&client, subject::chan_room(&post.channel)).await?)
    } else {
        None
    };
    let changes = followed_reactions(nats, &js, replies).await;
    let status = if room.is_some() && progress::wanted(nats.me.role.is_human()) {
        relayed::subscribe(&client, subject::status_on(&post.thread_id))
            .await
            .inspect_err(|e| tracing::debug!("showing no progress: {e}"))
            .ok()
    } else {
        None
    };
    attachments::store(&js, &files, |id| post.attachment_handle(id)).await?;
    let (stored, notified) = posting.deliver(&js, &nats.me).await?;
    if stored.is_none() {
        println!("{OUTBOX}");
    }
    if !notified.is_empty() {
        let names: Vec<&str> = notified.iter().map(UserId::as_str).collect();
        println!("notified {}", names.join(", "));
    }
    println!("thread {}", post.thread_id);
    println!("message {}", post.id);
    print_attached(&files, |id| post.attachment_handle(id));
    let Some(room) = room else { return Ok(()) };
    let _shown = status.map(|sub| progress::start(sub, progress::Status::new(vec![post.id]), replies.wait));
    // Waiting is for an answer, so the poster's own posts from elsewhere do not count; following shows them all. What
    // never comes live, as while the leaf's uplink is down, is read back from the hub's stream.
    let posts = channel::caught_up_posts(room, &js, &nats.me, post, stored);
    let on_thread = channel::thread_posts(posts, post, replies.wait);
    let shown = reacted::Shown::default();
    shown.add(post.id, &post.from, &post.text);
    let behalf = crate::people::behalf::Checker::new(nats, &js);
    let shown_replies = replies.show(&post.thread_id, on_thread, async |reply: ChannelPost| {
        let by = behalf.label(&reply.from, reply.on_behalf_of.as_ref()).await;
        progress::clear();
        shown.add(reply.id, &reply.from, &reply.text);
        println!("{}", said(&by, &reply.text));
        print!("{}", attachments::render(&listed_on(&reply.subject(), &reply.attachments), "  "));
        Ok(())
    });
    Box::pin(with_reactions(shown_replies, &shown, changes)).await
}

/// The reactions `ants ask` and `ants post` follow: with --follow, from before the message goes out on; none with
/// --wait, which only the first reply ends.
async fn followed_reactions(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    replies: &Replies,
) -> BoxStream<'static, reactions::Change> {
    if !replies.follow {
        return futures::stream::pending().boxed();
    }
    reacted::changes(nats, js, &std::sync::Arc::new(crate::people::behalf::Checker::new(nats, js))).await
}

/// Show the replies, printing meanwhile the reactions to the message sent and the replies shown.
async fn with_reactions(
    shown_replies: impl Future<Output = Result<()>>,
    shown: &reacted::Shown,
    changes: BoxStream<'static, reactions::Change>,
) -> Result<()> {
    tokio::select! {
        shown = shown_replies => shown,
        printed = shown.show(changes, false) => printed,
    }
}

pub(crate) async fn notes(
    nats: &Nats<'_>,
    thread: thread::Pick,
    attach: attachments::Attach,
    text: Vec<String>,
    json: bool,
) -> Result<()> {
    if text.is_empty() && !attach.paths.is_empty() {
        bail!("--attach posts to the notes: give TEXT to post with the files");
    }
    let files = attach.read()?;
    let (_, js) = nats.connect("ants-cli").await?;
    let thread = thread.resolve(nats, &js).await?;
    if !text.is_empty() {
        let post = post_notes(&js, &nats.me.id, thread, text.join(" "), &files, ATTACH).await?;
        if json {
            println!("{}", notes_json(&post, post.from.as_str()));
            return Ok(());
        }
        println!("{}", notes_posted(&post));
        print_attached(&files, |id| post.attachment_handle(id));
        return Ok(());
    }
    let read = notes::read(&js, &nats.me.id, nats.me.reading(), &thread).await?;
    if read.posts.is_empty() && !json {
        println!("thread {thread} has no notes");
    }
    let behalf = crate::people::behalf::Checker::new(nats, &js);
    for post in &read.posts {
        let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
        let by = behalf.label(&post.from, post.on_behalf_of.as_ref()).await;
        if json {
            println!("{}", notes_json(post, &by));
            continue;
        }
        println!("{}", said(&format!("{time} {by} ({} turns left)", post.turn_budget), &post.text));
        print!("{}", attachments::render(&listed_on(&post.subject(), &post.attachments), "  "));
    }
    Ok(())
}

/// A post of the notes as `ants notes --json` prints it: the post, with `by` naming its author as its text does.
fn notes_json(post: &ants_proto::NotesPost, by: &str) -> String {
    let mut json = serde_json::to_value(post).unwrap_or_default();
    json["by"] = serde_json::json!(by);
    json.to_string()
}

/// How the command line attaches a file, as [`check_fits`] suggests it.
const ATTACH: &str = "send it with --attach";

/// A reply, post or note as `ants ask --wait`, `ants post --wait` and `ants notes` print it: `by`, then its text
/// without control characters but line breaks, nor those reordering text.
fn said(by: &str, text: &str) -> String {
    format!("{by}: {}", ants_proto::lines(text))
}

/// Print the handle of each file attached to the message sent, as `attachment <handle>`.
fn print_attached(files: &[attachments::File], handle_of: impl Fn(Uuid) -> ants_proto::AttachmentHandle) {
    for file in files {
        println!("attachment {}", handle_of(file.attachment.id));
    }
}

pub(crate) async fn react(
    nats: &Nats<'_>,
    message: thread::MessagePick,
    reaction: Reaction,
    remove: bool,
) -> Result<()> {
    let (client, js) = nats.connect("ants-cli").await?;
    let message = message.resolve(&js, &nats.me).await?;
    let me = &nats.me.id;
    let changed = match nats.me.role {
        Role::Human if remove => reactions::remove(&js, nats.domain, me, &message, &reaction).await?,
        Role::Human => reactions::add(&js, nats.domain, me, &message, &reaction).await?,
        // An ant's credentials write no reactions: `ants signer` makes the change once it found the ant reads the
        // message.
        Role::Ant => {
            let change = ReactionChange::new(reaction.clone(), remove);
            match reactions::request(&client, &ReactionsRequest::change(me.clone(), message, change)).await? {
                Some(ReactionsOutcome::Applied { changed }) => changed,
                Some(ReactionsOutcome::Unknown) => {
                    bail!("message {message} is none {me} reads, or the stream no longer keeps it: nothing changed")
                }
                Some(ReactionsOutcome::Listed { .. }) => bail!("an answer to another request"),
                None => bail!(
                    "no answer about the reaction, so it may not be made: `ants signer`, on the machine with the \
                     account key, makes such changes"
                ),
            }
        }
    };
    println!("{}", reacted_line(&reaction, message, remove, changed));
    Ok(())
}

/// Answer the questions `message` asks the CLI's human with `words`, one pick per question in their order (all of
/// them one pick when it asks one): an option's number, numbers joined by commas where several may be picked, else a
/// typed answer. The answer goes to the asking ant on the question's thread, as the human's message on it.
pub(crate) async fn answer(nats: &Nats<'_>, message: thread::MessagePick, words: Vec<String>) -> Result<()> {
    let me = &nats.me;
    if !me.role.is_human() {
        bail!("the credentials of {} are an ant's: only the human an ant asked answers its question", me.id);
    }
    let (_, js) = nats.connect("ants-cli").await?;
    let id = message.resolve(&js, me).await?;
    let envelope = answering(&js, &me.id, id, Picks::Words(words)).await?;
    if deliver(&js, nats.domain, &envelope).await? == bus::Stored::Outbox {
        println!("{OUTBOX}");
    }
    println!("thread {}", envelope.thread_id);
    println!("message {}", envelope.id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_waited_for_print_without_control_or_reordering_characters() {
        let shown = said("dev-a", crate::test_support::hostile::HOSTILE);
        crate::test_support::hostile::assert_printable(&shown);
        assert_eq!(shown, "dev-a: [2J]0;pwned]52;c;cHduZWQ=spoof31m\nline two");
    }

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn a_note_in_json_is_the_post_with_its_author_as_shown() {
        let post = ants_proto::NotesPost::new("t1".parse().unwrap(), "dev-a".parse().unwrap(), "split the work");
        let json: serde_json::Value = serde_json::from_str(&notes_json(&post, "dev-a for alice")).unwrap();
        assert_eq!((json["thread_id"].as_str(), json["text"].as_str()), (Some("t1"), Some("split the work")));
        assert_eq!((json["from"].as_str(), json["by"].as_str()), (Some("dev-a"), Some("dev-a for alice")));
        assert_eq!(json["turn_budget"], post.turn_budget);
    }

    #[test]
    fn leading_at_picks_the_recipient() {
        let (to, text) = recipient(words("@dev-b check the build"), &compose::Input::default()).unwrap();
        assert_eq!((to.unwrap().as_str(), text.as_str()), ("dev-b", "check the build"));
    }

    #[test]
    fn words_naming_no_one_leave_the_recipient_open() {
        let (to, text) = recipient(words("who owns billing?"), &compose::Input::default()).unwrap();
        assert_eq!((to, text.as_str()), (None, "who owns billing?"));
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
