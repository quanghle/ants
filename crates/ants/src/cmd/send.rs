//! `ants ask`, `ants post`, `ants notes`, `ants react` and `ants answer`: what the CLI's human sends.

use ants_daemon::bus;
use ants_daemon::bus::relayed;
use ants_daemon::records::{owners, reactions};
use ants_daemon::stream::reading::Reading;
use ants_daemon::stream::{notes, questions};
use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, Envelope, Humans, JsDomain, NotesPost, Pick, Quote, Reaction, ReactionChange,
    ReactionsOutcome, ReactionsRequest, Role, ThreadId, UserId, Uuid, subject,
};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream;
use futures::StreamExt as _;
use futures::stream::BoxStream;

use clap_complete::engine::{ArgValueCandidates, ArgValueCompleter};

use crate::cli::{Replies, Sending};
use crate::connection::Nats;
use crate::messages::{attachments, compose, forward, inbox, progress, reacted};
use crate::people::{assistant, humans, sealed};
use crate::threads::thread;
use crate::{channel, colony, completions};

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

/// Who a message goes to, with the human it assists when it got the message as that human's assistant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recipient {
    pub to: UserId,
    pub assistant_of: Option<UserId>,
}

impl From<UserId> for Recipient {
    fn from(to: UserId) -> Self {
        Self { to, assistant_of: None }
    }
}

impl std::fmt::Display for Recipient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.assistant_of {
            Some(human) => write!(f, "{} (assistant of {human})", self.to),
            None => write!(f, "{}", self.to),
        }
    }
}

/// Who gets what `me` sends without naming anyone: the human's assistant while the grant naming it stands, as the
/// CLI verifies it for the team of `account` ([`assistant::designated`]), else the messenger.
pub(crate) async fn unaddressed(js: &jetstream::Context, me: &humans::Me, account: Option<&str>) -> Recipient {
    if me.role.is_human()
        && let Some(assistant) = assistant::designated(js, account, &me.id).await
    {
        return Recipient { to: assistant, assistant_of: Some(me.id.clone()) };
    }
    UserId::messenger().into()
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
        if message.sealed.is_some_and(|s| s != sealed::State::Opened) {
            bail!("message {id} is sealed and does not open on this device: there is nothing to quote");
        }
        let from = message.from.clone().ok_or_else(|| anyhow!("message {id} names no sender to reply to"))?;
        if let Some(thread) = thread.filter(|t| *t != message.thread_id) {
            bail!("message {id} is on thread {}, not {thread}", message.thread_id);
        }
        let quote = Quote::new(from, &message.text);
        let replied = Replied { id, quote, sealed: message.sealed.is_some(), channel: message.channel };
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
) -> Result<thread::Entry> {
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

/// The message a send replies to ([`Target::reply_to`]): its id, its quote, whether it was sealed, and the channel of
/// a post.
struct Replied {
    id: Uuid,
    quote: Quote,
    sealed: bool,
    channel: Option<ChannelId>,
}

impl Replied {
    /// The quote as a reply carries it, `sealed` or not: a sealed message's excerpt only goes sealed.
    fn quote(self, sealed: bool) -> Quote {
        if self.sealed && !sealed { self.quote.withheld() } else { self.quote }
    }
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
    let author = replied.as_ref().map(|r| &r.quote.from).filter(|from| **from != nats.me.id);
    let (to, unaddressed) = addressee(&js, nats, to.or_else(|| author.cloned())).await;
    let mut envelope = match &forwarded {
        Some(source) => source.envelope(&nats.me, to, &text),
        None => ask_envelope(&nats.me, thread, to, text),
    };
    if let Some(r) = &replied {
        envelope = envelope.quoting(r.id, r.quote.clone());
    }
    envelope.attachments = attachments::references(&files);
    envelope.queue = queue;
    let to_human = nats.me.role.is_human() && humans::load(&js, &nats.me).await.contains(&envelope.to);
    let text = envelope.text.clone();
    let sealer = sealing(nats, &js, &mut envelope, &mut files, to_human && !plaintext).await?;
    // Sealing took the quote into the sealed body; one left is in the clear.
    if let Some(r) = replied.filter(|_| envelope.quote.is_some()) {
        envelope.quote = Some(r.quote(false));
    }
    let (envelope, files) = (envelope, files);
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
    print_attached(files, |id| envelope.attachment_handle(id));
}

/// The replies in `inbox` on `thread`, those with a forged sender skipped.
fn replies_on(inbox: bus::LiveInbox, thread: ThreadId) -> impl futures::Stream<Item = Envelope> {
    inbox.filter_map(move |msg| {
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
    println!("{}: {}", opened.from, opened.text);
    print!("{}", attachments::render(&attachments::listed_on(&opened.subject(), &opened.attachments), "  "));
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

/// What a send kept in the leaf node's outbox prints.
pub(crate) const OUTBOX: &str = "kept in this machine's outbox until the hub is reachable";

/// The warnings `ants ask` prints for sending to `to` when no daemon serves it now ([`colony::unserved_warnings`]),
/// none for a human; and whether `to` is a human, as `me` tells: a human knows the humans, an ant asks only of a
/// user the colony lacks.
pub(crate) async fn unserved(js: &jetstream::Context, me: &humans::Me, to: &UserId) -> (Vec<String>, bool) {
    let warnings = colony::unserved_warnings(js, &[to]).await;
    let to_human = (me.role.is_human() || !warnings.is_empty()) && humans::load(js, me).await.contains(to);
    (if to_human { Vec::new() } else { warnings }, to_human)
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

/// Seal `envelope` as `ants chat` sends it, when from a human to another human, with what `sealing` reads, and
/// `files`, those it carries, which become what to store: refused when it cannot be sealed, as `ants chat` has no
/// `--plaintext`.
pub(crate) async fn seal_to_human(
    js: &jetstream::Context,
    me: &humans::Me,
    sealing: &sealed::Sealing,
    envelope: &mut Envelope,
    files: &mut [attachments::File],
) -> Result<()> {
    if me.role.is_human() && humans::load(js, me).await.contains(&envelope.to) {
        sealing.sealer(js, &envelope.to, sealed::PLAINTEXT_ASK).await?.seal(envelope, files)?;
    }
    Ok(())
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

/// Send `envelope` from [`ask_envelope`] as `ants ask` does: its owner recorded for the thread and recipient first,
/// then the message stored in the hub's stream, or else in the leaf node's outbox.
pub(crate) async fn deliver(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    envelope: &Envelope,
) -> Result<bus::Stored> {
    if let Some(owner) = &envelope.owner {
        owners::recording(js, js_domain, &envelope.thread_id, &envelope.to, owner).await;
    }
    bus::publish(js, envelope).await
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
    let mut posting = Posting::compose(&js, &nats.me, channel, thread, text).await?;
    if let Some(r) = replied {
        posting.post = posting.post.quoting(r.id, r.quote(false));
    }
    posting.post.attachments = attachments::references(&files);
    posting.post.queue = queue;
    for warning in &posting.warnings {
        tracing::warn!("{warning}");
    }
    let post = &posting.post;
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
        println!("{by}: {}", reply.text);
        print!("{}", attachments::render(&attachments::listed_on(&reply.subject(), &reply.attachments), "  "));
        Ok(())
    });
    Box::pin(with_reactions(shown_replies, &shown, changes)).await
}

/// A post `ants post` makes, with the warnings it prints before posting.
pub(crate) struct Posting {
    pub post: ChannelPost,
    /// The channel's entry, None when there is no such channel.
    entry: Option<ChannelEntry>,
    humans: Humans,
    /// That there is no such channel, and the users the post @mentions that it neither wakes nor notifies.
    pub warnings: Vec<String>,
}

impl Posting {
    /// The post of `text` that `me` makes to `channel`, or on its `thread` ([`channel::compose`]).
    pub async fn compose(
        js: &jetstream::Context,
        me: &humans::Me,
        channel: ChannelId,
        thread: Option<ThreadId>,
        text: String,
    ) -> Result<Self> {
        let humans = humans::load(js, me).await;
        let post = channel::compose(js, me, &humans, channel, thread, text).await;
        let entry = channel::get(&channel::open(js).await?, &post.channel).await?;
        let mut warnings = Vec::new();
        if entry.is_none() {
            warnings.push(format!(
                "no channel #{0} (`ants channel create {0}` makes one): the post wakes nobody",
                post.channel
            ));
        }
        for user in channel::idle_mentions(&post, entry.as_ref()) {
            warnings
                .push(format!("@{user} is no member of #{}: the post neither wakes nor notifies {user}", post.channel));
        }
        Ok(Self { post, entry, humans, warnings })
    }

    /// Store the post, then, when `me` is a human, notify the humans it @mentions ([`channel::notify_mentioned`]):
    /// an ant's daemon notifies those of its posts. Returns the post's stream sequence, None when kept in the leaf
    /// node's outbox, and the humans notified.
    pub async fn deliver(&self, js: &jetstream::Context, me: &humans::Me) -> Result<(Option<u64>, Vec<UserId>)> {
        let post = &self.post;
        let stored = bus::store_acked(js, post.subject(), post, &post.id.to_string()).await?.map(|ack| ack.sequence);
        let notified = if me.role.is_human() {
            channel::notify_mentioned(js, post, self.entry.as_ref(), &self.humans).await
        } else {
            Vec::new()
        };
        Ok((stored, notified))
    }
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
) -> Result<()> {
    if text.is_empty() && !attach.paths.is_empty() {
        bail!("--attach posts to the notes: give TEXT to post with the files");
    }
    let files = attach.read()?;
    let (_, js) = nats.connect("ants-cli").await?;
    let thread = thread.resolve(nats, &js).await?;
    if !text.is_empty() {
        let post = post_notes(&js, &nats.me.id, thread, text.join(" "), &files).await?;
        println!("{}", notes_posted(&post));
        print_attached(&files, |id| post.attachment_handle(id));
        return Ok(());
    }
    let read = notes::read(&js, &nats.me.id, nats.me.reading(), &thread).await?;
    if read.posts.is_empty() {
        println!("thread {thread} has no notes");
    }
    let behalf = crate::people::behalf::Checker::new(nats, &js);
    for post in &read.posts {
        let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
        let by = behalf.label(&post.from, post.on_behalf_of.as_ref()).await;
        println!("{time} {by} ({} turns left): {}", post.turn_budget, post.text);
        print!("{}", attachments::render(&attachments::listed_on(&post.subject(), &post.attachments), "  "));
    }
    Ok(())
}

/// Post `text` from `me` to the notes of `thread` with `files` attached, stored before it, as `ants notes THREAD TEXT`
/// does: in the clear, waking nobody, with a fresh turn budget.
pub(crate) async fn post_notes(
    js: &jetstream::Context,
    me: &UserId,
    thread: ThreadId,
    text: String,
    files: &[attachments::File],
) -> Result<NotesPost> {
    let mut post = NotesPost::new(thread, me.clone(), text);
    post.attachments = attachments::references(files);
    attachments::store(js, files, |id| post.attachment_handle(id)).await?;
    notes::publish(js, &post, None).await?;
    Ok(post)
}

/// What `ants notes` prints once it posted `post`.
pub(crate) fn notes_posted(post: &NotesPost) -> String {
    format!("posted to the notes of thread {} ({} turns left)", post.thread_id, post.turn_budget)
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

/// What `ants react` prints once it added `reaction` to `message`, or with `remove` removed it, and whether that
/// `changed` anything.
pub(crate) fn reacted_line(reaction: &Reaction, message: Uuid, remove: bool, changed: bool) -> String {
    match (remove, changed) {
        (true, true) => format!("removed {reaction} from message {message}"),
        (true, false) => format!("no {reaction} reaction to message {message} to remove"),
        (false, true) => format!("reacted {reaction} to message {message}"),
        (false, false) => format!("already reacted {reaction} to message {message}"),
    }
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

/// An answer's picks: made already, one per question, or words to read them from ([`ants_proto::Questions::picks`]).
pub(crate) enum Picks {
    Words(Vec<String>),
    Made(Vec<Pick>),
}

/// `human`'s answer `picks` to the questions message `id`, read back from those to `human`, asks: a message `human`
/// owns the thread of, as `ants ask` sends. Fails when no such message is kept, it asks no question, a human asked it,
/// or `picks` is no answer to it. Only ants ask questions (`ask_human`), so an answer goes to an ant, in the clear
/// like any message to one; a question from a human, which no CLI sends, is refused rather than answered unsealed.
pub(crate) async fn answering(js: &jetstream::Context, human: &UserId, id: Uuid, picks: Picks) -> Result<Envelope> {
    let question = questions::find(js, "", Reading::Gets, &subject::inbox_of(human), id)
        .await?
        .ok_or_else(|| anyhow!("no message {id} to {human} is kept"))?;
    let me = humans::Me { id: human.clone(), role: Role::Human, assistant_of: None };
    let asker = &question.from;
    if humans::load(js, &me).await.contains(asker) {
        bail!(
            "message {id} is from {asker}, a human, and only an ant's question takes an answer: `ants ask @{asker}` replies"
        );
    }
    let picks = match (picks, question.asks()) {
        (Picks::Made(picks), _) => picks,
        (Picks::Words(words), Some(asked)) => asked.picks(&words)?,
        (Picks::Words(_), None) => Vec::new(),
    };
    Ok(Envelope::answering(&question, human.clone(), picks)?.with_owner(Some(human.clone())))
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
        assert_eq!((to.unwrap().as_str(), text.as_str()), ("dev-b", "check the build"));
    }

    #[test]
    fn words_naming_no_one_leave_the_recipient_open() {
        let (to, text) = recipient(words("who owns billing?"), &compose::Input::default()).unwrap();
        assert_eq!((to, text.as_str()), (None, "who owns billing?"));
    }

    #[test]
    fn an_ask_comes_from_and_belongs_to_the_cli_human() {
        let alice: UserId = "alice".parse().unwrap();
        let me = humans::Me { id: alice.clone(), role: ants_proto::Role::Human, assistant_of: None };
        let ask = ask_envelope(&me, None, "dev-b".parse().unwrap(), "check the build".into());
        assert_eq!((&ask.from, ask.to.as_str(), ask.owner.as_ref()), (&alice, "dev-b", Some(&alice)));
        let thread: ThreadId = "t1".parse().unwrap();
        let follow_up = ask_envelope(&me, Some(thread.clone()), "dev-b".parse().unwrap(), "and the docs".into());
        assert_eq!((follow_up.thread_id, follow_up.owner), (thread, Some(alice)));
    }

    #[test]
    fn an_ask_with_an_ant_s_credentials_has_no_owner() {
        let me = humans::Me { id: "dev-a".parse().unwrap(), role: ants_proto::Role::Ant, assistant_of: None };
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

    fn picked(words: &[&str]) -> Picks {
        Picks::Words(words.iter().map(|w| (*w).to_owned()).collect())
    }

    #[tokio::test]
    async fn a_human_answers_a_question_asked_of_it_with_an_option_or_typed_text() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, dev_a): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        bus::ensure_stream(&js).await.unwrap();
        let options = ["yes".to_owned(), "not yet".to_owned()];
        let question = ants_proto::Question::new("Ship it?", &options, false).unwrap();
        let asked = Envelope::asking(ThreadId::generate(), dev_a.clone(), alice.clone(), question);
        let plain = Envelope::new(dev_a.clone(), alice.clone(), "hi");
        let mut forged = asked.clone();
        forged.id = Uuid::now_v7();
        for (subject, envelope) in [
            (asked.subject(), &asked),
            (plain.subject(), &plain),
            (subject::inbox(&alice, &"dev-b".parse().unwrap()), &forged),
        ] {
            js.publish(subject, serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
        }

        let answer = answering(&js, &alice, asked.id, picked(&["2"])).await.unwrap();
        assert_eq!((&answer.to, &answer.thread_id, answer.text.as_str()), (&dev_a, &asked.thread_id, "not yet"));
        assert_eq!((answer.owner.as_ref(), answer.in_reply_to), (Some(&alice), None));
        assert!(answer.answers(&asked).is_some());
        let refused = |picks| answering(&js, &alice, asked.id, picks);
        let wrong = refused(picked(&["3"])).await.unwrap_err().to_string();
        assert!(wrong.contains("no option 3; pick 1-2"), "{wrong}");
        let typed = refused(picked(&["after", "lunch"])).await.unwrap_err().to_string();
        assert!(typed.contains("takes only"), "{typed}");
        let none = answering(&js, &alice, plain.id, picked(&["1"])).await.unwrap_err().to_string();
        assert!(none.contains("asks no question"), "{none}");
        let unknown = answering(&js, &alice, forged.id, picked(&["1"])).await.unwrap_err().to_string();
        assert!(unknown.contains("is kept"), "a forged question is none: {unknown}");

        // A question from a human, which only a hand-made message carries, is not answered in the clear.
        let bob: UserId = "bob".parse().unwrap();
        let (_, bob_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
        humans::register(&bob_js, &humans::Me { id: bob.clone(), role: ants_proto::Role::Human, assistant_of: None })
            .await
            .unwrap();
        let options = ["yes".to_owned(), "no".to_owned()];
        let question = ants_proto::Question::new("Lunch?", &options, true).unwrap();
        let by_bob = Envelope::asking(ThreadId::generate(), bob.clone(), alice.clone(), question);
        bob_js.publish(by_bob.subject(), serde_json::to_vec(&by_bob).unwrap().into()).await.unwrap().await.unwrap();
        let human = answering(&js, &alice, by_bob.id, picked(&["1"])).await.unwrap_err().to_string();
        assert!(human.contains("from bob, a human") && human.contains("`ants ask @bob`"), "{human}");
    }

    #[tokio::test]
    async fn a_human_answers_several_questions_with_a_pick_for_each() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, dev_a): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        bus::ensure_stream(&js).await.unwrap();
        let regions = ["eu".to_owned(), "us".to_owned(), "ap".to_owned()];
        let regions = ants_proto::Question::new("Which regions?", &regions, false).unwrap().multi().headed("Regions");
        let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        let questions = ants_proto::Questions::new(vec![regions, ship]).unwrap();
        let asked = Envelope::asking(ThreadId::generate(), dev_a.clone(), alice.clone(), questions);
        js.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();

        let answer = answering(&js, &alice, asked.id, picked(&["3,1", "after lunch"])).await.unwrap();
        let picks = &answer.answer.as_ref().unwrap().picks;
        assert_eq!(picks, &[Pick::Options(vec![1, 3]), Pick::Typed("after lunch".to_owned())]);
        assert_eq!(answer.text, "Regions: eu, ap\nQ2: after lunch");
        let refused = |words| answering(&js, &alice, asked.id, picked(words));
        let short = refused(&["1"]).await.unwrap_err().to_string();
        assert!(short.contains("1 answers to 2 questions"), "{short}");
        let wrong = refused(&["1", "3"]).await.unwrap_err().to_string();
        assert!(wrong.contains("question 2: no option 3"), "{wrong}");
        let made = answering(&js, &alice, asked.id, Picks::Made(vec![Pick::option(2), Pick::option(1)])).await;
        assert_eq!(made.unwrap().text, "Regions: us\nQ2: yes");
    }
}
