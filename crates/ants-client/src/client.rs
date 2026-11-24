//! A human's reads and writes over a [`Transport`], as `ants ask`, `ants inbox` and `ants chat` make them with
//! async-nats: the stream read back window by window ([`Client::reader`]), a message sent with its thread's owner
//! recorded first and stored once the stream acks it ([`Client::send`]), the human's read marker moved by
//! compare-and-set ([`Client::mark_read`]), and what is heard live ([`Client::status`], [`Client::inbox`]); beside
//! them the colony as it is now ([`Client::colony`]) and a device's presence ([`Client::present`]).
//!
//! This is what a binding wraps, for a browser or another platform: [`Client`], [`Reader`] and the feeds carry no
//! lifetime or type parameter, and what their calls take and return are owned values that serialize.
//!
//! Unlike the CLI behind a leaf node, a client here speaks to the server holding the stream: a send the stream does
//! not ack fails, with no outbox to keep it.

use std::rc::Rc;

use ants_proto::{
    AssistantGrant, ColonyEntry, DeviceId, Envelope, Following, JsDomain, Presence, ReadMarker, StatusEvent, ThreadId,
    UserId, Uuid, subject,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entry::Entry;
use crate::error::{Error, ErrorKind};
use crate::inbox;
use crate::parser::Parser;
use crate::requests::{self, Ack, Got, Position, REQUEST_WAIT};
use crate::sending::{self, Recipient, Send};
use crate::transport::{Connect, Message, Subscription, Transport};

/// Who a client acts as, and where.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// The human: the name its credentials carry, or `human` on a server without authentication.
    pub human: UserId,
    /// The JetStream domain of the hub, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<JsDomain>,
    /// The account that issued the human's credentials (`A...`), by which the grant of its assistant is verified; none
    /// on a server without authentication, where no grant counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// The name the connection tells the server.
    pub name: String,
}

/// Which of the stream's messages a [`Reader`] reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// The inbox messages, every user's: what the threads with ants are made of.
    #[default]
    Inboxes,
    /// Every message of ants', as `ants thread` reads them.
    Everything,
}

impl Scope {
    fn filter(self) -> &'static str {
        match self {
            Self::Inboxes => subject::ALL_INBOXES,
            Self::Everything => subject::ALL,
        }
    }
}

/// The part of the stream a [`Reader`] reads, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    #[serde(default)]
    pub scope: Scope,
    /// From the message at this sequence on, as a read going on where another stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,
    /// Else from the first message stored then or later; without either, from the first message kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
    /// Up to the message at this sequence, as a read of what came before a window already read; without it, up to
    /// the last message stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_seq: Option<u64>,
}

/// What one batched get of a [`Reader`] found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Page {
    /// The entries of the messages read, in stream order.
    pub entries: Vec<Entry>,
    /// How many stored messages were skipped for parsing as no message of ants', an inbox message that is no
    /// [`Envelope`] among them.
    pub skipped: u64,
    /// How many stored messages were refused as ones a stream relayed ([`crate::relayed`]).
    pub relayed: u64,
    /// The sequence the next page starts at, which a later [`Window::from_seq`] reads on from.
    pub next_seq: u64,
    /// Whether the window is read to its end.
    pub done: bool,
}

/// The human's read marker with the revision of its entry, 0 when there is none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Marked {
    pub marker: ReadMarker,
    pub revision: u64,
}

/// The messages to mark read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkRead {
    /// Their stream sequences.
    pub seqs: Vec<u64>,
    /// The sequence the client read the human's inbox from, and the sequences, in order, of every message to the
    /// human stored from there on: with them the marker moves past the read messages that follow it
    /// ([`inbox::mark_read`]). Without them each message read keeps an entry of its own in the marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbox: Vec<u64>,
}

/// A message sent: stored, as the stream acked it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sent {
    pub envelope: Envelope,
    /// Who got it, when the message named no one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unaddressed: Option<Recipient>,
    pub ack: Ack,
    /// Why the thread's owner was not recorded, if it was not: the message went out all the same, and ants' messages
    /// on its thread name no owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_unrecorded: Option<String>,
}

/// A human's client on a connection. Cloning it shares the connection.
#[derive(Clone)]
pub struct Client {
    transport: Rc<dyn Transport>,
    session: Rc<Session>,
}

impl Client {
    pub fn new(transport: Rc<dyn Transport>, session: Session) -> Self {
        Self { transport, session: Rc::new(session) }
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Connect under the reply subjects of the human, the only ones its rights let it hear answers on.
    pub async fn connect(&self) -> Result<(), Error> {
        let Session { name, human, .. } = &*self.session;
        let options = Connect { name: name.clone(), inbox_prefix: ants_proto::inbox_prefix(human) };
        self.transport.connect(options).await.map_err(|e| Error::transport("connecting", e))
    }

    fn domain(&self) -> Option<&JsDomain> {
        self.session.domain.as_ref()
    }

    async fn request(&self, what: &str, message: Message) -> Result<Message, Error> {
        self.transport.request(message, REQUEST_WAIT).await.map_err(|e| Error::transport(what, e))
    }

    async fn subscribe(&self, what: &str, subject: String) -> Result<Box<dyn Subscription>, Error> {
        self.transport.subscribe(subject).await.map_err(|e| Error::transport(what, e))
    }

    /// A reader of `window`, which reads nothing until asked for a page ([`Reader::next`]).
    pub fn reader(&self, window: Window) -> Reader {
        let at = match (window.from_seq, window.since) {
            (Some(seq), _) => Position::from_seq(seq),
            (None, Some(since)) => Position::since(since),
            (None, None) => Position::all(),
        };
        Reader {
            client: self.clone(),
            parser: Parser::default(),
            filter: window.scope.filter(),
            at,
            until: window.until_seq,
            replies: None,
            done: false,
        }
    }

    /// The human's read marker as stored now; one that does not parse reads as none, to start over from.
    pub async fn marker(&self) -> Result<Marked, Error> {
        const WHAT: &str = "reading the read marker";
        let answer = self.request(WHAT, requests::marker_get(self.domain(), &self.session.human)).await?;
        let entry = requests::kv_entry(WHAT, answer)?;
        let marker = entry.value.and_then(|value| inbox::marker_of(&value).ok()).unwrap_or_default();
        Ok(Marked { marker, revision: entry.revision })
    }

    /// Mark the messages of `read` read on all the human's devices, on top of whatever other devices marked
    /// meanwhile.
    pub async fn mark_read(&self, read: MarkRead) -> Result<Marked, Error> {
        let MarkRead { seqs, start, inbox } = read;
        self.change("marking read", &seqs, |marker| inbox::mark_read(marker, &seqs, start, &inbox)).await
    }

    /// Mark the messages at `seqs` unread again on all the human's devices.
    pub async fn mark_unread(&self, seqs: Vec<u64>) -> Result<Marked, Error> {
        self.change("marking unread", &seqs, |marker| inbox::mark_unread(marker, &seqs)).await
    }

    /// Change the marker with `change` for `seqs` by compare-and-set, trying again while other devices move it first.
    async fn change(&self, what: &str, seqs: &[u64], change: impl Fn(&mut ReadMarker)) -> Result<Marked, Error> {
        if seqs.is_empty() {
            return self.marker().await;
        }
        for _ in 0..inbox::CONFLICTS {
            let Marked { mut marker, revision } = self.marker().await?;
            change(&mut marker);
            let put = requests::marker_put(self.domain(), &self.session.human, &marker, revision);
            let answer = self.request(what, put).await?;
            if let Some(revision) = requests::kv_updated(what, &answer)? {
                return Ok(Marked { marker, revision });
            }
        }
        Err(Error::new(what, ErrorKind::Conflict))
    }

    /// The human's assistant while the grant naming it stands, read and verified now; none without an account to
    /// verify it by, without a grant, or with one that cannot be read or does not verify.
    pub async fn assistant(&self) -> Option<UserId> {
        const WHAT: &str = "reading the assistant grant";
        let Session { human, account, .. } = &*self.session;
        let account = account.as_deref()?;
        let answer = self.request(WHAT, requests::grant_get(self.domain(), human)).await.ok()?;
        let value = requests::kv_entry(WHAT, answer).ok()?.value?;
        let grant = serde_json::from_slice::<AssistantGrant>(&value).ok()?;
        sending::designated(&grant, account, human, Utc::now()).ok()?
    }

    /// Send `send` as `ants ask` does: to whom it names, else to the human's assistant while its grant stands, else
    /// to the messenger; the human recorded as the thread's owner for the recipient first; then the envelope
    /// published under its id. Resolves once the stream acked it.
    pub async fn send(&self, send: Send) -> Result<Sent, Error> {
        const WHAT: &str = "sending the message";
        let me = &self.session.human;
        let (to, unaddressed) = match send.to.clone() {
            Some(to) => (to, None),
            None => {
                let recipient = sending::unaddressed(me, self.assistant().await);
                (recipient.to.clone(), Some(recipient))
            }
        };
        let envelope = sending::compose(me, to, send)?;
        let message = requests::publish(&envelope)?;
        let owner_unrecorded = self.record_owner(&envelope).await.err().map(|e| e.to_string());
        let ack = requests::stored(WHAT, &self.request(WHAT, message).await?)?;
        Ok(Sent { envelope, unaddressed, ack, owner_unrecorded })
    }

    async fn record_owner(&self, envelope: &Envelope) -> Result<(), Error> {
        const WHAT: &str = "recording the thread's owner";
        let Some(owner) = &envelope.owner else { return Ok(()) };
        let record = requests::owner_record(self.domain(), &envelope.thread_id, &envelope.to, owner);
        requests::owner_recorded(&self.request(WHAT, record).await?)
    }

    /// The colony as it is now: the entry of each ant served, by name, read by a batched direct get of the last of
    /// each. Entries that do not parse are passed over.
    pub async fn colony(&self) -> Result<Vec<ColonyEntry>, Error> {
        const WHAT: &str = "reading the colony";
        let reply = format!("{}.{}", ants_proto::inbox_prefix(&self.session.human), Uuid::now_v7().simple());
        let mut answers = self.subscribe(WHAT, reply.clone()).await?;
        let get = requests::colony_get(self.domain(), reply);
        self.transport.publish(get).await.map_err(|e| Error::transport(WHAT, e))?;
        let mut entries = Vec::new();
        loop {
            let answer = answers.next(Some(REQUEST_WAIT)).await.map_err(|e| Error::transport(WHAT, e))?;
            match requests::batch_answer(answer)? {
                Got::Stored(stored) => entries.extend(requests::colony_entry(&stored)),
                Got::Relayed(_) => {}
                Got::End { .. } | Got::Nothing => break,
            }
        }
        entries.sort_by(|a, b| a.ant.cmp(&b.ant));
        Ok(entries)
    }

    /// The presence of the human's `device` while it follows the inbox, as a page open on it does: put it with
    /// [`Client::present`] every [`ants_proto::PRESENCE_HEARTBEAT`], and delete it with [`Client::absent`] on
    /// leaving.
    pub fn presence(&self, device: DeviceId) -> Presence {
        Presence::new(self.session.human.clone(), device, Following::Inbox)
    }

    /// Put `presence`, resolving once stored.
    pub async fn present(&self, presence: &Presence) -> Result<(), Error> {
        const WHAT: &str = "putting the device's presence";
        let answer = self.request(WHAT, requests::presence_put(self.domain(), presence)).await?;
        requests::stored(WHAT, &answer).map(|_| ())
    }

    /// Delete `presence`, resolving once stored.
    pub async fn absent(&self, presence: &Presence) -> Result<(), Error> {
        const WHAT: &str = "deleting the device's presence";
        let answer = self.request(WHAT, requests::presence_delete(self.domain(), presence)).await?;
        requests::stored(WHAT, &answer).map(|_| ())
    }

    /// The live progress of every ant on `thread`, from now on.
    pub async fn status(&self, thread: ThreadId) -> Result<StatusFeed, Error> {
        let updates = self.subscribe("following the thread's ants", subject::status_on(&thread)).await?;
        Ok(StatusFeed { updates })
    }

    /// The messages to the human as they are published, from now on: none carries its stream sequence, so a client
    /// takes one as the sign to read the stream on from where it stopped.
    pub async fn inbox(&self) -> Result<InboxFeed, Error> {
        let messages = self.subscribe("following the inbox", subject::inbox_of(&self.session.human)).await?;
        Ok(InboxFeed { messages })
    }
}

/// Reads a [`Window`] of the stream page by page, one batched direct get each, so a client shows what it has before
/// reading on. A reader takes what a human's CLI reader takes: it skips a stored message that parses as no message of
/// ants' and refuses one a stream relayed. Sealed messages are entries marked sealed.
pub struct Reader {
    client: Client,
    parser: Parser,
    filter: &'static str,
    at: Position,
    until: Option<u64>,
    /// Where the answers to this reader's gets come, once it asked for a page.
    replies: Option<(String, Box<dyn Subscription>)>,
    done: bool,
}

impl Reader {
    /// The next page of the window; one with nothing and `done` once the window is read to its end.
    pub async fn next(&mut self) -> Result<Page, Error> {
        let mut page = Page { entries: Vec::new(), skipped: 0, relayed: 0, next_seq: self.at.seq, done: self.done };
        if self.done {
            return Ok(page);
        }
        let read = self.read(&mut page).await;
        if read.is_err() || self.done {
            // Answers still on their way belong to no later get.
            self.replies = None;
        }
        read?;
        (page.next_seq, page.done) = (self.at.seq, self.done);
        Ok(page)
    }

    async fn read(&mut self, page: &mut Page) -> Result<(), Error> {
        const WHAT: &str = "reading the stream";
        if self.replies.is_none() {
            let prefix = ants_proto::inbox_prefix(&self.client.session.human);
            let reply = format!("{prefix}.{}", Uuid::now_v7().simple());
            let replies = self.client.subscribe(WHAT, reply.clone()).await?;
            self.replies = Some((reply, replies));
        }
        let Some((reply, replies)) = &mut self.replies else { return Ok(()) };
        let get = requests::batch_get(self.client.session.domain.as_ref(), self.filter, &self.at, reply.clone());
        self.client.transport.publish(get).await.map_err(|e| Error::transport(WHAT, e))?;
        let until = self.until;
        let past = |seq: u64| until.is_some_and(|until| seq > until);
        let mut got = 0;
        loop {
            let answer = replies.next(Some(REQUEST_WAIT)).await.map_err(|e| Error::transport(WHAT, e))?;
            match requests::batch_answer(answer)? {
                Got::Nothing => self.done = true,
                Got::End { pending } => self.done = got == 0 || pending == Some(0),
                Got::Relayed(seq) if past(seq) => self.done = true,
                Got::Relayed(seq) => {
                    got += 1;
                    page.relayed += 1;
                    self.at.after(seq);
                    continue;
                }
                Got::Stored(stored) if past(stored.seq) => self.done = true,
                Got::Stored(stored) => {
                    got += 1;
                    self.at.after(stored.seq);
                    match self.parser.entry(stored.seq, stored.stored_at, &stored.subject, &stored.payload) {
                        Some(entry) => page.entries.push(entry),
                        None => page.skipped += 1,
                    }
                    continue;
                }
            }
            return Ok(());
        }
    }
}

/// The live progress of the ants on a thread ([`Client::status`]). Dropping it ends the subscription.
pub struct StatusFeed {
    updates: Box<dyn Subscription>,
}

impl StatusFeed {
    /// The next update, waited for; those a stream relayed, that do not parse or that name another ant or thread
    /// than their subject are passed over. Fails once the subscription has ended.
    pub async fn next(&mut self) -> Result<StatusEvent, Error> {
        loop {
            let message =
                self.updates.next(None).await.map_err(|e| Error::transport("following the thread's ants", e))?;
            if let Some(event) = requests::status_event(&message) {
                return Ok(event);
            }
        }
    }
}

/// The messages to the human as they are published ([`Client::inbox`]). Dropping it ends the subscription.
pub struct InboxFeed {
    messages: Box<dyn Subscription>,
}

impl InboxFeed {
    /// The next message, waited for; those a stream relayed, that are no [`Envelope`] or whose sender is forged are
    /// passed over. Fails once the subscription has ended.
    pub async fn next(&mut self) -> Result<Envelope, Error> {
        loop {
            let message = self.messages.next(None).await.map_err(|e| Error::transport("following the inbox", e))?;
            if let Some(envelope) = requests::heard(&message) {
                return Ok(envelope);
            }
        }
    }
}

#[cfg(test)]
mod tests;
