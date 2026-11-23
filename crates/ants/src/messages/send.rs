//! What the CLI's human sends, as `ants ask`, `ants post`, `ants notes`, `ants react` and `ants answer` send it and
//! `ants chat` and the MCP tools do too: who gets a message naming no one, the envelope and its delivery, a post with
//! its warnings, a note, the size one message carries, an answer to an ant's question, and the lines printed.

use ants_daemon::bus;
use ants_daemon::records::owners;
use ants_daemon::stream::reading::Reading;
use ants_daemon::stream::{notes, questions};
use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, Envelope, Humans, JsDomain, NotesPost, Pick, Quote, Reaction, Role, ThreadId,
    UserId, Uuid, subject,
};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream;

use crate::messages::attachments;
use crate::people::{assistant, humans, sealed};
use crate::{channel, colony};

/// The message or post a send replies to, quoting it ([`Envelope::quoting`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Quoted {
    pub id: Uuid,
    pub quote: Quote,
    /// Whether the message was sealed: only a sealed reply carries its excerpt ([`Quote::withheld`]).
    pub sealed: bool,
}

impl Quoted {
    /// The quote as a reply carries it, `sealed` or not: a sealed message's excerpt only goes sealed.
    pub fn quote(self, sealed: bool) -> Quote {
        if self.sealed && !sealed { self.quote.withheld() } else { self.quote }
    }
}

pub(crate) use ants_client::sending::Recipient;

/// Who gets what `me` sends without naming anyone: the human's assistant while the grant naming it stands, as the
/// CLI verifies it for the team of `account` ([`assistant::designated`]), else the messenger.
pub(crate) async fn unaddressed(js: &jetstream::Context, me: &humans::Me, account: Option<&str>) -> Recipient {
    let assistant = if me.role.is_human() { assistant::designated(js, account, &me.id).await } else { None };
    ants_client::sending::unaddressed(&me.id, assistant)
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
    ants_client::sending::ask_envelope(&me.id, me.role.is_human(), thread, to, text)
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

/// Post `text` from `me` to the notes of `thread` with `files` attached, stored before it, as `ants notes THREAD TEXT`
/// does: in the clear, waking nobody, with a fresh turn budget.
pub(crate) async fn post_notes(
    js: &jetstream::Context,
    me: &UserId,
    thread: ThreadId,
    text: String,
    files: &[attachments::File],
    attach: &str,
) -> Result<NotesPost> {
    let mut post = NotesPost::new(thread, me.clone(), text);
    post.attachments = attachments::references(files);
    check_fits(js, &post, attach)?;
    attachments::store(js, files, |id| post.attachment_handle(id)).await?;
    notes::publish(js, &post, None).await?;
    Ok(post)
}

/// Room a message leaves for its headers within the server's `max_payload`.
const HEADER_ROOM: u64 = 1024;

/// Refuse `message` when its JSON is longer than one NATS message carries, the `max_payload` of the server `js` is
/// connected to less [`HEADER_ROOM`], before anything of it is stored; suggesting to put the text in a file and
/// `attach` it, how the caller attaches one (`send it with --attach` on the command line).
pub(crate) fn check_fits(js: &jetstream::Context, message: &impl serde::Serialize, attach: &str) -> Result<()> {
    let max_payload = js.client().server_info().max_payload as u64;
    let len = serde_json::to_vec(message)?.len() as u64;
    let max = max_payload.saturating_sub(HEADER_ROOM);
    if max_payload == 0 || len <= max {
        return Ok(());
    }
    let (len, max) = ants_client::attachments::sizes(len, max);
    bail!("the message is {len}; one message carries at most {max}: put the text in a file and {attach} instead")
}

/// What `ants notes` prints once it posted `post`.
pub(crate) fn notes_posted(post: &NotesPost) -> String {
    format!("posted to the notes of thread {} ({} turns left)", post.thread_id, post.turn_budget)
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
