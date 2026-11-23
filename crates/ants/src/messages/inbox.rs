//! A human's inbox: the messages to the human in the stream, read by message gets ([`Reading::Gets`]), so every device of
//! the human reads all of them, and its [`ReadMarker`] in [`READ_BUCKET`](ants_proto::READ_BUCKET), shared by those devices, which says which
//! it has read on any of them.
//!
//! Showing a message marks it read, unless peeking, for every device at once: the marker is updated with
//! compare-and-set, so devices reading at the same time each keep the other's marks. Nothing is consumed, so a reply
//! one device reads still reaches the others following the inbox.

use ants_daemon::bus::relayed::GuardedKv as _;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::Duration;

use ants_client::attachments::{Listed, listed};
use ants_client::inbox::{CONFLICTS, ParseError, mark_read as mark_in, mark_unread, marker_of, parse};
use ants_client::sealed::State;
use ants_daemon::bus;
use ants_daemon::delivery::unserved;
use ants_daemon::people::humans::Registry;
use ants_daemon::records::reactions::{self, Reacted};
use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{Acting, Envelope, Question, Questions, ReadMarker, Role, ThreadId, UserId, Uuid, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::kv::{self, Operation};
use futures::{FutureExt as _, StreamExt as _};

use crate::ants::avatars::Faces;
use crate::cas;
use crate::messages::attachments;
use crate::people::sealed::Keyring;

/// How often a followed inbox is read again with no message announced, against an announcement missed.
const RESCAN: Duration = Duration::from_secs(30);

/// Which unread messages to take, and whether taking them marks them read.
#[derive(Debug, Clone, Default)]
pub struct Select {
    pub thread: Option<ThreadId>,
    pub limit: Option<usize>,
    /// Show without marking read.
    pub peek: bool,
    /// Take summary requests and replies ([`Envelope::summary`]) too. Otherwise they are left out, and marked read
    /// unless peeking: the CLI that asked takes its reply as it comes.
    pub summaries: bool,
    /// Take an assistant's reports of its acts ([`ThreadId::is_activity`]) too. Otherwise, unless a thread is
    /// picked, they are left out, counted per thread ([`Inbox::reports`]) and marked read unless peeking.
    pub activity: bool,
}

impl Select {
    fn wants(&self, e: &Envelope) -> bool {
        self.thread.as_ref().is_none_or(|t| *t == e.thread_id)
            && (self.summaries || e.summary.is_none())
            && !self.leaves_out_report(e)
    }

    /// Whether `e` is an assistant's report left out, to count instead.
    fn leaves_out_report(&self, e: &Envelope) -> bool {
        !self.activity && self.thread.is_none() && e.thread_id.is_activity()
    }

    /// Whether `e` is left out only for being a summary's, so taken as read.
    fn skips_summary(&self, e: &Envelope) -> bool {
        !self.peek && !self.summaries && e.summary.is_some()
    }
}

/// The read bucket, created when missing. Entries never expire: a marker says what was read until it moves on.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<kv::Store> {
    Box::pin(ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Read))
        .await
        .context("opening the read bucket")
}

/// A message to the human, unless malformed or forged.
type Message = (u64, Result<Envelope, ParseError>);

pub struct Inbox {
    js: jetstream::Context,
    kv: kv::Store,
    human: UserId,
    /// The first sequence read, once a read started.
    start: Option<u64>,
    /// The sequences of the inbox messages read from `start` on, in order, to move the marker past the read ones.
    seqs: Vec<u64>,
    /// The last sequence read.
    cursor: u64,
    taken: usize,
    /// The assistant's reports left out, by activity thread, since [`Inbox::reports`] last took them.
    reports: BTreeMap<ThreadId, usize>,
}

impl Inbox {
    /// The inbox of `human`.
    pub async fn open(js: &jetstream::Context, human: &UserId) -> Result<Self> {
        bus::ensure_stream(js).await?;
        let kv = ensure_bucket(js).await?;
        Ok(Self {
            js: js.clone(),
            kv,
            human: human.clone(),
            start: None,
            seqs: Vec::new(),
            cursor: 0,
            taken: 0,
            reports: BTreeMap::new(),
        })
    }

    /// The unread reports of an assistant's acts left out since last asked, how many on each activity thread.
    pub fn reports(&mut self) -> BTreeMap<ThreadId, usize> {
        std::mem::take(&mut self.reports)
    }

    /// Count `e`, an assistant's report left out, and whether to mark it read.
    fn report(&mut self, select: &Select, e: Envelope) -> bool {
        *self.reports.entry(e.thread_id).or_default() += 1;
        !select.peek
    }

    fn left(&self, select: &Select) -> usize {
        select.limit.map_or(usize::MAX, |limit| limit.saturating_sub(self.taken))
    }

    /// The marker and its revision, 0 when there is none.
    pub(crate) async fn marker(&self) -> Result<(ReadMarker, u64)> {
        let Some(entry) = self.kv.guarded_entry(self.human.as_str()).await.context("reading the read marker")? else {
            return Ok((ReadMarker::default(), 0));
        };
        if entry.operation != Operation::Put {
            return Ok((ReadMarker::default(), entry.revision));
        }
        let marker = marker_of(&entry.value).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "a malformed read marker; starting over");
            ReadMarker::default()
        });
        Ok((marker, entry.revision))
    }

    /// Mark the messages at `seqs` read, on top of whatever other devices marked meanwhile.
    async fn mark(&self, seqs: &[u64]) -> Result<()> {
        self.change(seqs, "marking read", |marker| mark_in(marker, seqs, self.start, &self.seqs)).await
    }

    /// Mark the messages at `seqs` unread again on all the human's devices, on top of whatever other devices marked
    /// meanwhile.
    pub async fn unmark(&self, seqs: &[u64]) -> Result<()> {
        self.change(seqs, "marking unread", |marker| mark_unread(marker, seqs)).await
    }

    /// Change the marker with `change` for `seqs`, `doing` so as errors say, by compare-and-set.
    async fn change(&self, seqs: &[u64], doing: &str, change: impl Fn(&mut ReadMarker)) -> Result<()> {
        if seqs.is_empty() {
            return Ok(());
        }
        for _ in 0..CONFLICTS {
            let (mut marker, revision) = self.marker().await?;
            change(&mut marker);
            let stored = cas::update(&self.kv, self.human.as_str(), serde_json::to_vec(&marker)?, revision).await;
            if stored.with_context(|| doing.to_owned())? {
                return Ok(());
            }
        }
        bail!("{doing}: the read marker kept moving")
    }

    /// The sequences, in order, of the human's inbox messages on `thread` stored from `start` on, but summaries'.
    pub async fn on_thread(&self, thread: &ThreadId, start: DeliverPolicy) -> Result<Vec<u64>> {
        let mut found = Vec::new();
        let filter = subject::inbox_of(&self.human);
        reading::read_back(&self.js, self.human.as_str(), Reading::Gets, &filter, start, |seq, _, subject, payload| {
            if parse(subject, payload).is_ok_and(|e| e.thread_id == *thread && e.summary.is_none()) {
                found.push(seq);
            }
            ControlFlow::Continue(())
        })
        .await
        .context("reading the inbox")?;
        Ok(found)
    }

    /// The messages to the human stored from `from` on.
    async fn read(&mut self, from: u64) -> Result<Vec<Message>> {
        self.start.get_or_insert(from);
        let mut found = Vec::new();
        let filter = subject::inbox_of(&self.human);
        let start = DeliverPolicy::ByStartSequence { start_sequence: from };
        reading::read_back(&self.js, self.human.as_str(), Reading::Gets, &filter, start, |seq, _, subject, payload| {
            found.push((seq, parse(subject, payload)));
            ControlFlow::Continue(())
        })
        .await
        .context("reading the inbox")?;
        for (seq, _) in &found {
            if *seq > self.cursor {
                self.seqs.push(*seq);
                self.cursor = *seq;
            }
        }
        Ok(found)
    }

    /// The selected unread messages available now, oldest first.
    pub async fn unread(&mut self, select: &Select) -> Result<Vec<Envelope>> {
        self.unread_where(select, |_| true).await.map(|found| found.into_iter().map(|(_, e)| e).collect())
    }

    async fn unread_where(
        &mut self,
        select: &Select,
        wanted: impl Fn(&Envelope) -> bool,
    ) -> Result<Vec<(u64, Envelope)>> {
        let (marker, _) = self.marker().await?;
        let mut found = Vec::new();
        let mut marks = Vec::new();
        for (seq, envelope) in self.read(marker.first_unread()).await? {
            if marker.is_read(seq) {
                continue;
            }
            match envelope {
                Ok(e) if select.wants(&e) && wanted(&e) && self.left(select) > found.len() => found.push((seq, e)),
                Ok(e) if select.skips_summary(&e) => marks.push(seq),
                Ok(e) if select.leaves_out_report(&e) => {
                    if self.report(select, e) {
                        marks.push(seq);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    if !select.peek {
                        tracing::warn!(error = %e, seq, "marking a malformed message read");
                        marks.push(seq);
                    }
                }
            }
        }
        self.taken += found.len();
        if !select.peek {
            marks.extend(found.iter().map(|(seq, _)| *seq));
            self.mark(&marks).await?;
        }
        Ok(found)
    }

    /// Wait for new selected messages and hand each to `show`, its sending device looked up by `keyring` if sealed
    /// ([`Keyring::learn`]), and the assistant's reports left out of each read to `reported` ([`Inbox::reports`]),
    /// until `stop` resolves or the limit is reached. Every
    /// device following the inbox shows each new message, even one another device already read.
    pub async fn follow(
        &mut self,
        select: &Select,
        keyring: Option<&Keyring>,
        behalf: Option<&crate::people::behalf::Checker>,
        mut show: impl FnMut(&Envelope) -> Result<()>,
        mut reported: impl FnMut(BTreeMap<ThreadId, usize>) -> Result<()>,
        stop: impl Future<Output = ()>,
    ) -> Result<()> {
        tokio::pin!(stop);
        // The stream announces each message it stores on the human's own inboxes.
        let client = self.js.client();
        #[expect(clippy::disallowed_methods, reason = "a wake-up alone: the messages are read from the stream")]
        let mut stored = client.subscribe(subject::stored_inbox(&self.human)).await.context("following the inbox")?;
        if self.start.is_none() {
            // Only what is stored from now on.
            let (marker, _) = self.marker().await?;
            self.read(marker.first_unread()).await?;
        }
        let mut rescan = tokio::time::interval(RESCAN);
        rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        rescan.tick().await;
        while self.left(select) > 0 {
            let mut marks = Vec::new();
            for (seq, envelope) in self.read(self.cursor + 1).await? {
                match envelope {
                    Ok(e) if select.wants(&e) && self.left(select) > 0 => {
                        self.taken += 1;
                        if let Some(keyring) = keyring {
                            keyring.learn(&e).await;
                        }
                        if let Some(behalf) = behalf {
                            behalf.learn(&e).await;
                        }
                        show(&e)?;
                        marks.push(seq);
                    }
                    Ok(e) if select.skips_summary(&e) => marks.push(seq),
                    Ok(e) if select.leaves_out_report(&e) => {
                        if self.report(select, e) {
                            marks.push(seq);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, seq, "skipping a malformed message");
                        marks.push(seq);
                    }
                }
            }
            if !select.peek {
                self.mark(&marks).await?;
            }
            let reports = self.reports();
            if !reports.is_empty() {
                reported(reports)?;
            }
            if self.left(select) == 0 {
                break;
            }
            tokio::select! {
                _ = &mut stop => break,
                msg = stored.next() => if msg.is_none() { break },
                _ = rescan.tick() => {}
            }
            // One read takes in every message announced so far.
            while let Some(Some(_)) = stored.next().now_or_never() {}
        }
        Ok(())
    }
}

/// Mark `reply`, already shown by `ants ask --wait`, read in the inbox of `human`. JetStream may store it a moment
/// after it was delivered, so a miss is retried briefly. Returns whether it was found unread.
pub async fn mark_read(js: &jetstream::Context, human: &UserId, reply: &Envelope) -> Result<bool> {
    let mut inbox = Inbox::open(js, human).await?;
    let select = Select { thread: Some(reply.thread_id.clone()), summaries: true, limit: Some(1), ..Select::default() };
    for _ in 0..5 {
        if !inbox.unread_where(&select, |e| e.id == reply.id).await?.is_empty() {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(false)
}

/// A message as `ants inbox --json` prints it: the envelope, each attachment with its handle, and the reactions to it.
#[derive(serde::Serialize)]
pub struct Shown<'a> {
    #[serde(flatten)]
    envelope: Envelope,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<Listed>,
    reactions: &'a [Reacted],
    /// What the CLI made of a sealed message, whose text it opened or shows `[sealed]` or `[forged]` for.
    #[serde(skip_serializing_if = "Option::is_none")]
    sealed: Option<State>,
    /// What the CLI made of the message's `on_behalf_of` ([`crate::people::behalf::Checker`]).
    #[serde(skip_serializing_if = "Acting::is_own")]
    acting: Acting,
}

impl<'a> Shown<'a> {
    pub fn new(envelope: &Envelope, reactions: &'a [Reacted]) -> Self {
        let attachments = listed(&envelope.attachments, |id| envelope.attachment_handle(id));
        Self {
            envelope: Envelope { attachments: Vec::new(), ..envelope.clone() },
            attachments,
            reactions,
            sealed: None,
            acting: Acting::Own,
        }
    }

    /// With what the CLI made of the message's `on_behalf_of`.
    pub fn acting(self, acting: Acting) -> Self {
        Self { acting, ..self }
    }

    /// With what the CLI made of the message if sealed ([`crate::people::sealed::read`]).
    pub fn sealed(self, sealed: Option<State>) -> Self {
        Self { sealed, ..self }
    }
}

/// Print, while it runs, the notices of the messages the human of `nats` sent that keep waiting for an ant no daemon
/// serves ([`unserved`]); none with credentials that may not mark what it was told of.
pub async fn tell_unserved(nats: &crate::connection::Nats<'_>, js: &jetstream::Context) {
    let me = &nats.me.id;
    if !nats.rights.may_publish(&unserved::marker_subject(me)) {
        return;
    }
    let colony = ants_daemon::mcp::jetstream_colony(js.clone());
    let mut watch = match unserved::Watch::new(js, nats.domain, me.clone(), Role::Human, colony).await {
        Ok(watch) => watch,
        Err(e) => return tracing::warn!("not watching the messages sent: {e:#}"),
    };
    let humans = Registry::new(me.clone());
    let _watching = humans.watch(js.clone(), ants_daemon::people::humans::RETRY);
    loop {
        for found in watch.next(|user| humans.is_human(user)).await {
            if let Some(found) = watch.claim(found).await {
                println!("{}", found.text(me, chrono::Utc::now()));
            }
        }
    }
}

/// The reactions to `messages`, none if they cannot be read.
pub async fn reactions_or_none(js: &jetstream::Context, messages: &[Uuid]) -> reactions::Reactions {
    reactions::load(js, messages).await.unwrap_or_else(|e| {
        tracing::warn!("not showing reactions: {e:#}");
        reactions::Reactions::new()
    })
}

/// Reactions beneath a message: a line for each, naming who reacted so.
pub fn render_reactions(reactions: &[Reacted], indent: &str) -> String {
    reactions
        .iter()
        .map(|r| {
            let by: Vec<String> = r.labels().collect();
            format!("{indent}reacted {}: {}\n", r.reaction, by.join(", "))
        })
        .collect()
}

/// Questions for the terminal ([`Envelope::question`]): each question's text, its options numbered, whether several
/// may be picked and whether a typed answer is taken, then the `answers` given, a line each, or with none how to
/// answer them.
pub fn render_question(id: Uuid, questions: &Questions, answers: Vec<String>) -> String {
    let mut text = questions.render();
    if answers.is_empty() {
        let several = questions.questions.len() > 1;
        let how: Vec<String> = questions.questions.iter().map(|q| answer_hint(q, several)).collect();
        let them = if several { "them" } else { "it" };
        let _ = write!(text, "\n(`ants answer {id} {}` answers {them})", how.join(" "));
    }
    for answer in answers {
        text.push('\n');
        text.push_str(&answer);
    }
    text
}

/// How `ants answer` answers `question`: with an option's number, numbers joined by commas where several may be
/// picked, or text where a typed answer is taken, quoted when it is one of `several` questions.
fn answer_hint(question: &Question, several: bool) -> String {
    let numbers = if question.multi_select { "N,M" } else { "N" };
    match (question.free_text, several) {
        (false, _) => numbers.to_owned(),
        (true, false) => format!("{numbers}|TEXT"),
        (true, true) => format!("{numbers}|'TEXT'"),
    }
}

/// A message for the terminal: a header line with its id, then the text indented, its attachments and the reactions to
/// it. A mention notice names its channel, the sender that relayed it if any, and how to answer there; a question shows
/// its options numbered, whether a typed answer is taken and how to answer it. An assistant's message for its human
/// names both ([`Acting::label`]). An ant's name comes after its face in `faces`.
pub fn render(e: &Envelope, acting: &Acting, reactions: &[Reacted], faces: Option<&Faces>) -> String {
    let from = faces.and_then(|f| f.known(&e.from)).unwrap_or_else(|| e.from.to_string());
    let from = acting.label(&from);
    let at = e.sent_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M");
    let indent =
        |text: &str| ants_proto::lines(text).lines().map(|line| format!("  {line}")).collect::<Vec<_>>().join("\n");
    let attached = attachments::render(&listed(&e.attachments, |id| e.attachment_handle(id)), "  ");
    let under = attached + &render_reactions(reactions, "  ");
    if let Some(question) = e.asks() {
        let text = indent(&render_question(e.id, question, Vec::new()));
        return format!("{at}  {from} asks on thread {}, message {}\n{text}\n{under}", e.thread_id, e.id);
    }
    let text = indent(&e.text);
    let Some(mention) = &e.mention else {
        return format!("{at}  {from} on thread {}, message {}\n{text}\n{under}", e.thread_id, e.id);
    };
    let (channel, thread) = (&mention.channel, &e.thread_id);
    let (author, relayed) = match &mention.author {
        Some(author) => (author.to_string(), format!(" (relayed by {from})")),
        None => (from, String::new()),
    };
    format!(
        "{at}  {author} mentioned {} in #{channel} on thread {thread}{relayed}, message {}\n{text}\n  (`ants post \
         {channel} --thread {thread} ...` answers on the thread)\n{under}",
        e.to, e.id,
    )
}

#[cfg(test)]
mod tests {
    use ants_proto::{Role, STREAM, inbox_consumer};

    use tokio::time::timeout;

    use super::*;
    use crate::test_support::Server;

    fn texts(envelopes: &[Envelope]) -> Vec<&str> {
        envelopes.iter().map(|e| e.text.as_str()).collect()
    }

    async fn read(js: &jetstream::Context, select: Select) -> Vec<Envelope> {
        read_as(js, &UserId::human(), select).await
    }

    async fn read_as(js: &jetstream::Context, human: &UserId, select: Select) -> Vec<Envelope> {
        Inbox::open(js, human).await.unwrap().unread(&select).await.unwrap()
    }

    /// A connection of a new device of `human`, with credentials of its own.
    async fn device(server: &Server, human: &UserId, label: &str) -> (async_nats::Client, jetstream::Context) {
        let creds = server.dir().join(format!("{human}-{label}.creds"));
        let permissions = ants_proto::Permissions::for_user(human, Role::Human);
        let team = server.dir().join("auth");
        let device = crate::auth::Issue::new(&team, human, Role::Human.into(), permissions)
            .labelled(Some(label))
            .into_file(&creds);
        assert!(device.unwrap().is_some());
        let (client, mut js) = bus::connect(&server.url, Some(&creds), None, human, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        (client, js)
    }

    #[test]
    fn a_relayed_mention_notice_shows_the_post_s_author_and_the_relay() {
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let post = ants_proto::ChannelPost::new("ops".parse().unwrap(), "github".parse().unwrap(), "deploy failed")
            .from_webhook(vec![alice.clone()]);
        let notice = post.relayed_notice(&alice, &dev_a);
        let shown = render(&notice, &Acting::Own, &[], None);
        let header = format!(
            "github mentioned alice in #ops on thread {} (relayed by dev-a), message {}\n",
            post.thread_id, notice.id
        );
        assert!(shown.contains(&header), "{shown}");
        assert!(shown.contains("\n  deploy failed\n"), "{shown}");
    }

    #[test]
    fn a_message_shows_without_control_or_reordering_characters() {
        use crate::test_support::hostile::{HOSTILE, assert_printable};
        let said = Envelope::on_thread("t1".parse().unwrap(), "dev-a".parse().unwrap(), UserId::human(), HOSTILE);
        let shown = render(&said, &Acting::Own, &[], None);
        assert_printable(&shown);
        assert!(shown.contains("\n  [2J]0;pwned]52;c;cHduZWQ=spoof31m\n  line two\n"), "{shown}");
        let mut question = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        (question.text, question.options[0].label) = (HOSTILE.to_owned(), HOSTILE.to_owned());
        let asked = Envelope::asking("t1".parse().unwrap(), "dev-a".parse().unwrap(), UserId::human(), question);
        assert_printable(&render(&asked, &Acting::Own, &[], None));
    }

    #[test]
    fn a_question_shows_its_options_numbered_and_how_to_answer_it() {
        let options = ["yes".to_owned(), "not yet".to_owned()];
        let question = Question::new("Ship it?", &options, false).unwrap();
        let asked = Envelope::asking("t1".parse().unwrap(), "dev-a".parse().unwrap(), UserId::human(), question);
        let shown = render(&asked, &Acting::Own, &[], None);
        assert!(shown.contains(&format!("dev-a asks on thread t1, message {}\n  Ship it?\n", asked.id)), "{shown}");
        assert!(shown.contains("\n  1. yes\n  2. not yet\n"), "{shown}");
        assert!(shown.contains("\n  Answer with an option's number.\n"), "{shown}");
        assert!(shown.contains(&format!("\n  (`ants answer {} N` answers it)\n", asked.id)), "{shown}");
    }

    #[test]
    fn several_questions_show_how_to_answer_them_with_a_pick_each() {
        let options = ["eu".to_owned(), "us".to_owned()];
        let regions = Question::new("Which regions?", &options, false).unwrap().multi().headed("Regions");
        let ship = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        let questions = ants_proto::Questions::new(vec![regions, ship]).unwrap();
        let asked = Envelope::asking("t1".parse().unwrap(), "dev-a".parse().unwrap(), UserId::human(), questions);
        let shown = render(&asked, &Acting::Own, &[], None);
        assert!(shown.contains("\n  Question 1 of 2, Regions: Which regions?\n  \n  1. eu\n"), "{shown}");
        assert!(shown.contains("\n  Question 2 of 2: Ship it?\n"), "{shown}");
        assert!(shown.contains(&format!("\n  (`ants answer {} N,M N|'TEXT'` answers them)\n", asked.id)), "{shown}");
    }

    #[test]
    fn an_ant_s_face_comes_before_its_name() {
        let dev_a: UserId = "dev-a".parse().unwrap();
        let record = ants_proto::AntIdentity::new(dev_a.clone(), "a-id".to_owned(), chrono::Utc::now());
        let identities = crate::ants::avatars::Identities::from([(dev_a.clone(), record)]);
        let faces = Faces::new(identities, crate::ants::avatars::Depth::Plain, ants_proto::Theme::Dark);
        let face = ants_proto::Avatar::derive("a-id").face();
        let reply = Envelope::new(dev_a, UserId::human(), "done");
        let shown = render(&reply, &Acting::Own, &[], Some(&faces));
        assert!(shown.contains(&format!("  {face} dev-a on thread ")), "{shown}");
        let from_human = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "go");
        assert!(
            render(&from_human, &Acting::Own, &[], Some(&faces)).contains("  human on thread "),
            "no record, no face"
        );
    }

    #[test]
    fn attachments_are_listed_with_their_handles() {
        let mut reply = Envelope::new("dev-a".parse().unwrap(), UserId::human(), "the screenshot");
        let shot = ants_daemon::files::reference("shot.png", "image/png", &[0; 2048]).unwrap();
        reply.attachments = vec![shot.clone()];
        let handle = format!("dev-a.inbox.human.{}", shot.id);
        let shown = render(&reply, &Acting::Own, &[], None);
        assert!(shown.contains(&format!("\n  the screenshot\n  📎 shot.png (2.0 KiB) {handle}\n")), "{shown}");
        let json = serde_json::to_string(&Shown::new(&reply, &[])).unwrap();
        assert_eq!(json.matches("\"attachments\"").count(), 1, "{json}");
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!((&json["attachments"][0]["handle"], &json["text"]), (&handle.into(), &"the screenshot".into()));
        assert_eq!(json["attachments"][0]["sha256"], shot.sha256);
    }

    /// Runs as the human and dev-a with their own credentials, so it also checks the human's permissions.
    #[tokio::test]
    async fn replies_stay_unread_until_shown() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        bus::ensure_stream(&human).await.unwrap();
        let (_, ant_js) = server.connect(&dev_a).await;
        let asks =
            [Envelope::new(UserId::human(), dev_a.clone(), "a"), Envelope::new(UserId::human(), dev_a.clone(), "b")];
        for (ask, text) in [(&asks[0], "one"), (&asks[1], "two"), (&asks[0], "three")] {
            bus::publish(&ant_js, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        let forged = asks[0].reply(dev_a.clone(), "forged", Role::Human).unwrap();
        let subject = ants_proto::subject::inbox(&UserId::human(), &UserId::human());
        bus::publish_json(&human, subject, &forged, None).await.unwrap();

        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&read(&human, peek.clone()).await), ["one", "two", "three"]);
        assert_eq!(texts(&read(&human, peek).await), ["one", "two", "three"], "peeking marks nothing read");

        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        assert_eq!(texts(&read(&human, thread_b).await), ["two"]);
        let first = Select { limit: Some(1), ..Default::default() };
        assert_eq!(texts(&read(&human, first).await), ["one"]);

        let last = asks[0].reply(dev_a.clone(), "four", Role::Human).unwrap();
        bus::publish(&ant_js, &last).await.unwrap();
        assert!(mark_read(&human, &UserId::human(), &last).await.unwrap(), "ask --wait marks its reply read");
        assert_eq!(texts(&read(&human, Select::default()).await), ["three"]);
        assert!(read(&human, Select::default()).await.is_empty());
        assert!(!mark_read(&human, &UserId::human(), &last).await.unwrap());
    }

    #[tokio::test]
    async fn an_assistant_s_reports_are_counted_apart_unless_asked_for() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        bus::ensure_stream(&human).await.unwrap();
        let (_, ant_js) = server.connect(&dev_a).await;
        let acts = ThreadId::activity(&UserId::human(), chrono::Utc::now().date_naive());
        let report = |text: &str| Envelope::on_thread(acts.clone(), dev_a.clone(), UserId::human(), text);
        for e in [report("sent bob a message"), Envelope::new(dev_a.clone(), UserId::human(), "done"), report("held")] {
            bus::publish(&ant_js, &e).await.unwrap();
        }
        let mut inbox = Inbox::open(&human, &UserId::human()).await.unwrap();
        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&inbox.unread(&peek).await.unwrap()), ["done"]);
        assert_eq!(inbox.reports(), BTreeMap::from([(acts.clone(), 2)]));
        let activity = Select { activity: true, ..peek };
        assert_eq!(texts(&read(&human, activity).await), ["sent bob a message", "done", "held"]);
        let on_acts = Select { thread: Some(acts.clone()), peek: true, ..Default::default() };
        assert_eq!(texts(&read(&human, on_acts).await), ["sent bob a message", "held"], "picked, its thread shows");

        assert_eq!(texts(&inbox.unread(&Select::default()).await.unwrap()), ["done"]);
        assert_eq!(inbox.reports().values().sum::<usize>(), 2);
        assert!(read(&human, Select { activity: true, ..Default::default() }).await.is_empty(), "summed up, read");
    }

    #[tokio::test]
    async fn every_device_of_a_human_gets_every_reply_and_reading_on_one_reads_on_all() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a): (UserId, UserId, UserId) =
            ("alice".parse().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, laptop) = device(&server, &alice, "laptop").await;
        let (_, phone) = device(&server, &alice, "phone").await;
        let (b_client, b_js) = device(&server, &bob, "laptop").await;
        bus::ensure_stream(&laptop).await.unwrap();
        let (_, ant_js) = server.connect(&dev_a).await;
        let asks = [Envelope::new(alice.clone(), dev_a.clone(), "a"), Envelope::new(alice.clone(), dev_a.clone(), "b")];

        // Both devices follow the inbox; each shows the reply, and the first to show it marks it read for both.
        let opened = tokio::sync::Barrier::new(3);
        let follow = async |js: &jetstream::Context| {
            let mut inbox = Inbox::open(js, &alice).await.unwrap();
            assert!(inbox.unread(&Select::default()).await.unwrap().is_empty());
            opened.wait().await;
            let mut shown = Vec::new();
            let select = Select { limit: Some(1), ..Default::default() };
            let follow = inbox.follow(
                &select,
                None,
                None,
                |e| {
                    shown.push(e.text.clone());
                    Ok(())
                },
                |_| Ok(()),
                std::future::pending(),
            );
            timeout(Duration::from_secs(10), follow).await.expect("no reply followed").unwrap();
            shown
        };
        let send = async {
            opened.wait().await;
            bus::publish(&ant_js, &asks[0].reply(dev_a.clone(), "one", Role::Human).unwrap()).await.unwrap();
        };
        let (on_laptop, on_phone, ()) = tokio::join!(follow(&laptop), follow(&phone), send);
        assert_eq!((on_laptop, on_phone), (vec!["one".to_owned()], vec!["one".to_owned()]));
        assert!(read_as(&phone, &alice, Select::default()).await.is_empty(), "read on both");

        // Reading one thread on one device leaves the other unread on both.
        for (ask, text) in [(&asks[0], "two"), (&asks[1], "three"), (&asks[0], "four")] {
            bus::publish(&ant_js, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        assert_eq!(texts(&read_as(&laptop, &alice, thread_b).await), ["three"]);
        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&read_as(&phone, &alice, peek).await), ["two", "four"]);
        assert_eq!(texts(&read_as(&phone, &alice, Select::default()).await), ["two", "four"]);
        assert!(read_as(&laptop, &alice, Select::default()).await.is_empty());
        let kv = ensure_bucket(&laptop).await.unwrap();
        let marker: ReadMarker = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert!(marker.read.is_empty(), "every read message is below the marker: {marker:?}");

        // Bob has an inbox and a marker of his own, and reaches neither of Alice's.
        bus::publish(&ant_js, &Envelope::new(dev_a.clone(), bob.clone(), "for bob")).await.unwrap();
        assert_eq!(texts(&read_as(&b_js, &bob, Select::default()).await), ["for bob"]);
        let alices = async { Inbox::open(&b_js, &alice).await?.unread(&Select::default()).await };
        assert!(alices.await.is_err(), "bob reads alice's read marker");
        let b_kv = ensure_bucket(&b_js).await.unwrap();
        assert!(b_kv.get("alice").await.is_err());
        assert!(b_kv.put("alice", "{}".into()).await.is_err(), "bob moves alice's read marker");
        let marker: ReadMarker = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert!(marker.below > 0);
        let consumer = jetstream::consumer::pull::Config {
            durable_name: Some(inbox_consumer(&alice)),
            filter_subject: subject::inbox_of(&alice),
            ..Default::default()
        };
        assert!(b_js.create_consumer_on_stream(consumer, STREAM).await.is_err(), "bob reads alice's inbox");
        let mut announced = b_client.subscribe(subject::stored_inbox(&alice)).await.unwrap();
        b_client.flush().await.unwrap();
        bus::publish(&ant_js, &asks[0].reply(dev_a.clone(), "five", Role::Human).unwrap()).await.unwrap();
        assert!(timeout(Duration::from_millis(500), announced.next()).await.is_err(), "bob hears alice's replies");
    }

    #[tokio::test]
    async fn marking_messages_unread_on_one_device_shows_them_unread_on_all() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, laptop) = device(&server, &alice, "laptop").await;
        let (_, phone) = device(&server, &alice, "phone").await;
        bus::ensure_stream(&laptop).await.unwrap();
        let (_, ant_js) = server.connect(&dev_a).await;
        let asks = [Envelope::new(alice.clone(), dev_a.clone(), "a"), Envelope::new(alice.clone(), dev_a.clone(), "b")];
        for (ask, text) in [(&asks[0], "one"), (&asks[1], "two"), (&asks[0], "three"), (&asks[0], "four")] {
            bus::publish(&ant_js, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        assert_eq!(read_as(&laptop, &alice, Select::default()).await.len(), 4);

        let on_phone = Inbox::open(&phone, &alice).await.unwrap();
        let seqs = on_phone.on_thread(&asks[0].thread_id, DeliverPolicy::All).await.unwrap();
        assert_eq!(seqs.len(), 3, "{seqs:?}");
        on_phone.unmark(&seqs[1..]).await.unwrap();
        assert_eq!(
            texts(&read_as(&laptop, &alice, Select { peek: true, ..Default::default() }).await),
            ["three", "four"]
        );
        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        on_phone.unmark(&on_phone.on_thread(&asks[1].thread_id, DeliverPolicy::All).await.unwrap()).await.unwrap();
        assert_eq!(texts(&read_as(&laptop, &alice, thread_b).await), ["two"]);
        assert_eq!(texts(&read_as(&laptop, &alice, Select::default()).await), ["three", "four"]);
        assert!(read_as(&phone, &alice, Select::default()).await.is_empty(), "read again on both");
    }
}
