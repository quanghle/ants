//! Acts done for a human by its assistant ([`ants_proto::Acting`]): the record of an assistant's credentials, which
//! its daemon keeps in [`DEVICES_BUCKET`] ([`keep_record`]), and the check of an act's `on_behalf_of` against it and
//! the human's grant ([`Records::acting`]).

use std::path::Path;

use ants_proto::{
    Acting, ApprovalDecision, AssistantRecord, DEVICES_BUCKET, Envelope, Grants, JsDomain, ThreadId, UserId,
    assistant_record_key, revoked_device_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream;

use crate::creds::{self, Kind};
use crate::direct_kv::{DirectKv, Lookup};
use crate::humans::Registry;

/// The grants standing, as a reader keeps them.
pub trait Standing {
    /// The ant `human` designates as its assistant.
    fn designated(&self, human: &UserId) -> Option<UserId>;
    /// Whether `assistant`, whose user JWT's assistant_of tag names `assistant_of`, acts for `human`.
    fn honours(&self, human: &UserId, assistant: &UserId, assistant_of: Option<&UserId>) -> bool;
}

impl Standing for Registry {
    fn designated(&self, human: &UserId) -> Option<UserId> {
        self.assistant(human)
    }

    fn honours(&self, human: &UserId, assistant: &UserId, assistant_of: Option<&UserId>) -> bool {
        self.honours_assistant(human, assistant, assistant_of)
    }
}

impl Standing for Grants {
    fn designated(&self, human: &UserId) -> Option<UserId> {
        Grants::designated(self, human).cloned()
    }

    fn honours(&self, human: &UserId, assistant: &UserId, assistant_of: Option<&UserId>) -> bool {
        Grants::honours(self, human, assistant, assistant_of)
    }
}

/// The assistants' records in [`DEVICES_BUCKET`], verified for the team's account.
#[derive(Clone)]
pub struct Records {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The team's account (`A...`), the issuer of the reader's credentials; none: no record is taken.
    account: Option<String>,
}

impl Records {
    pub fn new(js: jetstream::Context, js_domain: Option<JsDomain>, account: Option<String>) -> Self {
        Self { js, js_domain, account }
    }

    /// For whom `publisher` did an act claiming `claim` as its `on_behalf_of`: the human, while `standing` holds the
    /// human's grant naming the publisher and the publisher's record names the human; else the claim is forged.
    pub async fn acting(&self, standing: &impl Standing, publisher: &UserId, claim: Option<&UserId>) -> Acting {
        let Some(human) = claim else { return Acting::Own };
        // The grant is kept in memory: no record is read for a claim it already refuses.
        if standing.designated(human).as_ref() != Some(publisher) {
            return Acting::Forged(human.clone());
        }
        let assistant_of = self.assistant_of(publisher).await;
        Acting::of(claim, |human| standing.honours(human, publisher, assistant_of.as_ref()))
    }

    /// The human the record of `assistant`'s credentials makes it the assistant of: none without a record, or with one
    /// not verified for the account, or for a key revoked. Failing to read either, it warns and takes none.
    pub async fn assistant_of(&self, assistant: &UserId) -> Option<UserId> {
        self.read(assistant).await.unwrap_or_else(|e| {
            tracing::warn!(%assistant, error = format!("{e:#}"), "reading the record of an assistant");
            None
        })
    }

    async fn read(&self, assistant: &UserId) -> Result<Option<UserId>> {
        let Some(account) = &self.account else { return Ok(None) };
        let devices = DirectKv::new(&self.js, self.js_domain.as_ref(), DEVICES_BUCKET).await?;
        let Lookup::Value(value) = devices.lookup(assistant_record_key(assistant)).await? else { return Ok(None) };
        let record: AssistantRecord = serde_json::from_slice(&value).context("parsing the record")?;
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
        let user = match record.verify(assistant, [account.as_str()], now) {
            Ok(user) => user,
            Err(e) => {
                tracing::warn!(%assistant, error = %e, "not taking the record of an assistant");
                return Ok(None);
            }
        };
        if let Lookup::Value(_) = devices.lookup(revoked_device_key(&user.public_key)).await? {
            tracing::warn!(%assistant, key = user.public_key, "not taking the record of revoked credentials");
            return Ok(None);
        }
        Ok(user.assistant_of())
    }
}

/// Keep the record of the credentials at `creds` in [`DEVICES_BUCKET`] when they make `ant` an assistant, so readers
/// check what it does for its human against them. Returns the human they make it the assistant of.
pub async fn keep_record(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    ant: &UserId,
    creds: &Path,
) -> Result<Option<UserId>> {
    let finding = creds::check(creds)?;
    let Kind::Assistant { of } = finding.kind else { return Ok(None) };
    let record = AssistantRecord { jwt: creds::jwt(creds)? };
    let devices = DirectKv::new(js, js_domain, DEVICES_BUCKET).await?;
    devices.put(assistant_record_key(ant), serde_json::to_vec(&record)?).await.with_context(|| {
        format!("keeping the record of the assistant of {of} (`ants init` as a human creates the bucket)")
    })?;
    Ok(Some(of))
}

/// A report from `from` to `human` of an act its assistant did for it: on the human's activity thread of the day
/// ([`ThreadId::activity`]), which gathers them all, so no wait on the act's thread takes it for a reply and they do
/// not each start a thread.
pub fn report(from: UserId, human: &UserId, text: String) -> Envelope {
    let thread = ThreadId::activity(human, chrono::Utc::now().date_naive());
    Envelope::on_thread(thread, from, human.clone(), text)
        .with_owner(Some(human.clone()))
        .as_notice(ants_proto::NoticeKind::Assistant)
}

/// The message telling `human` that `envelope`, which its assistant sent for it and a reader verified, reached its
/// recipient: a [`report`] from the recipient.
pub fn sent_notice(envelope: &Envelope, human: &UserId) -> Envelope {
    let (from, to, thread) = (&envelope.from, &envelope.to, &envelope.thread_id);
    let text = format!(
        "[ants] {from} sent {to} a message for {human}, as its assistant, on thread {thread}: {}",
        excerpt(&envelope.text)
    );
    report(to.clone(), human, text)
}

/// The message telling `human` that `envelope`, carrying `decision`, is its assistant's decision for it, verified by a
/// reader, and that it holds until the human consents to it: no decision made for a human takes effect as the human's
/// unconfirmed ([`crate::approvals`]). A [`report`] from the requesting ant.
pub fn held_notice(envelope: &Envelope, decision: &ApprovalDecision, human: &UserId) -> Envelope {
    let (from, to, id) = (&envelope.from, &envelope.to, decision.request_id);
    let text = format!(
        "[ants] {from}, as the assistant of {human}, decided tool request {to}/{id} for {human}: {}. Held until \
         {human} confirms it (`ants assistant confirm {to}/{id}`, or `--deny` to drop it), unless a pre-allow rule \
         of {human} covers the call (`ants assistant allow`). {human} may also decide the request itself (`ants \
         tools approve {to}/{id}` or `ants tools reject {to}/{id}`).",
        decision.verdict.as_str()
    );
    report(to.clone(), human, text)
}

/// The first line of `text`, cut to a short excerpt.
pub fn excerpt(text: &str) -> String {
    const MAX: usize = 120;
    let line = text.trim().lines().next().unwrap_or_default();
    match line.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}…", &line[..cut]),
        None => line.to_owned(),
    }
}
