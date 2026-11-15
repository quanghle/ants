//! A human's consent to what its assistant does for it: the confirmation of a pending act, and pre-allow rules.
//!
//! An assistant's tool-request decision for its human ([`crate::ApprovalDecision::on_behalf_of`]), or its admin order
//! for the human, is an act that waits: the daemon that would apply it holds it until the human confirms it, or a
//! pre-allow rule the human signed covers it. Both are records one of the human's devices signs, as it signs its
//! grant ([`crate::AssistantGrant`]), kept in [`HUMANS_BUCKET`] under the human's own name, which only its
//! credentials write:
//!
//! - [`Confirmations`] under `<human>.confirmed` ([`confirmations_key`]): the human's answers ([`ActAnswer`]) to its
//!   assistant's recent acts ([`ConfirmedAct`]), each kept until the act can no longer take effect. One record holds
//!   them all, so the bucket keeps one key per human however many acts it answers.
//! - [`PreAllow`] under `<human>.preallow` ([`pre_allow_key`]): rules ([`pre_allow_rule`]) under which the
//!   assistant's acts take effect unconfirmed, naming the assistant they are for, so that another assistant designated
//!   later inherits none.
//!
//! A reader keeping them ([`Consents`]) verifies each for the team's account, as a grant, and refuses a record issued
//! before one it already read: a replay. A deleted or refused record leaves the human with no answer and no rule.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::HUMANS_BUCKET;
use crate::grant::device_signed;
use crate::{DeviceId, GRANT_CLOCK_SKEW, GrantError, Machine, UserId, Uuid, Verdict, canonical_json};

/// Last token of the key of a human's [`Confirmations`] in [`HUMANS_BUCKET`].
pub const CONFIRMATIONS: &str = "confirmed";

/// Last token of the key of a human's [`PreAllow`] in [`HUMANS_BUCKET`].
pub const PRE_ALLOW: &str = "preallow";

/// The most answers a [`Confirmations`] record holds, which keeps it small; the oldest go first.
pub const MAX_CONFIRMED: usize = 64;

/// The most rules a [`PreAllow`] record holds.
pub const MAX_PRE_ALLOW: usize = 64;

/// Key in [`HUMANS_BUCKET`] of the [`Confirmations`] of `human`: `<human>.confirmed`.
pub fn confirmations_key(human: &UserId) -> String {
    format!("{human}.{CONFIRMATIONS}")
}

/// Key in [`HUMANS_BUCKET`] of the [`PreAllow`] of `human`: `<human>.preallow`.
pub fn pre_allow_key(human: &UserId) -> String {
    format!("{human}.{PRE_ALLOW}")
}

/// Which consent record `key` of [`HUMANS_BUCKET`] holds, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsentKey {
    Confirmations(UserId),
    PreAllow(UserId),
}

impl ConsentKey {
    pub fn parse(key: &str) -> Option<Self> {
        let (human, last) = key.split_once('.')?;
        let human = human.parse().ok()?;
        match last {
            CONFIRMATIONS => Some(Self::Confirmations(human)),
            PRE_ALLOW => Some(Self::PreAllow(human)),
            _ => None,
        }
    }

    pub fn human(&self) -> &UserId {
        match self {
            Self::Confirmations(human) | Self::PreAllow(human) => human,
        }
    }
}

/// An act an assistant made for its human, which takes effect only once the human consents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Act {
    /// A decision on the tool request `request_id` of `ant`.
    Decision { ant: UserId, request_id: Uuid, verdict: Verdict },
    /// The admin order `id` to `ant` on `machine`.
    Order { machine: Machine, ant: UserId, id: Uuid },
}

impl Act {
    /// The act written for the command line: `<ant>/<request id>`, or `<machine>/<ant>/<order id>`.
    pub fn qualified(&self) -> String {
        match self {
            Self::Decision { ant, request_id, .. } => format!("{ant}/{request_id}"),
            Self::Order { machine, ant, id } => format!("{machine}/{ant}/{id}"),
        }
    }
}

/// The human's answer to an act.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActAnswer {
    /// The act takes effect, as the human's own.
    Confirm,
    /// The act is dropped; what it decided waits for another decision.
    Deny,
}

/// The human's answer to one act of `assistant`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedAct {
    pub assistant: UserId,
    pub act: Act,
    pub answer: ActAnswer,
    /// When the act can no longer take effect, such as its request's expiry: the answer may be dropped after.
    pub until: DateTime<Utc>,
}

/// Why a consent record is not honoured.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConsentError {
    #[error("record of {human} found at key {key}")]
    ForeignKey { human: UserId, key: String },
    #[error("malformed record: {0}")]
    Malformed(String),
    #[error("issued at {0}, ahead of the reader's clock")]
    Ahead(DateTime<Utc>),
    #[error(transparent)]
    Device(#[from] GrantError),
    #[error("issued at {issued_at}, before the record of {latest} already read: a replay")]
    Replayed { issued_at: DateTime<Utc>, latest: DateTime<Utc> },
}

/// What the device signs of a consent record: `context`, then `fields`, `issued_at` (RFC 3339, nanoseconds) and the
/// body as canonical JSON, each on a line of its own.
fn signed_lines(context: &str, fields: &[&str], issued_at: DateTime<Utc>, body: &impl Serialize) -> Vec<u8> {
    let issued_at = issued_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let body = canonical_json(&serde_json::to_value(body).expect("a consent record serializes"));
    let mut lines = vec![context];
    lines.extend(fields);
    lines.extend([issued_at.as_str(), body.as_str()]);
    lines.join("\n").into_bytes()
}

/// The checks every consent record passes, read from `key` where `expected` is its key: its device's signature, as
/// for a grant, and an issue time no further ahead than [`GRANT_CLOCK_SKEW`].
#[allow(clippy::too_many_arguments)]
fn verify_record(
    key: &str,
    expected: &str,
    human: &UserId,
    jwt: &str,
    signature: &str,
    signed: &[u8],
    issued_at: DateTime<Utc>,
    account: &str,
    now: DateTime<Utc>,
) -> Result<DeviceId, ConsentError> {
    if key != expected {
        return Err(ConsentError::ForeignKey { human: human.clone(), key: key.to_owned() });
    }
    if issued_at > now + GRANT_CLOCK_SKEW {
        return Err(ConsentError::Ahead(issued_at));
    }
    Ok(device_signed(human, jwt, signature, signed, issued_at, account)?.0)
}

/// The answers of `human` to its assistant's recent acts, signed by a device of the human. See the
/// [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Confirmations {
    pub human: UserId,
    /// Oldest first, at most [`MAX_CONFIRMED`].
    pub acts: Vec<ConfirmedAct>,
    /// When the device signed it: of two records of the human, the one issued later stands.
    pub issued_at: DateTime<Utc>,
    /// The user JWT of the signing device's credentials.
    pub jwt: String,
    /// Signature of [`Confirmations::signed`] by the key `jwt` is for, base64url without padding.
    pub signature: String,
}

impl Confirmations {
    /// The answers `acts` of `human`, issued at `issued_at` by the device whose user JWT is `jwt`, to be completed with
    /// its signature.
    pub fn new(human: UserId, acts: Vec<ConfirmedAct>, issued_at: DateTime<Utc>, jwt: impl Into<String>) -> Self {
        Self { human, acts, issued_at, jwt: jwt.into(), signature: String::new() }
    }

    /// What the device signs: `ants-assistant-confirmations`, the human, `issued_at` and the acts as canonical JSON.
    pub fn signed(&self) -> Vec<u8> {
        signed_lines("ants-assistant-confirmations", &[self.human.as_str()], self.issued_at, &self.acts)
    }

    /// Check the record, read from `key`, for a reader of the team whose account is `account` at `now`, as
    /// [`crate::AssistantGrant::verify`] checks a grant. Returns the device that signed it.
    pub fn verify(&self, key: &str, account: &str, now: DateTime<Utc>) -> Result<DeviceId, ConsentError> {
        if self.acts.len() > MAX_CONFIRMED {
            return Err(ConsentError::Malformed(format!("{} answers, more than {MAX_CONFIRMED}", self.acts.len())));
        }
        let expected = confirmations_key(&self.human);
        verify_record(
            key,
            &expected,
            &self.human,
            &self.jwt,
            &self.signature,
            &self.signed(),
            self.issued_at,
            account,
            now,
        )
    }

    /// The human's answer to `act` of `assistant`, the latest if it answered twice.
    pub fn answer(&self, assistant: &UserId, act: &Act) -> Option<ActAnswer> {
        self.acts.iter().rev().find(|a| a.assistant == *assistant && a.act == *act).map(|a| a.answer)
    }

    /// The answers, with `answer` to `act` of `assistant` in place of any earlier one, those past `until` at `now`
    /// dropped and the oldest dropped beyond [`MAX_CONFIRMED`].
    #[must_use]
    pub fn answering(
        acts: &[ConfirmedAct],
        assistant: &UserId,
        act: Act,
        answer: ActAnswer,
        until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Vec<ConfirmedAct> {
        let mut acts: Vec<ConfirmedAct> =
            acts.iter().filter(|a| a.until > now && !(a.assistant == *assistant && a.act == act)).cloned().collect();
        acts.push(ConfirmedAct { assistant: assistant.clone(), act, answer, until });
        let excess = acts.len().saturating_sub(MAX_CONFIRMED);
        acts.drain(..excess);
        acts
    }
}

/// The rules under which what `assistant` does for `human` takes effect unconfirmed, signed by a device of the human.
/// An empty list allows nothing. See [`pre_allow_rule`] for the rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreAllow {
    pub human: UserId,
    /// The assistant the rules are for: none other's acts take effect under them.
    pub assistant: UserId,
    pub rules: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub jwt: String,
    pub signature: String,
}

impl PreAllow {
    /// The rules `rules` of `human` for `assistant`, issued at `issued_at` by the device whose user JWT is `jwt`, to be
    /// completed with its signature.
    pub fn new(
        human: UserId,
        assistant: UserId,
        rules: Vec<String>,
        issued_at: DateTime<Utc>,
        jwt: impl Into<String>,
    ) -> Self {
        Self { human, assistant, rules, issued_at, jwt: jwt.into(), signature: String::new() }
    }

    /// What the device signs: `ants-assistant-pre-allow`, the human, the assistant, `issued_at` and the rules as
    /// canonical JSON.
    pub fn signed(&self) -> Vec<u8> {
        let fields = [self.human.as_str(), self.assistant.as_str()];
        signed_lines("ants-assistant-pre-allow", &fields, self.issued_at, &self.rules)
    }

    /// Check the record, read from `key`, for a reader of the team whose account is `account` at `now`, as
    /// [`crate::AssistantGrant::verify`] checks a grant, and each rule's form. Returns the device that signed it.
    pub fn verify(&self, key: &str, account: &str, now: DateTime<Utc>) -> Result<DeviceId, ConsentError> {
        if self.rules.len() > MAX_PRE_ALLOW {
            return Err(ConsentError::Malformed(format!("{} rules, more than {MAX_PRE_ALLOW}", self.rules.len())));
        }
        if let Some(rule) = self.rules.iter().find(|r| pre_allow_rule(r).is_none()) {
            return Err(ConsentError::Malformed(format!("rule {rule:?} is not <ant>/<rule> or */<rule>")));
        }
        let expected = pre_allow_key(&self.human);
        verify_record(
            key,
            &expected,
            &self.human,
            &self.jwt,
            &self.signature,
            &self.signed(),
            self.issued_at,
            account,
            now,
        )
    }

    /// The tool rules of the record for requests of `ant` ([`pre_allow_rule`]).
    pub fn rules_for<'a>(&'a self, ant: &'a UserId) -> impl Iterator<Item = &'a str> {
        self.rules
            .iter()
            .filter_map(|r| pre_allow_rule(r))
            .filter(move |(of, _)| of.as_ref().is_none_or(|of| of == ant))
            .map(|(_, r)| r)
    }
}

/// A pre-allow rule's parts: `<ant>/<tool rule>`, or `*/<tool rule>` for every ant (None), where the tool rule is an
/// allowlist rule of the ant's config (`*`, `<kind>` or `<kind>:<pattern>`). None when it has neither part.
pub fn pre_allow_rule(rule: &str) -> Option<(Option<UserId>, &str)> {
    let (ant, tool) = rule.trim().split_once('/')?;
    let tool = tool.trim();
    if tool.is_empty() {
        return None;
    }
    match ant {
        "*" => Some((None, tool)),
        ant => Some((Some(ant.parse().ok()?), tool)),
    }
}

/// The consent records of the team's humans as a reader of [`HUMANS_BUCKET`] keeps them, fed each record as it reads
/// or watches it ([`Consents::apply`]).
#[derive(Debug, Clone, Default)]
pub struct Consents {
    confirmations: BTreeMap<UserId, Kept<Confirmations>>,
    pre_allows: BTreeMap<UserId, Kept<PreAllow>>,
}

#[derive(Debug, Clone)]
struct Kept<T> {
    /// None while the human has no record the reader honours.
    record: Option<T>,
    /// When the newest record the reader verified was issued: an older one is a replay.
    latest: DateTime<Utc>,
}

/// What a record changed ([`Consents::apply`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsentApplied {
    pub human: UserId,
    /// Why the record was refused, leaving the human without one.
    pub refused: Option<ConsentError>,
}

/// The record a reader keeps under `key`, `value` applied: verified with `verify`, refused when issued before the
/// newest it read.
fn keep<T: serde::de::DeserializeOwned>(
    kept: &mut BTreeMap<UserId, Kept<T>>,
    human: &UserId,
    value: Option<&[u8]>,
    verify: impl FnOnce(&T) -> Result<DateTime<Utc>, ConsentError>,
) -> Option<ConsentError> {
    let latest = kept.remove(human).map(|k| k.latest);
    let verified = value.map(|value| {
        let record: T = serde_json::from_slice(value).map_err(|e| ConsentError::Malformed(e.to_string()))?;
        let issued_at = verify(&record)?;
        match latest {
            Some(latest) if issued_at < latest => Err(ConsentError::Replayed { issued_at, latest }),
            _ => Ok((record, issued_at)),
        }
    });
    let (record, refused) = match verified {
        Some(Ok((record, issued_at))) => (Some((record, issued_at)), None),
        Some(Err(e)) => (None, Some(e)),
        None => (None, None),
    };
    let latest = latest.into_iter().chain(record.as_ref().map(|(_, at)| *at)).max();
    if let Some(latest) = latest {
        kept.insert(human.clone(), Kept { record: record.map(|(r, _)| r), latest });
    }
    refused
}

impl Consents {
    /// Apply `value`, what the key `key` of [`HUMANS_BUCKET`] holds now (none: deleted or purged), checked for the
    /// team of the account `account` at `now`; None when `key` is no consent record's key.
    pub fn apply(
        &mut self,
        key: &str,
        value: Option<&[u8]>,
        account: &str,
        now: DateTime<Utc>,
    ) -> Option<ConsentApplied> {
        let parsed = ConsentKey::parse(key)?;
        let human = parsed.human().clone();
        let refused = match parsed {
            ConsentKey::Confirmations(_) => keep(&mut self.confirmations, &human, value, |r: &Confirmations| {
                r.verify(key, account, now).map(|_| r.issued_at)
            }),
            ConsentKey::PreAllow(_) => keep(&mut self.pre_allows, &human, value, |r: &PreAllow| {
                r.verify(key, account, now).map(|_| r.issued_at)
            }),
        };
        Some(ConsentApplied { human, refused })
    }

    /// The human's answer to `act` of `assistant`, by the record standing.
    pub fn answer(&self, human: &UserId, assistant: &UserId, act: &Act) -> Option<ActAnswer> {
        self.confirmations.get(human)?.record.as_ref()?.answer(assistant, act)
    }

    /// The answers of `human` standing, whatever assistant they answer.
    pub fn confirmations(&self, human: &UserId) -> Option<&Confirmations> {
        self.confirmations.get(human)?.record.as_ref()
    }

    /// The pre-allow rules of `human` standing for `assistant`.
    pub fn pre_allow(&self, human: &UserId, assistant: &UserId) -> Option<&PreAllow> {
        self.pre_allows.get(human)?.record.as_ref().filter(|p| p.assistant == *assistant)
    }

    /// The humans with a record standing, of either kind.
    pub fn humans(&self) -> impl Iterator<Item = &UserId> {
        let confirmations = self.confirmations.iter().filter(|(_, k)| k.record.is_some()).map(|(h, _)| h);
        confirmations.chain(self.pre_allows.iter().filter(|(_, k)| k.record.is_some()).map(|(h, _)| h))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use chrono::TimeDelta;
    use data_encoding::BASE64URL_NOPAD;
    use nkeys::KeyPair;
    use serde_json::json;

    use super::*;
    use crate::p;

    struct Device {
        account: KeyPair,
        key: KeyPair,
        id: DeviceId,
    }

    impl Device {
        fn new() -> Self {
            Self { account: KeyPair::new_account(), key: KeyPair::new_user(), id: DeviceId::generate() }
        }

        fn jwt(&self, human: &str) -> String {
            let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
            let claims = json!({
                "sub": self.key.public_key(), "iss": self.account.public_key(), "name": human,
                "nats": { "type": "user", "tags": [self.id.tag()] },
            });
            let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
            let signature = BASE64URL_NOPAD.encode(&self.account.sign(format!("{header}.{body}").as_bytes()).unwrap());
            format!("{header}.{body}.{signature}")
        }

        fn sign(&self, signed: &[u8]) -> String {
            BASE64URL_NOPAD.encode(&self.key.sign(signed).unwrap())
        }

        fn confirmations(&self, human: &str, acts: Vec<ConfirmedAct>, at: DateTime<Utc>) -> Confirmations {
            let mut record = Confirmations::new(p(human), acts, at, self.jwt(human));
            record.signature = self.sign(&record.signed());
            record
        }

        fn pre_allow(&self, human: &str, assistant: &str, rules: &[&str], at: DateTime<Utc>) -> PreAllow {
            let rules = rules.iter().map(ToString::to_string).collect();
            let mut record = PreAllow::new(p(human), p(assistant), rules, at, self.jwt(human));
            record.signature = self.sign(&record.signed());
            record
        }
    }

    fn decision(ant: &str, request_id: Uuid, verdict: Verdict) -> Act {
        Act::Decision { ant: p(ant), request_id, verdict }
    }

    #[test]
    fn consent_keys_sit_under_the_human() {
        assert_eq!(confirmations_key(&p("alice")), "alice.confirmed");
        assert_eq!(pre_allow_key(&p("alice")), "alice.preallow");
        assert_eq!(ConsentKey::parse("alice.confirmed"), Some(ConsentKey::Confirmations(p("alice"))));
        assert_eq!(ConsentKey::parse("alice.preallow"), Some(ConsentKey::PreAllow(p("alice"))));
        for other in ["alice", "alice.assistant", "a.b.confirmed", "confirmed", ".preallow"] {
            assert_eq!(ConsentKey::parse(other), None, "{other}");
        }
    }

    #[test]
    fn pre_allow_rules_name_an_ant_or_every_one() {
        assert_eq!(pre_allow_rule("dev-a/execute:cargo test*"), Some((Some(p("dev-a")), "execute:cargo test*")));
        assert_eq!(pre_allow_rule("*/read:/srv/docs/*"), Some((None, "read:/srv/docs/*")));
        for malformed in ["read", "dev-a/", "Bad Name/read", "/read"] {
            assert_eq!(pre_allow_rule(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn an_answer_counts_only_signed_by_a_device_of_its_human() {
        let (device, now) = (Device::new(), Utc::now());
        let act = decision("dev-a", Uuid::now_v7(), Verdict::AllowOnce);
        let acts =
            Confirmations::answering(&[], &p("pa"), act.clone(), ActAnswer::Confirm, now + TimeDelta::minutes(5), now);
        let record = device.confirmations("alice", acts, now);
        let account = device.account.public_key();
        assert_eq!(record.verify("alice.confirmed", &account, now).unwrap(), device.id);
        assert_eq!(record.answer(&p("pa"), &act), Some(ActAnswer::Confirm));
        assert_eq!(record.answer(&p("pb"), &act), None, "another assistant's act");
        let other_verdict = decision("dev-a", act_id(&act), Verdict::AllowAlways);
        assert_eq!(record.answer(&p("pa"), &other_verdict), None, "the human confirmed another verdict");

        assert_matches!(record.verify("bob.confirmed", &account, now), Err(ConsentError::ForeignKey { .. }));
        assert_matches!(
            record.verify("alice.confirmed", &Device::new().account.public_key(), now),
            Err(ConsentError::Device(_))
        );
        let mut tampered = record;
        tampered.acts[0].answer = ActAnswer::Deny;
        assert_matches!(
            tampered.verify("alice.confirmed", &account, now),
            Err(ConsentError::Device(GrantError::BadSignature))
        );
        let mut bobs = device.confirmations("alice", Vec::new(), now);
        bobs.human = p("bob");
        assert_matches!(bobs.verify("bob.confirmed", &account, now), Err(ConsentError::Device(GrantError::Signer(_))));
        let ahead = device.confirmations("alice", Vec::new(), now + TimeDelta::minutes(10));
        assert_matches!(ahead.verify("alice.confirmed", &account, now), Err(ConsentError::Ahead(_)));
    }

    fn act_id(act: &Act) -> Uuid {
        match act {
            Act::Decision { request_id, .. } => *request_id,
            Act::Order { id, .. } => *id,
        }
    }

    #[test]
    fn answering_replaces_an_earlier_answer_and_drops_the_stale_and_the_oldest() {
        let now = Utc::now();
        let (later, past) = (now + TimeDelta::minutes(5), now - TimeDelta::seconds(1));
        let act = decision("dev-a", Uuid::now_v7(), Verdict::AllowOnce);
        let stale = ConfirmedAct {
            assistant: p("pa"),
            act: decision("dev-a", Uuid::now_v7(), Verdict::Reject),
            answer: ActAnswer::Confirm,
            until: past,
        };
        let acts = Confirmations::answering(&[stale], &p("pa"), act.clone(), ActAnswer::Confirm, later, now);
        let acts = Confirmations::answering(&acts, &p("pa"), act.clone(), ActAnswer::Deny, later, now);
        assert_eq!(acts.len(), 1, "the stale answer and the earlier one went: {acts:?}");
        assert_eq!(acts[0].answer, ActAnswer::Deny);
        let mut many = Vec::new();
        for _ in 0..MAX_CONFIRMED + 3 {
            many = Confirmations::answering(
                &many,
                &p("pa"),
                decision("dev-a", Uuid::now_v7(), Verdict::Reject),
                ActAnswer::Confirm,
                later,
                now,
            );
        }
        many = Confirmations::answering(&many, &p("pa"), act.clone(), ActAnswer::Confirm, later, now);
        assert_eq!(many.len(), MAX_CONFIRMED);
        assert_eq!(many.last().unwrap().act, act, "the newest stays");
    }

    #[test]
    fn pre_allow_rules_stand_for_the_assistant_named_and_newest_records_stand() {
        let (device, now) = (Device::new(), Utc::now());
        let account = device.account.public_key();
        let mut consents = Consents::default();
        let put = |consents: &mut Consents, key: &str, value: &[u8], at: DateTime<Utc>| {
            consents.apply(key, Some(value), &account, at).unwrap()
        };
        let rules = device.pre_allow("alice", "pa", &["dev-a/execute:cargo test*", "*/read"], now);
        let applied = put(&mut consents, "alice.preallow", &serde_json::to_vec(&rules).unwrap(), now);
        assert_eq!(applied.refused, None);
        let standing = consents.pre_allow(&p("alice"), &p("pa")).unwrap();
        assert_eq!(standing.rules_for(&p("dev-a")).collect::<Vec<_>>(), ["execute:cargo test*", "read"]);
        assert_eq!(standing.rules_for(&p("dev-b")).collect::<Vec<_>>(), ["read"]);
        assert!(consents.pre_allow(&p("alice"), &p("pb")).is_none(), "rules for another assistant");

        let older = device.pre_allow("alice", "pa", &["*/*"], now - TimeDelta::minutes(1));
        let applied = put(&mut consents, "alice.preallow", &serde_json::to_vec(&older).unwrap(), now);
        assert_matches!(applied.refused, Some(ConsentError::Replayed { .. }));
        assert!(consents.pre_allow(&p("alice"), &p("pa")).is_none(), "a refused record leaves none");
        let malformed = device.pre_allow("alice", "pa", &["read"], now + TimeDelta::seconds(1));
        let applied = put(&mut consents, "alice.preallow", &serde_json::to_vec(&malformed).unwrap(), now);
        assert_matches!(applied.refused, Some(ConsentError::Malformed(_)));

        let act = decision("dev-a", Uuid::now_v7(), Verdict::AllowOnce);
        let acts =
            Confirmations::answering(&[], &p("pa"), act.clone(), ActAnswer::Confirm, now + TimeDelta::minutes(5), now);
        let record = device.confirmations("alice", acts, now);
        put(&mut consents, "alice.confirmed", &serde_json::to_vec(&record).unwrap(), now);
        assert_eq!(consents.answer(&p("alice"), &p("pa"), &act), Some(ActAnswer::Confirm));
        assert!(consents.apply("alice.confirmed", None, &account, now).unwrap().refused.is_none());
        assert_eq!(consents.answer(&p("alice"), &p("pa"), &act), None, "deleted");
        assert!(consents.apply("alice.assistant", None, &account, now).is_none(), "a grant's key");
    }
}
