//! A human's designation of its assistant ant: an [`AssistantGrant`], signed by one of the human's devices and kept
//! in [`HUMANS_BUCKET`] under [`assistant_grant_key`].
//!
//! - The grant carries the user JWT of the device's credentials, which the team's account signed, and the device's
//!   signature of what it says ([`AssistantGrant::signed`]): a reader checks both against the account of its own
//!   credentials ([`AssistantGrant::verify`]), so no one but a device of the human makes a grant the reader honours.
//! - Clearing puts a signed withdrawal ([`AssistantGrant::assistant`] none) in its place, rather than deleting it, so
//!   the newest record the human signed is always the one stored.
//! - Replays: only the human's credentials write its grant key (`$KV.ants_humans.<human>.>`), and a reader keeping
//!   the grants ([`Grants`]) refuses a record issued before one it already read, and one issued further ahead of its
//!   clock than [`GRANT_CLOCK_SKEW`]. A refused, malformed or deleted record leaves the human without an assistant.
//! - An ant acts for the human only while the grant names it and its own user JWT, which the account signed, names the
//!   human in its assistant_of tag ([`VerifiedGrant::honours`]): neither the human nor the account key alone makes an
//!   assistant.
//! - A grant may also enroll the assistant as one of the human's devices ([`AssistantGrant::sealing`]): an encryption
//!   key the assistant holds, which senders seal the human's sealed messages for, and with which the assistant seals
//!   for the human. It stands and falls with the grant, so a withdrawal, or a grant without it, stops every sender
//!   sealing for it at once.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::{KeyPair, KeyPairType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(doc)]
use crate::HUMANS_BUCKET;
use crate::{DeviceId, DeviceKey, UserId};

/// Last token of the key of a human's grant in [`HUMANS_BUCKET`] ([`assistant_grant_key`]).
pub const ASSISTANT_GRANT: &str = "assistant";

/// How far ahead of a reader's clock a grant may have been issued. A grant issued further ahead would outrank, for
/// [`Grants`], every grant the human makes until then.
pub const GRANT_CLOCK_SKEW: TimeDelta = TimeDelta::minutes(5);

/// Key in [`HUMANS_BUCKET`] of the grant of `human`: `<human>.assistant`, under the human's own name, which its
/// credentials alone write.
pub fn assistant_grant_key(human: &UserId) -> String {
    format!("{human}.{ASSISTANT_GRANT}")
}

/// The human whose grant `key` is the key of ([`assistant_grant_key`]), if it is one.
pub fn assistant_grant_of(key: &str) -> Option<UserId> {
    key.strip_suffix(ASSISTANT_GRANT)?.strip_suffix('.')?.parse().ok()
}

/// That `human` designates `assistant` as its assistant, or none any more, signed by a device of the human. See the
/// [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantGrant {
    pub human: UserId,
    /// The ant designated; none in a withdrawal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant: Option<UserId>,
    /// When the device signed it: of two records of the human, the one issued later stands.
    pub issued_at: DateTime<Utc>,
    /// The assistant enrolled as a device of the human, for sealed messages; none: it reads and sends none. Never in a
    /// withdrawal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealing: Option<SealingDevice>,
    /// The user JWT of the signing device's credentials, which binds the key that signed to the human.
    pub jwt: String,
    /// Signature of [`AssistantGrant::signed`] by the key `jwt` is for, base64url without padding.
    pub signature: String,
}

/// The assistant of a grant enrolled as a device of its human ([`AssistantGrant::sealing`]): a device id of its own and
/// the public key of an encryption key whose seed the assistant's machine holds. Senders seal the human's sealed
/// messages for it as for the human's other devices, and readers take it as the sending device of a sealed message
/// from the assistant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealingDevice {
    pub device: DeviceId,
    pub xkey: DeviceKey,
}

impl AssistantGrant {
    /// The grant of `assistant` (none: a withdrawal) by `human`, issued at `issued_at` by the device whose user JWT is
    /// `jwt`, to be completed with its signature.
    pub fn new(human: UserId, assistant: Option<UserId>, issued_at: DateTime<Utc>, jwt: impl Into<String>) -> Self {
        Self { human, assistant, issued_at, sealing: None, jwt: jwt.into(), signature: String::new() }
    }

    /// The grant, enrolling its assistant as the device `sealing` of the human, if any.
    #[must_use]
    pub fn with_sealing(mut self, sealing: Option<SealingDevice>) -> Self {
        self.sealing = sealing;
        self
    }

    /// The key of the grant in [`HUMANS_BUCKET`].
    pub fn key(&self) -> String {
        assistant_grant_key(&self.human)
    }

    /// What the device signs: every field but the JWT, which the signing key is checked against, and the signature.
    /// The device and key of [`AssistantGrant::sealing`] follow on lines of their own when it enrolls one.
    pub fn signed(&self) -> Vec<u8> {
        let issued_at = self.issued_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let assistant = self.assistant.as_ref().map_or("", UserId::as_str);
        let mut lines =
            vec!["ants-assistant-grant".to_owned(), self.human.to_string(), assistant.to_owned(), issued_at];
        if let Some(sealing) = &self.sealing {
            lines.extend([sealing.device.to_string(), sealing.xkey.to_string()]);
        }
        lines.join("\n").into_bytes()
    }

    /// Check the grant, read from `key`, for a reader of the team whose account is `account` (`A...`, the issuer of
    /// the reader's own credentials), at `now`: under its human's key, naming another ant than the human, issued no
    /// further ahead than [`GRANT_CLOCK_SKEW`], and signed by credentials of a device of the human that the account
    /// signed and that had not expired when it was issued.
    pub fn verify(&self, key: &str, account: &str, now: DateTime<Utc>) -> Result<VerifiedGrant, GrantError> {
        if key != self.key() {
            return Err(GrantError::ForeignKey { human: self.human.clone(), key: key.to_owned() });
        }
        if self.assistant.as_ref() == Some(&self.human) {
            return Err(GrantError::Malformed(format!("{} names itself", self.human)));
        }
        if self.assistant.is_none() && self.sealing.is_some() {
            return Err(GrantError::Malformed("a withdrawal enrolls a device".to_owned()));
        }
        if self.issued_at > now + GRANT_CLOCK_SKEW {
            return Err(GrantError::Ahead(self.issued_at));
        }
        let (device, signer) =
            device_signed(&self.human, &self.jwt, &self.signature, &self.signed(), self.issued_at, account)?;
        Ok(VerifiedGrant {
            human: self.human.clone(),
            assistant: self.assistant.clone(),
            issued_at: self.issued_at,
            sealing: self.sealing.clone(),
            device,
            signer,
        })
    }
}

/// Check that `signature` (base64url without padding) over `signed` was made by the credentials whose user JWT is
/// `jwt`: a device of `human` the team's account `account` signed, not expired at `issued_at`. Returns the device and
/// the public key (`U...`) that signed. For every record a human's device signs ([`AssistantGrant`],
/// [`crate::Confirmations`], [`crate::PreAllow`]).
pub(crate) fn device_signed(
    human: &UserId,
    jwt: &str,
    signature: &str,
    signed: &[u8],
    issued_at: DateTime<Utc>,
    account: &str,
) -> Result<(DeviceId, String), GrantError> {
    let jwt = UserJwt::verify(jwt).map_err(|e| GrantError::Signer(e.to_string()))?;
    let signer = |reason: String| Err(GrantError::Signer(reason));
    if jwt.iss != account {
        return signer(format!("signed by {}, not by the team's account {account}", jwt.iss));
    }
    if jwt.name != human.as_str() {
        return signer(format!("named {}, not {human}", jwt.name));
    }
    let Some(device) = DeviceId::from_tags(jwt.tags.iter().map(String::as_str)) else {
        return signer("not a human device's".to_owned());
    };
    if let Some(exp) = jwt.exp.filter(|exp| *exp <= issued_at.timestamp()) {
        return signer(format!("expired at {exp}, before the record was issued"));
    }
    let signature = BASE64URL_NOPAD.decode(signature.as_bytes()).map_err(|_| GrantError::BadSignature)?;
    let key = KeyPair::from_public_key(&jwt.sub).map_err(|e| GrantError::Signer(e.to_string()))?;
    key.verify(signed, &signature).map_err(|_| GrantError::BadSignature)?;
    Ok((device, jwt.sub))
}

/// A grant a reader verified ([`AssistantGrant::verify`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedGrant {
    pub human: UserId,
    /// None for a withdrawal.
    pub assistant: Option<UserId>,
    pub issued_at: DateTime<Utc>,
    /// The assistant enrolled as a device of the human, if it is.
    pub sealing: Option<SealingDevice>,
    /// The device that signed it.
    pub device: DeviceId,
    /// The public key of the credentials that signed it, `U...`.
    pub signer: String,
}

impl VerifiedGrant {
    /// Whether `assistant` acts for the grant's human: the grant names it, and `assistant_of`, the human the
    /// assistant_of tag of its user JWT names (as the reader verified it), is the grant's human.
    pub fn honours(&self, assistant: &UserId, assistant_of: Option<&UserId>) -> bool {
        self.assistant.as_ref() == Some(assistant) && assistant_of == Some(&self.human)
    }
}

/// Why a grant is not honoured.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrantError {
    #[error("grant of {human} found at key {key}")]
    ForeignKey { human: UserId, key: String },
    #[error("malformed grant: {0}")]
    Malformed(String),
    #[error("issued at {0}, ahead of the reader's clock")]
    Ahead(DateTime<Utc>),
    #[error("the credentials it carries: {0}")]
    Signer(String),
    #[error("not signed by the credentials it carries")]
    BadSignature,
    #[error("issued at {issued_at}, before the record of {latest} already read: a replay")]
    Replayed { issued_at: DateTime<Utc>, latest: DateTime<Utc> },
}

/// The claims of a user JWT whose issuer signed it ([`UserJwt::verify`]).
#[derive(Debug, Clone, PartialEq)]
pub struct UserJwt {
    /// The user's public key, `U...`.
    pub sub: String,
    /// The key that signed it.
    pub iss: String,
    pub name: String,
    /// Expiry, in seconds since the epoch.
    pub exp: Option<i64>,
    pub tags: Vec<String>,
    /// All of its claims.
    pub claims: Value,
}

/// Why [`UserJwt::verify`] refuses a user JWT.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UserJwtError {
    #[error("not a JWT")]
    Malformed,
    #[error("no {0}")]
    Missing(&'static str),
    #[error("issuer {issuer}: {reason}")]
    InvalidIssuer { issuer: String, reason: String },
    #[error("not signed by its issuer {0}")]
    NotSignedByIssuer(String),
    #[error("not a user JWT")]
    NotUser,
    #[error("public key {key}: {reason}")]
    InvalidKey { key: String, reason: String },
    #[error("{0} is not the public key of a user")]
    NotUserKey(String),
}

impl UserJwt {
    /// Decode `jwt`, a user JWT, checking that its issuer signed it; else why not.
    pub fn verify(jwt: &str) -> Result<Self, UserJwtError> {
        let (signed, signature) = jwt.trim().rsplit_once('.').ok_or(UserJwtError::Malformed)?;
        let (_, body) = signed.split_once('.').ok_or(UserJwtError::Malformed)?;
        let decode = |part: &str| {
            BASE64URL_NOPAD.decode(part.trim_end_matches('=').as_bytes()).map_err(|_| UserJwtError::Malformed)
        };
        let claims: Value = serde_json::from_slice(&decode(body)?).map_err(|_| UserJwtError::Malformed)?;
        let text = |field: &'static str| claims[field].as_str().map(str::to_owned).ok_or(UserJwtError::Missing(field));
        let iss = text("iss")?;
        let issuer = KeyPair::from_public_key(&iss)
            .map_err(|e| UserJwtError::InvalidIssuer { issuer: iss.clone(), reason: e.to_string() })?;
        issuer
            .verify(signed.as_bytes(), &decode(signature)?)
            .map_err(|_| UserJwtError::NotSignedByIssuer(iss.clone()))?;
        if claims["nats"]["type"] != "user" {
            return Err(UserJwtError::NotUser);
        }
        let sub = text("sub")?;
        let user = KeyPair::from_public_key(&sub)
            .map_err(|e| UserJwtError::InvalidKey { key: sub.clone(), reason: e.to_string() })?;
        if user.key_pair_type() != KeyPairType::User {
            return Err(UserJwtError::NotUserKey(sub));
        }
        let tags = claims["nats"]["tags"].as_array().into_iter().flatten().filter_map(Value::as_str);
        let tags = tags.map(str::to_owned).collect();
        Ok(Self { name: text("name")?, exp: claims["exp"].as_i64(), tags, sub, iss, claims })
    }
}

/// The grants of the team's humans as a reader of [`HUMANS_BUCKET`] keeps them, fed each record as it reads or
/// watches it ([`Grants::apply`]), so that a withdrawal holds from the moment the reader sees it.
#[derive(Debug, Clone, Default)]
pub struct Grants {
    humans: BTreeMap<UserId, Standing>,
}

#[derive(Debug, Clone)]
struct Standing {
    /// None while the human has no grant the reader honours.
    grant: Option<VerifiedGrant>,
    /// When the newest record of the human the reader verified was issued: an older one is a replay.
    latest: DateTime<Utc>,
}

/// What a record changed of its human's assistant ([`Grants::apply`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub human: UserId,
    /// The ant the human designated before, if any.
    pub before: Option<UserId>,
    /// The ant the human designates now, if any.
    pub after: Option<UserId>,
    /// Why the record was refused, leaving the human without an assistant.
    pub refused: Option<GrantError>,
}

impl Applied {
    /// The ant that stopped being the human's assistant, if one did: what it did for the human and is not confirmed
    /// yet no longer counts.
    pub fn withdrawn(&self) -> Option<&UserId> {
        self.before.as_ref().filter(|before| self.after.as_ref() != Some(*before))
    }
}

impl Grants {
    /// Apply `value`, what the key `key` of [`HUMANS_BUCKET`] holds now (none: deleted or purged), checked for the
    /// team of the account `account` at `now` ([`AssistantGrant::verify`]); None when `key` is no grant's key.
    pub fn apply(&mut self, key: &str, value: Option<&[u8]>, account: &str, now: DateTime<Utc>) -> Option<Applied> {
        let human = assistant_grant_of(key)?;
        let standing = self.humans.remove(&human);
        let before = standing.as_ref().and_then(|s| s.grant.as_ref()?.assistant.clone());
        let latest = standing.map(|s| s.latest);
        let verified = value.map(|value| {
            let grant: AssistantGrant =
                serde_json::from_slice(value).map_err(|e| GrantError::Malformed(e.to_string()))?;
            let verified = grant.verify(key, account, now)?;
            match latest {
                Some(latest) if verified.issued_at < latest => {
                    Err(GrantError::Replayed { issued_at: verified.issued_at, latest })
                }
                _ => Ok(verified),
            }
        });
        let (grant, refused) = match verified {
            Some(Ok(grant)) => (Some(grant), None),
            Some(Err(e)) => (None, Some(e)),
            None => (None, None),
        };
        let after = grant.as_ref().and_then(|g| g.assistant.clone());
        let latest = latest.into_iter().chain(grant.as_ref().map(|g| g.issued_at)).max();
        if let Some(latest) = latest {
            self.humans.insert(human.clone(), Standing { grant, latest });
        }
        Some(Applied { human, before, after, refused })
    }

    /// The ant `human` designates by a grant standing, whose own credentials the reader is still to check
    /// ([`Grants::honours`]).
    pub fn designated(&self, human: &UserId) -> Option<&UserId> {
        self.grant(human)?.assistant.as_ref()
    }

    /// The device `human` enrolled its assistant as, by a grant standing ([`AssistantGrant::sealing`]).
    pub fn sealing(&self, human: &UserId) -> Option<&SealingDevice> {
        self.grant(human)?.sealing.as_ref()
    }

    /// The record of `human` standing, a grant or a withdrawal.
    pub fn grant(&self, human: &UserId) -> Option<&VerifiedGrant> {
        self.humans.get(human)?.grant.as_ref()
    }

    /// Whether `assistant`, whose user JWT's assistant_of tag names `assistant_of`, acts for `human` now
    /// ([`VerifiedGrant::honours`]).
    pub fn honours(&self, human: &UserId, assistant: &UserId, assistant_of: Option<&UserId>) -> bool {
        self.grant(human).is_some_and(|grant| grant.honours(assistant, assistant_of))
    }

    /// The humans with a grant standing, and the ant each designates.
    pub fn designations(&self) -> impl Iterator<Item = (&UserId, &UserId)> {
        self.humans.iter().filter_map(|(human, s)| Some((human, s.grant.as_ref()?.assistant.as_ref()?)))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use serde_json::json;

    use super::*;
    use crate::p;

    struct Team {
        account: KeyPair,
        device: KeyPair,
        id: DeviceId,
    }

    impl Team {
        fn new() -> Self {
            Self { account: KeyPair::new_account(), device: KeyPair::new_user(), id: DeviceId::generate() }
        }

        fn jwt(&self, signer: &KeyPair, name: &str, tags: &[String], exp: Option<i64>) -> String {
            let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
            let mut claims = json!({
                "sub": self.device.public_key(), "iss": signer.public_key(), "name": name,
                "nats": { "type": "user", "tags": tags },
            });
            if let Some(exp) = exp {
                claims["exp"] = json!(exp);
            }
            let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
            let signature = BASE64URL_NOPAD.encode(&signer.sign(format!("{header}.{body}").as_bytes()).unwrap());
            format!("{header}.{body}.{signature}")
        }

        fn device_jwt(&self, human: &str) -> String {
            self.jwt(&self.account, human, &[self.id.tag()], None)
        }

        fn grant(&self, human: &str, assistant: Option<&str>, issued_at: DateTime<Utc>) -> AssistantGrant {
            self.signed(AssistantGrant::new(p(human), assistant.map(p), issued_at, self.device_jwt(human)))
        }

        fn signed(&self, mut grant: AssistantGrant) -> AssistantGrant {
            grant.signature = BASE64URL_NOPAD.encode(&self.device.sign(&grant.signed()).unwrap());
            grant
        }

        fn account(&self) -> String {
            self.account.public_key()
        }
    }

    #[test]
    fn grant_keys_sit_under_the_human() {
        assert_eq!(assistant_grant_key(&p("alice")), "alice.assistant");
        assert_eq!(assistant_grant_of("alice.assistant"), Some(p("alice")));
        for other in ["alice", "assistant", ".assistant", "alice.other", "a.b.assistant", "aliceassistant"] {
            assert_eq!(assistant_grant_of(other), None, "{other}");
        }
    }

    #[test]
    fn a_grant_counts_only_signed_by_a_device_of_its_human_the_account_vouches_for() {
        let (team, now) = (Team::new(), Utc::now());
        let grant = team.grant("alice", Some("pa"), now);
        let json = serde_json::to_string(&grant).unwrap();
        assert_eq!(serde_json::from_str::<AssistantGrant>(&json).unwrap(), grant);
        let verified = grant.verify("alice.assistant", &team.account(), now).unwrap();
        assert_eq!((verified.assistant.as_ref(), verified.device), (Some(&p("pa")), team.id));
        assert_eq!(verified.signer, team.device.public_key());
        assert!(verified.honours(&p("pa"), Some(&p("alice"))));
        assert!(!verified.honours(&p("pa"), None), "no assistant_of tag");
        assert!(!verified.honours(&p("pa"), Some(&p("bob"))), "the assistant of another human");
        assert!(!verified.honours(&p("pb"), Some(&p("alice"))), "not the ant designated");

        let refused = |grant: &AssistantGrant, key: &str| grant.verify(key, &team.account(), now).unwrap_err();
        assert_matches!(refused(&grant, "bob.assistant"), GrantError::ForeignKey { .. });
        let other_account = Team::new();
        let err = grant.verify("alice.assistant", &other_account.account(), now).unwrap_err().to_string();
        assert!(err.contains("not by the team's account"), "{err}");

        let mut tampered = grant.clone();
        tampered.assistant = Some(p("pb"));
        assert_eq!(refused(&tampered, "alice.assistant"), GrantError::BadSignature);
        let mut bobs = grant;
        bobs.human = p("bob");
        let err = refused(&bobs, "bob.assistant").to_string();
        assert!(err.contains("named alice, not bob"), "another human's device: {err}");

        let unsigned = |jwt: String| team.signed(AssistantGrant::new(p("alice"), Some(p("pa")), now, jwt));
        let by_itself = unsigned(team.jwt(&team.device, "alice", &[team.id.tag()], None));
        assert_matches!(refused(&by_itself, "alice.assistant"), GrantError::Signer(_));
        let no_device = unsigned(team.jwt(&team.account, "alice", &[], None));
        assert!(refused(&no_device, "alice.assistant").to_string().contains("not a human device's"));
        let expired = unsigned(team.jwt(&team.account, "alice", &[team.id.tag()], Some(now.timestamp() - 1)));
        assert!(refused(&expired, "alice.assistant").to_string().contains("before the record was issued"));
        let later = unsigned(team.jwt(&team.account, "alice", &[team.id.tag()], Some(now.timestamp() + 60)));
        assert!(later.verify("alice.assistant", &team.account(), now + TimeDelta::hours(1)).is_ok(), "since expired");

        let ahead = team.grant("alice", Some("pa"), now + GRANT_CLOCK_SKEW + TimeDelta::seconds(1));
        assert_matches!(refused(&ahead, "alice.assistant"), GrantError::Ahead(_));
        let itself = team.grant("alice", Some("alice"), now);
        assert_matches!(refused(&itself, "alice.assistant"), GrantError::Malformed(_));
    }

    #[test]
    fn an_enrolled_device_is_signed_with_the_grant_and_never_in_a_withdrawal() {
        let (team, now) = (Team::new(), Utc::now());
        let sealing = SealingDevice {
            device: DeviceId::generate(),
            xkey: "XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap(),
        };
        let enrolled = AssistantGrant::new(p("alice"), Some(p("pa")), now, team.device_jwt("alice"));
        let enrolled = team.signed(enrolled.with_sealing(Some(sealing.clone())));
        let json = serde_json::to_string(&enrolled).unwrap();
        assert_eq!(serde_json::from_str::<AssistantGrant>(&json).unwrap(), enrolled);
        let verified = enrolled.verify("alice.assistant", &team.account(), now).unwrap();
        assert_eq!(verified.sealing.as_ref(), Some(&sealing));
        assert_eq!(
            team.grant("alice", Some("pa"), now).verify("alice.assistant", &team.account(), now).unwrap().sealing,
            None
        );

        let refused = |grant: &AssistantGrant| grant.verify("alice.assistant", &team.account(), now).unwrap_err();
        let stripped = AssistantGrant { sealing: None, ..enrolled.clone() };
        assert_eq!(refused(&stripped), GrantError::BadSignature, "the device is signed");
        let mut swapped = enrolled.clone();
        swapped.sealing.as_mut().unwrap().xkey =
            "XAFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWBB".parse().unwrap();
        assert_eq!(refused(&swapped), GrantError::BadSignature, "the key is signed");
        let withdrawal = team.signed(
            AssistantGrant::new(p("alice"), None, now, team.device_jwt("alice")).with_sealing(Some(sealing.clone())),
        );
        assert_matches!(refused(&withdrawal), GrantError::Malformed(_));

        let mut grants = Grants::default();
        grants.apply("alice.assistant", Some(&serde_json::to_vec(&enrolled).unwrap()), &team.account(), now);
        assert_eq!(grants.sealing(&p("alice")), Some(&sealing));
        let unsealed = team.grant("alice", Some("pa"), now + TimeDelta::seconds(1));
        let applied =
            grants.apply("alice.assistant", Some(&serde_json::to_vec(&unsealed).unwrap()), &team.account(), now);
        assert_eq!((applied.unwrap().withdrawn(), grants.sealing(&p("alice"))), (None, None), "sealing off at once");
    }

    #[test]
    fn the_newest_record_stands_and_a_withdrawal_holds_at_once() {
        let (team, now) = (Team::new(), Utc::now());
        let account = team.account();
        let mut grants = Grants::default();
        let bytes = |grant: &AssistantGrant| serde_json::to_vec(grant).unwrap();
        let apply =
            |grants: &mut Grants, value: Option<&[u8]>| grants.apply("alice.assistant", value, &account, now).unwrap();
        assert_eq!(grants.apply("alice", Some(b"{}"), &account, now), None, "a human's entry");

        let to_pa = team.grant("alice", Some("pa"), now - TimeDelta::minutes(2));
        let applied = apply(&mut grants, Some(&bytes(&to_pa)));
        assert_eq!((applied.before, applied.after.clone(), applied.refused), (None, Some(p("pa")), None));
        assert_eq!(grants.designated(&p("alice")), Some(&p("pa")));
        assert!(grants.honours(&p("alice"), &p("pa"), Some(&p("alice"))));
        assert_eq!(grants.designations().collect::<Vec<_>>(), [(&p("alice"), &p("pa"))]);

        let to_pb = team.grant("alice", Some("pb"), now - TimeDelta::minutes(1));
        let applied = apply(&mut grants, Some(&bytes(&to_pb)));
        assert_eq!(applied.withdrawn(), Some(&p("pa")), "replaced");
        assert!(!grants.honours(&p("alice"), &p("pa"), Some(&p("alice"))));

        let replayed = apply(&mut grants, Some(&bytes(&to_pa)));
        assert_matches!(replayed.refused, Some(GrantError::Replayed { .. }));
        assert_eq!((replayed.withdrawn(), grants.designated(&p("alice"))), (Some(&p("pb")), None), "fails safe");

        let cleared = team.grant("alice", None, now);
        let applied = apply(&mut grants, Some(&bytes(&cleared)));
        assert_eq!((applied.before, applied.after, applied.refused), (None, None, None));
        assert_eq!(grants.grant(&p("alice")).map(|g| g.assistant.clone()), Some(None), "a withdrawal stands");
        let applied = apply(&mut grants, Some(&bytes(&to_pb)));
        assert_matches!(applied.refused, Some(GrantError::Replayed { .. }));

        let again = team.grant("alice", Some("pa"), now);
        assert_eq!(apply(&mut grants, Some(&bytes(&again))).after, Some(p("pa")));
        let deleted = apply(&mut grants, None);
        assert_eq!((deleted.withdrawn(), &deleted.refused), (Some(&p("pa")), &None));
        let applied = apply(&mut grants, Some(&bytes(&to_pb)));
        assert_matches!(applied.refused, Some(GrantError::Replayed { .. }), "deleting forgets nothing");
        let applied = apply(&mut grants, Some(b"not json"));
        assert_matches!(applied.refused, Some(GrantError::Malformed(_)));
        assert_eq!(apply(&mut grants, Some(&bytes(&again))).after, Some(p("pa")), "the newest record again");
    }
}
