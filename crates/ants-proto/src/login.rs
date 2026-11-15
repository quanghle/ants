//! Self-service registration of a human's device: `ants login` asks `ants signer` on [`subject::login`] with the
//! team's public bootstrap credentials ([`LoginRequest`]), and the signer answers on [`subject::login_reply`]
//! ([`LoginReply`]), sealing what it says to the request's one-off X25519 key: anyone holding the bootstrap
//! credentials hears every answer, and only the asking machine opens its own.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{DeviceKey, Error, UserId, subject};

/// The `name` of the user JWT of the team's bootstrap credentials (`ants auth bootstrap`).
pub const BOOTSTRAP_USER: &str = "bootstrap";

/// The user JWT tag of credentials a registration granted, which `ants signer` signs and renews, not
/// `ants enroll renew`: it tells their holder which renewal applies. The signer itself goes by its registry.
pub const REGISTERED_TAG: &str = "ants-registered";

/// Whether a user JWT's `tags` carry [`REGISTERED_TAG`], in either case.
pub fn tagged_registered<'a>(tags: impl IntoIterator<Item = &'a str>) -> bool {
    tags.into_iter().any(|tag| tag.eq_ignore_ascii_case(REGISTERED_TAG))
}

/// The largest message, in bytes, bootstrap credentials may publish.
pub const BOOTSTRAP_MAX_PAYLOAD: u32 = 4096;

/// How long a registration request is good for: `ants login` stops asking, and the signer stops answering, after it.
pub const LOGIN_TTL: Duration = Duration::from_mins(15);

/// How often `ants login` repeats its request until an answer settles it.
pub const LOGIN_REPEAT: Duration = Duration::from_secs(5);

/// Most registrations `ants signer` runs at once; it refuses more as busy.
pub const MAX_REGISTRATIONS: usize = 10;

/// A machine asking, on [`subject::login`], for the credentials of a new device of the human `name`: a user JWT for
/// `public_key`, the device's NKey, whose seed stays on the machine. The request is signed by that NKey
/// ([`LoginRequest::signed`]), so whoever else holds the bootstrap credentials cannot change it, and names
/// `reply_xkey`, a one-off X25519 key the answers are sealed to ([`LoginReply`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginRequest {
    pub name: UserId,
    /// The device's user NKey, `U...`.
    pub public_key: String,
    /// The one-off X25519 public key (`X...`) the answers are sealed to, generated for this request.
    pub reply_xkey: String,
    /// The device's own encryption key, generated beside its NKey, for its user JWT to bind ([`DeviceKey::tag`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xkey: Option<DeviceKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub made_at: DateTime<Utc>,
    /// When the machine stops asking, [`LOGIN_TTL`] after [`LoginRequest::made_at`].
    pub expires_at: DateTime<Utc>,
    /// Signature of [`LoginRequest::signed`] by `public_key`, base64url without padding.
    pub signature: String,
}

impl LoginRequest {
    /// A request made now, good for [`LOGIN_TTL`], to be completed with its signature.
    pub fn new(name: UserId, public_key: impl Into<String>, reply_xkey: impl Into<String>) -> Self {
        let made_at = Utc::now();
        Self {
            name,
            public_key: public_key.into(),
            reply_xkey: reply_xkey.into(),
            xkey: None,
            label: None,
            made_at,
            expires_at: made_at + LOGIN_TTL,
            signature: String::new(),
        }
    }

    /// What the device's NKey signs: every field but the signature, one per line, the label last as it is free text.
    pub fn signed(&self) -> Vec<u8> {
        let time = |t: &DateTime<Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let xkey = self.xkey.as_ref().map_or("", DeviceKey::as_str);
        let label = self.label.as_deref().map_or_else(String::new, |l| format!("+{l}"));
        let fields = [
            self.name.as_str(),
            &self.public_key,
            &self.reply_xkey,
            xkey,
            &time(&self.made_at),
            &time(&self.expires_at),
        ];
        let mut signed = ["ants-login"].into_iter().chain(fields).collect::<Vec<_>>().join("\n");
        signed.push('\n');
        signed.push_str(&label);
        signed.into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::login(&self.name)
    }

    /// Check that the request arrived on its own [`LoginRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "registration request", user: self.name.clone(), at: subject.to_owned() })
    }

    /// Whether the request is still good at `now`: made no later than now (give or take a minute of clock skew), not
    /// expired, and good for no longer than [`LOGIN_TTL`].
    pub fn is_live(&self, now: DateTime<Utc>) -> bool {
        let skew = chrono::TimeDelta::minutes(1);
        let ttl = chrono::TimeDelta::from_std(LOGIN_TTL).unwrap_or_default();
        self.made_at <= now + skew && now < self.expires_at && self.expires_at <= self.made_at + ttl
    }
}

/// An answer to a [`LoginRequest`], on [`subject::login_reply`]: the request's `public_key` in the clear, so a machine
/// skips answers to others, and a [`LoginAnswer`] sealed by the signer's X25519 key `sender` to the request's
/// `reply_xkey` (nkeys xkeys, base64url without padding).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginReply {
    pub name: UserId,
    pub public_key: String,
    /// The signer's X25519 public key, `X...`.
    pub sender: String,
    pub sealed: String,
}

impl LoginReply {
    pub fn subject(&self) -> String {
        subject::login_reply(&self.name)
    }

    /// Whether the reply answers the request of `public_key`.
    pub fn answers(&self, public_key: &str) -> bool {
        self.public_key == public_key
    }
}

/// What the signer says to a [`LoginRequest`], sealed in its [`LoginReply`]. A JWT or a refusal settles the request;
/// the others say what it waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum LoginAnswer {
    /// Sign in to GitHub at `url` and enter `user_code` there, before `expires_at`.
    Verify { url: String, user_code: String, expires_at: DateTime<Utc> },
    /// Signed in as GitHub user `login`, outside the registration policy: a human of the team decides.
    Waiting { login: String, reason: String },
    /// Refused, and why.
    Refused { reason: String },
    /// The user JWT of the request's key, for a new device of the human.
    Signed { jwt: String },
}

impl LoginAnswer {
    /// Whether the answer settles the request: no other comes after it.
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Refused { .. } | Self::Signed { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn a_registration_request_signs_all_but_its_signature_and_is_good_for_15_minutes() {
        let mut request = LoginRequest::new(p("alice"), "UKEY", "XKEY");
        request.label = Some("laptop".into());
        let bare = request.signed();
        let back: LoginRequest = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back.signed(), bare, "the same bytes after the wire");
        request.signature = "sig".into();
        assert_eq!(request.signed(), bare);
        for change in [
            |r: &mut LoginRequest| r.name = p("bob"),
            |r: &mut LoginRequest| r.public_key = "UOTHER".into(),
            |r: &mut LoginRequest| r.reply_xkey = "XOTHER".into(),
            |r: &mut LoginRequest| r.xkey = Some(format!("X{}", "A".repeat(55)).parse().unwrap()),
            |r: &mut LoginRequest| r.label = None,
            |r: &mut LoginRequest| r.label = Some(String::new()),
            |r: &mut LoginRequest| r.made_at += chrono::TimeDelta::nanoseconds(1),
            |r: &mut LoginRequest| r.expires_at += chrono::TimeDelta::nanoseconds(1),
        ] {
            let mut changed = request.clone();
            change(&mut changed);
            assert_ne!(changed.signed(), bare);
        }
        assert_eq!(request.check_subject("ants.login.alice"), Ok(()));
        assert!(request.check_subject("ants.login.bob").is_err());

        let now = request.made_at;
        assert!(request.is_live(now));
        assert!(!request.is_live(request.expires_at));
        assert!(!request.is_live(now - chrono::TimeDelta::minutes(2)), "made in the future");
        let mut long = request;
        long.expires_at += chrono::TimeDelta::seconds(1);
        assert!(!long.is_live(now), "good for longer than LOGIN_TTL");
    }

    #[test]
    fn only_the_registered_tag_marks_registered_credentials() {
        assert!(tagged_registered(["ants-perms:2", "ants-registered"]));
        assert!(tagged_registered(["ANTS-REGISTERED"]));
        assert!(!tagged_registered(["ants-perms:2", "ants-registered:x", "ants-device:x"]));
    }

    #[test]
    fn answers_name_their_outcome_and_settle_with_a_jwt_or_a_refusal() {
        let verify = LoginAnswer::Verify { url: "u".into(), user_code: "C".into(), expires_at: Utc::now() };
        let json = serde_json::to_value(&verify).unwrap();
        assert_eq!(json["outcome"], "verify");
        assert_eq!(serde_json::from_value::<LoginAnswer>(json).unwrap(), verify);
        assert!(!verify.is_final());
        assert!(LoginAnswer::Signed { jwt: "j".into() }.is_final());
        assert!(LoginAnswer::Refused { reason: "r".into() }.is_final());
        let reply = LoginReply { name: p("alice"), public_key: "UKEY".into(), sender: "X".into(), sealed: "s".into() };
        assert_eq!(reply.subject(), "ants.login.alice.reply");
        assert!(reply.answers("UKEY") && !reply.answers("UOTHER"));
    }
}
