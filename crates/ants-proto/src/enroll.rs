use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::inbox_prefix;
use crate::{Error, Machine, UserId, subject};

/// Reply prefix of the connections of `ants enroll`: its first token starts with `_`, which no user id does, so no
/// user's own reply inboxes ([`inbox_prefix`]) cover it.
pub const ENROLL_INBOX: &str = "_INBOX._enroll";

/// The `name` of the user JWT of `ant`'s enrollment credentials.
pub fn enrollment_user(ant: &UserId) -> String {
    format!("enroll-{ant}")
}

/// How often `ants add --enroll` repeats its [`EnrollRequest`] while it waits for an answer. Requests are core
/// NATS and stored nowhere, so `ants enroll` sees those sent while it listens.
pub const ENROLL_REPEAT: Duration = Duration::from_secs(5);

/// A machine asking for the credentials of `ant` on [`subject::enroll`]: the public key of the user NKey it
/// generated, whose seed stays there. The machine holding the account key signs a user JWT for that key, or refuses,
/// and answers with an [`EnrollReply`].
///
/// The request carries the user JWT of the enrollment credentials it is sent with and their key's `signature` of
/// [`EnrollRequest::signed`], so the approver takes only requests of a holder of the ant's enrollment credentials,
/// whatever connection carried them (a leaf node's uplink may carry anyone's).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub ant: UserId,
    /// The user's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// The user JWT of the enrollment credentials of `ant`.
    pub enrollment: String,
    /// Signature of [`EnrollRequest::signed`] by the enrollment credentials' key, base64url without padding.
    pub signature: String,
    /// The leaf node the asking machine reaches the hub through, whose uplink the approval extends to the ant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf: Option<EnrollLeaf>,
}

/// The leaf node of an asking machine ([`EnrollRequest::leaf`]): its machine name, the user JWT of its uplink
/// credentials, and their key's signature of [`EnrollRequest::signed`], base64url without padding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollLeaf {
    pub machine: Machine,
    pub jwt: String,
    pub signature: String,
}

impl EnrollRequest {
    /// A request sent now, to be completed with the enrollment credentials' JWT and signatures.
    pub fn new(ant: UserId, public_key: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            ant,
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            enrollment: String::new(),
            signature: String::new(),
            leaf: None,
        }
    }

    /// What the enrollment key and the leaf's uplink key sign: every field but the signatures, one per line.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let (machine, leaf) = self.leaf.as_ref().map_or(("", ""), |l| (l.machine.as_str(), l.jwt.as_str()));
        let fields =
            ["ants-enroll", self.ant.as_str(), &self.public_key, &sent_at, &self.enrollment, machine, leaf, &self.host];
        fields.join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::enroll(&self.ant)
    }

    /// Check that the request arrived on its own [`EnrollRequest::subject`], which enrollment credentials pin.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "enrollment request", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// The answer to an [`EnrollRequest`], on [`subject::enroll_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EnrollReply {
    /// The user JWT of `public_key`, which completes the ant's credentials with the seed kept on its machine, and
    /// for a request through a leaf node ([`EnrollRequest::leaf`]) the renewed user JWT of the leaf's uplink key,
    /// allowing the ant as well.
    Approved {
        ant: UserId,
        public_key: String,
        jwt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_jwt: Option<String>,
    },
    /// No credentials for `public_key`, or for any key asking for `ant` when it is `None`.
    Rejected {
        ant: UserId,
        public_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl EnrollReply {
    pub fn ant(&self) -> &UserId {
        match self {
            Self::Approved { ant, .. } | Self::Rejected { ant, .. } => ant,
        }
    }

    pub fn subject(&self) -> String {
        subject::enroll_reply(self.ant())
    }

    /// Whether the answer is meant for the request of `public_key`.
    pub fn answers(&self, public_key: &str) -> bool {
        match self {
            Self::Approved { public_key: key, .. } => key == public_key,
            Self::Rejected { public_key: key, .. } => key.as_deref().is_none_or(|key| key == public_key),
        }
    }
}

/// A holder of credentials asking for them renewed on [`subject::renew`] (`ants renew`): the public key of the user
/// NKey it generated, whose seed stays with it. The machine holding the account key signs a user JWT for that key with
/// the same name and kind as the credentials renewed, with the permissions their kind has now, and answers with a
/// [`RenewReply`].
///
/// The request carries the user JWT of the credentials renewed (`current`) and their key's `signature` of
/// [`RenewRequest::signed`], so the approver renews only credentials whose holder asks, whatever connection carried the
/// request: a human's carries the uplink credentials of its machine's leaf node, which connect as no client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRequest {
    /// The name the credentials carry ([`subject::renewable_name`]).
    pub name: String,
    /// The new user's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// The user JWT of the credentials renewed.
    pub current: String,
    /// Signature of [`RenewRequest::signed`] by the key of the credentials renewed, base64url without padding.
    pub signature: String,
}

impl RenewRequest {
    /// A request sent now, to be completed with the current credentials' JWT and signature.
    pub fn new(name: impl Into<String>, public_key: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            current: String::new(),
            signature: String::new(),
        }
    }

    /// What the key of the credentials renewed signs: every field but the signature, one per line.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        ["ants-renew", &self.name, &self.public_key, &sent_at, &self.current, &self.host].join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::renew(&self.name)
    }

    /// Check that the request names credentials renewable over NATS and arrived on its own
    /// [`RenewRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject::renewable_name(&self.name) && subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignRenewal { name: self.name.clone(), at: subject.to_owned() })
    }
}

/// The answer to a [`RenewRequest`], on [`subject::renew_reply`]: the user JWT of `public_key`, which completes the
/// renewed credentials with the seed kept by the asking holder. A request the approver refuses gets no answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewReply {
    pub name: String,
    pub public_key: String,
    pub jwt: String,
}

impl RenewReply {
    pub fn subject(&self) -> String {
        subject::renew_reply(&self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn enrollment_answers_name_the_key_they_answer() {
        let request = EnrollRequest::new(p("dev-a"), "UKEY", "macbook");
        assert_eq!(request.check_subject("ants.enroll.dev-a"), Ok(()));
        assert!(request.check_subject("ants.enroll.dev-b").is_err());

        let approved =
            EnrollReply::Approved { ant: p("dev-a"), public_key: "UKEY".into(), jwt: "j".into(), leaf_jwt: None };
        let json = serde_json::to_value(&approved).unwrap();
        assert_eq!(json["outcome"], "approved");
        assert!(json.get("leaf_jwt").is_none());
        assert_eq!(serde_json::from_value::<EnrollReply>(json).unwrap(), approved);
        assert_eq!(approved.subject(), "ants.enroll.dev-a.reply");
        assert!(approved.answers("UKEY") && !approved.answers("UOTHER"));

        let all: EnrollReply =
            serde_json::from_str(r#"{"outcome":"rejected","ant":"dev-a","public_key":null}"#).unwrap();
        assert!(all.answers("UKEY") && all.answers("UOTHER"));
        let one = EnrollReply::Rejected { ant: p("dev-a"), public_key: Some("UOTHER".into()), reason: None };
        assert!(!one.answers("UKEY"));
    }

    #[test]
    fn an_enrollment_request_signs_all_but_its_signatures() {
        let mut request = EnrollRequest::new(p("dev-a"), "UKEY", "macbook");
        request.enrollment = "e.j.s".into();
        let bare = request.signed();
        let json = serde_json::to_string(&request).unwrap();
        assert!(!json.contains("\"leaf\""), "{json}");
        let back: EnrollRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.signed(), bare, "the same bytes after the wire");
        request.signature = "sig".into();
        assert_eq!(request.signed(), bare);
        let leaf = EnrollLeaf { machine: "m1".parse().unwrap(), jwt: "l.j.s".into(), signature: String::new() };
        request.leaf = Some(leaf.clone());
        let with_leaf = request.signed();
        assert_ne!(with_leaf, bare, "the leaf is signed");
        request.leaf = Some(EnrollLeaf { signature: "other".into(), ..leaf });
        assert_eq!(request.signed(), with_leaf);
        for change in [
            |r: &mut EnrollRequest| r.public_key = "UOTHER".into(),
            |r: &mut EnrollRequest| r.ant = "dev-b".parse().unwrap(),
            |r: &mut EnrollRequest| r.host = "other".into(),
            |r: &mut EnrollRequest| r.enrollment = "x.y.z".into(),
            |r: &mut EnrollRequest| r.sent_at += chrono::TimeDelta::nanoseconds(1),
            |r: &mut EnrollRequest| r.leaf.as_mut().unwrap().machine = "m2".parse().unwrap(),
        ] {
            let mut changed = request.clone();
            change(&mut changed);
            assert_ne!(changed.signed(), with_leaf);
        }
    }

    #[test]
    fn a_renewal_request_signs_all_but_its_signature_and_names_renewable_credentials() {
        let mut request = RenewRequest::new("dev-a", "UKEY", "macbook");
        request.current = "c.j.s".into();
        let bare = request.signed();
        let back: RenewRequest = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back.signed(), bare, "the same bytes after the wire");
        request.signature = "sig".into();
        assert_eq!(request.signed(), bare);
        for change in [
            |r: &mut RenewRequest| r.public_key = "UOTHER".into(),
            |r: &mut RenewRequest| r.name = "dev-b".into(),
            |r: &mut RenewRequest| r.host = "other".into(),
            |r: &mut RenewRequest| r.current = "x.y.z".into(),
            |r: &mut RenewRequest| r.sent_at += chrono::TimeDelta::nanoseconds(1),
        ] {
            let mut changed = request.clone();
            change(&mut changed);
            assert_ne!(changed.signed(), bare);
        }
        assert_eq!(request.check_subject("ants.renew.dev-a"), Ok(()));
        assert!(request.check_subject("ants.renew.dev-b").is_err());
        let leaf = RenewRequest::new("leaf-Mac_1", "UKEY", "macbook");
        assert_eq!(leaf.check_subject("ants.renew.leaf-Mac_1"), Ok(()));
        for name in ["a.b", "*", ">", "", "leaf-a.b", "Upper"] {
            let odd = RenewRequest::new(name, "UKEY", "macbook");
            assert!(odd.check_subject(&odd.subject()).is_err(), "{name:?}");
        }
        let reply = RenewReply { name: "leaf-m1".into(), public_key: "UKEY".into(), jwt: "j".into() };
        assert_eq!(reply.subject(), "ants.renew.leaf-m1.reply");
    }
}
