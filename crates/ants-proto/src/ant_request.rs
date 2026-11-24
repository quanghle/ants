//! Ants added from a template: a manager ant's [`AntRequest`] on [`crate::subject::antreq`] (`ants.antreq.<manager>`)
//! for a new ant, or to retire one it added, and the [`AntReply`] of `ants signer`, on the machine holding the team's
//! account key, on [`crate::subject::antreq_reply`]. Core NATS, not stored.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{EnrollLeaf, Error, ThreadId, UserId, subject};

/// A manager ant asking on [`subject::antreq`] for an ant added from a template (its `request_ant` tool): the
/// ant's name, the template, the settings asked for within the template's bounds (`request`, the JSON of a template
/// request, as the manager's config file would list them), and the public key of the user NKey the manager's machine
/// generated for the ant, whose seed stays there. The signer on the machine holding the account key (`ants signer`)
/// signs a user JWT for that key when the request keeps within its own copy of the template's policy, asks the humans
/// otherwise, and answers with an [`AntReply`].
///
/// With `retire` set, the manager asks instead to retire ant `name`, one it added (its `retire_ant` tool): the signer
/// revokes the ant's live keys if the bus issued them all for the manager to add, and answers
/// [`AntReply::Retired`]; `public_key` is then that of the ant's credentials on the manager's machine, `template` the
/// ant's, `request` and `reason` unused.
///
/// The request carries the manager's user JWT (`credentials`) and its key's `signature` of [`AntRequest::signed`], so
/// the signer takes only requests the manager made, whatever connection carried them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntRequest {
    pub manager: UserId,
    /// The ant asked for.
    pub name: UserId,
    pub template: String,
    /// The settings asked for, a JSON object of the keys a template request may set.
    pub request: String,
    /// Why the manager wants the ant, shown to a human asked to approve.
    pub reason: String,
    /// The manager's thread the request comes from, where a human's approval request appears.
    pub thread_id: ThreadId,
    /// The ant's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// When the manager stops waiting: no answer counts after it, and no human is asked past it.
    pub expires_at: DateTime<Utc>,
    /// The user JWT of the manager's credentials.
    pub credentials: String,
    /// Signature of [`AntRequest::signed`] by the manager's key, base64url without padding.
    pub signature: String,
    /// The leaf node the asking machine reaches the hub through, whose uplink an approval extends to the ant and a
    /// retirement narrows to drop it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf: Option<EnrollLeaf>,
    /// Retire ant `name` instead of adding it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retire: bool,
}

/// The ant an [`AntRequest`] asks for, or asks to retire: its name, its template, the settings asked for and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskedAnt {
    pub name: UserId,
    pub template: String,
    /// The settings asked for, a JSON object of the keys a template request may set.
    pub request: String,
    /// Why the manager wants the ant, shown to a human asked to approve.
    pub reason: String,
}

impl AntRequest {
    /// A request of `manager` on its thread `thread_id` for the `asked` ant with `public_key`, sent now from `host`,
    /// waiting until `expires_at`, to be completed with the manager's JWT and signatures.
    pub fn new(
        manager: UserId,
        asked: AskedAnt,
        thread_id: ThreadId,
        public_key: impl Into<String>,
        host: impl Into<String>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        let AskedAnt { name, template, request, reason } = asked;
        Self {
            manager,
            name,
            template,
            request,
            reason,
            thread_id,
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            expires_at,
            credentials: String::new(),
            signature: String::new(),
            leaf: None,
            retire: false,
        }
    }

    /// What the manager's key and the leaf's uplink key sign: every field but the signatures, one per line, the
    /// free texts prefixed with their length in bytes, so that none can pass for the next; a retirement under
    /// another first line than an addition, so that neither passes for the other.
    pub fn signed(&self) -> Vec<u8> {
        let at = |t: &DateTime<Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let quoted = |t: &str| format!("{}:{t}", t.len());
        let (machine, leaf) = self.leaf.as_ref().map_or(("", ""), |l| (l.machine.as_str(), l.jwt.as_str()));
        let fields = [
            if self.retire { "ants-antretire" } else { "ants-antreq" },
            self.manager.as_str(),
            self.name.as_str(),
            &quoted(&self.template),
            &quoted(&self.request),
            &quoted(&self.reason),
            self.thread_id.as_str(),
            &self.public_key,
            &quoted(&self.host),
            &at(&self.sent_at),
            &at(&self.expires_at),
            &self.credentials,
            machine,
            leaf,
        ];
        fields.join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::antreq(&self.manager)
    }

    /// Check that the request arrived on its manager's own [`AntRequest::subject`], which only the manager's
    /// credentials publish on.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "ant request", user: self.manager.clone(), at: subject.to_owned() })
    }
}

/// The answer to an [`AntRequest`], on [`subject::antreq_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AntReply {
    /// The user JWT of `public_key` for ant `name`, which completes its credentials with the seed kept on the
    /// manager's machine, and for a request through a leaf node ([`AntRequest::leaf`]) the renewed user JWT of the
    /// leaf's uplink key, allowing the ant as well.
    Approved {
        manager: UserId,
        name: UserId,
        public_key: String,
        jwt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_jwt: Option<String>,
    },
    /// No credentials, or for a retirement nothing revoked, and why.
    Refused { manager: UserId, name: UserId, public_key: String, reason: String },
    /// Ant `name` retired ([`AntRequest::retire`]): the keys of it revoked now (none when an earlier request revoked
    /// them all), and the error of pushing the revocations to the hub, if it failed; the hub then refuses the keys
    /// only once it reloads its `server.conf`. For a request through a leaf node ([`AntRequest::leaf`]), the renewed
    /// user JWT of the leaf's uplink key without the ant's permissions, or why the uplink could not be renewed so.
    Retired {
        manager: UserId,
        name: UserId,
        public_key: String,
        revoked: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        push_error: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_jwt: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_error: Option<String>,
    },
}

impl AntReply {
    pub fn manager(&self) -> &UserId {
        match self {
            Self::Approved { manager, .. } | Self::Refused { manager, .. } | Self::Retired { manager, .. } => manager,
        }
    }

    pub fn subject(&self) -> String {
        subject::antreq_reply(self.manager())
    }

    /// Whether the answer is meant for the request of ant `name` with `public_key`.
    pub fn answers(&self, name: &UserId, public_key: &str) -> bool {
        match self {
            Self::Approved { name: n, public_key: key, .. }
            | Self::Refused { name: n, public_key: key, .. }
            | Self::Retired { name: n, public_key: key, .. } => n == name && key == public_key,
        }
    }
}
