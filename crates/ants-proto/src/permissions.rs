//! What each kind of credentials may publish and subscribe to ([`Permissions`]), which `ants auth` puts in the user
//! JWTs it issues: a human's or an ant's, an assistant's, a leaf node's uplink, a machine's host connection, a
//! webhook's, an observer's, and those of enrollment, bootstrap and the services, versioned by [`PERMISSIONS_VERSION`].

use std::fmt;

use serde::{Deserialize, Serialize};

mod assistant;
mod domain;
mod enrollment;
mod hooks;
mod host;
mod human;
mod leaf;
mod services;
mod user;

pub use hooks::Destination;
pub use leaf::LeafRole;

/// JetStream stream backing a KV bucket.
fn kv_stream(bucket: &str) -> String {
    format!("KV_{bucket}")
}

/// Whether the permission subject `pattern` allows publishing `subject`, a permission subject too: token by token, `*`
/// standing for any one token and `>` for the rest.
fn covers(pattern: &str, subject: &str) -> bool {
    let mut subject = subject.split('.');
    for token in pattern.split('.') {
        match (token, subject.next()) {
            (">", Some(_)) => return true,
            ("*", Some(s)) if s != ">" => {}
            (t, Some(s)) if t == s => {}
            _ => return false,
        }
    }
    subject.next().is_none()
}

/// What acks the messages `consumer` of `stream` delivers, in both forms of nats-server's ack subjects (ADR-15): v1
/// `$JS.ACK.<stream>.<consumer>.…` and v2 `$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.…`, the default from
/// nats-server 2.16 (feature flag `js_ack_fc_v2`). The domain token is the JetStream domain of the server holding the
/// stream, `_` without one: any is allowed, so credentials issued without a domain still ack on a hub that has one.
/// So is any account hash: credentials publish only in their own account.
fn ack_subjects(stream: &str, consumer: &str) -> [String; 2] {
    [format!("$JS.ACK.{stream}.{consumer}.>"), format!("$JS.ACK.*.*.{stream}.{consumer}.>")]
}

/// What answers the flow control of the consumers of `stream` in JetStream domain `domain` (`*` for any, as in
/// [`ack_subjects`]), in both forms: v1 `$JS.FC.<stream>.…` and v2 `$JS.FC.<domain>.<account hash>.<stream>.…`.
fn flow_control_subjects(domain: &str, stream: &str) -> [String; 2] {
    [format!("$JS.FC.{stream}.>"), format!("$JS.FC.{domain}.*.{stream}.>")]
}

/// What the ordered consumers of `stream` need, behind listing and watching a KV bucket: creating one, first unnamed
/// (`CONSUMER.CREATE.<stream>`); answering its flow control; and, when async-nats recreates it after a reconnect or
/// missed heartbeats, deleting the old one by name and creating the next under a name, with its filter when it has
/// one (`CONSUMER.CREATE.<stream>.<name>[.<filter>]`). The named form also lets the credentials create a durable
/// consumer of `stream`, which each stream's `max_consumers` bounds.
fn ordered_consumer_subjects(stream: &str) -> [String; 5] {
    let [fc, fc_v2] = flow_control_subjects("*", stream);
    [
        format!("$JS.API.CONSUMER.CREATE.{stream}"),
        format!("$JS.API.CONSUMER.CREATE.{stream}.>"),
        format!("$JS.API.CONSUMER.DELETE.{stream}.*"),
        fc,
        fc_v2,
    ]
}

/// Subjects a set of credentials may publish and subscribe to. Everything else is denied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permissions {
    pub publish: Vec<String>,
    pub subscribe: Vec<String>,
}

/// The version of the permissions `ants auth` gives credentials of every kind (ants, humans, webhooks, leaf node
/// uplinks, the channel manager, enrollment), stamped into each user JWT it issues as the tag
/// [`PermissionsVersion::tag`]. Credentials carrying an older version were issued before the permissions changed:
/// ants processes using them warn and name the command that renews them.
///
/// Bump it whenever the permissions of any role change: [`Permissions::for_user`], [`Permissions::for_assistant`], [`Permissions::for_leaf`],
/// [`Permissions::for_webhook`], [`Permissions::for_channel_manager`], [`Permissions::for_host`],
/// [`Permissions::for_enrollment`],
/// [`Permissions::for_enroll_approver`], [`Permissions::for_reactions_responder`], [`Permissions::for_trail_keeper`],
/// [`Permissions::for_account_holder`],
/// [`Permissions::for_device_revoker`]
/// or [`Permissions::with_js_domain`], or
/// the other claims `ants auth` puts in a
/// user JWT.
pub const PERMISSIONS_VERSION: PermissionsVersion = PermissionsVersion(3);

/// A version of the permissions `ants auth` gives ([`PERMISSIONS_VERSION`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PermissionsVersion(pub u32);

impl PermissionsVersion {
    /// Prefix of the user JWT tag carrying the version.
    pub const TAG: &str = "ants-perms:";

    /// The user JWT tag carrying the version.
    pub fn tag(&self) -> String {
        format!("{}{}", Self::TAG, self.0)
    }

    /// The version among a user JWT's `tags`, if any.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        tags.into_iter().find_map(|tag| tag.strip_prefix(Self::TAG)?.parse().ok().map(Self))
    }
}

impl fmt::Display for PermissionsVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests;
