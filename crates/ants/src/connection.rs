//! How the CLI reaches NATS: the connection the global options name, and connecting to it as the human or ant of
//! the credentials, creating what the hub lacks on the way.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use ants_daemon::{bus, hub};
use ants_proto::{DeviceId, JsDomain, UserId};
use anyhow::Result;

use crate::people::{devices, humans};

/// How the CLI reaches NATS.
pub struct Connection<'a> {
    pub nats: &'a str,
    pub creds: Option<&'a Path>,
    pub js_domain: Option<&'a JsDomain>,
    /// Whom the CLI acts as without credentials: the config file's `human`, or `ANTS_HUMAN`.
    pub human: Option<&'a UserId>,
}

impl<'a> Connection<'a> {
    pub(crate) fn nats(&self) -> Result<Nats<'a>> {
        Nats::new(self.nats, self.creds, self.js_domain, self.human)
    }
}

/// What a [`Nats`] is made of, owned, to make it again in a task of its own ([`Nats::reach`]).
#[derive(Clone)]
pub(crate) struct Reach {
    url: String,
    creds: Option<PathBuf>,
    domain: Option<JsDomain>,
    me: UserId,
}

impl Reach {
    pub(crate) fn nats(&self) -> Result<Nats<'_>> {
        Nats::new(&self.url, self.creds.as_deref(), self.domain.as_ref(), Some(&self.me))
    }
}

/// How a listing command prints: once or following new entries until interrupted, as text or one JSON value a line.
#[derive(Debug, Clone, Copy)]
pub struct Listing {
    pub follow: bool,
    pub json: bool,
}

pub struct Nats<'a> {
    pub(crate) url: &'a str,
    pub(crate) creds: Option<&'a Path>,
    pub(crate) domain: Option<&'a JsDomain>,
    pub me: humans::Me,
    /// The device of the human the credentials were issued for; none for an ant's, and without credentials.
    pub device: Option<DeviceId>,
    pub(crate) rights: humans::Rights,
    /// The account that signed the credentials, whose devices' keys alone the CLI takes from the directory
    /// ([`devices::Directory`]); none without credentials.
    pub account: Option<String>,
    /// The human's device of the credentials, if they bind an encryption key, which a connection puts in the directory.
    pub(crate) own_device: Option<devices::Own>,
    /// Whether a connection checked the hub's stream and buckets already ([`create_missing`]).
    hub_checked: AtomicBool,
    /// Whether a connection put [`Nats::own_device`] in the directory already.
    device_published: AtomicBool,
}

impl<'a> Nats<'a> {
    pub(crate) fn new(
        url: &'a str,
        creds: Option<&'a Path>,
        domain: Option<&'a JsDomain>,
        human: Option<&UserId>,
    ) -> Result<Self> {
        let me = humans::Me::from_creds(creds, human)?;
        let device = humans::device(creds)?;
        let rights = humans::Rights::from_creds(creds)?;
        let account = devices::account_of(creds)?;
        let own_device = devices::Own::from_creds(creds)?;
        Ok(Self {
            url,
            creds,
            domain,
            me,
            device,
            rights,
            account,
            own_device,
            hub_checked: AtomicBool::new(false),
            device_published: AtomicBool::new(false),
        })
    }

    /// What the connection is made of, owned.
    pub(crate) fn reach(&self) -> Reach {
        Reach {
            url: self.url.to_owned(),
            creds: self.creds.map(Path::to_path_buf),
            domain: self.domain.cloned(),
            me: self.me.id.clone(),
        }
    }

    /// Connect as [`Nats::me`], and register it in the humans bucket ([`humans::register_or_warn`]). The first
    /// connection of a human also creates the hub's stream and buckets that are missing ([`create_missing`]), and puts
    /// the user JWT of its device in the directory of encryption keys when it differs ([`devices::publish_or_warn`]).
    pub async fn connect(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let (client, js) = self.connect_only(name).await?;
        self.prepare(&js).await;
        Ok((client, js))
    }

    /// What [`Nats::connect`] does once connected: register [`Nats::me`], create what the hub lacks, and put the
    /// device's user JWT in the directory of encryption keys.
    pub(crate) async fn prepare(&self, js: &async_nats::jetstream::Context) {
        let create = async {
            if self.me.role.is_human() && !self.hub_checked.swap(true, Ordering::Relaxed) {
                create_missing(js, &self.rights).await;
            }
        };
        let publish = async {
            if let Some(own) = &self.own_device
                && !self.device_published.swap(true, Ordering::Relaxed)
            {
                devices::publish_or_warn(js, own).await;
            }
        };
        tokio::join!(humans::register_or_warn(js, &self.me), create, publish);
    }

    /// Connect as [`Nats::me`] without registering it, naming the connection after its device, if any, too.
    pub(crate) async fn connect_only(
        &self,
        name: &str,
    ) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let name = match &self.device {
            Some(device) => format!("{name} {device}"),
            None => name.to_owned(),
        };
        bus::connect(self.url, self.creds, self.domain, &self.me.id, &name).await
    }
}

/// Create the hub's stream and buckets that are missing, and deny the rollups of those allowing them, as `ants init`
/// does ([`hub::check`]), saying what was done; an object already there otherwise stays as it is. Those the credentials
/// may not fix are reported as needing `ants init`;
/// those whose stream info the credentials do not get, or that cannot be checked (the hub out of reach), are left
/// alone. Returns what was created.
async fn create_missing(js: &async_nats::jetstream::Context, rights: &humans::Rights) -> Vec<hub::Finding> {
    let mut created = Vec::new();
    for finding in hub::check(js, |subject| rights.may_publish(subject)).await {
        if let hub::Problem::Unchecked(_) = finding.problem {
            tracing::debug!("{finding}");
        } else if !finding.rights().iter().all(|subject| rights.may_publish(subject)) {
            tracing::warn!("{finding}; `ants init` with a human's current credentials (`ants auth user`) fixes it");
        } else {
            match hub::apply(js, &finding).await {
                Ok(()) => {
                    tracing::info!("{}", finding.fixed());
                    created.push(finding);
                }
                Err(e) => tracing::warn!("{finding}; `ants init` as a human fixes it: {e:#}"),
            }
        }
    }
    created
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::UserId;

    use super::*;
    use crate::test_support;

    /// A human's credentials create what the hub lacks, leaving what is there as it is; an ant's ask for nothing they
    /// are denied, which would only time out, and create nothing.
    #[tokio::test]
    async fn a_human_creates_what_the_hub_lacks() {
        let Some(server) = test_support::Server::start() else { return };
        let connect = async |user: &UserId, role| {
            let creds = server.creds_as(user, role);
            let (_, js) = bus::connect(&server.url, Some(&creds), None, user, "test").await.unwrap();
            (js, humans::Rights::from_creds(Some(&creds)).unwrap())
        };
        let (a_js, a_rights) = connect(&"dev-a".parse().unwrap(), ants_proto::Role::Ant).await;
        assert!(!a_rights.may_publish(&hub::Object::Sessions.info_subject()));
        let started = tokio::time::Instant::now();
        assert!(create_missing(&a_js, &a_rights).await.is_empty());
        assert!(started.elapsed() < Duration::from_secs(2), "no request was denied");

        let (h_js, h_rights) = connect(&"alice".parse().unwrap(), ants_proto::Role::Human).await;
        bus::ensure_stream(&h_js).await.unwrap();
        let created: Vec<_> = create_missing(&h_js, &h_rights).await.into_iter().map(|f| f.object).collect();
        let seen: Vec<_> = hub::Object::ALL
            .into_iter()
            .filter(|o| ![hub::Object::Stream, hub::Object::Presence, hub::Object::Identities].contains(o))
            .collect();
        assert_eq!(created, seen, "every missing bucket a human may look up created");
        for watched in hub::Object::ALL.into_iter().filter(|o| o.watched()) {
            let config = h_js.get_stream(watched.stream_name()).await.unwrap().cached_info().config.clone();
            assert_eq!(config.max_consumers, hub::MAX_CONSUMERS, "{watched}");
        }
        assert!(hub::check(&h_js, |s| h_rights.may_publish(s)).await.is_empty());
        assert!(create_missing(&h_js, &h_rights).await.is_empty());
    }
}
