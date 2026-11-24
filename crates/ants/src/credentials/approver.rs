//! `ants enroll` on the machine holding the account key: the [`Approver`] hears what machines ask for over NATS and
//! answers it, the enrollment requests of new ants ([`enrollments`], asked with [`crate::credentials::enroll`]) and
//! the renewal requests of credentials in use ([`renewals`], asked with [`crate::credentials::renew`]).

pub(crate) mod enrollments;
pub(crate) mod renewals;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{ENROLL_INBOX, ENROLL_REPEAT, JsDomain};
use anyhow::{Context as _, Result};

use crate::auth;
use crate::credentials::revoke;

/// How long `ants enroll` listens for requests: one repeat of every machine still asking, and some slack.
pub const LISTEN: Duration = ENROLL_REPEAT.saturating_add(Duration::from_secs(1));

/// How long `ants enroll renew --auto` keeps one connection, under the lifetime of the credentials it issues itself
/// for it ([`auth::approver_creds`]).
pub const AUTO_RECONNECT: Duration = Duration::from_mins(5);

/// `ants enroll` on the machine holding the account key, connected with short-lived credentials it issues itself
/// ([`auth::approver_creds`]).
pub struct Approver {
    pub(crate) client: async_nats::Client,
    hub: async_nats::jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The bus's account, which must have issued the credentials a request proves.
    pub(crate) account: String,
    /// User keys revoked in the auth directory (`ants auth revoke`), whose credentials prove nothing.
    pub(crate) revoked: HashSet<String>,
    pub(crate) listen: Duration,
    /// The auth directory, whose registry tells which credentials a registration granted.
    pub(crate) dir: PathBuf,
}

impl Approver {
    /// Connect to the server at `url`, signing the credentials with the account key in the auth directory `dir`, for
    /// the hub's JetStream domain if it has one, and listen for `listen` each time requests are gathered.
    pub async fn connect(url: &str, dir: &Path, js_domain: Option<&JsDomain>, listen: Duration) -> Result<Self> {
        Self::connect_with(url, &auth::approver_creds(dir, js_domain)?, dir, js_domain, listen).await
    }

    /// [`Approver::connect`] with `ants signer`'s credentials, which also answer ants' reactions requests
    /// ([`auth::signer_creds`]).
    pub async fn connect_signer(url: &str, dir: &Path, js_domain: Option<&JsDomain>, listen: Duration) -> Result<Self> {
        Self::connect_with(url, &auth::signer_creds(dir, js_domain)?, dir, js_domain, listen).await
    }

    async fn connect_with(
        url: &str,
        creds: &str,
        dir: &Path,
        js_domain: Option<&JsDomain>,
        listen: Duration,
    ) -> Result<Self> {
        bus::check_url(url)?;
        let client = async_nats::ConnectOptions::with_credentials(creds)?
            .name("ants-enroll")
            .custom_inbox_prefix(ENROLL_INBOX)
            .connect(url)
            .await
            .context("connecting to NATS")?;
        let hub = match js_domain {
            Some(domain) => async_nats::jetstream::with_domain(client.clone(), domain.as_str()),
            None => async_nats::jetstream::new(client.clone()),
        };
        let account = auth::account_key(dir)?;
        let revoked = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
        Ok(Self { client, hub, js_domain: js_domain.cloned(), account, revoked, listen, dir: dir.to_owned() })
    }

    /// The hub's JetStream.
    pub(crate) fn hub(&self) -> &async_nats::jetstream::Context {
        &self.hub
    }

    pub(crate) fn js_domain(&self) -> Option<&JsDomain> {
        self.js_domain.as_ref()
    }
}
