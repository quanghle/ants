//! Fixtures of webhooks: the hub they deliver to, their credentials and what they stored.

use std::path::PathBuf;

use ants_daemon::bus;
use ants_proto::{ChannelEntry, Destination, Permissions, STREAM, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use futures::TryStreamExt as _;

use crate::test_support::Server;
use crate::test_support::issue_with;

/// The hub with its stream and buckets, as `ants init` leaves it, with #ops of dev-a and dev-b, and a private
/// secret file.
pub(crate) async fn hub() -> Option<(Server, jetstream::Context, PathBuf)> {
    let server = Server::start()?;
    let (_, js) = server.connect(&ants_test_support::human()).await;
    bus::ensure_stream(&js).await.unwrap();
    ensure_bucket(&js).await.unwrap();
    bus::ensure_owners(&js).await.unwrap();
    let channels = bus::ensure_channels(&js).await.unwrap();
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member("dev-a".parse().unwrap());
    ops.add_member("dev-b".parse().unwrap());
    channels.put(ops.key(), serde_json::to_vec(&ops).unwrap().into()).await.unwrap();
    let secret = server.dir().join("hooks.secret");
    std::fs::write(&secret, "s3cret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    Some((server, js, secret))
}

pub(crate) fn creds(server: &Server, webhook: &UserId, to: Destination) -> PathBuf {
    let out = server.dir().join(format!("{webhook}.creds"));
    issue_with(
        &server.dir().join("auth"),
        webhook,
        crate::credentials::issued::Kind::Webhook,
        Permissions::for_webhook(webhook, &[to]),
        &out,
    )
    .unwrap();
    out
}

/// The messages stored on `subject`, oldest first.
pub(crate) async fn stored<T: serde::de::DeserializeOwned>(js: &jetstream::Context, subject: &str) -> Vec<T> {
    let stream = js.get_stream(STREAM).await.unwrap();
    let count: usize = stream
        .info_with_subjects(subject)
        .await
        .unwrap()
        .try_fold(0, |n, (_, c)| async move { Ok(n + c) })
        .await
        .unwrap();
    let mut found = Vec::new();
    let mut seq = 1;
    while found.len() < count {
        if let Ok(msg) = stream.get_raw_message(seq).await
            && msg.subject.as_str() == subject
        {
            found.push(serde_json::from_slice(&msg.payload).unwrap());
        }
        seq += 1;
    }
    found
}

/// The bucket of delivered events ([`ants_proto::WEBHOOKS_BUCKET`]), created when missing and the credentials allow
/// it (a human's), as `ants init` does.
pub(crate) async fn ensure_bucket(js: &jetstream::Context) -> Result<jetstream::kv::Store> {
    Box::pin(ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Webhooks))
        .await
        .context("opening the webhooks bucket (with credentials, `ants init` as a human creates it)")
}
