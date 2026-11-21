//! Where a human reads: while `ants inbox --follow` (the inbox, and the tool requests it shows), `ants tools list
//! --follow` or `ants dash` runs, the CLI keeps its device's [`Presence`] in [`PRESENCE_BUCKET`], so `ants webhook
//! serve` pushes nothing to that device's push targets that it shows already ([`Present`]), and `ants chat` shows which
//! humans have a device following ([`online`]). Only a human's credentials name a device, so only those keep a
//! presence.

use std::collections::HashSet;
use std::time::Duration;

use ants_daemon::bus::relayed::{self, Read};
use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_proto::{DeviceId, Following, JsDomain, PRESENCE_BUCKET, PRESENCE_HEARTBEAT, Presence, UserId, presence_key};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream::stream::ConsumerErrorKind;
use async_nats::jetstream::{self, ErrorCode, consumer};
use futures::StreamExt as _;

/// How long reading the presences waits for each one the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);

/// The presence bucket, created as `ants init` does when missing and the credentials allow it (a human's). This needs
/// no stream info, which a human's credentials lack for it: a bucket already there as `ants init` leaves it, its
/// rollups denied, stays as it is.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<()> {
    Box::pin(ants_daemon::hub::ensure_unseen_bucket(js, ants_daemon::hub::Object::Presence))
        .await
        .context("creating the presence bucket")
}

/// Run `work` with `device` of `human` present as following `what`: its presence is put first, renewed every
/// [`PRESENCE_HEARTBEAT`] while `work` runs and deleted once it ends. Without a device (an ant's credentials, or
/// none), or when the presence cannot be written, `work` runs without one.
pub async fn while_following<T>(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    human: &UserId,
    device: Option<DeviceId>,
    what: Following,
    work: impl Future<Output = T>,
) -> T {
    let Some(device) = device else {
        tracing::debug!("no device: no presence kept");
        return work.await;
    };
    let kv = match open(js, js_domain).await {
        Ok(kv) => kv,
        Err(e) => {
            tracing::warn!(
                "not telling notifications that this device reads, so its push targets get what it shows: {e:#}"
            );
            return work.await;
        }
    };
    let presence = Presence::new(human.clone(), device, what);
    let (key, value) = (presence.key(), serde_json::to_vec(&presence).expect("a presence serializes"));
    let heartbeat = async {
        let mut tick = tokio::time::interval(PRESENCE_HEARTBEAT);
        loop {
            tick.tick().await;
            if let Err(e) = kv.put(&key, value.clone()).await {
                tracing::debug!(error = format!("{e:#}"), "renewing the presence");
            }
        }
    };
    let out = tokio::select! {
        out = work => out,
        never = heartbeat => match never {},
    };
    if let Err(e) = kv.delete(&key).await {
        tracing::debug!(error = format!("{e:#}"), "deleting the presence");
    }
    out
}

async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<DirectKv> {
    ensure_bucket(js).await?;
    DirectKv::new(js, js_domain, PRESENCE_BUCKET).await
}

/// Which devices of a human follow what, read by direct gets with the human's credentials.
pub struct Present {
    kv: DirectKv,
}

impl Present {
    /// Naming the bucket takes no request; a missing one shows as nobody present.
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Self> {
        Ok(Self { kv: DirectKv::new(js, js_domain, PRESENCE_BUCKET).await? })
    }

    /// Those of `devices` of `human` following `what` now. A device whose presence cannot be read counts as away, so
    /// a hub that does not answer costs a push too many, never one too few.
    pub async fn following(
        &self,
        human: &UserId,
        devices: impl IntoIterator<Item = DeviceId>,
        what: Following,
    ) -> HashSet<DeviceId> {
        let mut present = HashSet::new();
        for device in devices.into_iter().collect::<HashSet<_>>() {
            match self.kv.lookup(presence_key(human, &device, what)).await {
                Ok(Lookup::Value(_)) => _ = present.insert(device),
                Ok(Lookup::NoValue | Lookup::NoBucket) => {}
                Err(e) => tracing::debug!(%human, %device, error = format!("{e:#}"), "reading a presence"),
            }
        }
        present
    }
}

/// What credentials need to read every human's presence ([`online`]): a human's and an assistant's have it, an ant's
/// not.
pub fn online_right() -> String {
    format!("$JS.API.CONSUMER.CREATE.KV_{PRESENCE_BUCKET}")
}

/// The humans with a device following anything now, from the last entry of every key of the bucket, read through an
/// ordered consumer: deleted keys are gone, a presence not renewed expires with the bucket's max age, and a key whose
/// last message a stream relayed ([`relayed::stored_relayed`]) counts as gone. Nobody when the bucket is missing.
pub async fn online(js: &jetstream::Context) -> Result<HashSet<UserId>> {
    let stream = js.get_stream_no_info(format!("KV_{PRESENCE_BUCKET}")).await.context("naming the presences")?;
    let prefix = format!("$KV.{PRESENCE_BUCKET}.");
    let consumer = match stream
        .create_consumer(consumer::push::OrderedConfig {
            deliver_subject: js.client().new_inbox(),
            filter_subject: format!("{prefix}>"),
            deliver_policy: consumer::DeliverPolicy::LastPerSubject,
            replay_policy: consumer::ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
    {
        Ok(consumer) => consumer,
        Err(e) if matches!(e.kind(), ConsumerErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND) =>
        {
            return Ok(HashSet::new());
        }
        Err(e) => return Err(e).context("reading the presences"),
    };
    let pending = consumer.cached_info().num_pending;
    let mut messages = consumer.messages().await.context("reading the presences")?;
    let mut online = HashSet::new();
    for _ in 0..pending {
        let msg = tokio::time::timeout(WAIT, messages.next())
            .await
            .context("waiting for the presences")?
            .context("the presences ended early")?
            .map_err(|e| anyhow!(e))
            .context("reading the presences")?;
        if relayed::stored_relayed(msg.headers.as_ref(), &msg.payload, Read::Consumer) {
            tracing::warn!(subject = %msg.subject, "skipping a presence a stream relayed");
            continue;
        }
        let deleted = msg.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
        let human = msg.subject.strip_prefix(&prefix).and_then(|key| key.split('.').next()?.parse().ok());
        if let (false, Some(human)) = (deleted, human) {
            online.insert(human);
        }
    }
    Ok(online)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::Role;

    use super::*;
    use crate::test_support::Server;
    use crate::test_support::presence::device;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// Runs as two devices of alice and one of bob, with their own credentials, so it also checks their permissions.
    #[tokio::test]
    async fn a_device_is_present_while_it_follows_and_every_human_sees_it() {
        let Some(server) = Server::start() else { return };
        let (alice, bob) = (p("alice"), p("bob"));
        let (laptop, laptop_id) = device(&server, &alice, "laptop").await;
        let (phone, phone_id) = device(&server, &alice, "phone").await;
        let (b_js, b_id) = device(&server, &bob, "laptop").await;
        let present = Present::new(&phone, None).await.unwrap();
        let devices = [laptop_id, phone_id];
        assert!(present.following(&alice, devices, Following::Inbox).await.is_empty(), "no bucket: nobody");
        assert!(online(&phone).await.unwrap().is_empty(), "no bucket: nobody online");

        let (seen, stop) = (tokio::sync::oneshot::channel::<()>(), tokio::sync::oneshot::channel::<()>());
        let (seen_tx, seen_rx) = seen;
        let (stop_tx, stop_rx) = stop;
        let follow = while_following(&laptop, None, &alice, Some(laptop_id), Following::Inbox, async {
            seen_rx.await.unwrap();
            stop_rx.await.unwrap();
            7
        });
        let check = async {
            let mut found = HashSet::new();
            for _ in 0..50 {
                found = present.following(&alice, devices, Following::Inbox).await;
                if !found.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(found, HashSet::from([laptop_id]), "the laptop follows the inbox");
            assert!(present.following(&alice, devices, Following::Approvals).await.is_empty());
            seen_tx.send(()).unwrap();

            // Bob sees where alice reads but does not move it; an ant sees nothing of it.
            let b_present = Present::new(&b_js, None).await.unwrap();
            assert_eq!(b_present.following(&alice, devices, Following::Inbox).await, HashSet::from([laptop_id]));
            assert_eq!(online(&b_js).await.unwrap(), HashSet::from([alice.clone()]));
            let b_kv = DirectKv::new(&b_js, None, PRESENCE_BUCKET).await.unwrap();
            assert!(b_kv.put(presence_key(&alice, &phone_id, Following::Inbox), "{}").await.is_err());
            assert!(b_kv.delete(presence_key(&alice, &laptop_id, Following::Inbox)).await.is_err());
            assert!(b_js.get_key_value(PRESENCE_BUCKET).await.is_err(), "bob gets the bucket's stream info");
            b_kv.put(presence_key(&bob, &b_id, Following::Inbox), "{}").await.unwrap();
            assert_eq!(present.following(&alice, devices, Following::Inbox).await, HashSet::from([laptop_id]));
            assert_eq!(online(&phone).await.unwrap(), HashSet::from([alice.clone(), bob.clone()]));
            b_kv.delete(presence_key(&bob, &b_id, Following::Inbox)).await.unwrap();
            assert_eq!(online(&phone).await.unwrap(), HashSet::from([alice.clone()]), "bob left");
            // A message a stream relayed onto bob's key, as stored there: bob shows as away.
            let mut headers = async_nats::HeaderMap::new();
            let stream_headers = [
                ("Nats-Stream", "KV_ants_trail_runs"),
                ("Nats-Subject", "$KV.ants_trail_runs.dev-a.x"),
                ("Nats-Sequence", "1"),
            ];
            stream_headers.into_iter().for_each(|(name, value)| headers.insert(name, value));
            let onto = format!("$KV.{PRESENCE_BUCKET}.{}", presence_key(&bob, &b_id, Following::Inbox));
            b_js.publish_with_headers(onto, headers, "{}".into()).await.unwrap().await.unwrap();
            assert_eq!(online(&phone).await.unwrap(), HashSet::from([alice.clone()]), "a relayed presence");
            let (_, ant) = server.connect_as(&p("dev-a"), Role::Ant).await;
            assert!(online(&ant).await.is_err(), "an ant reads the presences");
            let ant_kv = DirectKv::new(&ant, None, PRESENCE_BUCKET).await.unwrap();
            assert!(ant_kv.get(presence_key(&alice, &laptop_id, Following::Inbox)).await.is_err());
            stop_tx.send(()).unwrap();
        };
        let (out, ()) = tokio::join!(follow, check);
        assert_eq!(out, 7);
        assert!(present.following(&alice, devices, Following::Inbox).await.is_empty(), "gone once it ends");
        assert!(online(&phone).await.unwrap().is_empty(), "nobody online once it ends");

        let deviceless = while_following(&phone, None, &alice, None, Following::Inbox, async {
            present.following(&alice, devices, Following::Inbox).await
        });
        assert!(deviceless.await.is_empty(), "no device, no presence");
    }

    /// `ants init` under a human's rights leaves the bucket denying rollups, which the KV configuration a create sends
    /// cannot say: a device still keeps its presence there, and the rollups stay denied.
    #[tokio::test]
    async fn a_device_is_present_on_the_bucket_ants_init_made() {
        let Some(server) = Server::start() else { return };
        let alice = p("alice");
        let (laptop, laptop_id) = device(&server, &alice, "laptop").await;
        let creds = server.dir().join("alice-laptop.creds");
        let rights = crate::people::humans::Rights::from_creds(Some(&creds)).unwrap();
        ants_daemon::hub::init(&laptop, |s| rights.may_publish(s)).await.unwrap();
        ensure_bucket(&laptop).await.unwrap();

        let present = Present::new(&laptop, None).await.unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel::<()>();
        let follow = while_following(&laptop, None, &alice, Some(laptop_id), Following::Inbox, seen_rx);
        let check = async {
            let mut found = HashSet::new();
            for _ in 0..50 {
                found = present.following(&alice, [laptop_id], Following::Inbox).await;
                if !found.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(found, HashSet::from([laptop_id]), "the laptop follows the inbox");
            assert_eq!(online(&laptop).await.unwrap(), HashSet::from([alice.clone()]));
            seen_tx.send(()).unwrap();
        };
        let (_, ()) = tokio::join!(follow, check);

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Rollup", "sub");
        let subject = format!("$KV.{PRESENCE_BUCKET}.{}", presence_key(&alice, &laptop_id, Following::Inbox));
        let rollup = laptop.publish_with_headers(subject, headers, "".into()).await.unwrap().await;
        assert!(rollup.is_err(), "the bucket denies rollups");
    }
}
