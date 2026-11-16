//! Where a human reads: while `ants inbox --follow` (the inbox, and the tool requests it shows), `ants tools list
//! --follow` or `ants dash` runs, the CLI keeps its device's [`Presence`] in [`PRESENCE_BUCKET`], so `ants webhook
//! serve` pushes nothing to that device's push targets that it shows already ([`Present`]), and `ants chat` shows which
//! humans have a device following ([`online`]). Only a human's credentials name a device, so only those keep a
//! presence.

use std::collections::HashSet;
use std::time::Duration;

use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_proto::{DeviceId, Following, JsDomain, PRESENCE_BUCKET, PRESENCE_HEARTBEAT, Presence, UserId, presence_key};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream::stream::ConsumerErrorKind;
use async_nats::jetstream::{self, ErrorCode, consumer};
use futures::StreamExt as _;

/// How long reading the presences waits for each one the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);

/// The presence bucket, created when missing and the credentials allow it (a human's). Creating it again with the
/// same settings changes nothing, so this needs no stream info, which a human's credentials lack for it.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<()> {
    js.create_key_value(ants_daemon::hub::presence_config()).await.context("creating the presence bucket")?;
    Ok(())
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
/// ordered consumer: deleted keys are gone, and a presence not renewed expires with the bucket's max age. Nobody when
/// the bucket is missing.
pub async fn online(js: &jetstream::Context) -> Result<HashSet<UserId>> {
    let stream = js.get_stream_no_info(format!("KV_{PRESENCE_BUCKET}")).await.context("naming the presences")?;
    let prefix = format!("$KV.{PRESENCE_BUCKET}.");
    let consumer = match stream
        .create_consumer(consumer::push::OrderedConfig {
            deliver_subject: js.client().new_inbox(),
            filter_subject: format!("{prefix}>"),
            headers_only: true,
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
        let deleted = msg.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
        let human = msg.subject.strip_prefix(&prefix).and_then(|key| key.split('.').next()?.parse().ok());
        if let (false, Some(human)) = (deleted, human) {
            online.insert(human);
        }
    }
    Ok(online)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use ants_daemon::bus;
    use ants_proto::Role;

    use super::*;
    use crate::auth::tests::Server;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// A connection of a new device of `human`, with credentials of its own, and the device's id.
    pub(crate) async fn device(server: &Server, human: &UserId, label: &str) -> (jetstream::Context, DeviceId) {
        let creds = server.dir().join(format!("{human}-{label}.creds"));
        let permissions = ants_proto::Permissions::for_user(human, Role::Human);
        let team = server.dir().join("auth");
        let device = crate::auth::issue_device(&team, human, Role::Human.into(), permissions, Some(label), &creds);
        let device = device.unwrap().expect("a human's device");
        let (_, mut js) = bus::connect(&server.url, Some(&creds), None, human, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        (js, device)
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
}
