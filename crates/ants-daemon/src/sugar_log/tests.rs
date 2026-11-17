use ants_proto::{DeviceRevocation, Machine, SugarId, UserId, revoked_device_key};
use ants_sugar::log::{AccountVerifier, AntLog, AppendError, EventLog, NoRevocations, Revocations, read_status};
use ants_test_support::{NatsServer, scratch_dir};
use async_nats::jetstream;
use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
use serde_json::json;

use super::*;
use crate::bus;

async fn connect(server: &NatsServer) -> jetstream::Context {
    bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1
}

/// A user JWT of `name`, for `key`, signed by `account`.
fn jwt(account: &KeyPair, key: &KeyPair, name: &str) -> String {
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let claims = json!({
        "sub": key.public_key(), "iss": account.public_key(), "name": name, "iat": 1_700_000_000,
        "nats": { "type": "user", "tags": [], "version": 2 },
    });
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(claims.to_string().as_bytes()));
    format!("{signed}.{}", BASE64URL_NOPAD.encode(&account.sign(signed.as_bytes()).unwrap()))
}

#[tokio::test]
async fn appends_are_gapless_per_identity_and_a_stale_writer_is_refused() {
    let Some(server) = NatsServer::start() else { return };
    let js = connect(&server).await;
    let log = JetStreamLog::open(&js).await.unwrap();
    let (a, b) = (SugarId::new(), SugarId::new());
    assert_eq!(log.last_sequence(&a).await.unwrap(), 0);
    assert_eq!(log.read_after(&a, 0, 10).await.unwrap(), []);
    assert_eq!(log.append(&a, b"a1".to_vec(), 0).await.unwrap(), 1);
    assert!(matches!(log.append(&a, b"stale".to_vec(), 0).await, Err(AppendError::Conflict { last: 1 })));
    assert_eq!(log.append(&b, b"b1".to_vec(), 0).await.unwrap(), 2, "another identity starts from none");
    assert!(matches!(log.append(&a, b"stale".to_vec(), 2).await, Err(AppendError::Conflict { last: 1 })));
    assert_eq!(log.append(&a, b"a2".to_vec(), 1).await.unwrap(), 3);
    assert_eq!(log.append(&a, b"a3".to_vec(), 3).await.unwrap(), 4);

    let read = log.read_after(&a, 0, 10).await.unwrap();
    let messages: Vec<_> = read.iter().map(|s| (s.sequence, s.payload.as_slice())).collect();
    assert_eq!(messages, [(1, &b"a1"[..]), (3, b"a2"), (4, b"a3")]);
    let after: Vec<_> = log.read_after(&a, 1, 1).await.unwrap().iter().map(|s| s.sequence).collect();
    assert_eq!(after, [3], "from after a sequence, at most the limit");
    assert_eq!(log.read_after(&a, 4, 10).await.unwrap(), []);
    assert_eq!(log.last_sequence(&a).await.unwrap(), 4);
    assert_eq!(log.last_sequence(&b).await.unwrap(), 2);

    let again = JetStreamLog::open(&js).await.unwrap();
    assert_eq!(again.incarnation(), log.incarnation(), "the same stream, found again");
    js.delete_stream(SUGAR_STREAM).await.unwrap();
    let anew = JetStreamLog::open(&js).await.unwrap();
    assert_ne!(anew.incarnation(), log.incarnation(), "a stream created anew");
    assert_eq!(anew.last_sequence(&a).await.unwrap(), 0);
}

#[tokio::test]
async fn a_stream_of_another_config_is_refused() {
    let Some(server) = NatsServer::start() else { return };
    let js = connect(&server).await;
    let republishing = stream::Config {
        republish: Some(stream::Republish {
            source: subject::ALL_SUGAR.into(),
            destination: "ants.leak.{{wildcard(1)}}".into(),
            headers_only: false,
        }),
        ..sugar_config()
    };
    js.create_stream(republishing).await.unwrap();
    let refused = JetStreamLog::open(&js).await.err().expect("refused");
    assert!(refused.to_string().contains("creating the ANTS_SUGAR stream"), "{refused:#}");
}

#[tokio::test]
async fn an_ants_events_chain_and_verify_signed_with_its_credentials() {
    let Some(server) = NatsServer::start() else { return };
    let js = connect(&server).await;
    let log = JetStreamLog::open(&js).await.unwrap();
    let (account, key) = (KeyPair::new_account(), KeyPair::new_user());
    let ant: UserId = "ant1".parse().unwrap();
    let signer = AntSigner::new(&jwt(&account, &key, "ant1"), key);
    let verifier = || AccountVerifier::new(vec![account.public_key()], NoRevocations);
    let (sugar, machine) = (SugarId::new(), Machine::from_hostname("m1"));
    let home = scratch_dir("sugar-log-chain");

    let mut writer =
        AntLog::new(&log, signer, verifier(), sugar.clone(), ant.clone(), machine.clone(), Some(home.clone())).unwrap();
    writer.catch_up().await.unwrap();
    writer.append("noted", json!({ "n": 1 })).await.unwrap();
    writer.append("noted", json!({ "n": 2 })).await.unwrap();

    let mut reader = AntLog::new(&log, AntSigner(None), verifier(), sugar.clone(), ant.clone(), machine, None).unwrap();
    let read = reader.catch_up().await.unwrap();
    assert_eq!(read.iter().map(|c| c.event.data["n"].as_u64().unwrap()).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(read[1].event.prev.as_deref(), Some(read[0].stored.sha256().as_str()));
    assert_eq!(reader.chain().broken(), None);
    let status = read_status(&home).unwrap().unwrap();
    assert_eq!((status.events, status.broken, status.incarnation.as_str()), (2, None, log.incarnation()));

    let forged = AntSigner::new(&jwt(&KeyPair::new_account(), &KeyPair::new_user(), "ant1"), KeyPair::new_user());
    let mut forger = AntLog::new(&log, forged, verifier(), sugar, ant, Machine::from_hostname("m2"), None).unwrap();
    forger.append("noted", json!({ "n": 3 })).await.unwrap();
    assert_eq!(reader.catch_up().await.unwrap(), [], "nothing verified");
    let at = reader.chain().broken().expect("broken");
    assert_eq!((at.index, at.sequence), (3, Some(3)), "the third event");
}

#[tokio::test]
async fn revocations_are_read_from_the_devices_bucket() {
    let Some(server) = NatsServer::start() else { return };
    let js = connect(&server).await;
    let devices = js.create_key_value(crate::hub::devices_config()).await.unwrap();
    let (revoked, kept) = (KeyPair::new_user().public_key(), KeyPair::new_user().public_key());
    let record = DeviceRevocation { key: revoked.clone(), revoked_at: 1_700_000_100 };
    devices.put(revoked_device_key(&revoked), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    let revocations = DeviceRevocations::new(&js, None).await.unwrap();
    assert_eq!(revocations.revoked_at(&revoked).await.unwrap(), Some(1_700_000_100));
    assert_eq!(revocations.revoked_at(&kept).await.unwrap(), None);
}
