use std::collections::HashMap;
use std::path::Path;

use ants_proto::{Machine, SenseId, UserId};
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
use serde_json::{Value, json};

use super::*;

fn jwt(signer: &KeyPair, body: &Value) -> String {
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(body.to_string().as_bytes()));
    format!("{signed}.{}", BASE64URL_NOPAD.encode(&signer.sign(signed.as_bytes()).unwrap()))
}

/// A team account and credentials of its ants, each signing with its own key.
struct Team {
    account: KeyPair,
}

impl Team {
    fn new() -> Self {
        Self { account: KeyPair::new_account() }
    }

    fn signer(&self, name: &str) -> KeySigner {
        let key = KeyPair::new_user();
        let body = json!({
            "iss": self.account.public_key(),
            "sub": key.public_key(),
            "name": name,
            "iat": 1_700_000_000,
            "nats": { "type": "user", "tags": [], "version": 2 },
        });
        KeySigner(jwt(&self.account, &body), key)
    }

    fn verifier(&self) -> AccountVerifier<NoRevocations> {
        AccountVerifier::new(vec![self.account.public_key()], NoRevocations)
    }
}

struct KeySigner(String, KeyPair);

impl Signer for KeySigner {
    fn sign(&self, bytes: &[u8]) -> Result<Option<Signature>, String> {
        let signature = self.1.sign(bytes).map_err(|e| e.to_string())?;
        Ok(Some(Signature { credentials: self.0.clone(), signature: BASE64URL_NOPAD.encode(&signature) }))
    }
}

/// Revocations by key, or none to tell.
struct Revoked(Option<HashMap<String, u64>>);

impl Revocations for Revoked {
    fn revoked_at(&self, key: &str) -> impl Future<Output = Result<Option<u64>, String>> + Send {
        let found = match &self.0 {
            Some(revoked) => Ok(revoked.get(key).copied()),
            None => Err("no answer".to_owned()),
        };
        std::future::ready(found)
    }
}

fn dev_a() -> UserId {
    "dev-a".parse().unwrap()
}

fn m1() -> Machine {
    "m1".parse().unwrap()
}

type Log<'a, S, V> = AntLog<&'a MemoryLog, S, V>;

fn ant_log<'a, S: Signer, V: Verifier>(
    log: &'a MemoryLog,
    signer: S,
    verifier: V,
    sense: &SenseId,
    home: Option<&Path>,
) -> Log<'a, S, V> {
    AntLog::new(log, signer, verifier, sense.clone(), dev_a(), m1(), home.map(Path::to_path_buf)).unwrap()
}

/// The sequences and payloads of `checked`, without when they were stored, which a writer only tells as it saw it.
fn messages(checked: &[Checked]) -> Vec<(u64, &[u8])> {
    checked.iter().map(|c| (c.stored.sequence, c.stored.payload.as_slice())).collect()
}

/// The indices and kinds of `checked`.
fn kinds(checked: &[Checked]) -> Vec<(u64, &str)> {
    checked.iter().map(|c| (c.event.index, c.event.kind.as_str())).collect()
}

#[tokio::test]
async fn events_chain_to_the_one_before_and_verify_for_any_reader() {
    let (team, log, sense, other) = (Team::new(), MemoryLog::default(), SenseId::new(), SenseId::new());
    let mut writer = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
    let (first, read) = writer.append("note", json!({ "text": "one" })).await.unwrap();
    assert!(read.is_empty());
    assert_eq!((first.event.index, first.event.prev.as_deref(), first.stored.sequence), (1, None, 1));
    log.append(&other, b"another identity's".to_vec(), 0).await.unwrap();
    writer.append("note", json!({ "text": "two" })).await.unwrap();
    let (third, _) = writer.append("other", json!(null)).await.unwrap();
    assert_eq!(third.stored.sequence, 4, "sequences are the store's, across identities");

    let mut reader = ant_log(&log, Unsigned, team.verifier(), &sense, None);
    let checked = reader.catch_up().await.unwrap();
    assert_eq!(kinds(&checked), [(1, "note"), (2, "note"), (3, "other")]);
    assert_eq!(checked[1].event.prev, Some(sha256(&checked[0].stored.payload)));
    assert_eq!(checked[1].event.data, json!({ "text": "two" }));
    assert_eq!(messages(&checked[2..]), messages(std::slice::from_ref(&third)));
    assert_eq!(checked[2].event, third.event);
    let status = reader.status();
    assert_eq!((status.events, status.broken, status.incarnation.as_str()), (3, None, "memory"));
    assert_eq!(status.seen, Some(Mark { index: 3, sha256: third.stored.sha256() }));
    assert_eq!(reader.catch_up().await.unwrap(), [], "nothing new");
    assert_eq!(log.last_sequence(&sense).await.unwrap(), 4);
}

#[tokio::test]
async fn a_stale_writer_is_refused_and_appends_after_what_it_missed() {
    let (team, log, sense) = (Team::new(), MemoryLog::default(), SenseId::new());
    let mut a = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
    let mut b = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
    let (first, _) = a.append("note", json!(1)).await.unwrap();
    assert_eq!(log.append(&sense, b"stale".to_vec(), 0).await, Err(AppendError::Conflict { last: 1 }));

    let (second, missed) = b.append("note", json!(2)).await.unwrap();
    assert_eq!(messages(&missed), messages(std::slice::from_ref(&first)));
    assert_eq!((second.event.index, second.event.prev), (2, Some(first.stored.sha256())));
    let (third, missed) = a.append("note", json!(3)).await.unwrap();
    assert_eq!((kinds(&missed), third.event.index), (vec![(2, "note")], 3));
    assert_eq!(log.read_after(&sense, 0, 10).await.unwrap().len(), 3, "nothing but the three");
    assert_eq!(messages(&b.catch_up().await.unwrap()), messages(&[third]));
    assert_eq!(b.chain().broken(), None);
}

#[tokio::test]
async fn a_rewritten_or_removed_event_breaks_the_chain_where_it_is() {
    let (team, sense) = (Team::new(), SenseId::new());
    for removed in [false, true] {
        let log = MemoryLog::default();
        let mut writer = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
        for n in 1..=4 {
            writer.append("note", json!(n)).await.unwrap();
        }
        let second = log.read_after(&sense, 1, 1).await.unwrap().remove(0);
        if removed {
            log.rewrite(second.sequence, None);
        } else {
            let mut event = second.event().unwrap();
            event.data = json!("forged");
            log.rewrite(second.sequence, Some(serde_json::to_vec(&event).unwrap()));
        }
        let mut reader = ant_log(&log, Unsigned, team.verifier(), &sense, None);
        let checked = reader.catch_up().await.unwrap();
        let broken = reader.chain().broken().unwrap().clone();
        if removed {
            assert_eq!((broken.index, broken.sequence), (2, Some(3)));
            assert!(broken.reason.contains("event 3 where event 2 follows"), "{}", broken.reason);
            // Reading went on from the event after the gap, chained to the one removed.
            assert_eq!(kinds(&checked), [(1, "note"), (4, "note")]);
        } else {
            assert_eq!((broken.index, broken.sequence), (2, Some(2)));
            assert!(broken.reason.contains("not signed by"), "{}", broken.reason);
            assert_eq!(kinds(&checked), [(1, "note"), (4, "note")], "the third chains to the second as it was");
        }
        assert_eq!(reader.status().broken, Some(broken));
    }
}

#[tokio::test]
async fn only_the_ant_s_own_unrevoked_credentials_sign_its_events() {
    let (team, sense) = (Team::new(), SenseId::new());
    let stranger = Team::new();
    let refused = async |signer: Option<KeySigner>, revoked: Option<HashMap<String, u64>>| -> Option<String> {
        let log = MemoryLog::default();
        match signer {
            Some(signer) => ant_log(&log, signer, team.verifier(), &sense, None).append("note", json!(1)).await,
            None => ant_log(&log, Unsigned, team.verifier(), &sense, None).append("note", json!(1)).await,
        }
        .unwrap();
        let verifier = AccountVerifier::new(vec![team.account.public_key()], Revoked(revoked));
        let mut reader = ant_log(&log, Unsigned, verifier, &sense, None);
        reader.catch_up().await.unwrap();
        reader.chain().broken().map(|b| b.reason.clone())
    };
    let reason = refused(Some(team.signer("dev-b")), Some(HashMap::new())).await.unwrap();
    assert!(reason.contains("signed by the credentials of dev-b, not of dev-a"), "{reason}");
    let reason = refused(Some(stranger.signer("dev-a")), Some(HashMap::new())).await.unwrap();
    assert!(reason.contains("the signing credentials"), "{reason}");
    let reason = refused(None, Some(HashMap::new())).await.unwrap();
    assert!(reason.contains("signed by no credentials"), "{reason}");

    let signer = team.signer("dev-a");
    let key = signer.1.public_key();
    let now = u64::try_from(Utc::now().timestamp()).unwrap();
    let reason = refused(Some(signer), Some(HashMap::from([(key, now - 60)]))).await.unwrap();
    assert!(reason.contains("revoked"), "{reason}");
    let signer = team.signer("dev-a");
    let key = signer.1.public_key();
    assert_eq!(
        refused(Some(signer), Some(HashMap::from([(key, now + 60)]))).await,
        None,
        "revoked after it was stored"
    );

    // No answer about revocations is no break: the read stops there, to go on later.
    let log = MemoryLog::default();
    ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None).append("note", json!(1)).await.unwrap();
    let verifier = AccountVerifier::new(vec![team.account.public_key()], Revoked(None));
    let mut reader = ant_log(&log, Unsigned, verifier, &sense, None);
    assert!(reader.catch_up().await.unwrap_err().0.contains("no answer"));
    assert_eq!((reader.chain().last_sequence(), reader.chain().broken()), (0, None));
    // Without accounts, on a server without authentication, every event holds.
    let mut open = ant_log(&log, Unsigned, AccountVerifier::new(vec![], NoRevocations), &sense, None);
    assert_eq!(open.catch_up().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_log_ending_before_or_differing_from_what_was_verified_is_broken() {
    let (team, sense) = (Team::new(), SenseId::new());
    for rewritten in [false, true] {
        let home = ants_test_support::scratch_dir("sense-chain");
        let log = MemoryLog::default();
        let mut writer = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, Some(&home));
        for n in 1..=3 {
            writer.append("note", json!(n)).await.unwrap();
        }
        let kept = read_status(&home).unwrap().unwrap();
        assert_eq!((kept.events, kept.seen.map(|s| s.index), kept.broken), (3, Some(3), None));
        let third = log.read_after(&sense, 2, 1).await.unwrap().remove(0);
        log.rewrite(third.sequence, None);
        if rewritten {
            // Validly signed, at the place of the third, yet not the third as verified.
            let mut other = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
            other.append("note", json!("other")).await.unwrap();
        }
        let mut reader = ant_log(&log, Unsigned, team.verifier(), &sense, Some(&home));
        reader.catch_up().await.unwrap();
        let broken = read_status(&home).unwrap().unwrap().broken.unwrap();
        if rewritten {
            assert_eq!(broken.index, 3);
            assert!(broken.reason.contains("differs from event 3 as verified before"), "{}", broken.reason);
        } else {
            assert_eq!((broken.index, broken.sequence), (3, None));
            assert!(broken.reason.contains("ends at event 2, event 3 was verified before"), "{}", broken.reason);
        }
        std::fs::remove_dir_all(&home).unwrap();
    }
    assert_eq!(read_status(Path::new("/nonexistent")).unwrap(), None);
}

#[tokio::test]
async fn the_log_s_size_counts_each_message_of_the_identity_read_or_appended() {
    let (team, log, sense, other) = (Team::new(), MemoryLog::default(), SenseId::new(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-size");
    let mut writer = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, Some(&home));
    writer.append("note", json!(1)).await.unwrap();
    log.append(&other, b"another identity's".to_vec(), 0).await.unwrap();
    writer.append("note", json!(2)).await.unwrap();
    let total: u64 = log.read_after(&sense, 0, 10).await.unwrap().iter().map(|m| m.payload.len() as u64).sum();
    assert_eq!(writer.chain().bytes(), total);
    assert_eq!(read_status(&home).unwrap().unwrap().bytes, total);

    let mut reader = ant_log(&log, Unsigned, team.verifier(), &sense, None);
    reader.catch_up().await.unwrap();
    assert_eq!(reader.chain().bytes(), total);
    let position = reader.chain().position();
    log.append(&sense, b"not an event".to_vec(), 3).await.unwrap();
    let mut resumed = ant_log(&log, Unsigned, team.verifier(), &sense, None);
    resumed.resume(position);
    resumed.catch_up().await.unwrap();
    assert!(resumed.chain().broken().is_some());
    assert_eq!(resumed.chain().bytes(), total + 12, "a message breaking the chain takes room too");
    resumed.resume(None);
    assert_eq!(resumed.chain().bytes(), 0);
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn a_renamed_ant_goes_on_with_its_log_under_its_new_name() {
    let (team, log, sense) = (Team::new(), MemoryLog::default(), SenseId::new());
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut old = ant_log(&log, team.signer("dev-a"), team.verifier(), &sense, None);
    old.append("note", json!(1)).await.unwrap();
    old.append("note", json!(2)).await.unwrap();
    let renamed = |signer: KeySigner, earlier: Vec<UserId>| {
        AntLog::new(&log, signer, team.verifier(), sense.clone(), dev_b.clone(), m1(), None)
            .unwrap()
            .with_earlier_names(earlier)
    };

    let mut new = renamed(team.signer("dev-b"), vec![dev_a()]);
    assert_eq!(kinds(&new.catch_up().await.unwrap()), [(1, "note"), (2, "note")]);
    assert_eq!(new.chain().named(), Some(&dev_a()));
    let (third, read) = new.append("note", json!(3)).await.unwrap();
    assert_eq!((kinds(&read), third.event.index), (vec![(3, RENAMED)], 4), "the rename first");
    assert_eq!((&read[0].event.ant, &read[0].event.data), (&dev_b, &json!({ "from": "dev-a" })));
    assert_eq!(new.chain().named(), Some(&dev_b));

    let mut reader = renamed(team.signer("dev-b"), vec![dev_a()]);
    assert_eq!(reader.catch_up().await.unwrap().len(), 4);
    assert_eq!(reader.chain().broken(), None);
    let mut unaware = renamed(team.signer("dev-b"), vec![]);
    unaware.catch_up().await.unwrap();
    let broken = unaware.chain().broken().unwrap();
    assert_eq!(broken.index, 1, "events of a name its home does not list are not its own");
    assert!(broken.reason.contains("an event of dev-a, not of dev-b nor of an earlier name"), "{}", broken.reason);

    // The old name signs no event past the rename, nor does another ant rename itself into the log.
    old.append("note", json!("stale")).await.unwrap();
    reader.catch_up().await.unwrap();
    let broken = reader.chain().broken().unwrap();
    assert_eq!(broken.index, 5);
    assert!(broken.reason.contains("an event of dev-a, not of dev-b"), "{}", broken.reason);
    let mut other =
        AntLog::new(&log, team.signer("dev-c"), team.verifier(), sense.clone(), "dev-c".parse().unwrap(), m1(), None)
            .unwrap()
            .with_earlier_names(vec![dev_b.clone()]);
    other.append("note", json!("taken")).await.unwrap();
    let mut reader = renamed(team.signer("dev-b"), vec![dev_a()]);
    let checked = reader.catch_up().await.unwrap();
    assert_eq!(checked.len(), 4, "nothing past the stale event verifies: {:?}", kinds(&checked));
}
