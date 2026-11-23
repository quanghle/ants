//! Tests of sense exports and imports: landing under the same sense id, damaged exports refused, logs already holding
//! events, renames, moves across buses, anchors, skill files, and thread records.

use ants_proto::{SenseId, UserId};
use serde_json::json;

use super::*;
use crate::cache::Cache;
use crate::items::Values;
use crate::log::tests::{KeySigner, Team};
use crate::log::{AccountVerifier, Break, MemoryLog, RENAMED};
use crate::store::Store;

/// A bus: its team account and its sense stream.
struct Bus {
    team: Team,
    log: MemoryLog,
}

impl Bus {
    fn new() -> Self {
        Self { team: Team::new(), log: MemoryLog::default() }
    }

    fn account(&self) -> String {
        self.team.account.public_key()
    }

    /// The log of `sense` read and written on this bus by `name`, once named `earlier` too.
    fn ant_log(&self, sense: &SenseId, name: &str, earlier: &[&str]) -> Log<'_> {
        let log =
            AntLog::new(&self.log, self.team.signer(name), self.team.verifier(), sense.clone(), ant(name), m1(), None);
        log.unwrap().with_earlier_names(earlier.iter().map(|name| ant(name)).collect())
    }

    async fn store(&self, sense: &SenseId, name: &str, earlier: &[&str]) -> Store<&MemoryLog, KeySigner, Verify> {
        Store::open(self.ant_log(sense, name, earlier), Cache::in_memory().unwrap()).await.unwrap()
    }

    /// The export of `sense`, the identity of `name`, on this bus.
    async fn export(&self, sense: &SenseId, name: &str, skills: Vec<(String, Vec<u8>)>) -> SenseExport {
        let messages = self.store(sense, name, &[]).await.messages().await.unwrap();
        pack(sense.clone(), ant(name), &messages, &[self.account()], skills).unwrap()
    }

    /// The kinds of the events of `sense`'s log on this bus, as stored.
    async fn kinds(&self, sense: &SenseId) -> Vec<String> {
        let read = self.log.read_after(sense, 0, 100).await.unwrap();
        read.iter().map(|stored| stored.event().unwrap().kind).collect()
    }
}

type Verify = AccountVerifier<crate::log::NoRevocations>;
type Log<'a> = AntLog<&'a MemoryLog, KeySigner, Verify>;

fn ant(name: &str) -> UserId {
    name.parse().unwrap()
}

fn m1() -> ants_proto::Machine {
    "m1".parse().unwrap()
}

fn note(title: &str) -> Values {
    json!({ "title": title, "body": format!("about {title}") }).as_object().unwrap().clone()
}

/// `sense`'s log on `bus` with a note of each title, written by `name`.
async fn notes(bus: &Bus, sense: &SenseId, name: &str, titles: &[&str]) {
    let mut store = bus.store(sense, name, &[]).await;
    for title in titles {
        store.create("note", note(title), "t").await.unwrap();
    }
}

/// The titles of the notes of `store`, sorted, and the first break of its log.
fn titles(store: &Store<&MemoryLog, KeySigner, Verify>) -> (Vec<String>, Option<Break>) {
    let mut titles: Vec<_> =
        store.list("note", 100).unwrap().iter().map(|item| item.text("title").to_owned()).collect();
    titles.sort();
    (titles, store.log().chain().broken().cloned())
}

/// The lines of `export`'s log.
fn lines(export: &SenseExport) -> Vec<Vec<u8>> {
    let log = BASE64.decode(export.files[0].data.as_bytes()).unwrap();
    log.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()).map(<[u8]>::to_vec).collect()
}

/// `export` with its log of `lines`, listed as such in its manifest, its tip left as it was.
fn with_lines(export: &SenseExport, lines: &[Vec<u8>]) -> SenseExport {
    let log: Vec<u8> = lines.iter().flat_map(|line| line.iter().chain(b"\n")).copied().collect();
    let mut export = export.clone();
    export.files[0] = File::new(LOG_FILE.to_owned(), &log);
    export.manifest.files[0] = entry(LOG_FILE, &log);
    export.manifest.events = lines.len() as u64;
    export
}

#[tokio::test]
async fn an_import_lands_under_the_same_sense_id_and_verifies_on_the_new_bus() {
    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![("review/SKILL.md".to_owned(), b"review well".to_vec())]).await;
    let manifest = &export.manifest;
    assert_eq!((manifest.version, &manifest.sense, manifest.events), (VERSION, &sense, 2));
    assert_eq!((manifest.ant.as_str(), &manifest.accounts), ("dev-a", &vec![a.account()]));
    let paths: Vec<_> = manifest.files.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, [LOG_FILE, "skills/review/SKILL.md"]);
    let read = a.log.read_after(&sense, 0, 10).await.unwrap();
    assert_eq!(manifest.tip, Some(Mark { index: 2, sha256: read[1].sha256() }));
    assert_eq!(lines(&export), [read[0].payload.clone(), read[1].payload.clone()], "the messages as stored");

    let mut log = b.ant_log(&sense, "dev-a", &[]);
    let imported = import(&mut log, &export).await.unwrap();
    assert_eq!(imported.appended, 2);
    assert_eq!(imported.skills, [(PathBuf::from("review/SKILL.md"), b"review well".to_vec())]);
    assert_eq!(b.kinds(&sense).await, ["item", "item", IMPORTED]);
    let anchor = b.log.read_after(&sense, 2, 1).await.unwrap()[0].event().unwrap();
    let anchor: Anchor = serde_json::from_value(anchor.data).unwrap();
    assert_eq!(anchor, Anchor { accounts: vec![a.account()], through: manifest.tip.clone().unwrap() });

    // A reader on the new bus, its cache built anew from the log, takes the imported events on the anchor's word.
    let mut store = b.store(&sense, "dev-a", &[]).await;
    assert_eq!(titles(&store), (vec!["one".to_owned(), "two".to_owned()], None));
    assert_eq!(store.log().chain().events(), 3);
    store.create("note", note("three"), "t").await.unwrap();
    let store = b.store(&sense, "dev-a", &[]).await;
    assert_eq!(titles(&store).0, ["one", "three", "two"]);
    assert_eq!(store.log().chain().broken(), None);

    let home = ants_test_support::scratch_dir("sense-import");
    imported.write_skills(&home).unwrap();
    assert_eq!(skill_files(&home).unwrap(), [("review/SKILL.md".to_owned(), b"review well".to_vec())]);
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn a_damaged_or_edited_export_is_refused() {
    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![("ok.md".to_owned(), b"ok".to_vec())]).await;
    let lines = lines(&export);
    let edited = String::from_utf8(lines[1].clone()).unwrap().replace("two", "TWO").into_bytes();
    let mut damaged = export.clone();
    damaged.files[0] = File::new(LOG_FILE.to_owned(), &[edited.clone(), b"\n".to_vec()].concat());
    let mut skill = export.clone();
    skill.files[1] = File::new("skills/ok.md".to_owned(), b"changed");
    let mut elsewhere = export.clone();
    elsewhere.files[1].path = "skills/../../escape".to_owned();
    elsewhere.manifest.files[1].path = "skills/../../escape".to_owned();
    let mut unlisted = export.clone();
    unlisted.files.push(File::new("skills/more.md".to_owned(), b"more"));
    let mut other_bus = export.clone();
    other_bus.manifest.accounts = vec![b.account()];
    let mut other_sense = export.clone();
    other_sense.manifest.sense = SenseId::new();
    let cases = [
        (damaged, "log.jsonl is not the file the manifest lists"),
        (skill, "skills/ok.md is not the file the manifest lists"),
        (with_lines(&export, &[lines[0].clone(), edited]), "event 2: the signing credentials"),
        (with_lines(&export, &lines[1..]), "event 1: event 2 where event 1 follows"),
        (with_lines(&export, &[lines[1].clone(), lines[0].clone()]), "event 1: event 2 where event 1 follows"),
        (with_lines(&export, &lines[..1]), "does not end at the event the manifest tells"),
        (elsewhere, "a file at skills/../../escape"),
        (unlisted, "skills/more.md, which the manifest does not list"),
        (other_bus, "event 1: the signing credentials"),
        (other_sense, "of sense identity"),
    ];
    for (export, why) in cases {
        let error = import(&mut b.ant_log(&sense, "dev-a", &[]), &export).await.unwrap_err();
        assert!(matches!(error, ImportError::Invalid(_)), "{error}");
        assert!(error.to_string().contains(why), "{error}, not {why}");
    }
    assert_eq!(b.kinds(&sense).await, Vec::<String>::new(), "nothing imported");
}

#[tokio::test]
async fn a_log_holding_other_events_refuses_and_one_holding_the_first_takes_the_rest() {
    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![]).await;

    let mut other = b.ant_log(&sense, "dev-a", &[]);
    other.append("item", json!({ "written": "here first" })).await.unwrap();
    let refused = import(&mut b.ant_log(&sense, "dev-a", &[]), &export).await.unwrap_err();
    assert!(refused.to_string().contains("holds other events, from event 1: nothing imported"), "{refused}");
    assert_eq!(b.kinds(&sense).await, ["item"], "nothing merged");

    // Cut short after the first event, as on a failure, the import goes on from there.
    let (c, sense) = (Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![]).await;
    c.log.append(&sense, lines(&export)[0].clone(), 0).await.unwrap();
    let imported = import(&mut c.ant_log(&sense, "dev-a", &[]), &export).await.unwrap();
    assert_eq!(imported.appended, 1);
    assert_eq!(c.kinds(&sense).await, ["item", "item", IMPORTED]);
    let again = import(&mut c.ant_log(&sense, "dev-a", &[]), &export).await.unwrap();
    assert_eq!(again.appended, 0);
    assert_eq!(c.kinds(&sense).await.len(), 3, "taken in already");
    assert_eq!(titles(&c.store(&sense, "dev-a", &[]).await), (vec!["one".to_owned(), "two".to_owned()], None));

    let empty = Bus::new().export(&SenseId::new(), "dev-a", vec![]).await;
    assert_eq!((empty.manifest.events, &empty.manifest.tip), (0, &None));
    let imported = import(&mut c.ant_log(&empty.manifest.sense, "dev-a", &[]), &empty).await.unwrap();
    assert_eq!(imported.appended, 0);
    assert_eq!(c.kinds(&empty.manifest.sense).await, Vec::<String>::new(), "no anchor of nothing");
}

#[tokio::test]
async fn an_ant_imported_under_another_name_is_renamed_before_the_anchor() {
    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one"]).await;
    let export = a.export(&sense, "dev-a", vec![]).await;
    let unknown = import(&mut b.ant_log(&sense, "dev-b", &[]), &export).await.unwrap_err();
    assert!(unknown.to_string().contains("an event of dev-a, not of dev-b"), "{unknown}");

    import(&mut b.ant_log(&sense, "dev-b", &["dev-a"]), &export).await.unwrap();
    assert_eq!(b.kinds(&sense).await, ["item", RENAMED, IMPORTED]);
    let store = b.store(&sense, "dev-b", &["dev-a"]).await;
    assert_eq!(titles(&store), (vec!["one".to_owned()], None));
    assert_eq!(store.log().chain().named(), Some(&ant("dev-b")));
}

#[tokio::test]
async fn a_log_moved_across_three_buses_verifies_on_the_last() {
    let (a, b, c, sense) = (Bus::new(), Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one"]).await;
    import(&mut b.ant_log(&sense, "dev-a", &[]), &a.export(&sense, "dev-a", vec![]).await).await.unwrap();
    notes(&b, &sense, "dev-a", &["two"]).await;
    let export = b.export(&sense, "dev-a", vec![]).await;
    let mut accounts = vec![a.account(), b.account()];
    accounts.sort();
    assert_eq!(export.manifest.accounts, accounts, "the accounts of both buses");

    import(&mut c.ant_log(&sense, "dev-a", &[]), &export).await.unwrap();
    assert_eq!(c.kinds(&sense).await, ["item", IMPORTED, "item", IMPORTED]);
    let store = c.store(&sense, "dev-a", &[]).await;
    assert_eq!(titles(&store), (vec!["one".to_owned(), "two".to_owned()], None));
}

#[tokio::test]
async fn only_an_anchor_signed_on_the_bus_for_the_events_imported_vouches_for_them() {
    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![]).await;
    let tip = export.manifest.tip.clone().unwrap();
    let broken = |store: &Store<&MemoryLog, KeySigner, Verify>| {
        store.log().chain().broken().map(|b| (b.index, b.reason.clone()))
    };

    // With no anchor, or one signed with the other bus's credentials, the imported events break the chain.
    let mut sequence = 0;
    for line in lines(&export) {
        sequence = b.log.append(&sense, line, sequence).await.unwrap();
    }
    let store = b.store(&sense, "dev-a", &[]).await;
    let (index, reason) = broken(&store).unwrap();
    assert!(index == 1 && reason.contains("the signing credentials"), "{reason}");
    let anchor = |accounts: Vec<String>, through: Mark| json!(Anchor { accounts, through });
    let position = Position { sequence, tip: tip.clone(), events: 2, bytes: 0, broken: None, ant: Some(ant("dev-a")) };
    let mut forger =
        AntLog::new(&b.log, a.team.signer("dev-a"), b.team.verifier(), sense.clone(), ant("dev-a"), m1(), None)
            .unwrap();
    forger.resume(Some(position.clone()));
    forger.append(IMPORTED, anchor(vec![a.account()], tip.clone())).await.unwrap();
    assert_eq!(broken(&b.store(&sense, "dev-a", &[]).await).unwrap().0, 1, "an anchor not of this bus");

    // An anchor of this bus naming other events vouches for them alone: the log differs from what it names.
    let (b, sense) = (Bus::new(), SenseId::new());
    notes(&a, &sense, "dev-a", &["one", "two"]).await;
    let export = a.export(&sense, "dev-a", vec![]).await;
    let mut sequence = 0;
    for line in lines(&export) {
        sequence = b.log.append(&sense, line, sequence).await.unwrap();
    }
    let mut log = b.ant_log(&sense, "dev-a", &[]);
    let tip = export.manifest.tip.clone().unwrap();
    log.resume(Some(Position {
        sequence,
        tip: tip.clone(),
        events: 2,
        bytes: 0,
        broken: None,
        ant: Some(ant("dev-a")),
    }));
    let other = Mark { index: 2, sha256: "0".repeat(64) };
    log.append(IMPORTED, anchor(vec![a.account()], other)).await.unwrap();
    let (index, reason) = broken(&b.store(&sense, "dev-a", &[]).await).unwrap();
    assert_eq!((index, reason.as_str()), (2, "differs from event 2 as imported"));
}

#[test]
fn only_files_under_skills_are_skill_files() {
    assert_eq!(skill_path("skills/a/b.md"), Some(PathBuf::from("a/b.md")));
    for path in ["skills", "skills/", "skills/../x", "/skills/x", "other/x", "skills/./x/../y"] {
        assert_eq!(skill_path(path), None, "{path}");
    }
}

#[tokio::test]
async fn the_records_of_threads_sense_writes_survive_an_export_and_import() {
    use ants_proto::provider::{Event, EventKind, Origin, Starter};

    use crate::thread::record::{record, record_event};

    let (a, b, sense) = (Bus::new(), Bus::new(), SenseId::new());
    let thread: ants_proto::ThreadId = "019a7d2c-0000-7000-8000-000000000001".parse().unwrap();
    let origin = Origin { by: Starter::Human { user: ant("alice") }, channel: None };
    let started = EventKind::ThreadStarted { origin, repo: Some("github.com/o/r".to_owned()) };
    let event = |kind| Event {
        id: uuid::Uuid::now_v7(),
        ant: ant("dev-a"),
        identity: "0f1e".into(),
        thread: Some(thread.clone()),
        at: chrono::Utc::now(),
        kind,
    };
    let store = crate::store::Shared::new(a.store(&sense, "dev-a", &[]).await);
    record_event(&store, &event(started)).await.unwrap();
    let recorded = record_event(&store, &event(EventKind::ThreadDone)).await.unwrap().unwrap();
    drop(store);

    let export = a.export(&sense, "dev-a", vec![]).await;
    import(&mut b.ant_log(&sense, "dev-a", &[]), &export).await.unwrap();
    let store = crate::store::Shared::new(b.store(&sense, "dev-a", &[]).await);
    assert_eq!(record(&store, &thread).await.unwrap(), Some(recorded));
}
