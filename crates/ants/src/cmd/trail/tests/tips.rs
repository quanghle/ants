//! `ants trail tips`, and `ants trail rm` purging a trail's tips.

use ants_proto::{
    Retirement, Role, RunId, TRAIL_TIPS_BUCKET, TipBoard, TipRetired, TipStatus, TipVerdict, TipVote, TrailName,
    TrailTip, script_sha256,
};
use async_nats::jetstream;
use chrono::{DateTime, TimeDelta, Utc};
use clap::Parser;

use super::super::saved;
use super::super::tips::{self, Json};
use super::super::{TipsArgs, TrailCommand};
use super::{human, p, script, write};
use crate::cli::{Cli, Command};
use crate::connection::Nats;
use crate::test_support::Server;

fn parsed(args: &[&str]) -> Result<(TipsArgs, bool), clap::Error> {
    let cli = Cli::try_parse_from(["ants", "trail", "tips"].iter().chain(args))?;
    let Some(Command::Trail(TrailCommand::Tips { tips, json })) = cli.command else { panic!("not tips") };
    Ok((tips, json))
}

fn args(name: &str) -> TipsArgs {
    TipsArgs { name: name.parse().unwrap(), key: None, all: false, retire: None, writer: None }
}

fn now() -> DateTime<Utc> {
    "2026-03-01T00:00:00Z".parse().unwrap()
}

/// A tip of `writer` on trail `deploy`, left `days` before [`now`] on the script `script`.
fn tip(writer: &str, id: &str, key: Option<&str>, text: &str, days: i64, script: &str) -> TrailTip {
    TrailTip {
        trail: "deploy".parse().unwrap(),
        id: id.parse().unwrap(),
        writer: p(writer),
        text: text.to_owned(),
        tip_key: key.map(str::to_owned),
        run: RunId::generate(),
        script: script.to_owned(),
        written_at: now() - TimeDelta::days(days),
        retired: None,
    }
}

fn vote(voter: &str, verdict: TipVerdict, days: i64) -> TipVote {
    TipVote {
        voter: p(voter),
        verdict,
        reason: None,
        text: String::new(),
        script: None,
        at: now() - TimeDelta::days(days),
    }
}

/// The entries of `tips`, their retirements and their `votes` under their keys, each vote judging its tip's text, as a
/// trail's whole board reads them.
fn entries(tips: &[&TrailTip], votes: &[(&TrailTip, TipVote)]) -> Vec<(String, Vec<u8>)> {
    let retired =
        tips.iter().filter_map(|tip| Some((tip.retired_key(), serde_json::to_vec(tip.retired.as_ref()?).unwrap())));
    let tips = tips.iter().map(|tip| (tip.key(), serde_json::to_vec(tip).unwrap()));
    let votes = votes.iter().map(|(tip, vote)| {
        (tip.vote_key(&vote.voter), serde_json::to_vec(&TipVote { text: tip.text_sha256(), ..vote.clone() }).unwrap())
    });
    tips.chain(retired).chain(votes).collect()
}

#[test]
fn tips_take_a_key_and_all_or_a_tip_to_retire() {
    assert_eq!(parsed(&["deploy"]).unwrap(), (args("deploy"), false));
    let (listed, json) = parsed(&["deploy", "--key", "build image", "--all", "--json"]).unwrap();
    assert_eq!((listed.key.as_deref(), listed.all, json), (Some("build image"), true, true));
    let (retired, _) = parsed(&["deploy", "--retire", "abcdefgh"]).unwrap();
    assert_eq!(retired.retire, Some("abcdefgh".parse().unwrap()));
    for refused in [&["deploy", "--retire", "abcdefgh", "--all"][..], &["deploy", "--retire", "abcdefgh", "--key", "x"]]
    {
        let error = parsed(refused).unwrap_err().to_string();
        assert!(error.contains("cannot be used with"), "{refused:?}: {error}");
    }
    assert!(parsed(&["deploy", "--retire", "abcdefg1"]).is_err(), "no tip id");
    assert!(parsed(&["Not_A_Name"]).is_err());
}

#[test]
fn tips_list_live_ones_in_rank_order_and_retired_ones_with_all_or_under_a_key() {
    let pinned = tip("dev-a", "aaaaaaaa", Some("build"), "pin the base image", 2, "old");
    let migrations = tip("dev-b", "bbbbbbbb", None, "run the migrations first\n\n\tthen \u{1b}[31mdeploy", 5, "new");
    let mut by_human = tip("dev-a", "cccccccc", Some("build"), "use the cache", 9, "new");
    by_human.retired = Some(TipRetired { by: p("alice"), at: now() - TimeDelta::days(1) });
    let wrong = tip("dev-c", "dddddddd", Some("test"), "skip the flaky test", 3, "new");
    let votes = [
        (&pinned, vote("dev-b", TipVerdict::Helpful, 1)),
        (&pinned, vote("dev-c", TipVerdict::Helpful, 1)),
        (&migrations, vote("dev-a", TipVerdict::Stale, 1)),
        (&wrong, vote("dev-a", TipVerdict::Wrong, 1)),
        (&wrong, vote("dev-b", TipVerdict::Wrong, 1)),
    ];
    let board = TipBoard::new("deploy".parse().unwrap(), entries(&[&pinned, &migrations, &by_human, &wrong], &votes));
    let script = Some("new");

    let listed = tips::render(&board, &args("deploy"), script, now());
    assert_eq!(
        listed,
        "trail deploy: 2 live tips, 2 retired, which --all lists\n\
         \n\
         TIP       KEY         WRITER  VOTES     STATUS              WRITTEN\n\
         aaaaaaaa  build       dev-a   +2 ~0 !0  live, older script  2d ago\n\
         \x20 pin the base image\n\
         bbbbbbbb  trail-wide  dev-b   +0 ~1 !0  live                5d ago\n\
         \x20 run the migrations first\n\
         \x20 then [31mdeploy\n"
    );
    let all = TipsArgs { all: true, ..args("deploy") };
    let listed = tips::render(&board, &all, script, now());
    assert!(listed.starts_with("trail deploy: 2 live tips, 2 retired\n"), "{listed}");
    let statuses: Vec<&str> = listed.lines().filter(|l| !l.starts_with(' ')).skip(5).collect();
    assert_eq!(statuses.len(), 2, "{listed}");
    assert!(statuses[0].starts_with("cccccccc  build") && statuses[0].contains("retired by alice"), "{listed}");
    assert!(statuses[1].starts_with("dddddddd  test") && statuses[1].contains("voted wrong"), "{listed}");

    let build = TipsArgs { key: Some("build".to_owned()), ..args("deploy") };
    let listed = tips::render(&board, &build, script, now());
    assert!(listed.starts_with("trail deploy, key build: 1 live tip, 1 retired, which --all lists\n"), "{listed}");
    assert!(listed.contains("aaaaaaaa") && !listed.contains("bbbbbbbb") && !listed.contains("cccccccc"), "{listed}");
    let none = TipsArgs { key: Some("lint".to_owned()), all: true, ..args("deploy") };
    assert_eq!(tips::render(&board, &none, script, now()), "no tips under key lint on trail deploy\n");
    let empty = TipBoard::new("deploy".parse().unwrap(), Vec::<(String, Vec<u8>)>::new());
    assert_eq!(tips::render(&empty, &args("deploy"), script, now()), "no tips on trail deploy\n");
    let unsaved = tips::render(&board, &args("deploy"), None, now());
    assert!(!unsaved.contains("older script"), "no saved script, no older one: {unsaved}");

    let listed = tips::listed(&board, &args("deploy"), script, now());
    let json = serde_json::to_value(Json::from(&listed[0])).unwrap();
    assert_eq!((json["id"].as_str(), json["tip_key"].as_str()), (Some("aaaaaaaa"), Some("build")));
    assert_eq!((json["status"].as_str(), json["live"].as_bool()), (Some("live"), Some(true)));
    assert_eq!((json["helpful"].as_u64(), json["older_script"].as_bool()), (Some(2), Some(true)));
    assert_eq!(json["votes"].as_array().map(Vec::len), Some(2));
}

/// Put `tip` in [`TRAIL_TIPS_BUCKET`], as its writer's daemon does.
async fn put_tip(js: &jetstream::Context, tip: &TrailTip) {
    let payload = serde_json::to_vec(tip).unwrap();
    js.publish(format!("$KV.{TRAIL_TIPS_BUCKET}.{}", tip.key()), payload.into()).await.unwrap().await.unwrap();
}

async fn put_vote(js: &jetstream::Context, tip: &TrailTip, vote: &TipVote) {
    let payload = serde_json::to_vec(&TipVote { text: tip.text_sha256(), ..vote.clone() }).unwrap();
    let subject = format!("$KV.{TRAIL_TIPS_BUCKET}.{}", tip.vote_key(&vote.voter));
    js.publish(subject, payload.into()).await.unwrap().await.unwrap();
}

#[tokio::test]
async fn humans_retire_tips_whether_their_trail_is_saved_or_not_and_removing_it_purges_them() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-tips");
    let alice = p("alice");
    let js = human(&server, &alice).await;
    let saved = saved::read(&write(&dir, "deploy.py", &script("deploy")), None, &alice, Utc::now()).unwrap();
    saved::put(&js, &saved).await.unwrap();
    let name: TrailName = "deploy".parse().unwrap();
    let fresh = |tip: TrailTip| TrailTip { written_at: Utc::now(), ..tip };
    let sha = script_sha256(&saved.script);
    let current = fresh(tip("dev-a", "aaaaaaaa", Some("build"), "pin the base image", 0, &sha));
    let older = fresh(tip("dev-b", "bbbbbbbb", None, "run the migrations first", 0, "old"));
    let other =
        TrailTip { trail: "build".parse().unwrap(), ..fresh(tip("dev-a", "cccccccc", None, "elsewhere", 0, "")) };
    for tip in [&current, &older, &other] {
        put_tip(&js, tip).await;
    }
    put_vote(&js, &current, &TipVote { at: Utc::now(), ..vote("dev-b", TipVerdict::Helpful, 0) }).await;

    let (board, script) = tips::read(&js, &name).await.unwrap();
    assert_eq!(script.as_deref(), Some(sha.as_str()));
    let listed = tips::render(&board, &args("deploy"), script.as_deref(), Utc::now());
    assert!(listed.starts_with("trail deploy: 2 live tips\n"), "{listed}");
    let rows: Vec<&str> = listed.lines().filter(|l| l.contains("dev-")).collect();
    assert!(rows[0].starts_with("aaaaaaaa") && rows[0].contains("+1 ~0 !0  live "), "{listed}");
    assert!(rows[1].starts_with("bbbbbbbb") && rows[1].contains("live, older script"), "{listed}");

    let dev_a = p("dev-a");
    let creds = server.creds_as(&dev_a, Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, ant_js) = ant.connect("test").await.unwrap();
    assert_eq!(tips::read(&ant_js, &name).await.unwrap().0.len(), 2, "an ant reads the tips");
    let retire = TipsArgs { retire: Some(current.id.clone()), ..args("deploy") };
    let error = super::super::run(&ant, TrailCommand::Tips { tips: retire, json: false }).await.unwrap_err();
    assert_eq!(error.to_string(), "retiring a tip needs a human's credentials; those of dev-a are an ant's");

    let at = Utc::now();
    let retired = tips::retire(&js, &name, &current.id, None, &alice, at).await.unwrap();
    assert_eq!(retired, TrailTip { retired: Some(TipRetired { by: alice.clone(), at }), ..current.clone() });
    let later = retired.written_at + TimeDelta::hours(3);
    let said = tips::retired(&retired, later);
    assert_eq!(said, "retired tip aaaaaaaa of trail deploy, left by dev-a 3h ago: pin the base image");
    let (board, _) = tips::read(&js, &name).await.unwrap();
    let status = board.tip(&current.id, None, Utc::now(), None).unwrap().status;
    assert_eq!(status, TipStatus::Retired(Retirement::ByHuman { by: alice.clone(), at }));
    assert_eq!(board.tip(&current.id, None, Utc::now(), None).unwrap().votes.len(), 1, "its votes stay");
    // Its writer puts it again, under its own credentials: it stays retired.
    put_tip(&ant_js, &current).await;
    let (board, _) = tips::read(&ant_js, &name).await.unwrap();
    let status = board.tip(&current.id, None, Utc::now(), None).unwrap().status;
    assert_eq!(status, TipStatus::Retired(Retirement::ByHuman { by: alice.clone(), at }), "its writer revives none");
    let error = tips::retire(&js, &name, &current.id, None, &alice, at).await.unwrap_err().to_string();
    assert_eq!(error, "tip aaaaaaaa of trail deploy was retired by alice 0s ago");
    let error = tips::retire(&js, &name, &other.id, None, &alice, at).await.unwrap_err().to_string();
    assert_eq!(error, "trail deploy has no tip cccccccc: `ants trail tips deploy --all` lists them");

    let local = dir.join("trails");
    assert_eq!(saved::remove(&js, &local, &name).await.unwrap().tips, 4, "two tips, a vote and a retirement");
    assert!(saved::get(&js, &name).await.unwrap().is_none());
    assert!(tips::read(&js, &name).await.unwrap().0.is_empty());
    assert_eq!(tips::read(&js, &"build".parse().unwrap()).await.unwrap().0.len(), 1, "another trail's tips stay");
    assert_eq!((tips::purged(0).as_str(), tips::purged(1).as_str()), ("", " and its tips: 1 entry"));
    assert_eq!(tips::purged(3), " and its tips: 3 entries, votes included");

    put_tip(&js, &older).await;
    let (board, script) = tips::read(&js, &name).await.unwrap();
    assert_eq!((board.len(), script), (1, None), "tips outlive a trail no longer saved");
    tips::retire(&js, &name, &older.id, None, &alice, Utc::now()).await.unwrap();
    let listed = tips::render(&tips::read(&js, &name).await.unwrap().0, &args("deploy"), None, Utc::now());
    assert_eq!(listed, "trail deploy: no live tips, 1 retired, which --all lists\n");

    let first = TrailTip { written_at: Utc::now(), ..tip("dev-a", "dddddddd", None, "tag before pushing", 0, &sha) };
    let copy = TrailTip { written_at: Utc::now(), ..tip("dev-b", "dddddddd", None, "push before tagging", 0, &sha) };
    for tip in [&first, &copy] {
        put_tip(&js, tip).await;
    }
    let error = tips::retire(&js, &name, &first.id, None, &alice, Utc::now()).await.unwrap_err().to_string();
    let named = "tips of dev-a, dev-b on trail deploy share id dddddddd: name the tip's writer with --writer";
    assert_eq!(error, named);
    let retired = tips::retire(&js, &name, &copy.id, Some(&p("dev-b")), &alice, Utc::now()).await.unwrap();
    assert_eq!(retired.writer, p("dev-b"));
    let (board, _) = tips::read(&js, &name).await.unwrap();
    assert!(board.tip(&first.id, Some(&p("dev-a")), Utc::now(), None).unwrap().status.is_live(), "the other stays");
    assert!(parsed(&["deploy", "--writer", "dev-b"]).is_err(), "--writer goes with --retire");

    let purged = saved::remove(&js, &local, &name).await.unwrap();
    assert!(!purged.trail && purged.tips > 0, "removing a trail removed already purges what is left of it");
    let error = saved::remove(&js, &local, &name).await.unwrap_err().to_string();
    assert_eq!(error, "the colony saved no trail deploy");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn tips_of_a_trail_only_this_machine_s_files_hold_are_refused_as_no_colony_trail_s() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-tips-local");
    let local = dir.join("trails");
    std::fs::create_dir_all(&local).unwrap();
    let alice = p("alice");
    let js = human(&server, &alice).await;
    write(&local, "solo.py", &script("solo"));
    let saved = saved::read(&write(&local, "deploy.py", &script("deploy")), None, &alice, Utc::now()).unwrap();
    saved::put(&js, &saved).await.unwrap();

    let error = tips::listing_in(&js, &local, &"solo".parse().unwrap()).await.unwrap_err().to_string();
    let file = local.join("solo.py");
    let refused =
        format!("trail solo is not a colony trail, only this machine's file {}: tips are left", file.display());
    assert!(error.starts_with(&refused), "{error}");
    let (board, script) = tips::listing_in(&js, &local, &"deploy".parse().unwrap()).await.unwrap();
    assert!(board.is_empty() && script.is_some(), "the colony's trail, which a local file shadows, takes tips");
    let (board, script) = tips::listing_in(&js, &local, &"gone".parse().unwrap()).await.unwrap();
    assert!(board.is_empty() && script.is_none(), "tips outlive a trail no longer saved");
    std::fs::remove_dir_all(&dir).unwrap();
}
