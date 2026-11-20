//! The tips ants leave on saved trails ([`ants_daemon::trail::Tips`]), and their votes, read back as a trail's board.

use ants_daemon::trail::Tips;
use ants_proto::{TipId, TipVerdict, TipVote, TrailName, TrailTip, script_sha256};

use super::*;

#[tokio::test]
async fn an_ant_leaves_a_tip_once_another_votes_on_it_and_the_trail_s_board_reads_both() {
    let Some(server) = NatsServer::start() else { return };
    let (_dev, dev_js) = ants_daemon::bus::connect(&server.url, None, None, &ant("dev"), "dev").await.unwrap();
    let deploy: TrailName = "deploy".parse().unwrap();
    let missing = Tips::new(&dev_js, None).await.unwrap().board(&deploy).await.unwrap_err();
    assert!(format!("{missing:#}").contains("`ants init` creates it"), "{missing:#}");
    init(&server).await;
    let lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let tips = lead.trails.tips();
    assert!(tips.board(&deploy).await.unwrap().is_empty());

    let now = chrono::Utc::now();
    let script = script_sha256("agent('build')");
    let tip = TrailTip {
        trail: deploy.clone(),
        id: TipId::generate(),
        writer: ant("lead"),
        text: "Run the migrations before the build".to_owned(),
        tip_key: Some("build".to_owned()),
        run: RunId::generate(),
        script: script.clone(),
        written_at: now,
        retired: None,
    };
    tips.leave(&tip).await.unwrap();
    assert!(tips.leave(&tip).await.is_err(), "a tip is left once");
    let elsewhere = TrailTip { trail: "release".parse().unwrap(), id: TipId::generate(), ..tip.clone() };
    tips.leave(&elsewhere).await.unwrap();
    let vote = |verdict| TipVote {
        voter: ant("dev"),
        verdict,
        reason: None,
        text: tip.text_sha256(),
        script: Some(script.clone()),
        at: now,
    };
    let dev = Tips::new(&dev_js, None).await.unwrap();
    dev.vote(&tip, &vote(TipVerdict::Stale)).await.unwrap();
    dev.vote(&tip, &vote(TipVerdict::Helpful)).await.unwrap();

    let board = tips.board(&deploy).await.unwrap();
    assert_eq!(board.len(), 1, "only the trail's own tips");
    let ranked = board.tip(&tip.id, None, now, Some(&script)).unwrap();
    assert_eq!((ranked.tip, ranked.votes), (&tip, &[vote(TipVerdict::Helpful)][..]), "a vote again replaces it");
    assert!(ranked.status.is_live());
}
