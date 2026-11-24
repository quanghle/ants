//! Posting to channels and asking the channel manager for changes.

use super::*;

/// A channel manager in memory: it applies requests to its channels as `ants channel serve` does, colony aside,
/// and records them; with `silent` it answers nothing.
struct MemoryAdmin {
    channels: StdMutex<Vec<ChannelEntry>>,
    requests: StdMutex<Vec<ChannelAdminRequest>>,
    silent: bool,
}

impl Admin for MemoryAdmin {
    fn channels(&self) -> BoxFuture<'_, Result<Vec<ChannelEntry>>> {
        let channels = self.channels.locked().clone();
        Box::pin(async move { Ok(channels) })
    }

    fn request(&self, request: ChannelAdminRequest) -> BoxFuture<'_, Result<Option<ChannelAdminReply>>> {
        self.requests.locked().push(request.clone());
        let reply = (!self.silent).then(|| {
            let mut channels = self.channels.locked();
            let outcome = match channels.iter_mut().find(|e| e.channel == request.channel) {
                None => ChannelAdminOutcome::Refused { refusal: ants_proto::Refusal::NoSuchChannel },
                Some(entry) => match entry.apply(&request.ant, &request.ops, &ants_proto::Humans::default()) {
                    Ok(changed) => ChannelAdminOutcome::Applied { entry: entry.clone(), revision: 2, changed },
                    Err(refusal) => ChannelAdminOutcome::Refused { refusal },
                },
            };
            ChannelAdminReply { request_id: request.id, outcome }
        });
        Box::pin(async move { Ok(reply) })
    }
}

#[tokio::test]
async fn manage_channel_asks_the_channel_manager_for_the_owner_s_changes() {
    let home = home("chanadm");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({})).await;
    assert!(!ok && text.contains("cannot be managed"), "{text}");

    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.owner = Some(user("dev-a"));
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.owner = Some(user("lead"));
    let admin =
        Arc::new(MemoryAdmin { channels: StdMutex::new(vec![ops, dev]), requests: StdMutex::default(), silent: false });
    mcp.manage_channels(admin.clone());
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({})).await;
    assert!(ok && text.contains("#ops") && !text.contains("#dev"), "{text}");
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "#dev"})).await;
    assert_eq!((ok, text.as_str()), (true, "#dev: members none; messenger system-messenger (default); owner lead."));
    assert!(admin.requests.locked().is_empty(), "showing asks nothing");

    let args = serde_json::json!({"channel": "#ops", "add": ["@dev-b", "dev-c"], "messenger": "dev-b"});
    let (ok, text) = call(&client, "manage_channel", args).await;
    assert!(ok && text.starts_with("Changed #ops: added dev-b; added dev-c; messenger dev-b."), "{text}");
    let request = admin.requests.locked()[0].clone();
    assert_eq!((&request.ant, &request.thread_id), (&user("dev-a"), &current.thread_id));
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "ops", "add": ["dev-b"]})).await;
    assert!(ok && text.starts_with("Nothing to change."), "{text}");

    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "dev", "remove": ["x"]})).await;
    assert!(!ok && text.contains("#dev was not changed") && text.contains("lead"), "{text}");
    let both = serde_json::json!({"channel": "ops", "messenger": "dev-b", "clear_messenger": true});
    let (ok, text) = call(&client, "manage_channel", both).await;
    assert!(!ok && text.contains("not both"), "{text}");

    mcp.with_teams(super::teams::memory_teams(super::teams::forest(), false).0);
    let asked = admin.requests.locked().len();
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "ops", "add": ["@infra"]})).await;
    let refused = "#ops was not changed: infra is a team, which is no channel member; an @infra in a post of #ops \
                   mentions its lead, boss, who has to be a member";
    assert_eq!((ok, text.as_str()), (false, refused));
    assert_eq!(admin.requests.locked().len(), asked, "nothing asked of the channel manager");

    let silent = Arc::new(MemoryAdmin { channels: StdMutex::default(), requests: StdMutex::default(), silent: true });
    let other_home = super::home("chanadm-silent");
    let (mcp, _sent) = start(&other_home).await;
    mcp.manage_channels(silent);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) =
        call(&client, "manage_channel", serde_json::json!({"channel": "ops", "clear_messenger": true})).await;
    assert!(!ok && text.contains("no channel manager answered") && text.contains("ants channel"), "{text}");
}

#[tokio::test]
async fn post_channel_starts_a_thread_the_ant_created_where_it_may_post() {
    let home = home("post-channel");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let post = |channel: &str, text: &str| serde_json::json!({"channel": channel, "text": text});
    let (ok, text) = call(&client, "post_channel", post("ops", "hi")).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    let turn = mcp.begin(&current);
    let (ok, text) = call(&client, "post_channel", post("ops", "hi")).await;
    assert!(!ok && text.contains("channels are not served here"), "{text}");

    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(user("dev-a"));
    ops.add_member(user("dev-b"));
    ops.add_member(ants_test_support::human());
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.add_member(user("dev-b"));
    let mut triage = ChannelEntry::new("triage".parse().unwrap());
    triage.messenger = Some(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    let sources = Arc::new(StdMutex::new(Vec::new()));
    let mut access = channels(vec![ops, dev, triage], posted.clone());
    let store = access.post;
    access.post = Arc::new({
        let sources = sources.clone();
        move |posted, source| {
            sources.locked().push(source.clone());
            store(posted, source)
        }
    });
    mcp.with_channels(access);

    let (ok, text) = call(&client, "post_channel", post("#ops", "@dev-b @human check the build")).await;
    assert!(ok && text.contains("that dev-a created (8 turns left); it wakes dev-b and notifies human."), "{text}");
    let got = posted.locked().pop().unwrap();
    assert_eq!((got.channel.as_str(), &got.from, got.in_reply_to), ("ops", &user("dev-a"), None));
    assert_eq!((&got.creator, &got.owner), (&Some(user("dev-a")), &None));
    assert_eq!(got.turn_budget, ants_proto::DEFAULT_TURN_BUDGET, "a human's message spends no turn");
    assert!(text.contains(&format!("starting thread {}", got.thread_id)) && got.thread_id != current.thread_id);
    assert_eq!(*sources.locked(), [Some(current.thread_id.clone())]);

    let room = "ops".parse().unwrap();
    mcp.settle_origin(&current, Some(&room)).await.unwrap();
    let (ok, text) = call(&client, "post_channel", post("triage", "noted")).await;
    assert!(ok && text.contains("it wakes nobody"), "the messenger posts: {text}");
    assert_eq!(sources.locked()[1], None, "a post during channel work does not link to an inbox");
    let (ok, text) = call(&client, "post_channel", post("dev", "hi")).await;
    assert!(!ok && text.contains("dev-a is neither a member nor the messenger of #dev"), "{text}");
    let (ok, text) = call(&client, "post_channel", post("qa", "hi")).await;
    assert!(!ok && text.contains("there is no channel #qa"), "{text}");

    // Between ants, the post gets the turns the message leaves, and none left refuses it.
    drop(turn);
    let mut asked = Envelope::on_thread(current.thread_id.clone(), user("dev-b"), user("dev-a"), "post it");
    asked.turn_budget = 2;
    let turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "post_channel", post("ops", "@dev-b go")).await;
    assert!(ok && text.contains("(1 turns left)"), "{text}");
    drop(turn);
    asked.turn_budget = 0;
    let _turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "post_channel", post("ops", "@dev-b go")).await;
    assert!(!ok && text.contains("has no turns left"), "{text}");
    assert_eq!(posted.locked().len(), 2);
}

/// A post's @mention of a team mentions its lead, who has to be a member of the channel.
#[tokio::test]
async fn post_channel_mentions_a_team_s_lead_for_the_team() {
    let home = home("post-channel-team");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    for member in ["dev-a", "boss", "dev-b"] {
        ops.add_member(user(member));
    }
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.add_member(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops, dev], posted.clone()));
    mcp.with_teams(super::teams::memory_teams(super::teams::forest(), false).0);
    let post = |channel: &str, text: &str| serde_json::json!({"channel": channel, "text": text});

    let (ok, text) = call(&client, "post_channel", post("ops", "@infra @boss disk full")).await;
    assert!(ok && text.contains("it wakes boss."), "{text}");
    let got = posted.locked().pop().unwrap();
    assert_eq!(got.mentions, [user("boss")], "the lead once");
    let teams: Vec<_> = got.teams.iter().map(|t| (t.team.as_str(), t.lead.as_str())).collect();
    assert_eq!(teams, [("infra", "boss")]);

    let (ok, text) = call(&client, "post_channel", post("ops", "@orphans anyone?")).await;
    assert!(!ok && text.starts_with("nothing posted: team orphans has no lead, nor has any team above it"), "{text}");
    let (ok, text) = call(&client, "post_channel", post("dev", "@eng help")).await;
    let refused = "nothing posted: @eng mentions the lead of team eng, boss, who is no member of #dev";
    assert!(!ok && text.starts_with(refused), "{text}");
    assert!(posted.locked().is_empty());
}

#[tokio::test]
async fn post_channel_on_thread_replies_on_the_thread_of_the_post_being_handled() {
    let home = home("post-on-thread");
    let (mcp, _sent) = start(&home).await;
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(user("dev-a"));
    ops.add_member(user("dev-b"));
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.add_member(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops, dev], posted.clone()));
    // Spends as the daemon's replies do, from the turns the post answered leaves after those stored on its thread.
    let stored = Arc::new(StdMutex::new(Vec::<(ChannelPost, u64)>::new()));
    mcp.with_thread_posts(Arc::new({
        let stored = stored.clone();
        move |answered: ChannelPost, seq, post: ChannelPost| {
            let later: Vec<_> = stored.locked().iter().map(|(post, _)| post.clone()).collect();
            let left = answered.turns_left(&later, &ants_proto::Humans::default());
            let kept = post.spend(left).ok();
            if let Some(post) = &kept {
                stored.locked().push((post.clone(), seq));
            }
            Box::pin(async move { Ok(kept) })
        }
    }));
    let humans = ants_proto::Humans::default();
    let root = ChannelPost::new("ops".parse().unwrap(), user("dev-a"), "@dev-b review this")
        .with_creator(Some(user("dev-a")))
        .with_allow(Some(vec!["read:*".to_owned()]));
    let mut review = root.reply(user("dev-b"), "@dev-a looks good", &[], &humans).unwrap();
    review.turn_budget = 2;
    let envelope = Envelope::on_thread(review.thread_id.clone(), user("dev-b"), user("dev-a"), "looks good");
    let client = {
        let (url, token) = http_server(&mcp, &review.thread_id);
        http_client(&url, &token).await
    };
    let on_thread = |channel: &str| serde_json::json!({"channel": channel, "text": "@dev-b thanks", "on_thread": true});

    // Outside a channel post's turn, on_thread is refused.
    let turn = mcp.begin(&envelope);
    let (ok, text) = call(&client, "post_channel", on_thread("ops")).await;
    assert!(!ok && text.contains("on_thread posts only while the message being handled is a post"), "{text}");
    drop(turn);

    let _turn = mcp.begin(&envelope).answering(Some((review.clone(), 41)));
    let (ok, text) = call(&client, "post_channel", on_thread("dev")).await;
    assert!(!ok && text.contains("a post on a thread of #dev"), "another channel: {text}");
    let (ok, text) = call(&client, "post_channel", on_thread("#ops")).await;
    let thread = &review.thread_id;
    assert!(ok && text == format!("Posted to #ops on thread {thread} (1 turns left); it wakes dev-b."), "{text}");
    let (got, seq) = stored.locked()[0].clone();
    assert_eq!((&got.thread_id, got.in_reply_to, got.turn_budget, seq), (thread, Some(review.id), 1, 41));
    assert_eq!((&got.creator, &got.owner, &got.allow), (&Some(user("dev-a")), &None, &root.allow));
    assert!(posted.locked().is_empty(), "no new thread is started");

    let (ok, text) = call(&client, "post_channel", on_thread("ops")).await;
    assert!(ok && text.contains("(0 turns left)"), "{text}");
    let (ok, text) = call(&client, "post_channel", on_thread("ops")).await;
    assert!(!ok && text.contains("has no turns left") && text.contains("escalate"), "{text}");
    assert_eq!(stored.locked().len(), 2);
}
