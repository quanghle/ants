//! A provider's brief and a channel's description open the sessions of a thread.

use super::*;

/// A prompt for thread 1 carrying the briefing of a fake provider answering `answer`.
async fn briefed(answer: Answer, text: &str) -> (Prompt, Arc<Fake>) {
    let (provider, fake) = crate::provider::brief::fake::connected(answer, true).await;
    let caller = Caller { ant: "dev-a".parse().unwrap(), identity: "id-a".to_owned(), machine: "m1".to_owned() };
    let origin = Origin { by: Starter::Human { user: ants_proto::UserId::human() }, channel: None };
    let brief = Briefing::new(provider, Ok(caller), thread(1), origin);
    (Prompt { brief, ..with_history(text) }, fake)
}

fn reasons(fake: &Fake) -> Vec<BriefReason> {
    fake.asked.locked().iter().map(|request| request.reason).collect()
}

#[tokio::test]
async fn the_brief_follows_the_instructions_when_a_session_starts_or_is_loaded_or_rebuilt() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let launch = launch.with_instructions("[ants memory]\nUse notes.".to_owned());
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    let brief = Answer::Text("Working on the parser.".to_owned());
    let (prompt, fake) = briefed(brief.clone(), "a").await;
    let instructions = "[ants memory]\nUse notes.\n\n[ants brief from fake]\nWorking on the parser.";
    assert_eq!(pool.run(&thread(1), prompt).await.unwrap().text, format!("s1: {instructions}\n\n[ants message]\na"));
    let (prompt, _) = briefed(brief.clone(), "b").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert_eq!(reply, "s1: [ants memory]\nUse notes.\n\n[ants message]\nb", "only the first prompt of a session");
    pool.evict_idle(Instant::now() + IDLE * 2);
    let (prompt, loaded) = briefed(brief.clone(), "c").await;
    assert_eq!(pool.run(&thread(1), prompt).await.unwrap().text, format!("s1: {instructions}\n\n[ants message]\nc"));
    assert_eq!((reasons(&fake), reasons(&loaded)), (vec![BriefReason::Started], vec![BriefReason::Loaded]));
    pool.shutdown().await;

    log.locked().lose_sessions = true;
    let pool = Pool::new(launch, store, 4, IDLE);
    let (prompt, rebuilt) = briefed(brief, "d").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert!(reply.starts_with(&format!("s2: {instructions}\n\n[ants] The earlier session")), "{reply}");
    assert!(reply.ends_with("HISTORY 65536\n\n[ants message]\nd"), "{reply}");
    assert_eq!(reasons(&rebuilt), [BriefReason::Rebuilt]);
}

#[tokio::test]
async fn a_session_rebuilt_after_a_context_overflow_is_briefed_again() {
    let (pool, _, _) = fake_pool(4, Sessions::Loadable);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    let (prompt, fake) = briefed(Answer::Text("Brief.".to_owned()), "overflow").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    let brief = "[ants brief from fake]\nBrief.";
    assert!(reply.starts_with(&format!("s2: {brief}\n\n[ants] The session of this thread outgrew")), "{reply}");
    assert_eq!(reasons(&fake), [BriefReason::Rebuilt], "the first session was not opened by this prompt");
}

#[tokio::test]
async fn a_channel_s_description_opens_each_session_of_its_thread_after_the_brief() {
    let (pool, _, _) = fake_pool(4, Sessions::Loadable);
    let described =
        |text: &str, description: &str| Prompt { channel: Some(description.to_owned()), ..with_history(text) };
    assert_eq!(pool.run(&thread(1), described("a", "Ops.")).await.unwrap().text, "s1: Ops.\n\n[ants message]\na");
    let reply = pool.run(&thread(1), described("b", "Ops, changed.")).await.unwrap().text;
    assert_eq!(reply, "s1: b", "a running session keeps the description it started with");
    pool.evict_idle(Instant::now() + IDLE * 2);
    let reply = pool.run(&thread(1), described("c", "Ops, changed.")).await.unwrap().text;
    assert_eq!(reply, "s1: Ops, changed.\n\n[ants message]\nc", "a loaded session takes the description of now");
    let (prompt, _) = briefed(Answer::Text("Brief.".to_owned()), "overflow").await;
    let reply = pool.run(&thread(1), Prompt { channel: Some("Ops.".to_owned()), ..prompt }).await.unwrap().text;
    let opening = "[ants brief from fake]\nBrief.\n\nOps.";
    assert!(reply.starts_with(&format!("s2: {opening}\n\n[ants] The session of this thread outgrew")), "{reply}");
}

#[tokio::test(start_paused = true)]
async fn a_brief_not_sent_in_time_leaves_a_line_and_the_prompt_goes_on() {
    let (pool, _, _) = fake_pool(1, Sessions::Loadable);
    let (prompt, _) = briefed(Answer::Never, "a").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert_eq!(reply, "s1: [ants] The fake brief is left out: no answer within 5s.\n\n[ants message]\na");
}
