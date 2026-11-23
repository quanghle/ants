//! A prompt's limits: past its time or tool calls it is cancelled, and its spend is reported.

use super::*;

#[tokio::test]
async fn a_prompt_past_its_time_limit_is_cancelled_and_the_session_goes_on() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let limits = PromptLimits { timeout: Some(Duration::from_millis(50)), max_tool_calls: None };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("stall") }).await.unwrap();
    let cut = "Prompt time limit reached (50ms)".to_owned();
    assert_eq!(reply, Reply { text: "partial".to_owned(), cut: Some(cut), rebuilt: false });
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    let log = log.locked();
    assert_eq!((log.starts, log.cancelled), (1, 1));
}

#[tokio::test]
async fn a_prompt_past_its_tool_call_limit_is_cancelled() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let limits = PromptLimits { timeout: None, max_tool_calls: Some(2) };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 2") }).await.unwrap();
    assert_eq!(reply.cut, None, "up to the limit is fine");
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 3 stall") }).await.unwrap();
    assert_eq!(reply.cut.as_deref(), Some("Tool call limit reached (2 tool calls in one prompt)"));
    assert_eq!(log.locked().cancelled, 1);
}

#[tokio::test]
async fn spend_is_reported_and_a_stop_cancels_the_prompt() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel();
    let prompt = Prompt { spend: Some(spend), stop: Some(stopped), ..Prompt::new("cost 0.5 stall") };
    let watch = async {
        assert_eq!(spent.recv().await, Some(Spend::Cost { session: SessionId::new("s1"), total: 0.5 }));
        stop.send("Enough".to_owned()).unwrap();
    };
    let t1 = thread(1);
    let (reply, ()) = tokio::join!(pool.run(&t1, prompt), watch);
    assert_eq!(reply.unwrap().cut.as_deref(), Some("Enough"));
    assert_eq!(log.locked().cancelled, 1);

    let (spend, mut spent) = mpsc::unbounded_channel();
    pool.run(&thread(1), Prompt { spend: Some(spend), ..Prompt::new("tokens 42") }).await.unwrap();
    let cost = Spend::Cost { session: SessionId::new("s1"), total: 0.5 };
    assert_eq!(spent.try_recv(), Ok(cost), "the last cost comes again as the next prompt starts");
    assert_eq!(spent.try_recv(), Ok(Spend::Tokens(42)));
}
