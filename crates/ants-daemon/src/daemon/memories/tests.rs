//! Tests of the memories read any human asks the daemon serving an ant for over nats-server: its core memories and
//! its provider's view answered on the ant's reply subject, the request refused where the config refuses remote admin
//! or names no human, and a forged one left unanswered.

use std::sync::Arc;

use ants_proto::provider::{AntRef, MemoryEntry, MemoryFields};
use ants_proto::{MemoriesShown, ProviderMemory};
use ants_test_support::NatsServer as Server;
use tokio::time::timeout;

use super::*;
use crate::admin::memories::tests::{Fake, MEMORIES_AND_EXPORT};

const WAIT: Duration = Duration::from_secs(10);

/// Ask on `client` for the memories of dev-a as `by`, publishing on `on` (the request's own subject when none), until
/// an answer to it comes on dev-a's reply subject, or none within `within`.
async fn ask(client: &async_nats::Client, by: &str, on: Option<&str>, within: Duration) -> Option<MemoriesOutcome> {
    let request = MemoriesRequest::new("dev-a".parse().unwrap(), by.parse().unwrap());
    let mut answers = client.subscribe(request.reply_subject()).await.unwrap();
    let subject = on.map_or_else(|| request.subject(), str::to_owned);
    let asked = async {
        loop {
            client.publish(subject.clone(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
            while let Ok(Some(msg)) = timeout(Duration::from_millis(200), answers.next()).await {
                let answer: MemoriesReply = serde_json::from_slice(&msg.payload).unwrap();
                assert_eq!(answer.check_subject(&msg.subject), Ok(()));
                if answer.request_id == request.id {
                    return answer.outcome;
                }
            }
        }
    };
    timeout(within, asked).await.ok()
}

fn core() -> CoreMemories {
    CoreMemories { whoami: Some("a tester".into()), memory: None, tasks: Some("triage".into()) }
}

#[tokio::test]
async fn any_human_reads_the_core_and_provider_memories_of_the_ant_served() {
    let Some(server) = Server::start() else { return };
    let client = async_nats::connect(&server.url).await.unwrap();
    let mut fake = Fake::new("brain", MEMORIES_AND_EXPORT);
    // A typed memory, its fields shown as an admin's read shows them.
    let fields = MemoryFields { kind: Some("lesson".into()), title: Some("Style".into()), ..MemoryFields::default() };
    fake.memories = vec![MemoryEntry { fields: fields.clone(), ..MemoryEntry::new("style", "terse") }];
    let (fake, connected) = fake.connect().await;
    let (ant, core, humans) = ("dev-a".parse().unwrap(), core(), Registry::new("alice".parse().unwrap()));
    let shown = Shown {
        client: &client,
        ant: &ant,
        core: &core,
        remote: true,
        humans: &humans,
        provider: async || (Some(connected.clone()), Some("1f".to_owned())),
    };
    let asking = async {
        let read = ask(&client, "alice", None, WAIT).await.expect("an answer");
        let style = ProviderMemory { key: "style".into(), content: "terse".into(), fields, truncated: false };
        let MemoriesOutcome::Read(MemoriesShown { core: read_core, provider, skipped }) = read else {
            panic!("refused: {read:?}")
        };
        assert_eq!((read_core, skipped), (core.clone(), vec![]));
        let provider = provider.expect("the provider's view");
        assert_eq!((provider.provider.as_str(), provider.memories, provider.error), ("brain", vec![style], None));
        assert_eq!(fake.asked.lock().unwrap().clone(), [AntRef { ant: ant.clone(), identity: "1f".into() }]);

        let not_human = ask(&client, "dev-b", None, WAIT).await.expect("an answer");
        assert_eq!(not_human, MemoriesOutcome::Refused { error: "dev-b is no human of the colony".into() });
        assert_eq!(ask(&client, "alice", Some("ants.memories.dev-b"), Duration::from_secs(1)).await, None, "forged");
    };
    tokio::select! {
        () = shown.hear() => unreachable!("hearing never ends"),
        () = asking => {}
    }
}

#[tokio::test]
async fn a_config_refusing_remote_admin_refuses_the_read_and_no_provider_means_none_shown() {
    let Some(server) = Server::start() else { return };
    let client = async_nats::connect(&server.url).await.unwrap();
    let (ant, core, humans) = ("dev-a".parse().unwrap(), core(), Registry::new("alice".parse().unwrap()));
    let provided = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shown = |remote| Shown {
        client: &client,
        ant: &ant,
        core: &core,
        remote,
        humans: &humans,
        provider: {
            let provided = provided.clone();
            async move || {
                provided.store(true, std::sync::atomic::Ordering::SeqCst);
                (None, None)
            }
        },
    };
    let refusing = shown(false);
    let asking = async {
        let refused = ask(&client, "alice", None, WAIT).await.expect("an answer");
        assert_eq!(refused, MemoriesOutcome::Refused { error: NOT_REMOTE.into() });
    };
    tokio::select! {
        () = refusing.hear() => unreachable!("hearing never ends"),
        () = asking => {}
    }
    assert!(!provided.load(std::sync::atomic::Ordering::SeqCst), "a refused read asks no provider");

    let serving = shown(true);
    let asking = async {
        let read = ask(&client, "alice", None, WAIT).await.expect("an answer");
        assert_eq!(read, MemoriesOutcome::Read(MemoriesShown { core: core.clone(), ..MemoriesShown::default() }));
    };
    tokio::select! {
        () = serving.hear() => unreachable!("hearing never ends"),
        () = asking => {}
    }
}
