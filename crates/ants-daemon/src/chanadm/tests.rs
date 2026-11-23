//! Tests of the channel manager: only a human's assistant describes a channel for it.

use std::time::Duration;

use ants_proto::{AssistantGrant, AssistantRecord, ChannelId, ChannelOp, DEVICES_BUCKET, ThreadId};
use ants_test_support::NatsServer as Server;
use tokio::time::timeout;

use super::*;
use crate::people::humans::TestDevice;

const WAIT: Duration = Duration::from_secs(10);

fn p(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A human's assistant sets and clears a channel's description for it, announced as the assistant's; an ant the
/// human's grant does not name, a claim for a human with no grant, the channel's owner and an assistant asking for
/// more than the description are refused.
#[tokio::test]
async fn only_a_human_s_assistant_describes_a_channel_for_it() {
    let Some(server) = Server::start() else { return };
    let (human, alice, pa, pb, lead) = (UserId::human(), p("alice"), p("pa"), p("pb"), p("lead"));
    let ops: ChannelId = "ops".parse().unwrap();
    let (h_client, h_js) = bus::connect(&server.url, None, None, &human, "test").await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    let channels = bus::ensure_channels(&h_js).await.unwrap();
    let mut entry = ChannelEntry::new(ops.clone());
    entry.owner = Some(lead.clone());
    channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();

    let device = TestDevice::default();
    let humans = Registry::with(human.clone(), &["alice"]).verifying_grants(Some(device.account()));
    let mut grant = AssistantGrant::new(alice.clone(), Some(pa.clone()), Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    let devices = kv::Config { bucket: DEVICES_BUCKET.to_owned(), ..Default::default() };
    let devices = h_js.create_key_value(devices).await.unwrap();
    for ant in [&pa, &pb] {
        let record = AssistantRecord { jwt: device.assistant_jwt(ant, &alice) };
        devices.put(ants_proto::assistant_record_key(ant), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    let behalf = Records::new(h_js.clone(), None, Some(device.account()));
    let colony = crate::mcp::jetstream_colony(h_js.clone());
    let serving = tokio::spawn(serve(h_client.clone(), h_js.clone(), colony, humans, behalf));
    let mut room = h_client.subscribe(subject::chan_room(&ops)).await.unwrap();
    h_client.flush().await.unwrap();

    let ask = async |ant: &UserId, for_human: Option<&UserId>, ops_: Vec<ChannelOp>| {
        let (client, js) = bus::connect(&server.url, None, None, ant, "test").await.unwrap();
        let request = ChannelAdminRequest::new(ops.clone(), ant.clone(), ThreadId::generate(), ops_)
            .on_behalf(for_human.cloned());
        nats_admin(client, js).request(request).await.unwrap().expect("an answer").outcome
    };
    let refused = |outcome: ChannelAdminOutcome| match outcome {
        ChannelAdminOutcome::Refused { refusal } => refusal,
        applied => panic!("applied: {applied:?}"),
    };
    let describe = || vec![ChannelOp::SetDescription { description: "Incidents and deploys".to_owned() }];

    assert_eq!(refused(ask(&pb, Some(&alice), describe()).await), Refusal::NotAssistant(alice.clone()));
    assert_eq!(refused(ask(&pa, Some(&p("bob")), describe()).await), Refusal::NotAssistant(p("bob")));
    assert_eq!(refused(ask(&lead, None, describe()).await), Refusal::Description, "not even the owner");
    let add = vec![ChannelOp::AddMember { ant: pa.clone() }];
    assert_eq!(refused(ask(&pa, Some(&alice), add).await), Refusal::NotForHuman);

    let ChannelAdminOutcome::Applied { entry: applied, changed, .. } = ask(&pa, Some(&alice), describe()).await else {
        panic!("refused")
    };
    assert_eq!(changed, [r#"description set to "Incidents and deploys""#]);
    let stored: ChannelEntry = serde_json::from_slice(&channels.get("ops").await.unwrap().unwrap()).unwrap();
    assert_eq!((stored.description.as_deref(), &stored), (Some("Incidents and deploys"), &applied));
    let msg = timeout(WAIT, room.next()).await.unwrap().unwrap();
    let post: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(
        (post.from, post.text.as_str()),
        (
            UserId::channel_manager(),
            r#"pa, as the assistant of alice, changed #ops: description set to "Incidents and deploys"."#
        )
    );
    let ChannelAdminOutcome::Applied { entry, changed, .. } =
        ask(&pa, Some(&alice), vec![ChannelOp::ClearDescription]).await
    else {
        panic!("refused")
    };
    assert_eq!((entry.description, changed), (None, vec!["description cleared".to_owned()]));
    serving.abort();
}
