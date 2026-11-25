//! Tests of `/channel`: its words read as `ants channel create|set|add|remove|delete` reads them into a job from any
//! view, malformed ones refused on the status line, and the job creating, changing and deleting the channel as `ants
//! channel` does, telling the channel to read back for the nav.

use ants_proto::{ChannelId, Role};

use super::*;
use crate::channel::{self, Change, Manage};
use crate::connection::Nats;
use crate::tui::exec::{Ctx, Exec};

#[test]
fn channel_runs_a_job_of_ants_channel_and_refuses_malformed_words_on_the_status_line() {
    let mut m = model();
    m.pages.push(Page::Thread(thread::Page::new(ants_proto::ThreadId::generate())));
    let text = "/channel create ops --member alice,@coder --owner dev-a";
    let ops: ChannelId = "ops".parse().unwrap();
    let change = Change {
        add: vec![user("alice"), user("coder")],
        messenger: Some(None),
        owner: Some(Some(user("dev-a"))),
        description: Some(None),
        ..Change::default()
    };
    let job = Job::ManageChannel { channel: ops, manage: Manage::Create(change) };
    m.ui.bar.set_text(text);
    assert_eq!(m.submit(text.to_owned(), at(0)), Some(Action::Run(job, text.into())));
    assert_eq!(m.ui.bar.status, Some(Status { text: "running…".into(), tone: Tone::Busy }));
    assert!(m.ui.bar.is_empty());

    for (typed, told) in [
        ("/channel create", "the following required arguments were not provided: <CHANNEL>"),
        ("/channel set ops", "nothing to change: give --member"),
        ("/channel delete ops now", "unexpected argument 'now' found"),
    ] {
        m.ui.bar.set_text(typed);
        assert_eq!(m.submit(typed.to_owned(), at(0)), None, "{typed}");
        let status = m.ui.bar.status.take().unwrap();
        assert!(status.tone == Tone::Error && status.text.starts_with(told), "{typed}: {status:?}");
        assert_eq!(m.ui.bar.text(), typed, "kept to correct");
    }
    m.ui.bar.set_text("/channel list");
    assert_eq!(m.submit("/channel list".to_owned(), at(0)), None);
    let status = m.ui.bar.status.take().unwrap();
    assert!(status.text.starts_with("usage: /channel create NAME"), "{status:?}");
    assert!(m.ui.bar.refused.is_some(), "the palette shows the usage refused");
}

/// The jobs `/channel` runs create, change and delete the channel as `ants channel` does, the status line telling
/// what it prints and warns of and the channel named to read back.
#[tokio::test]
async fn the_channel_jobs_create_change_and_delete_as_ants_channel_does() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let alice = user("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None, None).unwrap();
    let (client, js) = nats.connect("test").await.unwrap();
    let ctx = Ctx {
        client,
        js: js.clone(),
        me: nats.me.clone(),
        domain: None,
        meta: Err("unused".into()),
        sealing: crate::people::sealed::Sealing::of(&nats),
        account: None,
        keyring: None,
        admin: Err("unused".into()),
        reach: nats.reach(),
    };
    let mut exec = Exec::new(ctx);
    let mut m = model();
    let ops: ChannelId = "ops".parse().unwrap();
    let mut said = async |text: &str| {
        m.ui.bar.set_text(text);
        let Some(Action::Run(job, text)) = m.submit(text.to_owned(), at(0)) else { panic!("{text} runs a job") };
        exec.run(job, text);
        let outcome = exec.next().await;
        let named = outcome.result.as_ref().ok().and_then(|done| done.channel.clone());
        m.outcome(outcome);
        let status = m.ui.bar.status.take().unwrap();
        (status.tone, status.text, named)
    };

    let (tone, line, named) = said("/channel create ops --member bob --description 'Deploys'").await;
    assert_eq!((tone, named.as_ref()), (Tone::Warn, Some(&ops)), "{line}");
    let created = "created #ops: members bob; messenger system-messenger (default)\ndescription: Deploys · ";
    assert!(
        line.starts_with(created) && line.contains("bob is not in the colony"),
        "warned as ants channel warns: {line}"
    );
    let entry = channel::get(&channel::open(&js).await.unwrap(), &ops).await.unwrap().unwrap();
    assert_eq!((entry.members, entry.description.as_deref()), (vec![user("bob")], Some("Deploys")));

    let (tone, line, _) = said("/channel create #ops").await;
    assert_eq!(tone, Tone::Error);
    assert!(line.contains("exists"), "{line}");

    let (tone, line, _) = said("/channel add ops @bob alice").await;
    assert_eq!(tone, Tone::Warn, "bob is a member already: {line}");
    assert!(line.starts_with("#ops: members bob,alice;") && line.contains("bob is already a member of #ops"), "{line}");

    let (_, line, named) = said("/channel delete ops").await;
    assert_eq!((line.as_str(), named), ("deleted #ops; the posts to #ops stay in the stream", Some(ops.clone())));
    assert!(channel::get(&channel::open(&js).await.unwrap(), &ops).await.unwrap().is_none());
}
