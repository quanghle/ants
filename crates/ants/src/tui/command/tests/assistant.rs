//! Tests of `/assistant`: its words read as `ants assistant enable|show|disable|set|clear|allow` reads them into a
//! job, malformed ones and the subcommands left to the CLI refused on the status line, `disable` without `--retire`
//! asking y or n first, and the jobs doing what `ants assistant` does, what it prints on the status line or in the
//! pager.

use ants_proto::Role;

use super::*;
use crate::cmd::assistant::AssistantCommand;
use crate::connection::Nats;
use crate::tui::assistant::Disabling;
use crate::tui::exec::{Ctx, Exec};
use crate::tui::model::Prompt;

/// What submitting `text` in the bar does.
fn submitted(m: &mut Model, text: &str) -> Option<Action> {
    m.ui.bar.set_text(text);
    m.submit(text.to_owned(), at(0))
}

/// The job of `ants assistant` that `action` runs.
fn command(action: Option<Action>) -> AssistantCommand {
    match action {
        Some(Action::Run(Job::Assistant(command), _)) => command,
        other => panic!("not an /assistant job: {other:?}"),
    }
}

#[test]
fn assistant_runs_a_job_of_ants_assistant_and_refuses_malformed_words_on_the_status_line() {
    let mut m = model();
    let AssistantCommand::Enable(args) = command(submitted(&mut m, "/assistant enable --name eyes --harness claude"))
    else {
        panic!("enable")
    };
    assert_eq!((args.name, args.harness.as_deref(), args.on), (Some(user("eyes")), Some("claude"), None));
    assert_eq!(m.ui.bar.status, Some(Status { text: "running…".into(), tone: Tone::Busy }));
    assert_eq!(command(submitted(&mut m, "/assistant set @dev-a")), AssistantCommand::Set { ant: user("dev-a") });
    assert_eq!(command(submitted(&mut m, "/assistant show")), AssistantCommand::Show { human: None, json: false });
    let allow = command(submitted(&mut m, "/assistant allow --remove 'dev-a/execute:cargo test*'"));
    let rules = vec!["dev-a/execute:cargo test*".to_owned()];
    assert_eq!(allow, AssistantCommand::Allow { rules, remove: true, clear: false, json: false });
    let AssistantCommand::Disable(args) = command(submitted(&mut m, "/assistant disable --retire")) else {
        panic!("disable --retire runs at once")
    };
    assert!(args.retire);

    for (typed, told) in [
        ("/assistant set", "the following required arguments were not provided: <ANT>"),
        ("/assistant allow --remove", "the following required arguments were not provided: <RULE>"),
        ("/assistant clear now", "unexpected argument 'now' found"),
    ] {
        assert_eq!(submitted(&mut m, typed), None, "{typed}");
        let status = m.ui.bar.status.take().unwrap();
        assert!(status.tone == Tone::Error && status.text.starts_with(told), "{typed}: {status:?}");
        assert_eq!(m.ui.bar.text(), typed, "kept to correct");
    }
    for typed in ["/assistant", "/assistant seal on", "/assistant key dev-a", "/assistant confirm"] {
        assert_eq!(submitted(&mut m, typed), None, "{typed}");
        let status = m.ui.bar.status.take().unwrap();
        assert!(status.text.starts_with("usage: /assistant enable"), "{typed}: {status:?}");
    }
}

#[test]
fn assistant_disable_asks_y_to_retire_the_dedicated_assistant_too_n_to_withdraw_it_alone() {
    let mut m = model();
    for (key, retire) in [('y', true), ('n', false)] {
        assert_eq!(submitted(&mut m, "/assistant disable --name pa"), None, "nothing runs before the answer");
        let Some(Prompt::Disable(Disabling(args))) = &m.prompt else { panic!("asks: {:?}", m.prompt) };
        assert_eq!((args.name.clone(), args.retire), (Some(user("pa")), false));
        assert!(m.prompt_line().unwrap().to_string().contains("y withdraws the assistant and retires"));
        let AssistantCommand::Disable(args) = command(m.key(Key::Char(key), at(0))) else { panic!("disable") };
        assert_eq!((args.name, args.retire), (Some(user("pa")), retire), "{key}");
        assert!(m.prompt.is_none());
        assert_eq!(m.ui.bar.status.take().map(|s| s.tone), Some(Tone::Busy));
    }
    submitted(&mut m, "/assistant disable");
    assert_eq!(m.key(Key::Esc, at(0)), None);
    assert!(m.prompt.is_none());
    assert_eq!(m.ui.bar.status, Some(Status { text: "the assistant stays".into(), tone: Tone::Done }));
}

/// The jobs `/assistant` runs do what `ants assistant` does for the human of the chat, its lines on the status
/// line, all of them in the pager past two, its refusal as the error.
#[tokio::test]
async fn the_assistant_jobs_show_allow_clear_and_refuse_as_ants_assistant_does() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, pa) = (user("alice"), user("pa"));
    let creds = server.creds_as(&alice, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None, None).unwrap();
    let (client, js) = nats.connect("test").await.unwrap();
    crate::people::assistant::designate(&js, &creds, &alice, &pa).await.unwrap();
    let ctx = Ctx {
        client,
        js,
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
    let mut said = async |text: &str| {
        let Some(Action::Run(job, text)) = submitted(&mut m, text) else { panic!("{text} runs a job") };
        exec.run(job, text);
        let action = m.outcome(exec.next().await);
        let status = m.ui.bar.status.take().unwrap();
        (status.tone, status.text, action)
    };

    let (tone, line, _) = said("/assistant show").await;
    assert!(tone == Tone::Done && line.starts_with("alice has assistant pa, granted "), "{line}");

    let (tone, line, paged) = said("/assistant allow 'dev-a/execute:cargo test*' '*/read:/work/*'").await;
    assert_eq!(tone, Tone::Done);
    let printed =
        "decisions of pa for alice take effect unconfirmed on:\n  dev-a/execute:cargo test*\n  */read:/work/*";
    assert_eq!(paged, Some(Action::Page(printed.to_owned())));
    assert_eq!(
        line,
        "decisions of pa for alice take effect unconfirmed on: · the pager shows all ants assistant printed"
    );

    let (tone, line, _) = said("/assistant clear").await;
    assert_eq!((tone, line.as_str()), (Tone::Done, "pa is no longer the assistant of alice"));

    let config = server.dir().join("none.toml");
    let (tone, line, _) = said(&format!("/assistant disable --retire --config {}", config.display())).await;
    assert_eq!(tone, Tone::Error);
    assert!(line.starts_with("alice-assistant is not a dedicated assistant of alice on this machine"), "{line}");
}
