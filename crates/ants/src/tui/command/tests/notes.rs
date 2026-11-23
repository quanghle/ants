//! Tests of the note on a tool request: `n` rejecting the request then sending the note to its ant, and a rejection refused sending none.

use super::*;

#[test]
fn n_rejects_a_tool_request_then_sends_the_note_to_its_ant_on_its_thread() {
    let mut m = model();
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
    m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
    m.ui.bar.set_text("draft");
    assert_eq!(m.key(Key::Char('n'), at(0)), None);
    assert!(m.ui.bar.noting.is_none() && m.ui.bar.status.as_ref().unwrap().text.starts_with("the bar holds a draft"));
    m.ui.bar.set_text("");

    m.compact = true;
    assert_eq!(m.key(Key::Char('n'), at(0)), None, "the top request");
    assert!(m.ui.bar.focused && m.ui.bar.noting == Some(Note::Reject(Box::new(push.clone()))));
    assert!(!m.compact, "the note written in the full view");
    let note = "open a pull request instead";
    typed(&mut m, note);
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Decide(Box::new(push.clone()), Verdict::Reject)));
    assert!(m.ui.bar.is_empty() && m.ui.bar.noting.is_none() && !m.ui.bar.focused);
    assert!(m.asking().is_some_and(|a| a.storing && a.note.as_deref() == Some(note)));
    assert!(m.asking().and_then(Asking::status).unwrap().to_string().contains("then sending the note"));

    let sent = m.take(live::Event::Decided(Ok("rejected".into())), at(1));
    let ask = Job::Ask { to: push.ant.clone(), thread: Some(push.thread_id.clone()), text: note.into(), quote: None };
    assert_eq!(sent, [Action::Run(ask.clone(), note.into())], "the note goes once the rejection is stored");
    assert!(m.asking().is_none());
    m.outcome(Outcome { result: Err(anyhow::anyhow!("no route")), text: note.into(), quoting: None });
    assert_eq!((m.ui.bar.text(), &m.ui.bar.noting), (note.to_owned(), &Some(Note::Reply(Box::new(push)))));
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(ask, note.into())), "sent again as a reply");
}

#[test]
fn a_refused_rejection_sends_no_note_and_puts_it_back_in_the_bar() {
    let mut m = model();
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
    m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
    m.key(Key::Char('n'), at(0));
    let note = "open a pull request instead";
    typed(&mut m, note);
    assert!(matches!(m.key(Key::Enter, at(0)), Some(Action::Decide(_, Verdict::Reject))));
    let refused = anyhow::anyhow!("request dev-a/{} was already answered by bob: allow-once", push.id);
    assert_eq!(m.take(live::Event::Decided(Err(refused)), at(1)), [], "no note sent");
    assert_eq!((m.ui.bar.text(), &m.ui.bar.noting), (note.to_owned(), &Some(Note::Reply(Box::new(push.clone())))));
    let status = m.ui.bar.status.as_ref().unwrap();
    assert!(status.text.contains("by bob") && status.text.contains("the note was not sent"), "{}", status.text);
    assert!(m.ui.bar.focused && m.asking().is_none());
    let ask = Job::Ask { to: push.ant.clone(), thread: Some(push.thread_id), text: note.into(), quote: None };
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(ask, note.into())), "Enter sends it as a reply");
    assert!(m.ui.bar.is_empty() && m.ui.bar.noting.is_none());
}
