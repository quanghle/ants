//! Prompting: the prompt's text, its status, timeline and limits, the attachments it carries, and the notices of its
//! reply, such as the thread's branch.

use super::*;

#[tokio::test]
async fn replies_are_redacted() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.redactor = Redactor::default().with_env([("DEPLOY_TOKEN", "d3pl0y-s3cret")]);
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "use d3pl0y-s3cret or ghp_0123456789abcdefghijABCDEFGHIJ").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "use [redacted] or [redacted]".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_review_nothing_holds_to_read_only_tools_is_declined_unprompted() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let agy = crate::harness::Harnesses::builtin().get("antigravity").unwrap();
    let pool = Pool::new(launch.with_harness(agy), SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let review = Envelope::on_thread(ThreadId::generate(), dev_b, bus.dev.clone(), "review this")
        .with_allow(Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec()));
    bus::publish_json(&bus.js, review.subject(), &review, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let text = next_envelope(&mut to_dev_b).await.text;
        assert!(
            text.starts_with("[ants] dev-a declines the review: harness antigravity runs its tools unasked"),
            "{text}"
        );
        assert_eq!(log.locked().starts, 0, "not prompted");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_human_s_summary_request_is_prompted_as_one_and_the_reply_carries_it_with_its_tokens() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let (asked, summarised) = (ThreadId::generate(), ThreadId::generate());
    let request = ants_proto::SummaryRequest::thread(summarised.clone(), 42);
    for (from, thread) in [(UserId::human(), asked.clone()), (dev_b.clone(), ThreadId::generate())] {
        let mut envelope = Envelope::on_thread(thread, from, bus.dev.clone(), "tokens 7");
        envelope.summary = Some(request.clone());
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.thread_id, asked);
        let spent = ants_proto::SummaryRequest { tokens: Some(7), ..request.clone() };
        assert_eq!(reply.summary, Some(spent), "the reply carries the request and its tokens");
        assert_eq!(next_envelope(&mut to_dev_b).await.summary, None, "only a human asks for a summary");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    // The two threads are prompted in either order.
    let mut prompts = log.locked().prompts.clone();
    prompts.sort_by_key(|p| p == "tokens 7");
    let tldr = format!("[ants] A human asks for a TL;DR of thread {summarised} from the entries below");
    assert!(prompts[0].starts_with(&tldr), "{prompts:?}");
    assert!(prompts[0].contains("use no tools and message no one") && prompts[0].ends_with("\n\ntokens 7"));
    assert_eq!(prompts[1], "tokens 7", "an ant's request is an ordinary message");
}

#[tokio::test]
async fn a_quoted_reply_is_prompted_after_its_quote() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let quote = ants_proto::Quote::new("dev-b".parse().unwrap(), "the build fails on main");
    let envelope = Envelope::on_thread(ThreadId::generate(), UserId::human(), bus.dev.clone(), "why?")
        .quoting(Uuid::now_v7(), quote);
    bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_envelope(&mut to_human).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let prompts = log.locked().prompts.clone();
    assert_eq!(prompts, ["[ants] In reply to dev-b: \"the build fails on main\"\nwhy?"]);
}

#[tokio::test]
async fn progress_streams_to_the_status_subject() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "tools 1").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "tools 1".to_owned()));
        let mut updates = Vec::new();
        while !matches!(updates.last(), Some(StatusUpdate::State { state: State::Idle })) {
            let msg = timeout(WAIT, statuses.next()).await.expect("no status").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(event.check_subject(&msg.subject), Ok(()));
            // A flush may fall between the chunks of the reply.
            match (updates.last_mut(), event.update) {
                (Some(StatusUpdate::Message { text }), StatusUpdate::Message { text: more }) => text.push_str(&more),
                (_, update) => updates.push(update),
            }
        }
        let tool = StatusUpdate::Tool {
            id: "call-0".to_owned(),
            title: Some("tool".to_owned()),
            tool_kind: Some("other".to_owned()),
            status: Some("pending".to_owned()),
            paths: None,
        };
        assert_eq!(
            updates,
            [
                StatusUpdate::State { state: State::Working },
                tool,
                StatusUpdate::Message { text: "s1: tools 1".to_owned() },
                StatusUpdate::State { state: State::Idle },
            ]
        );
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn tool_calls_are_stored_on_the_thread_timeline() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (sent, _) = bus.send(&t1, "tools 2").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "tools 2".to_owned()));
        let subject = subject::timeline(&t1, &bus.dev);
        let msg = timeout(WAIT, async {
            loop {
                if let Ok(msg) = bus.stream.direct_get_last_for_subject(&subject).await {
                    break msg;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no timeline record");
        let record: ants_proto::Timeline = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!(record.check_subject(&msg.subject), Ok(()));
        assert_eq!(record.message, sent.id);
        let calls: Vec<_> = record
            .entries
            .iter()
            .map(|e| match e {
                ants_proto::TimelineEntry::Tool(call) => {
                    (call.id.as_str(), call.title.as_str(), call.status.as_deref())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(calls, [("call-0", "tool", Some("pending")), ("call-1", "tool", Some("pending"))]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_prompt_too_large_even_for_a_new_session_goes_to_the_owner_with_the_thread_history_tried() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut hello = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hello");
    let to = hello.clone();
    let notes = attach(&bus, |id| to.attachment_handle(id), "notes.txt", "text/plain", b"notes").await;
    hello.attachments = vec![notes.clone()];
    bus::publish_json(&bus.js, hello.subject(), &hello, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await.0, t1);
        bus.send(&t1, "overflows").await;
        let notice = next_envelope(&mut replies).await;
        let why = "The prompt does not fit the context window of test's model, even in a new session: ";
        assert!(notice.text.starts_with(why), "{}", notice.text);
        assert!(notice.text.contains("Prompt is too long"), "{}", notice.text);
        assert!(notice.text.ends_with("; needs a decision. Last message from human:\noverflows"), "{}", notice.text);
        assert_eq!(notice.thread_id, t1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let line = format!(
        "[ants] Attached: \"notes.txt\" (text/plain, 5 bytes), handle human.inbox.dev-a.{}; call read_attachment with \
         the handle to read it.",
        notes.id
    );
    // The fake harness echoes its prompt, the attachment's line included.
    let history = format!(
        "[ants] The thread's history, read back from ants, oldest first:\n\nhuman to dev-a:\nhello\n{line}\n\n\
         dev-a to human:\ns1: hello\n\n{line}\n\n[ants] End of the thread's history.\n\noverflows"
    );
    let prompts = log.locked().prompts.clone();
    assert_eq!(prompts.len(), 3, "{prompts:?}");
    assert!(prompts[2].ends_with(&history), "{}", prompts[2]);
    assert_eq!(bus.unacked().await, 0);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "no dead letter");
}

#[tokio::test]
async fn a_prompt_past_its_limit_is_cancelled_and_the_human_told() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.limits.timeout = Some(Duration::from_millis(100));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send(&t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        let text = "partial\n\n[ants] Prompt time limit reached (100ms); the prompt was cancelled.";
        assert_eq!(notice.text, text);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "after".to_owned()), "and nothing else");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().cancelled, 1);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_reply_names_the_thread_branch_holding_new_commits() {
    let Some(repo) = crate::test_support::repo("daemon-branch-ahead") else { return };
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let home = repo.parent().unwrap().join("home");
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let setup = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let worktree = setup.checkout(&t1).await.unwrap();
    crate::test_support::commit(&worktree, "work of t1");
    let (launch, _, _) = fake::launch();
    let worktrees = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 2, IDLE).with_worktrees(worktrees);
    bus.send(&t1, "one").await;
    bus.send(&t2, "two").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = HashMap::new();
        for _ in 0..2 {
            let reply = next_envelope(&mut replies).await;
            texts.insert(reply.thread_id, reply.text);
        }
        let named = format!("one\n\n[ants] Branch ants/dev-a/{t1} holds 1 commit not in the repository's HEAD.");
        assert!(texts[&t1].ends_with(&named), "{}", texts[&t1]);
        assert!(texts[&t2].ends_with("two"), "no commits on its branch: {}", texts[&t2]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

/// A fake gh in `dir`, logging its arguments to `dir/gh.log`: `pr create` prints a pull request's URL, which `pr list`
/// prints from then on.
#[cfg(unix)]
fn fake_gh(dir: &std::path::Path) -> PathBuf {
    let gh = dir.join("gh");
    let script = r#"#!/bin/sh
dir=$(dirname "$0")
printf '[%s]' "$@" >> "$dir/gh.log"
echo >> "$dir/gh.log"
case "$1 $2" in
"pr list") cat "$dir/open" 2>/dev/null || true ;;
"pr create") echo "Creating draft pull request"; echo https://github.test/o/r/pull/7 | tee "$dir/open" ;;
esac
"#;
    std::fs::write(&gh, script).unwrap();
    std::fs::set_permissions(&gh, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    gh
}

#[cfg(unix)]
#[tokio::test]
async fn a_thread_branch_with_new_commits_is_pushed_with_a_draft_pull_request() {
    use crate::test_support::{commit, run};
    let Some(repo) = crate::test_support::repo("daemon-publish") else { return };
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let dir = repo.parent().unwrap().to_owned();
    let remote = dir.join("remote.git");
    run(&dir, &["init", "--quiet", "--bare", remote.to_str().unwrap()]);
    run(&repo, &["remote", "add", "origin", remote.to_str().unwrap()]);
    bus.js.create_stream(crate::hub::threads_config()).await.unwrap();
    let titles = crate::records::direct_kv::DirectKv::new(&bus.js, None, ants_proto::THREADS_BUCKET).await.unwrap();
    bus.publisher = Some(crate::publish::Publisher::new(bus.dev.clone()).with_gh(fake_gh(&dir)).with_titles(titles));
    let home = dir.join("home");
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let mut title = ants_proto::ThreadTitle::new(t1.clone(), "Lunch plans", UserId::human()).unwrap();
    let sealed =
        ants_proto::Sealed { v: 1, device: ants_proto::DeviceId::generate(), keys: Vec::new(), body: title.title };
    (title.title, title.sealed) = (String::new(), Some(Box::new(sealed)));
    let key = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::title_key(&t1));
    bus.js.publish(key, serde_json::to_vec(&title).unwrap().into()).await.unwrap().await.unwrap();
    let setup = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let (w1, w2) = (setup.checkout(&t1).await.unwrap(), setup.checkout(&t2).await.unwrap());
    commit(&w1, "work of t1");
    commit(&w2, "work of t2");
    let (launch, _, _) = fake::launch();
    let allow = Allowlist::parse(&["execute:git push origin ants/dev-a/*"]).unwrap();
    let launch = launch.in_dir(repo.clone()).with_policy(PermissionPolicy::new(allow, None));
    let worktrees = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE).with_worktrees(worktrees);
    let branch = format!("ants/dev-a/{t1}");

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        bus.send(&t1, "  Fix the build\nthen test it").await;
        let pushed = format!("[ants] Branch {branch} was pushed to origin, with draft pull request ");
        let text = next_envelope(&mut replies).await.text;
        assert!(text.ends_with(&format!("{pushed}https://github.test/o/r/pull/7.")), "{text}");
        assert_eq!(run(&repo, &["ls-remote", "--heads", "origin"]).matches(&branch).count(), 1);
        let log = std::fs::read_to_string(dir.join("gh.log")).unwrap();
        assert!(log.starts_with(&format!("[pr][list][--head][{branch}][--state][open]")), "{log}");
        let titled = log.contains("[--title][Fix the build][--body]");
        assert!(log.contains("[--draft][--head]") && titled, "the opening line, as the title is sealed: {log}");
        assert!(log.contains(&format!("thread {t1}, which `ants thread show {t1}` shows")), "{log}");

        bus.send(&t1, "nothing new").await;
        let text = next_envelope(&mut replies).await.text;
        assert!(text.ends_with("not in the repository's HEAD."), "pushed as it is: {text}");
        commit(&w1, "more work of t1");
        bus.send(&t1, "more").await;
        let text = next_envelope(&mut replies).await.text;
        let updated = format!("[ants] Branch {branch} was pushed to origin, updating pull request ");
        assert!(text.ends_with(&format!("{updated}https://github.test/o/r/pull/7.")), "{text}");
        assert_eq!(std::fs::read_to_string(dir.join("gh.log")).unwrap().matches("[pr][create]").count(), 1);

        // Another ant's message narrows the thread's rules to what it carries.
        let dev_b: UserId = "dev-b".parse().unwrap();
        let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
        bus.client.flush().await.unwrap();
        let narrowed =
            Envelope::on_thread(t2.clone(), dev_b, bus.dev.clone(), "two").with_allow(Some(vec!["read".to_owned()]));
        bus::publish_json(&bus.js, narrowed.subject(), &narrowed, None).await.unwrap();
        let text = next_envelope(&mut to_dev_b).await.text;
        let rejected = format!(
            "[ants] Branch ants/dev-a/{t2} was not pushed: `git push origin ants/dev-a/{t2}` was rejected. An allow \
             rule such as `execute:git push origin ants/dev-a/*` lets it run unasked."
        );
        assert!(text.ends_with(&rejected), "outside the thread's narrowed rules: {text}");
        assert!(!run(&repo, &["ls-remote", "--heads", "origin"]).contains(t2.as_str()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_kept_branch_is_told_to_the_thread_owner_else_the_default_human() {
    let humans = Registry::new("alice".parse().unwrap());
    let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
    let branch = Branch { thread: thread.clone(), name: "ants/dev-a/t1".to_owned(), commits: 2, upstream: None };
    let notice = kept_branch_notice(&ant, &branch, None, &humans);
    assert_eq!((notice.to.as_str(), notice.owner.as_ref(), &notice.thread_id), ("alice", None, &thread));
    assert_eq!(
        notice.text,
        "[ants] The session of thread t1 expired and its git worktree was removed; branch ants/dev-a/t1 is kept, \
             holding 2 commits not in the repository's HEAD."
    );
    let notice = kept_branch_notice(&ant, &branch, Some(UserId::human()), &humans);
    assert_eq!((&notice.to, notice.owner.as_ref()), (&UserId::human(), Some(&UserId::human())));
}

#[tokio::test]
async fn the_next_prompt_mentions_late_replies_waiting_on_its_thread() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    // Two ask_ant calls on t1 that gave up, remembered across a restart.
    let asks = [1, 2].map(|_| Envelope::on_thread(t1.clone(), bus.dev.clone(), dev_b.clone(), "?"));
    let home = bus._server.dir().join("late");
    std::fs::create_dir_all(&home).unwrap();
    let stored = serde_json::json!({"asks": asks.iter().map(|a| (a.id, &a.to)).collect::<Vec<_>>(), "late": []});
    std::fs::write(home.join("mcp.json"), stored.to_string()).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    for ask in &asks {
        let reply = ask.reply(dev_b.clone(), "late", Role::Ant).unwrap();
        bus::publish_json(&bus.js, reply.subject(), &reply, None).await.unwrap();
    }
    bus.send(&t1, "a").await;
    bus.send(&t2, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut got = [next_reply(&mut replies).await, next_reply(&mut replies).await];
        got.sort_by_key(|(thread, _)| thread != &t1);
        let note = "[ants] 2 late replies to ask_ant are waiting on this thread; call check_inbox to read them.";
        assert_eq!(got, [(t1.clone(), format!("{note}\n\na")), (t2.clone(), "b".to_owned())]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered.len(), 2, "the late replies are no prompts");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_human_s_answer_to_the_ant_s_questions_is_prompted_once_with_an_ants_line_for_each() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "not yet".to_owned()], false).unwrap();
    let regions = ants_proto::Question::new("Which regions?", &["eu".to_owned(), "us".to_owned()], false).unwrap();
    let questions = ants_proto::Questions::new(vec![ship.headed("Ship"), regions.multi()]).unwrap();
    let asked = Envelope::asking(thread.clone(), bus.dev.clone(), UserId::human(), questions);
    bus::publish_json(&bus.js, asked.subject(), &asked, None).await.unwrap();
    timeout(WAIT, replies.next()).await.expect("the question").unwrap();
    let picks = vec![Pick::option(2), Pick::Options(vec![1, 2])];
    let picked = Envelope::answering(&asked, UserId::human(), picks).unwrap();
    bus::publish_json(&bus.js, picked.subject(), &picked, None).await.unwrap();
    // No such option: an ordinary message.
    let mut wrong = picked.clone();
    wrong.id = Uuid::now_v7();
    wrong.answer.as_mut().unwrap().picks[0] = Pick::option(3);
    bus::publish_json(&bus.js, wrong.subject(), &wrong, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let id = asked.id;
        let lines = format!(
            "[ants] human answers question 1 of 2 asked with ask_human as message {id} (\"Ship it?\") with option 2, \
             \"not yet\".\n[ants] human answers question 2 of 2 asked with ask_human as message {id} (\"Which \
             regions?\") with option 1, \"eu\" and option 2, \"us\"."
        );
        let text = "Ship: not yet\nQ2: eu, us";
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), format!("{lines}\n\n{text}")));
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), text.to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

/// Store `bytes` as a new attachment named `name` of type `mime`, under the handle `handle` gives its id.
async fn attach(
    bus: &Bus,
    handle: impl Fn(Uuid) -> AttachmentHandle,
    name: &str,
    mime: &str,
    bytes: &[u8],
) -> Attachment {
    let attachment = crate::files::reference(name, mime, bytes).unwrap();
    crate::files::put(&bus.js, &handle(attachment.id), &attachment, bytes).await.unwrap();
    attachment
}

#[tokio::test]
async fn attachments_reach_a_harness_taking_images_as_images_and_any_other_as_a_line() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, _) = fake::launch();
    log.locked().takes_images = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let line = |a: &Attachment, handle: &AttachmentHandle| {
        format!("[ants] Attached: {:?} ({}, {} bytes), handle {handle}", a.name, a.mime, a.size)
    };

    let mut message = Envelope::on_thread(ThreadId::generate(), UserId::human(), bus.dev.clone(), "look");
    let to = message.clone();
    let handle = |id| to.attachment_handle(id);
    let shot = attach(&bus, handle, "shot.png", "image/png", b"\x89PNG shot").await;
    let log_file = attach(&bus, handle, "build.log", "text/plain", b"error: E0308").await;
    // Stored as other bytes than its reference says: unavailable, with no chunk missing to wait for.
    let gone = crate::files::reference("gone.png", "image/png", b"as referenced").unwrap();
    let other = Attachment { id: gone.id, ..crate::files::reference("gone.png", "image/png", b"other").unwrap() };
    crate::files::put(&bus.js, &handle(gone.id), &other, b"other").await.unwrap();
    message.attachments = vec![shot.clone(), log_file.clone(), gone.clone()];
    bus::publish_json(&bus.js, message.subject(), &message, None).await.unwrap();

    let mut post = ChannelPost::new(ops.clone(), UserId::human(), "@dev-a see the chart");
    let on = post.clone();
    let chart = attach(&bus, |id| on.attachment_handle(id), "chart.webp", "image/webp", b"RIFF chart").await;
    post.attachments = vec![chart.clone()];

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_reply(&mut replies).await;
        bus.post(&post).await;
        next_post(&mut posts).await;
        // A harness started now takes no images.
        log.locked().takes_images = false;
        let mut plain = Envelope::on_thread(ThreadId::generate(), UserId::human(), bus.dev.clone(), "again");
        plain.attachments = vec![attach(&bus, |id| plain.attachment_handle(id), "shot.png", "image/png", b"x").await];
        bus::publish_json(&bus.js, plain.subject(), &plain, None).await.unwrap();
        next_reply(&mut replies).await;
        ctrl_c.add_permits(1);
        plain
    };
    let ((), plain) = tokio::join!(bus.serve(&pool, &ctrl_c), check);

    let log = log.locked();
    let [first, second, third] = &log.prompts[..] else { panic!("{:?}", log.prompts) };
    let shown = format!("{}; shown as an image after this text.", line(&shot, &handle(shot.id)));
    let named = format!("{}; call read_attachment with the handle to read it.", line(&log_file, &handle(log_file.id)));
    assert!(first.starts_with("look\n\n") && first.contains(&format!("{shown}\n{named}\n")), "{first}");
    let unavailable =
        format!("{}: unavailable now (attachment {} does not match", line(&gone, &handle(gone.id)), handle(gone.id));
    assert!(
        first.contains(&unavailable) && first.ends_with("call read_attachment with the handle to try again."),
        "{first}"
    );
    let chart_handle = post.attachment_handle(chart.id);
    assert_eq!(chart_handle.to_string(), format!("human.chan.ops.{}", chart.id), "under the post's channel");
    assert!(
        second.ends_with(&format!("{}; shown as an image after this text.", line(&chart, &chart_handle))),
        "{second}"
    );
    let image = |mime: &str, bytes: &[u8]| (mime.to_owned(), data_encoding::BASE64.encode(bytes));
    assert_eq!(log.images, [image("image/png", b"\x89PNG shot"), image("image/webp", b"RIFF chart")]);

    let only = &plain.attachments[0];
    let named =
        format!("{}; call read_attachment with the handle to read it.", line(only, &plain.attachment_handle(only.id)));
    assert!(third.ends_with(&format!("again\n\n{named}")), "{third}");
}

#[tokio::test]
async fn the_messenger_answers_a_post_mentioning_nobody_with_the_members_it_concerns() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    // dev-a is the channel's messenger, not a member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member("dev-b".parse().unwrap());
    entry.add_member("dev-c".parse().unwrap());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut replies = bus.client.subscribe(subject::chan(&entry.channel, &bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let colony = bus::ensure_colony(&bus.js, ants_proto::COLONY_TTL).await.unwrap();
    let now = chrono::Utc::now();
    let dev_b = ants_proto::ColonyEntry {
        ant: "dev-b".parse().unwrap(),
        machine: "m".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: vec!["ci".to_owned()],
        whoami: Some("Keeps the build green.".to_owned()),
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        sage: None,
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
        model: None,
        paused: None,
        drift: Vec::new(),
        mcp: false,
    };
    colony.put(dev_b.key(), serde_json::to_vec(&dev_b).unwrap().into()).await.unwrap();
    let post = ChannelPost::new(entry.channel.clone(), UserId::human(), "the build is red");
    let seq = bus.post(&post).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((&answer.thread_id, answer.in_reply_to), (&post.thread_id, Some(post.id)));
        assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1);
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let prompt = log.locked().answered[0].clone();
    assert!(prompt.contains(channel::TRIAGE_RULES), "{prompt}");
    let members = "- @dev-b [ci]: Keeps the build green.\n- @dev-c (offline: not in the colony)\n";
    assert!(prompt.contains(members), "{prompt}");
}

#[test]
fn an_unnarrowing_harness_is_warned_of_once_per_sender_within_a_bounded_set() {
    let p = |name: &str| -> UserId { name.parse().unwrap() };
    let unnarrowed = Unnarrowed::default();
    let (nightly, deploys) = (p("nightly"), p("deploys"));
    assert!(unnarrowed.first(&nightly));
    assert!(!unnarrowed.first(&nightly), "the same schedule again");
    assert!(unnarrowed.first(&deploys), "another webhook");
    for n in 2..Unnarrowed::MAX {
        unnarrowed.first(&p(&format!("hook-{n}")));
    }
    assert!(unnarrowed.first(&p("one-too-many")), "past the bound, the set starts over");
    assert_eq!(unnarrowed.0.locked().len(), 1);
    assert!(unnarrowed.first(&nightly), "forgotten when the set started over");
}

#[tokio::test]
async fn the_first_prompt_of_a_session_carries_the_provider_s_brief_asked_with_the_ant_s_context() {
    use ants_proto::provider::{BriefReason, Origin, Starter};

    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    crate::hub::create_identities(&bus.js).await.unwrap();
    let identity = crate::profile::identity::Identity::new(&bus.js, None, &bus.dev).await.unwrap();
    let born = identity.ensure().await.unwrap().unwrap();
    bus.identity = Some(identity.clone());
    let home = bus._server.dir().join("briefed");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    let answer = crate::provider::brief::fake::Answer::Text("Working on the parser.".to_owned());
    let (provider, fake) = crate::provider::brief::fake::connected(answer, true).await;
    mcp.with_provider(provider);
    mcp.with_callers(crate::provider::context::Callers::new(bus.dev.clone(), "test-machine".to_owned(), identity));
    bus.mcp = Some(mcp);
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let briefed = "[ants brief from fake]\nWorking on the parser.\n\na".to_owned();
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), briefed));
        bus.send(&t1, "b").await;
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()), "only the first prompt");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let asked = fake.asked.lock().unwrap();
    let [request] = asked.as_slice() else { panic!("{asked:?}") };
    let context = &request.context;
    assert_eq!((request.reason, &context.ant, &context.identity), (BriefReason::Started, &bus.dev, &born.id));
    assert_eq!((&context.thread, context.harness.as_str(), context.machine.as_str()), (&t1, "test", "test-machine"));
    assert_eq!(context.origin, Origin { by: Starter::Human { user: UserId::human() }, channel: None });
}
