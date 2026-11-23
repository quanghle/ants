//! Oversized and malformed input: the CLI refuses a message past the server's payload limit, more than 8
//! attachments and a file past 8 MiB; junk published straight onto an ant's inbox and channel subjects (truncated
//! JSON, wrong types, absurd values, forged senders, huge headers) is dead-lettered or dropped once, never
//! redelivered, and the ant answers the next good message.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ants_proto::Uuid;
use ants_test_support::{NatsServer, scratch_dir};
use async_nats::HeaderMap;
use serde_json::{Value, json};

use super::{ANTS, Killed, link_self, run};

/// What goes onto the subjects, and whether the ant's daemon dead-letters it (else it is prompted, or a channel post
/// dropped).
struct Junk {
    subject: &'static str,
    payload: Vec<u8>,
    headers: Vec<(&'static str, String)>,
    buried: bool,
}

#[expect(clippy::too_many_lines, reason = "one scenario: refusals, then junk, then a good message")]
pub fn junk_on_the_subjects_is_dead_lettered_once_and_the_ant_answers_on() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("junk");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));
    let log = dir.join("daemon.log");
    let mut daemon = Killed(
        ants(&["daemon", "--harness", "copilot"])
            .arg("--home")
            .arg(dir.join("home"))
            .arg("--workdir")
            .arg(&dir)
            .env("ANTS_ANT", "dev-a")
            .env("PATH", &bin)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
    );
    let tail =
        || std::fs::read_to_string(&log).unwrap_or_default().lines().rev().take(30).collect::<Vec<_>>().join("\n");
    let ask = |words: &str| run(ants(&["ask", "--wait", "--timeout", "60s", "@dev-a", words]));
    let warm = ask("warmup");
    assert!(warm.status.success(), "ants ask: {}\n{}", String::from_utf8_lossy(&warm.stderr), tail());

    // The CLI's refusals, before anything is stored.
    let refused = |args: &[&str], input: Option<Vec<u8>>, says: &str| {
        let mut command = ants(args);
        let out = match input {
            Some(input) => {
                let mut child =
                    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
                std::io::Write::write_all(&mut child.stdin.take().unwrap(), &input).unwrap();
                child.wait_with_output().unwrap()
            }
            None => run(command),
        };
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success() && stderr.contains(says), "{args:?} not refused with {says:?}: {stderr}");
    };
    refused(&["ask", "--file", "-", "@dev-a"], Some(vec![b'x'; 1_100_000]), "exceeded");
    let files = dir.join("files");
    std::fs::create_dir_all(&files).unwrap();
    let mut many = vec!["ask"];
    let names: Vec<String> = (0..9).map(|i| files.join(format!("f{i}.txt")).to_str().unwrap().to_owned()).collect();
    for name in &names {
        std::fs::write(name, "f").unwrap();
        many.extend(["--attach", name]);
    }
    many.extend(["@dev-a", "nine files"]);
    refused(&many, None, "at most 8");
    let big = files.join("big.bin");
    // Sparse where the file system allows: nothing is written.
    std::fs::File::create(&big).unwrap().set_len(64 << 20).unwrap();
    refused(&["ask", "--attach", big.to_str().unwrap(), "@dev-a", "a big file"], None, "64 MiB");

    let inbox = "ants.inbox.dev-a.human";
    let chan = "ants.chan.general.human";
    let envelope = |changes: Value| {
        let mut envelope = json!({
            "id": Uuid::now_v7(), "thread_id": Uuid::now_v7(), "from": "human", "to": "dev-a", "text": "junk",
            "turn_budget": 10, "sent_at": "2026-11-23T06:00:00Z",
        });
        for (key, value) in changes.as_object().unwrap() {
            envelope[key] = value.clone();
        }
        serde_json::to_vec(&envelope).unwrap()
    };
    let raw = |subject, payload: &[u8], buried| Junk { subject, payload: payload.to_vec(), headers: vec![], buried };
    let attachment = |name: &str, size: Value| json!({ "id": Uuid::now_v7(), "name": name, "mime": "text/plain", "size": size, "sha256": "0".repeat(64) });
    let escapes = "\u{1b}[2J\u{1b}]0;pwned\u{7}\u{202e}\u{200b}e\u{301}".repeat(100);
    let mut junk = vec![
        raw(inbox, b"", true),
        raw(inbox, &envelope(json!({}))[..40], true),
        raw(inbox, b"null", true),
        raw(inbox, &[0xff, 0xfe, 0x00, 0x1b], true),
        raw(inbox, &[&b"["[..]; 100_000].concat(), true),
        raw(
            inbox,
            &String::from_utf8(envelope(json!({ "text": "SURROGATE" })))
                .unwrap()
                .replace("SURROGATE", "\\ud800")
                .into_bytes(),
            true,
        ),
        raw(inbox, &envelope(json!({ "text": 12345 })), true),
        raw(inbox, &envelope(json!({ "turn_budget": -1 })), true),
        raw(inbox, &envelope(json!({ "turn_budget": u64::MAX })), true),
        raw(inbox, &envelope(json!({ "id": "not-a-uuid" })), true),
        raw(inbox, &envelope(json!({ "thread_id": "a.b.*" })), true),
        raw(inbox, &envelope(json!({ "thread_id": "t".repeat(100_000) })), true),
        raw(inbox, &envelope(json!({ "sent_at": "+275760-09-13T00:00:00Z" })), true),
        raw(inbox, &envelope(json!({ "from": "dev-b" })), true),
        raw(inbox, &envelope(json!({ "to": "dev-b" })), true),
        raw(inbox, &envelope(json!({ "owner": "\u{1b}[31m" })), true),
        raw(inbox, &envelope(json!({ "attachments": [attachment("a", json!(-5))] })), true),
        raw(inbox, &envelope(json!({ "escalation": { "nonsense": true } })), true),
        raw(inbox, &envelope(json!({ "mention": "x" })), true),
        // Valid, if absurd: prompted and answered.
        raw(inbox, &envelope(json!({ "turn_budget": 0 })), false),
        raw(inbox, &envelope(json!({ "turn_budget": u32::MAX, "sent_at": "0001-01-01T00:00:00Z" })), false),
        raw(inbox, &envelope(json!({ "text": escapes, "title": escapes })), false),
        raw(inbox, &envelope(json!({ "allow": ["", "*", "\u{0}", "other:".repeat(2000)] })), false),
        raw(
            inbox,
            &envelope(
                json!({ "attachments": (0..1000).map(|i| attachment(&format!("f{i}"), json!(10))).collect::<Vec<_>>() }),
            ),
            false,
        ),
        raw(inbox, &envelope(json!({ "attachments": [attachment(&escapes, json!(u64::MAX))] })), false),
        raw(inbox, &envelope(Value::Object((0..5000).map(|i| (format!("unknown{i}"), json!(i))).collect())), false),
        raw(chan, b"", false),
        raw(chan, b"{\"channel\":", false),
    ];
    junk.push(Junk { headers: vec![("X-Big", "v".repeat(60_000))], ..raw(inbox, &envelope(json!({})), false) });
    junk.push(Junk {
        headers: vec![("Nats-Stream", "ANTS".into()), ("Nats-Subject", inbox.into()), ("Nats-Sequence", "1".into())],
        ..raw(inbox, &envelope(json!({})), true)
    });
    let buried = junk.iter().filter(|j| j.buried).count();

    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let consumer = runtime.block_on(async {
        let client = async_nats::connect(&server.url).await.unwrap();
        let js = async_nats::jetstream::new(client);
        for (i, j) in junk.into_iter().enumerate() {
            let mut headers = HeaderMap::new();
            headers.insert("Nats-Msg-Id", format!("junk-{i}").as_str());
            for (name, value) in j.headers {
                headers.insert(name, value.as_str());
            }
            let ack = js.publish_with_headers(j.subject, headers, j.payload.into()).await.unwrap();
            ack.await.unwrap_or_else(|e| panic!("junk {i} not stored: {e}"));
        }
        js.get_stream("ANTS")
            .await
            .unwrap()
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>("inbox-dev-a")
            .await
            .unwrap()
    });

    // Junk first in the stream, then a good message: answered.
    let good = ask("after the junk");
    let out = String::from_utf8_lossy(&good.stdout);
    assert!(good.status.success() && out.contains("stub: "), "no answer after the junk: {out}\n{}", tail());

    // Every junk message settled at its first delivery: none pending, none redelivered, each malformed one a dead letter.
    let start = Instant::now();
    loop {
        let mut consumer = consumer.clone();
        let info = runtime.block_on(consumer.info()).unwrap().clone();
        let dead = run(ants(&["dead", "list"]));
        let letters = String::from_utf8_lossy(&dead.stdout).lines().filter(|l| l.contains("\tdev-a\t")).count();
        if info.num_ack_pending == 0 && info.num_pending == 0 && letters == buried {
            assert_eq!(info.num_redelivered, 0, "junk redelivered\n{}", tail());
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "unsettled: {} in hand, {} pending, {letters} of {buried} dead letters\n{}",
            info.num_ack_pending,
            info.num_pending,
            tail()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(daemon.0.try_wait().unwrap().is_none(), "the daemon exited\n{}", tail());
}
