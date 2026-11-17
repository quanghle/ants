//! Attaching files to messages and reading attachments.

use super::*;

/// An attachments' store in memory, by handle.
type MemoryFiles = Arc<StdMutex<HashMap<AttachmentHandle, (Attachment, Vec<u8>)>>>;

fn memory_files(stored: MemoryFiles) -> FileAccess {
    let reader = stored.clone();
    FileAccess {
        put: Arc::new(move |handle, attachment, bytes| {
            stored.locked().insert(handle, (attachment, bytes.to_vec()));
            Box::pin(async { Ok(()) })
        }),
        get: Arc::new(move |handle, _| {
            let found = reader.locked().get(&handle).cloned();
            Box::pin(async move { found.ok_or_else(|| anyhow::anyhow!("attachment {handle} is not stored")) })
        }),
    }
}

/// A client for the sessions of `thread`, working in `dir`.
async fn client_in(mcp: &Mcp, thread: &ThreadId, dir: &Path) -> Client {
    let [McpServer::Http(server)] = &mcp.servers()(thread, dir, true)[..] else { panic!("one HTTP server") };
    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
    http_client(&server.url, token).await
}

#[tokio::test]
async fn attach_sends_files_from_the_working_directory_only() {
    let home = home("attach");
    let (mcp, mut sent) = start(&home).await;
    let work = home.join("work");
    std::fs::create_dir_all(work.join("sub")).unwrap();
    std::fs::write(work.join("notes.txt"), "hello").unwrap();
    std::fs::write(work.join("sub/shot.png"), [0x89, b'P', b'N', b'G']).unwrap();
    std::fs::write(home.join("secret.txt"), "keep out").unwrap();
    std::fs::File::create(work.join("big.bin")).unwrap().set_len(ants_proto::MAX_ATTACHMENT_SIZE + 1).unwrap();
    let current = from_human();
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &work).await;
    let send = |attach: serde_json::Value| serde_json::json!({"to": "dev-b", "text": "see", "attach": attach});

    let (ok, text) = call(&client, "send_message", send(serde_json::json!(["notes.txt"]))).await;
    assert!(!ok && text.contains("attachments are not available here; nothing sent"), "{text}");
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.to_path_buf());

    let (ok, text) = call(&client, "send_message", send(serde_json::json!(["notes.txt", "./sub/shot.png"]))).await;
    assert!(ok, "{text}");
    let out = sent.try_recv().unwrap();
    let got: Vec<_> = out.attachments.iter().map(|a| (a.name.as_str(), a.mime.as_str(), a.size)).collect();
    assert_eq!(got, [("notes.txt", "text/plain", 5), ("shot.png", "image/png", 4)]);
    let held = stored.locked().get(&out.attachment_handle(out.attachments[0].id)).cloned().unwrap();
    assert_eq!(held, (out.attachments[0].clone(), b"hello".to_vec()), "stored under the message's handle");

    #[cfg(unix)]
    std::os::unix::fs::symlink("../secret.txt", work.join("link")).unwrap();
    let nine = serde_json::json!(vec!["notes.txt"; 9]);
    for (attach, why) in [
        (serde_json::json!(["../secret.txt"]), "outside the working directory"),
        (serde_json::json!([home.join("secret.txt")]), "outside the working directory"),
        #[cfg(unix)]
        (serde_json::json!(["link"]), "outside the working directory"),
        (serde_json::json!(["sub"]), "no regular file"),
        (serde_json::json!(["notes.txt", "gone.txt"]), "cannot attach \"gone.txt\""),
        (serde_json::json!(["big.bin"]), "over the 8 MiB"),
        (nine, "at most 8 files"),
    ] {
        let (ok, text) = call(&client, "send_message", send(attach)).await;
        assert!(!ok && text.contains(why) && text.ends_with("nothing sent"), "{text}");
    }
    assert!(sent.try_recv().is_err(), "nothing sent");
    assert_eq!(stored.locked().len(), 2, "nothing stored");

    // Notes posts and channel posts carry them too, each under its own handle.
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "log", "attach": ["notes.txt"]})).await;
    assert!(ok, "{text}");
    let (ok, text) = call(&client, "read_notes", serde_json::json!({})).await;
    let handle = format!("dev-a.notes.{}.", current.thread_id);
    assert!(ok && text.contains("log\n📎 notes.txt (text/plain, 5 B) handle ") && text.contains(&handle), "{text}");
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops], posted.clone()));
    let post = serde_json::json!({"channel": "ops", "text": "shot", "attach": ["sub/shot.png"]});
    let (ok, text) = call(&client, "post_channel", post).await;
    assert!(ok, "{text}");
    let post = posted.locked().pop().unwrap();
    let held = stored.locked().get(&post.attachment_handle(post.attachments[0].id)).cloned().unwrap();
    assert_eq!(held.1, [0x89, b'P', b'N', b'G']);
}

#[tokio::test]
async fn attach_to_reply_names_the_files_the_turns_answer_carries() {
    let home = home("attach-to-reply");
    let (mcp, _sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.to_path_buf());
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("a.txt"), "aa").unwrap();
    std::fs::write(work.join("b.txt"), "b").unwrap();
    std::fs::write(home.join("secret.txt"), "keep out").unwrap();
    let current = from_human();
    let thread = &current.thread_id;
    let client = client_in(&mcp, thread, &work).await;
    let attach =
        async |files: serde_json::Value| call(&client, "attach_to_reply", serde_json::json!({"attach": files})).await;
    let reply = current.reply(user("dev-a"), "done", Role::Human).unwrap();
    let handle_of = |id| reply.attachment_handle(id);

    let (ok, text) = attach(serde_json::json!(["a.txt"])).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    let (ok, text) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok && text.contains("a.txt (text/plain, 2 B)"), "{text}");
    let (ok, text) = attach(serde_json::json!(["../secret.txt"])).await;
    assert!(!ok && text.contains("outside the working directory") && text.ends_with("nothing attached"), "{text}");
    assert!(stored.locked().is_empty(), "stored only as the answer is sent");
    let files = mcp.reply_files(thread, handle_of).await.unwrap();
    let got: Vec<_> = files.iter().map(|a| (a.name.as_str(), a.size)).collect();
    assert_eq!(got, [("a.txt", 2)], "the failed call left the earlier files");
    let held = stored.locked().get(&reply.attachment_handle(files[0].id)).cloned().unwrap();
    assert_eq!(held, (files[0].clone(), b"aa".to_vec()), "stored under the answer's handle");
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), [], "taken by the answer");

    // A later call replaces them, an empty one clears them.
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    let (ok, _) = attach(serde_json::json!(["b.txt"])).await;
    assert!(ok);
    let files = mcp.reply_files(thread, handle_of).await.unwrap();
    assert_eq!(files.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(), ["b.txt"]);
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    let (ok, text) = attach(serde_json::json!([])).await;
    assert!(ok && text.contains("carries no files"), "{text}");
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), []);

    // The files are read again as the answer is sent.
    let (ok, _) = attach(serde_json::json!(["b.txt"])).await;
    assert!(ok);
    std::fs::remove_file(work.join("b.txt")).unwrap();
    let why = mcp.reply_files(thread, handle_of).await.unwrap_err();
    assert!(why.contains("cannot attach \"b.txt\""), "{why}");

    // And forgotten with the turn.
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    drop(turn);
    let _turn = mcp.begin(&current);
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), []);
}

#[tokio::test]
async fn attach_redacts_secrets_in_text_files_only() {
    let home = home("attach-redact");
    let (mcp, mut sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.to_path_buf());
    mcp.with_redactor(Arc::new(crate::secrets::redact::Redactor::default()));
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let token = format!("ghp_{}", "a1B2".repeat(9));
    std::fs::write(work.join(".env"), format!("GITHUB_TOKEN={token}\nLOG=info\n")).unwrap();
    let mut binary = vec![0xff, 0xfe];
    binary.extend_from_slice(token.as_bytes());
    std::fs::write(work.join("blob.bin"), &binary).unwrap();
    std::fs::write(work.join("plain.txt"), "nothing secret").unwrap();
    let current = from_human();
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &work).await;

    let args = serde_json::json!({"to": "dev-b", "text": "see", "attach": [".env", "blob.bin", "plain.txt"]});
    let (ok, text) = call(&client, "send_message", args).await;
    assert!(ok, "{text}");
    let out = sent.try_recv().unwrap();
    let held: Vec<_> = out.attachments.iter().map(|a| stored.locked()[&out.attachment_handle(a.id)].clone()).collect();
    let env = b"GITHUB_TOKEN=[redacted]\nLOG=info\n";
    assert_eq!(held[0].1, env);
    let expected = crate::files::reference(".env", "application/octet-stream", env).unwrap();
    let reference = |a: &Attachment| (a.size, a.sha256.clone());
    assert_eq!(reference(&held[0].0), reference(&expected), "the reference is of the bytes stored");
    assert_eq!(held[1].1, binary, "not text: left alone");
    assert_eq!(held[2].1, b"nothing secret");
}

/// `read_attachment` reads an attachment of the message being handled as one stored when the hub stored that message,
/// so chunks still on their way from a leaf's outbox are waited for; any other as one stored when its id says.
#[tokio::test]
async fn read_attachment_waits_for_the_message_being_handled_as_its_stored_time_says() {
    let home = home("read-stored");
    let (mcp, _sent) = start(&home).await;
    let asked = Arc::new(StdMutex::new(Vec::new()));
    let seen = asked.clone();
    let files = FileAccess {
        put: Arc::new(|_, _, _| Box::pin(async { Ok(()) })),
        get: Arc::new(move |handle, stored| {
            seen.locked().push((handle.clone(), stored));
            Box::pin(async move { Err(anyhow::anyhow!("attachment {handle} is not stored")) })
        }),
    };
    mcp.with_files(files, home.to_path_buf());
    let current = Envelope::new(user("dev-b"), user("dev-a"), "files");
    let carried = current.attachment_handle(Uuid::now_v7());
    let other = current.attachment_handle(Uuid::now_v7());
    let stored = chrono::Utc::now();
    let turn = mcp.begin(&current).carrying(vec![carried.clone()], Some(stored));
    let client = client_in(&mcp, &current.thread_id, &home).await;
    for handle in [&carried, &other] {
        let (ok, _) = call(&client, "read_attachment", serde_json::json!({"handle": handle.to_string()})).await;
        assert!(!ok);
    }
    drop(turn);
    call(&client, "read_attachment", serde_json::json!({"handle": carried.to_string()})).await;
    assert_eq!(*asked.locked(), [(carried.clone(), Some(stored)), (other, None), (carried, None)]);
}

#[tokio::test]
async fn read_attachment_returns_images_and_text_and_saves_other_files() {
    let home = home("read-attachment");
    let (mcp, _sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.to_path_buf());
    let tree = home.join("tree");
    std::fs::create_dir_all(&tree).unwrap();
    let current = Envelope::new(user("dev-b"), user("dev-a"), "files");
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &tree).await;
    let put = |envelope: &Envelope, name: &str, bytes: &[u8]| {
        let attachment = crate::files::reference(name, ants_proto::guess_mime(name), bytes).unwrap();
        let handle = envelope.attachment_handle(attachment.id);
        stored.locked().insert(handle.clone(), (attachment, bytes.to_vec()));
        handle.to_string()
    };
    let read = async |handle: &str| {
        let args = serde_json::json!({"handle": handle});
        let serde_json::Value::Object(args) = args else { unreachable!() };
        client.call_tool(CallToolRequestParams::new("read_attachment").with_arguments(args)).await.unwrap()
    };

    let image = read(&format!("`{}`", put(&current, "shot.png", b"png bytes"))).await;
    let [text, picture] = &image.content[..] else { panic!("{image:?}") };
    assert!(text.as_text().unwrap().text.contains("shot.png (image/png, 9 B)"));
    let picture = picture.as_image().unwrap();
    assert_eq!((picture.data.as_str(), picture.mime_type.as_str()), ("cG5nIGJ5dGVz", "image/png"));

    let (ok, text) =
        call(&client, "read_attachment", serde_json::json!({"handle": put(&current, "a.md", b"# hi")})).await;
    assert!(ok && text.ends_with("a.md (text/markdown, 4 B)\n# hi"), "{text}");

    // Other files are saved into the thread's directory, the same bytes once, different ones beside them.
    let handle = put(&current, "data.bin", &[0, 1, 2]);
    for _ in 0..2 {
        let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": handle})).await;
        assert!(ok && text.ends_with(&format!("saved as {}", tree.canonicalize().unwrap().join("data.bin").display())));
    }
    let other = put(&current, "data.bin", &[3]);
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": other})).await;
    assert!(ok && text.ends_with("data-2.bin"), "{text}");
    assert_eq!(std::fs::read(tree.join("data.bin")).unwrap(), [0, 1, 2]);
    assert_eq!(std::fs::read(tree.join("data-2.bin")).unwrap(), [3]);
    assert!(!home.join("data.bin").exists(), "not in the workdir");

    // Only a message the ant reads: one between two other ants is not.
    let theirs = Envelope::new(user("dev-b"), user("dev-c"), "private");
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": put(&theirs, "x.txt", b"x")})).await;
    assert!(!ok && text.contains("is on a message dev-a does not read"), "{text}");
    let unknown = current.attachment_handle(Uuid::now_v7()).to_string();
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": unknown})).await;
    assert!(!ok && text.contains("is not stored"), "{text}");
    let (ok, _) = call(&client, "read_attachment", serde_json::json!({"handle": "nonsense"})).await;
    assert!(!ok);

    // A message with attachments lists them with their handles.
    let mut late = Envelope::on_thread(current.thread_id.clone(), user("dev-b"), user("dev-a"), "here");
    late.attachments = vec![crate::files::reference("log.txt", "text/plain", b"ok").unwrap()];
    let listed = super::shown(&late);
    let handle = late.attachment_handle(late.attachments[0].id);
    assert_eq!(listed, format!("here\n📎 log.txt (text/plain, 2 B) handle {handle}"));
}
