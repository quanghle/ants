//! Credentials of a machine before and after it joins: bootstrap and host.

use super::*;

#[tokio::test]
async fn bootstrap_credentials_only_ask_the_signer_to_register_and_hear_its_answers() {
    let Some(server) = Server::start() else { return };
    let (team, out, alice) = (server.dir().join("auth"), server.dir().join("bootstrap.creds"), p("alice"));
    let (key, exp) = issue_bootstrap(&team, Duration::ZERO, &out).unwrap();
    assert_eq!(exp, None);
    #[cfg(unix)]
    assert_eq!(std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&out).unwrap().permissions()) & 0o777, 0o644);
    let finding = Finding::from_creds(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!((finding.kind, finding.key.as_str(), finding.js_domain), (CredsKind::Bootstrap, key.as_str(), None));
    let issued = issued::issued(&team).unwrap();
    assert!(issued.iter().any(|i| i.key == key && i.kind == Kind::Bootstrap && i.name == "bootstrap"), "{issued:?}");

    let signer_file = server.dir().join("signer.creds");
    std::fs::write(&signer_file, signer_creds(&team, None).unwrap()).unwrap();
    let signer = async_nats::ConnectOptions::with_credentials_file(&signer_file)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::ENROLL_INBOX)
        .connect(&server.url)
        .await
        .unwrap();
    let mut requests = signer.subscribe(subject::ALL_LOGIN_REQUESTS).await.unwrap();
    // A round trip, as `subscribed` makes: any answer of JetStream does, even that there is no stream.
    let answered = jetstream::new(signer.clone()).get_stream(STREAM).await.map(drop).map_err(|e| e.kind());
    assert_matches!(answered, Ok(()) | Err(jetstream::context::GetStreamErrorKind::JetStream(_)));
    let bootstrap = p(ants_proto::BOOTSTRAP_USER);
    let (client, mut errors) = server.watched_with(&bootstrap, &out).await;
    let mut answers = client.subscribe(subject::login_reply(&alice)).await.unwrap();
    client.flush().await.unwrap();
    client.publish(subject::login(&alice), "hello".into()).await.unwrap();
    let request = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    assert_eq!((request.subject.as_str(), &request.payload[..]), (subject::login(&alice).as_str(), &b"hello"[..]));
    signer.publish(subject::login_reply(&alice), "answer".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, answers.next()).await.unwrap().unwrap().payload[..], b"answer");

    for denied in [subject::inbox(&alice, &bootstrap), format!("$JS.API.STREAM.INFO.{STREAM}")] {
        client.publish(denied.clone(), "x".into()).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    let _sub = client.subscribe(subject::login(&alice)).await.unwrap();
    let v = next_violation(&client, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::login(&alice))), "{v}");
    let big = vec![b'x'; usize::try_from(ants_proto::BOOTSTRAP_MAX_PAYLOAD).unwrap() + 1];
    client.publish(subject::login(&alice), big.into()).await.unwrap();
    let v = next_violation(&client, &mut errors).await;
    assert!(v.to_lowercase().contains("payload"), "{v}");
}

#[tokio::test]
async fn a_host_records_only_its_own_machine_s_ants_and_humans_read_where_each_ant_is() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let m1: Machine = "m1".parse().unwrap();
    let (_h, h_js) = server.connect(&human).await;
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_key_value(ants_daemon::hub::machines_config()).await.is_err(), "only a human creates it");
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();

    let creds = server.dir().join("host-m1.creds");
    issue_host(&server.dir().join("auth"), &m1, None, &creds).unwrap();
    let host = ants_daemon::host::Host {
        machine: None,
        fallback: None,
        nats: server.url.clone(),
        creds: Some(creds.clone()),
        js_domain: None,
        remote_admin: true,
        config: None,
        restarts: ants_daemon::admin::restart::Restarts::default(),
        providers: Default::default(),
        leaf: None,
    };
    let (ants, watched) = tokio::sync::watch::channel(vec![dev_a.clone(), UserId::messenger()]);
    let task = tokio::spawn(ants_daemon::host::run(host, watched));
    let recorded = async |want: &[UserId]| {
        timeout(WAIT, async {
            loop {
                let records = crate::ants::machines::list(&h_js).await.unwrap();
                if records.iter().any(|r| r.machine == m1 && r.ants == want) {
                    return records;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the host records its ants")
    };
    recorded(std::slice::from_ref(&dev_a)).await;
    ants.send_replace(vec![dev_b.clone(), dev_a.clone()]);
    assert_eq!(recorded(&[dev_a.clone(), dev_b.clone()]).await.len(), 1);
    assert_eq!(crate::ants::machines::machines_of(&h_js, &dev_b).await.unwrap(), std::slice::from_ref(&m1));

    let client = async_nats::ConnectOptions::with_credentials_file(&creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let mut m_js = jetstream::new(client);
    m_js.set_timeout(Duration::from_secs(1));
    let as_host = DirectKv::new(&m_js, None, ants_proto::MACHINES_BUCKET).await.unwrap();
    assert!(as_host.put("m2", b"{}".to_vec()).await.is_err(), "a host writes only its own machine's record");
    let holder = ants_proto::ACCOUNT_HOLDER_KEY;
    assert!(as_host.put(holder, b"{}".to_vec()).await.is_err(), "a host never says it holds the account key");
    let as_ant = DirectKv::new(&a_js, None, ants_proto::MACHINES_BUCKET).await.unwrap();
    assert!(as_ant.put("m1", b"{}".to_vec()).await.is_err(), "an ant has no host powers");

    ants_daemon::hub::create_identities(&h_js).await.unwrap();
    let identities = |js| DirectKv::new(js, None, ants_proto::IDENTITIES_BUCKET);
    let settings = ants_proto::SharedSettings::key_of(&dev_a);
    assert!(identities(&m_js).await.unwrap().put(&settings, b"{}".to_vec()).await.is_ok(), "a host writes them");
    assert!(identities(&h_js).await.unwrap().put(&settings, b"{}".to_vec()).await.is_err(), "a human orders changes");
    assert!(identities(&a_js).await.unwrap().put(&settings, b"{}".to_vec()).await.is_err(), "an ant only reads them");
    let read = DirectKv::whole(&a_js, None, ants_proto::IDENTITIES_BUCKET).await.unwrap();
    assert_eq!(read.get(&settings).await.unwrap(), Some(b"{}".to_vec()), "an ant reads them");
    drop(ants);
    timeout(WAIT, task).await.unwrap().unwrap().expect("the host stops once its daemon drops it");
}

#[tokio::test]
async fn only_a_host_reaches_the_sense_event_logs() {
    use ants_daemon::sense::log::{AntSigner, DeviceRevocations, JetStreamLog};
    use ants_sense::log::{AccountVerifier, AntLog, EventLog as _};

    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let m1: Machine = "m1".parse().unwrap();
    let (_h, h_js) = server.connect(&human).await;
    let (_a, a_js) = server.connect(&dev_a).await;
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    assert!(JetStreamLog::open(&a_js).await.is_err(), "an ant may not create the stream");
    assert!(JetStreamLog::open(&h_js).await.is_err(), "nor a human");

    let host_creds = server.dir().join("host-m1.creds");
    issue_host(&server.dir().join("auth"), &m1, None, &host_creds).unwrap();
    let client = async_nats::ConnectOptions::with_credentials_file(&host_creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let mut m_js = jetstream::new(client);
    m_js.set_timeout(Duration::from_secs(1));
    let log = JetStreamLog::open(&m_js).await.unwrap();
    let accounts = vec![ants_daemon::secrets::creds::account(&host_creds).unwrap()];
    let signer = AntSigner::load(&dev_a, Some(&server.creds(&dev_a)), &accounts).unwrap();
    let verifier = async || AccountVerifier::new(accounts.clone(), DeviceRevocations::new(&m_js, None).await.unwrap());
    let sense = ants_proto::SenseId::new();
    let new =
        async |signer| AntLog::new(&log, signer, verifier().await, sense.clone(), dev_a.clone(), m1.clone(), None);
    let (appended, _) = new(signer).await.unwrap().append("noted", serde_json::json!({})).await.unwrap();
    let mut reader = new(AntSigner::load(&dev_a, None, &[]).unwrap()).await.unwrap();
    let read = reader.catch_up().await.unwrap();
    assert_eq!(read.iter().map(|c| &c.event).collect::<Vec<_>>(), [&appended.event], "the host reads it back");
    assert_eq!(reader.chain().broken(), None);
    assert_eq!(log.last_sequence(&sense).await.unwrap(), appended.stored.sequence);

    for (who, js) in [("an ant", &a_js), ("a human", &h_js)] {
        let ack = js.publish(subject::sense(&sense), "forged".into()).await;
        assert!(ack.is_err() || ack.unwrap().await.is_err(), "{who} may not append");
        let stream = js.get_stream_no_info(ants_proto::SENSE_STREAM).await.unwrap();
        assert!(stream.direct_get_last_for_subject(subject::sense(&sense)).await.is_err(), "{who} may not read");
        assert!(js.get_stream(ants_proto::SENSE_STREAM).await.is_err(), "{who} may not look it up");
    }
    assert_eq!(log.last_sequence(&sense).await.unwrap(), appended.stored.sequence);
}
