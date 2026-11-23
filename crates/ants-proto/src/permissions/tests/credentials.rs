//! What credentials ask of the signer and carry of it: their own renewal, an ant's requests, a registration, and the
//! permissions version as a tag.

use super::*;

#[test]
fn credentials_ask_only_for_their_own_renewal_and_leaf_nodes_carry_any() {
    let (dev_a, alice) = (p("dev-a"), p("alice"));
    let ant = Permissions::for_user(&dev_a, Role::Ant);
    let renewals = |list: &[String]| -> Vec<String> {
        list.iter().filter(|s| s.starts_with("ants.renew.") || *s == subject::ALL).cloned().collect()
    };
    assert_eq!(renewals(&ant.publish), ["ants.renew.dev-a"]);
    assert_eq!(renewals(&ant.subscribe), ["ants.renew.dev-a.reply"]);
    // A human carries the renewal of its machine's leaf node uplink, and hears every answer anyway.
    let human = Permissions::for_user(&alice, Role::Human);
    assert_eq!(renewals(&human.publish), [subject::ALL_RENEW_REQUESTS]);
    assert_eq!(renewals(&human.subscribe), [subject::ALL]);
    let manager = Permissions::for_channel_manager();
    assert_eq!(
        (renewals(&manager.publish), renewals(&manager.subscribe)),
        (vec!["ants.renew.channel-manager".to_owned()], vec!["ants.renew.channel-manager.reply".to_owned()])
    );
    let enrollment = Permissions::for_enrollment(&dev_a);
    assert!(renewals(&enrollment.publish).is_empty() && renewals(&enrollment.subscribe).is_empty());
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&dev_a, Role::Ant), (&alice, Role::Human)]);
    assert_eq!(leaf.publish.iter().filter(|s| *s == subject::ALL_RENEW_REQUESTS).count(), 1);
    assert!(leaf.subscribe.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
    let approver = Permissions::for_enroll_approver();
    assert!(approver.subscribe.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
    assert!(approver.publish.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
    assert!(!approver.publish.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
}

#[test]
fn ants_ask_only_for_their_own_ant_requests_and_the_approver_answers_them() {
    let (lead, other) = (p("lead"), p("dev-b"));
    let ant = Permissions::for_user(&lead, Role::Ant);
    let asks =
        |list: &[String]| -> Vec<String> { list.iter().filter(|s| s.starts_with("ants.antreq.")).cloned().collect() };
    assert_eq!(asks(&ant.publish), ["ants.antreq.lead"]);
    assert_eq!(asks(&ant.subscribe), ["ants.antreq.lead.reply"]);
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&lead, Role::Ant), (&other, Role::Ant)]);
    assert_eq!(asks(&leaf.publish), ["ants.antreq.lead", "ants.antreq.dev-b"]);
    let approver = Permissions::for_enroll_approver();
    assert_eq!(asks(&approver.subscribe), [subject::ALL_ANT_REQUESTS]);
    assert_eq!(asks(&approver.publish), [subject::ALL_ANT_REPLIES]);
    assert!(approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_REQUESTS));
    assert!(
        !approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_DECISIONS),
        "only humans and the escalation chain decide"
    );
    let asked = AskedAnt {
        name: p("lead-w1"),
        template: "worker".to_owned(),
        request: "{}".to_owned(),
        reason: "more hands".to_owned(),
    };
    let request = AntRequest::new(lead.clone(), asked, "t1".parse().unwrap(), "UKEY", "host", Utc::now());
    assert_eq!(request.subject(), "ants.antreq.lead");
    assert!(request.check_subject("ants.antreq.dev-b").is_err());
    let mut moved = request.clone();
    moved.reason = "more hands\n".into();
    assert_ne!(moved.signed(), request.signed());
    let retiring = AntRequest { retire: true, ..request.clone() };
    assert_ne!(retiring.signed(), request.signed(), "an addition never passes for a retirement");
    let reply = AntReply::Refused { manager: lead, name: p("lead-w1"), public_key: "UKEY".into(), reason: "no".into() };
    assert_eq!(reply.subject(), "ants.antreq.lead.reply");
    assert!(reply.answers(&p("lead-w1"), "UKEY") && !reply.answers(&p("lead-w2"), "UKEY"));
}

#[test]
fn the_permissions_version_travels_as_a_user_jwt_tag() {
    let tag = PERMISSIONS_VERSION.tag();
    assert_eq!(tag, format!("ants-perms:{}", PERMISSIONS_VERSION.0));
    let device = DeviceId::generate().tag();
    assert_eq!(PermissionsVersion::from_tags([device.as_str(), tag.as_str()]), Some(PERMISSIONS_VERSION));
    assert_eq!(PermissionsVersion::from_tags([device.as_str()]), None);
    assert_eq!(PermissionsVersion::from_tags(["ants-perms:x"]), None);
    assert!(PermissionsVersion(0) < PERMISSIONS_VERSION);
}

#[test]
fn only_bootstrap_credentials_ask_to_register_and_only_the_signer_hears_them() {
    let bootstrap = Permissions::for_bootstrap();
    assert_eq!(bootstrap.publish, [subject::ALL_LOGIN_REQUESTS]);
    assert_eq!(bootstrap.subscribe, [subject::ALL_LOGIN_REPLIES]);
    let registrar = Permissions::for_registrar();
    assert_eq!((registrar.publish, registrar.subscribe), (bootstrap.subscribe, bootstrap.publish));
    let (dev_a, alice): (UserId, UserId) = ("dev-a".parse().unwrap(), "alice".parse().unwrap());
    let m1: Machine = "m1".parse().unwrap();
    let mut leaf = Permissions::for_leaf(&m1, [(&dev_a, Role::Ant), (&alice, Role::Human)]);
    leaf.publish.extend(Permissions::for_enroll_approver().publish);
    leaf.subscribe.extend(Permissions::for_enroll_approver().subscribe);
    let others = [Permissions::for_user(&dev_a, Role::Ant), Permissions::for_user(&alice, Role::Human), leaf];
    for permissions in others {
        let subjects = permissions.publish.iter().chain(&permissions.subscribe);
        assert!(subjects.clone().all(|s| !s.starts_with("ants.login.") && s != ">"), "{subjects:?}");
    }
}
