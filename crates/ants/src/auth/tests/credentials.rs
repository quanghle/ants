//! Credentials as issued and renewed: what they carry, for whom and for how long.

use super::*;

#[test]
fn enrollment_credentials_issued_with_a_zero_ttl_never_expire() {
    let dir = ants_test_support::scratch_dir("enroll-ttl");
    let (team, out, dev_a) = (dir.join("auth"), dir.join("enroll-dev-a.creds"), p("dev-a"));
    init(&team).unwrap();
    let issued = |ttl| {
        let exp = issue_enrollment(&team, &dev_a, ttl, &out)?;
        let text = std::fs::read_to_string(&out).unwrap();
        anyhow::Ok((exp, verified(creds_jwt(&text).unwrap()).unwrap().exp))
    };
    assert_eq!(issued(Duration::ZERO).unwrap(), (None, None));
    let (exp, claimed) = issued(Duration::from_secs(60)).unwrap();
    assert!(exp.is_some_and(|exp| exp >= now().unwrap() + 59) && claimed == exp, "{exp:?} {claimed:?}");
    let err = issued(Duration::from_millis(500)).unwrap_err().to_string();
    assert!(err.contains("under a second"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn no_user_device_or_enrollment_takes_a_reserved_name() {
    let dir = ants_test_support::scratch_dir("reserved-names");
    let team = dir.join("auth");
    init(&team).unwrap();
    issue_bootstrap(&team, Duration::ZERO, &dir.join("bootstrap.creds")).unwrap();
    let key = KeyPair::new_user().public_key();
    for name in ["bootstrap", "signer", "channel-manager", "revoked", "system-messenger", "system-sage"] {
        let (user, out) = (p(name), dir.join(format!("{name}.creds")));
        for role in [Role::Human, Role::Ant] {
            let err = issue(&team, &user, role, None, &out).unwrap_err().to_string();
            assert!(err.contains("is reserved"), "{name}: {err}");
            let permissions = Permissions::for_user(&user, role);
            let err = sign_with(&team, &user, role.into(), permissions, &key).unwrap_err().to_string();
            assert!(err.contains("is reserved"), "{name}: {err}");
        }
        assert!(issue_enrollment(&team, &user, Duration::ZERO, &out).is_err(), "{name}");
        assert!(sign_added(&team, &user, None, &key, "worker", &p("lead")).is_err(), "{name}");
        assert!(!out.exists() || name == "bootstrap", "{name}");
    }
    let issued = issued::issued(&team).unwrap();
    assert_eq!(issued.iter().map(|i| i.kind).collect::<Vec<_>>(), [Kind::Bootstrap], "only the bootstrap credentials");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn builtin_credentials_are_issued_as_ants_and_can_be_reissued_only_by_service_name() {
    let dir = ants_test_support::scratch_dir("builtin-creds");
    let team = dir.join("auth");
    init(&team).unwrap();
    let domain = "hub".parse().unwrap();
    for builtin in ants_proto::BuiltinAnt::ALL {
        let out = dir.join(format!("{}.creds", builtin.id()));
        issue_builtin(&team, builtin, Some(&domain), &out).unwrap();
        let finding = ants_daemon::secrets::creds::check(&out).unwrap();
        assert_eq!(finding.name, builtin.id().as_str());
        assert_eq!(finding.kind, ants_daemon::secrets::creds::Kind::Ant);
        assert_eq!(finding.renew(), [format!("ants auth builtin {} --js-domain hub", builtin.name())]);
    }
    for name in ["messenger", "sage"] {
        issue(&team, &p(name), Role::Ant, None, &dir.join(format!("{name}.creds"))).unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn renewed_credentials_keep_what_they_are_for_a_new_key() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("renew");
    let team = dir.join("auth");
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    let out = |name: &str| team.join(format!("{name}.creds"));
    let renewed = |name: &str| {
        let (jwt, key) = read_creds(&out(name)).unwrap();
        let new = KeyPair::new_user();
        let renewed = renew(&team, &jwt, &new.public_key(), None).unwrap();
        assert!(creds::Finding::from_jwt(&jwt).unwrap().same_credentials(&renewed.finding), "{name}");
        assert_eq!(renewed.finding.version, PERMISSIONS_VERSION, "{name}");
        assert_eq!(
            (verified(&renewed.jwt).unwrap().sub, renewed.previous.as_str()),
            (new.public_key(), &*key.public_key())
        );
        let line = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!((line.key, line.renewed_from), (new.public_key(), Some(key.public_key())), "{name}");
        renewed
    };
    issue(&team, &p("dev-a"), Role::Ant, Some(&hub), &out("dev-a")).unwrap();
    assert_eq!(renewed("dev-a").finding.js_domain, Some(hub.clone()));
    let device = Issue::new(&team, &p("alice"), Kind::Human, Permissions::for_user(&p("alice"), Role::Human))
        .labelled(Some("laptop"))
        .into_file(&out("alice"))
        .unwrap();
    assert_eq!(renewed("alice").finding.device, device);
    let devices = issued::devices(&team, "alice").unwrap();
    assert_eq!(devices.len(), 1, "one device, renewed: {devices:?}");
    assert_eq!(devices[0].label.as_deref(), Some("laptop"));
    let destinations = [Destination::Ant(p("dev-a")), Destination::Channel("ops".parse().unwrap())];
    let hook = Permissions::for_webhook(&p("hook"), &destinations);
    issue_with(&team, &p("hook"), Kind::Webhook, hook, &out("hook")).unwrap();
    renewed("hook");
    let observer = Permissions::for_observer(&p("audit")).with_js_domain(Some(&hub));
    issue_with(&team, &p("audit"), Kind::Observer, observer, &out("audit")).unwrap();
    let audit = renewed("audit");
    assert_eq!((&audit.finding.kind, audit.finding.js_domain.as_ref()), (&creds::Kind::Observer, Some(&hub)));
    assert_eq!(audit.finding.renew(), ["ants auth user audit --observer --js-domain hub"]);
    assert_eq!(issued::issued(&team).unwrap().pop().unwrap().kind, Kind::Observer);
    issue_channel_manager(&team, None, &out("channel-manager")).unwrap();
    renewed("channel-manager");
    let m1: Machine = "m1".parse().unwrap();
    let members = members(&[p("dev-a"), p("dev-b")], &[p("alice")]);
    issue_leaf(&team, &m1, &members, &[p("hook")], EnrollApprover::With, Some(&hub), &out("leaf-m1")).unwrap();
    let leaf = renewed("leaf-m1");
    check_leaf(&account_key(&team).unwrap(), &leaf.jwt, &m1).unwrap();
    let uplink = allowed(&verified_body(&leaf.jwt).unwrap().1);
    let react = subject::ALL_REACT_REQUESTS.to_owned();
    assert!(uplink.subscribe.contains(&react), "`ants signer` on the machine answers ants' reactions requests");
    let kept = crate::node::members::issued(&team, &m1).unwrap().unwrap();
    assert_eq!(
        kept,
        crate::node::members::Members::new(&members, &[p("hook")], EnrollApprover::With),
        "the registry keeps its members"
    );

    // A lifetime carries over: as long from now as the earlier credentials had from their issue.
    let account = load(&team.join(ACCOUNT_SEED)).unwrap();
    let key = KeyPair::new_user().public_key();
    let ant = Permissions::for_user(&p("dev-c"), Role::Ant);
    let expiring = user_jwt(&account, &key, "dev-c", ant, json!({}), Some(now().unwrap() + 3600)).unwrap();
    let exp = verified(&renew(&team, &expiring, &KeyPair::new_user().public_key(), None).unwrap().jwt).unwrap().exp;
    assert!(exp.is_some_and(|exp| exp.abs_diff(now().unwrap() + 3600) <= 2), "{exp:?}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_human_device_binds_its_encryption_key_and_renewal_keeps_it_or_adds_one() {
    use ants_daemon::secrets::{creds, keystore};
    let dir = ants_test_support::scratch_dir("renew-xkey");
    let team = dir.join("auth");
    init(&team).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let human = || Permissions::for_user(&p("alice"), Role::Human);
    let new = || KeyPair::new_user().public_key();
    let bound = |jwt: &str| creds::Finding::from_jwt(jwt).unwrap().xkey;
    let registered = || issued::issued(&team).unwrap().pop().unwrap().xkey;

    Issue::new(&team, &p("alice"), Kind::Human, human()).labelled(Some("laptop")).into_file(&out("alice")).unwrap();
    let (jwt, _) = read_creds(&out("alice")).unwrap();
    let xkey = creds::load_xkey(&out("alice"), &keystore::Memory::default()).unwrap().unwrap();
    let laptop = device_key(&xkey).unwrap();
    assert_eq!((bound(&jwt), registered()), (Some(laptop.clone()), Some(laptop.clone())));
    let renewed = renew(&team, &jwt, &new(), None).unwrap();
    assert_eq!((bound(&renewed.jwt), registered()), (Some(laptop.clone()), Some(laptop.clone())), "kept");
    let other = device_key(&XKey::new()).unwrap();
    let err = renew(&team, &jwt, &new(), Some(&other)).unwrap_err().to_string();
    assert!(err.contains("already"), "{err}");

    let (unbound, _) = Issue::new(&team, &p("alice"), Kind::Human, human()).for_key(&new(), None).unwrap();
    assert_eq!(bound(&unbound), None, "signed for a key generated elsewhere, without --xkey");
    let added = renew(&team, &unbound, &new(), Some(&other)).unwrap();
    assert_eq!((bound(&added.jwt), registered()), (Some(other.clone()), Some(other.clone())));
    let devices = issued::devices(&team, "alice").unwrap();
    assert_eq!(devices.iter().map(|d| d.xkey.clone()).collect::<Vec<_>>(), [Some(laptop), Some(other.clone())]);

    issue(&team, &p("dev-a"), Role::Ant, None, &out("dev-a")).unwrap();
    let (ant, _) = read_creds(&out("dev-a")).unwrap();
    assert_eq!(bound(&ant), None);
    assert!(creds::xkey_of(&std::fs::read_to_string(out("dev-a")).unwrap()).is_none(), "an ant holds no xkey");
    let err = renew(&team, &ant, &new(), Some(&other)).unwrap_err().to_string();
    assert!(err.contains("only a human's"), "{err}");
    let ant_rights = Permissions::for_user(&p("dev-b"), Role::Ant);
    assert!(Issue::new(&team, &p("dev-b"), Kind::Ant, ant_rights).for_key(&new(), Some(&other)).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn an_admin_s_device_carries_the_role_through_renewal_until_revoked() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("admin");
    let team = dir.join("auth");
    init(&team).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let human = || Permissions::for_user(&p("alice"), Role::Human);
    let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    Issue::new(&team, &p("alice"), Kind::Human, human()).with(&admin).into_file(&out("admin")).unwrap();
    Issue::new(&team, &p("alice"), Kind::Human, human()).labelled(Some("phone")).into_file(&out("phone")).unwrap();
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let err = Issue::new(&team, &p("dev-a"), Kind::Ant, ant).with(&admin).into_file(&out("dev-a")).unwrap_err();
    assert!(err.to_string().contains("admin"), "{err}");

    let (jwt, key) = read_creds(&out("admin")).unwrap();
    let finding = creds::Finding::from_jwt(&jwt).unwrap();
    assert_eq!(
        (&finding.kind, finding.admin, finding.holder()),
        (&creds::Kind::Human, true, "human alice (admin)".into())
    );
    let account = account_key(&team).unwrap();
    let order = b"an order";
    let signature = BASE64URL_NOPAD.encode(&key.sign(order).unwrap());
    let verified = ants_proto::verify_admin(&jwt, [account.as_str()], order, &signature, now().unwrap()).unwrap();
    assert_eq!((verified.id, verified.public_key), (p("alice"), key.public_key()));
    let (phone, _) = read_creds(&out("phone")).unwrap();
    assert!(!creds::Finding::from_jwt(&phone).unwrap().admin);
    let refused = ants_proto::verify_admin(&phone, [account.as_str()], order, &signature, now().unwrap());
    assert_eq!(refused.unwrap_err(), ants_proto::JwtError::NotAdmin(p("alice")));

    let new = KeyPair::new_user().public_key();
    let renewed = renew(&team, &jwt, &new, None).unwrap();
    assert!(renewed.finding.admin, "the renewal keeps the role");
    let admins = |now| issued::admins(&team, now).unwrap().into_iter().map(|a| a.key).collect::<Vec<_>>();
    assert_eq!(admins(now().unwrap()), [key.public_key(), new.clone()], "the phone's are no admin's");
    revoke::revoke(&team, None, std::slice::from_ref(&new)).unwrap();
    assert_eq!(admins(now().unwrap()), [key.public_key()]);
    revoke::revoke(&team, Some("alice"), &[]).unwrap();
    assert!(admins(now().unwrap()).is_empty(), "revoking the human withdraws the role");
    assert!(renew(&team, &jwt, &KeyPair::new_user().public_key(), None).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn an_ant_becomes_a_human_s_only_assistant_through_its_user_jwt() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("assistant");
    let team = dir.join("auth");
    init(&team).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let (alice, pa, pb) = (p("alice"), p("pa"), p("pb"));
    let assistant = |user: &UserId, of: &UserId| {
        let new = NewDevice { assistant_of: Some(of.clone()), ..NewDevice::default() };
        let rights = Permissions::for_assistant(user, of);
        Issue::new(&team, user, Kind::Assistant, rights).with(&new).into_file(&out(user.as_str()))
    };
    let refused = |user: &UserId, of: &UserId, why: &str| {
        let err = check_assistant(&team, user, of).unwrap_err().to_string();
        assert!(err.contains(why), "{user} of {of}: {err}");
    };
    refused(&pa, &alice, "not a human of the team");
    check_assistant(&team, &pa, &UserId::human()).unwrap();
    Issue::new(&team, &alice, Kind::Human, Permissions::for_user(&alice, Role::Human))
        .into_file(&out("alice"))
        .unwrap();
    issue(&team, &p("carol"), Role::Human, None, &out("carol")).unwrap();
    refused(&alice, &alice, "its own assistant");
    refused(&UserId::human(), &alice, "is a human");
    refused(&p("carol"), &alice, "a human's credentials");
    check_assistant(&team, &pa, &alice).unwrap();
    assert!(issue_with(&team, &pa, Kind::Assistant, Permissions::for_assistant(&pa, &alice), &out("pa")).is_err());
    assistant(&pa, &alice).unwrap();

    let (jwt, key) = read_creds(&out("pa")).unwrap();
    let finding = creds::Finding::from_jwt(&jwt).unwrap();
    assert_eq!(
        (finding.kind.clone(), finding.holder()),
        (creds::Kind::Assistant { of: alice.clone() }, "ant pa, assistant of alice".into())
    );
    let account = account_key(&team).unwrap();
    let verified = ants_proto::verify_assistant_of(&jwt, [account.as_str()], &alice, now().unwrap()).unwrap();
    assert_eq!((verified.id, verified.public_key), (pa.clone(), key.public_key()));
    check_assistant(&team, &pa, &alice).unwrap();
    refused(&pb, &alice, "already the assistant of alice");
    refused(&pa, &p("carol"), "the assistant of alice");

    let renewed = renew(&team, &jwt, &KeyPair::new_user().public_key(), None).unwrap();
    assert_eq!(renewed.finding.kind, creds::Kind::Assistant { of: alice.clone() });
    let line = issued::issued(&team).unwrap().pop().unwrap();
    assert_eq!((line.kind, line.assistant_of), (Kind::Assistant, Some(alice.clone())));
    let listed: Vec<(String, bool)> =
        issued::assistants(&team, "alice").unwrap().into_iter().map(|(i, revoked)| (i.key, revoked)).collect();
    assert_eq!(listed, [(key.public_key(), false), (verified_key(&renewed.jwt), false)]);
    revoke::revoke(&team, Some("pa"), &[]).unwrap();
    assert!(issued::assistants(&team, "alice").unwrap().iter().all(|(_, revoked)| *revoked));
    check_assistant(&team, &pb, &alice).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn verified_key(jwt: &str) -> String {
    verified(jwt).unwrap().sub
}

#[test]
fn only_credentials_a_registration_granted_are_marked_registered() {
    let dir = ants_test_support::scratch_dir("renew-registered");
    let team = dir.join("auth");
    init(&team).unwrap();
    let human = || Permissions::for_user(&p("alice"), Role::Human);
    let new = || KeyPair::new_user().public_key();
    let registered = |jwt: &str| Finding::from_jwt(jwt).unwrap().registered;
    let github = || GithubAccount { id: 1, login: "octocat".into() };
    let grant = || Grant::Org("acme".into());

    let device = NewDevice { github: Some(github()), grant: Some(grant()), ..NewDevice::default() };
    let (jwt, _) = Issue::new(&team, &p("alice"), Kind::Human, human()).with(&device).for_key(&new(), None).unwrap();
    assert!(registered(&jwt));
    let renewed = renew_registered(&team, &jwt, &new(), None, github(), grant()).unwrap();
    assert!(registered(&renewed.jwt) && renewed.finding.registered, "ants signer keeps the mark");
    assert!(!registered(&renew(&team, &renewed.jwt, &new(), None).unwrap().jwt), "ants enroll renew drops it");

    let bound = NewDevice { github: Some(github()), ..NewDevice::default() };
    let (issued, _) = Issue::new(&team, &p("alice"), Kind::Human, human()).with(&bound).for_key(&new(), None).unwrap();
    assert!(!registered(&issued), "bound with `ants auth user --github`, without a grant");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn credentials_expired_revoked_foreign_or_of_no_renewable_kind_are_not_renewed() {
    let dir = ants_test_support::scratch_dir("renew-refused");
    let (team, other) = (dir.join("auth"), dir.join("other"));
    init(&team).unwrap();
    init(&other).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let jwt = |name: &str| read_creds(&out(name)).unwrap();
    let refused = |jwt: &str, key: &str| renew(&team, jwt, key, None).unwrap_err().to_string();
    let new = || KeyPair::new_user().public_key();

    issue(&team, &p("dev-a"), Role::Ant, None, &out("dev-a")).unwrap();
    let (dev_a, dev_a_key) = jwt("dev-a");
    assert!(refused(&dev_a, &dev_a_key.public_key()).contains("generate a new one"));
    assert!(refused(&dev_a, &account_key(&team).unwrap()).contains("not the public key of a user"));
    issue(&team, &p("dev-b"), Role::Ant, None, &out("dev-b")).unwrap();
    let (_, dev_b_key) = jwt("dev-b");
    let err = refused(&dev_a, &dev_b_key.public_key());
    assert!(err.contains("issued to other credentials"), "a key already dev-b's: {err}");
    let stolen = new();
    revoke::revoke(&team, None, std::slice::from_ref(&stolen)).unwrap();
    assert!(refused(&dev_a, &stolen).contains("revoked"));

    issue(&other, &p("dev-a"), Role::Ant, None, &out("foreign")).unwrap();
    assert!(refused(&jwt("foreign").0, &new()).contains("not by the team's account"));

    let account = load(&team.join(ACCOUNT_SEED)).unwrap();
    let ant = Permissions::for_user(&p("dev-c"), Role::Ant);
    let expired = user_jwt(&account, &new(), "dev-c", ant, json!({}), Some(now().unwrap() - 1)).unwrap();
    assert!(refused(&expired, &new()).contains("expired"));

    issue_enrollment(&team, &p("dev-c"), Duration::from_secs(60), &out("enroll-dev-c")).unwrap();
    assert!(refused(&jwt("enroll-dev-c").0, &new()).contains("cannot be renewed"));
    let m1: Machine = "m1".parse().unwrap();
    issue_leaf(&team, &m1, &[(p("dev-a"), Role::Ant)], &[], EnrollApprover::Without, None, &out("leaf-m1")).unwrap();
    let system = read_creds(&leaf_system_creds(&out("leaf-m1"))).unwrap().0;
    assert!(refused(&system, &new()).contains("team's account"), "the system account's");

    revoke::revoke(&team, Some("dev-a"), &[]).unwrap();
    assert!(refused(&dev_a, &new()).contains("revoked"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn issued_credentials_carry_the_current_permissions_version_and_name_their_renewal() {
    use ants_daemon::secrets::creds::{self, VersionStatus};
    let dir = ants_test_support::scratch_dir("creds-check");
    let team = dir.join("auth");
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    let renewal = |name: &str| {
        let finding = creds::check(&team.join(format!("{name}.creds"))).unwrap();
        assert_eq!(finding.version_status(), VersionStatus::Current, "{name}");
        assert!(
            finding.warnings(None, now().unwrap()).is_empty(),
            "{name}: {:?}",
            finding.warnings(None, now().unwrap())
        );
        finding.renew()
    };
    let out = |name: &str| team.join(format!("{name}.creds"));
    issue(&team, &p("dev-a"), Role::Ant, Some(&hub), &out("dev-a")).unwrap();
    assert_eq!(renewal("dev-a"), ["ants auth user dev-a --js-domain hub"]);
    issue(&team, &p("alice"), Role::Human, None, &out("alice")).unwrap();
    assert_eq!(renewal("alice"), ["ants auth user alice --human"]);
    let destinations = [Destination::Ant(p("dev-a")), Destination::Channel("ops".parse().unwrap())];
    let hook = Permissions::for_webhook(&p("hook"), &destinations);
    issue_with(&team, &p("hook"), Kind::Webhook, hook, &out("hook")).unwrap();
    assert_eq!(renewal("hook"), ["ants auth user hook --webhook --to dev-a --channel ops"]);
    issue_channel_manager(&team, None, &out("channel-manager")).unwrap();
    assert_eq!(renewal("channel-manager"), ["ants auth channel-manager"]);
    let m1: Machine = "m1".parse().unwrap();
    let members = members(&[p("dev-a")], &[p("alice")]);
    issue_leaf(&team, &m1, &members, &[p("hook")], EnrollApprover::With, Some(&hub), &out("leaf-m1")).unwrap();
    assert_eq!(
        renewal("leaf-m1"),
        [
            "ants auth leaf m1 --ant dev-a --human alice --webhook hook --enroll-approver --js-domain hub",
            "ants leaf outbox m1 --ant dev-a --human alice --js-domain hub",
        ]
    );
    let system = creds::check(&leaf_system_creds(&out("leaf-m1"))).unwrap();
    assert_eq!(
        (system.kind.clone(), system.version_status()),
        (creds::Kind::LeafSystem { machine: m1.clone() }, VersionStatus::Current)
    );
    let (jwt, _) = read_creds(&out("leaf-m1")).unwrap();
    let more = Permissions::for_leaf(&m1, [(&p("dev-c"), Role::Ant)]).with_uplink_js_domain(Some(&hub));
    let renewed = creds::Finding::from_jwt(&extend_leaf(&team, &jwt, &m1, more).unwrap()).unwrap();
    assert_eq!(renewed.version_status(), VersionStatus::Current);
    assert!(renewed.renew()[0].starts_with("ants auth leaf m1 --ant dev-a --ant dev-c --human alice"), "{renewed:?}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// Credentials of every kind of user fit in its `CONNECT` line with room for the rest of it (signature, name and
/// options, some 400 bytes) and for rights yet to come, 3 KiB in all: nats-server closes a connection whose line is
/// longer than `max_control_line`. A JWT grows with the user's name and the JetStream domain, so both are the longest
/// allowed.
#[test]
fn credentials_of_the_longest_names_fit_max_control_line() {
    let dir = ants_test_support::scratch_dir("jwt-size");
    let team = dir.join("auth");
    init(&team).unwrap();
    let (ant, human, assistant) = (p(&"a".repeat(64)), p(&"h".repeat(64)), p(&"p".repeat(64)));
    let hub: JsDomain = "d".repeat(64).parse().unwrap();
    issue(&team, &human, Role::Human, None, &dir.join("human.creds")).unwrap();
    for domain in [None, Some(&hub)] {
        let out = dir.join("user.creds");
        let mut sizes = Vec::new();
        for (user, role) in [(&ant, Role::Ant), (&human, Role::Human)] {
            issue(&team, user, role, domain, &out).unwrap();
            sizes.push((format!("{role:?}"), read_creds(&out).unwrap().0.len()));
        }
        let new = NewDevice { assistant_of: Some(human.clone()), ..NewDevice::default() };
        let rights = Permissions::for_assistant(&assistant, &human).with_js_domain(domain);
        Issue::new(&team, &assistant, Kind::Assistant, rights).with(&new).into_file(&out).unwrap();
        sizes.push(("Assistant".to_owned(), read_creds(&out).unwrap().0.len()));
        for (kind, size) in sizes {
            assert!(size + 3 * 1024 <= MAX_CONTROL_LINE as usize, "{kind}: {size} bytes with domain {domain:?}");
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// The longest credentials, an ant's of the longest name in the longest domain, connect to a server including the
/// team's server.conf and read through their inbox consumer.
#[tokio::test]
async fn the_longest_credentials_connect() {
    let domain: JsDomain = "d".repeat(64).parse().unwrap();
    let Some(hub) = Server::start_with(&format!("domain: {domain}"), "") else { return };
    let human = UserId::human();
    let h_js = connect_to(&hub.url, &hub.creds_in(&human, Some(&domain)), Some(&domain), &human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let ant = p(&"a".repeat(64));
    let js = connect_to(&hub.url, &hub.creds_in(&ant, Some(&domain)), Some(&domain), &ant).await;
    bus::open_stream(&js, &ant).await.unwrap();
}
