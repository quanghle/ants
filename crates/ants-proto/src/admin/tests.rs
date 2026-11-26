//! Tests of admin orders: JSON form and canonical signed lines, only admin-signed orders carried out, assistants'
//! orders, order rules, outcomes, and changes expecting what was read.

use std::assert_matches;

use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
use serde_json::json;

use super::*;
use crate::p;
use crate::{ADMIN_TAG, AssistantOf};

fn m(s: &str) -> Machine {
    s.parse().unwrap()
}

struct Team {
    account: KeyPair,
    admin: KeyPair,
}

impl Team {
    fn new() -> Self {
        Self { account: KeyPair::new_account(), admin: KeyPair::new_user() }
    }

    fn account(&self) -> String {
        self.account.public_key()
    }

    fn jwt(&self, name: &str, tags: &[&str], exp: Option<i64>) -> String {
        let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
        let mut claims = json!({
            "sub": self.admin.public_key(), "iss": self.account.public_key(), "name": name,
            "nats": { "type": "user", "tags": tags },
        });
        if let Some(exp) = exp {
            claims["exp"] = json!(exp);
        }
        let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
        let signature = BASE64URL_NOPAD.encode(&self.account.sign(format!("{header}.{body}").as_bytes()).unwrap());
        format!("{header}.{body}.{signature}")
    }

    fn order(&self, op: AdminOp, jwt: String) -> AdminOrder {
        self.signed(AdminOrder::new(p("alice"), m("box"), p("dev-a"), op, jwt))
    }

    fn signed(&self, mut order: AdminOrder) -> AdminOrder {
        order.signature = BASE64URL_NOPAD.encode(&self.admin.sign(&order.signed()).unwrap());
        order
    }
}

fn set() -> AdminOp {
    AdminOp::Set {
        changes: vec![
            FieldChange {
                field: vec!["model".to_owned()],
                expected: None,
                expected_digest: None,
                value: Some(json!("gpt-5")),
            },
            FieldChange {
                field: vec!["caps".to_owned(), "daily".to_owned()],
                expected: Some(json!({ "usd": 5, "tokens": 100 })),
                expected_digest: None,
                value: None,
            },
        ],
    }
}

#[test]
fn an_order_travels_as_json_and_signs_canonical_lines() {
    let team = Team::new();
    let order = team.order(set(), team.jwt("alice", &[ADMIN_TAG], None));
    let json = serde_json::to_string(&order).unwrap();
    assert_eq!(serde_json::from_str::<AdminOrder>(&json).unwrap(), order);
    assert_eq!(order.subject(), "ants.admin.box.dev-a");
    assert_eq!(order.outcome_key(), format!("box.dev-a.{}", order.id));
    assert_eq!(admin_outcome_of(&order.outcome_key()), Some((m("box"), p("dev-a"), order.id)));
    assert_eq!(admin_outcome_of("box"), None, "a machine's record");
    assert_eq!(admin_outcome_of(&format!("box.dev-a.{}.x", order.id)), None);

    let signed = String::from_utf8(order.signed()).unwrap();
    let lines: Vec<&str> = signed.lines().collect();
    let sent_at = order.sent_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
    assert_eq!(lines[..6], [ORDER_SIGNING_CONTEXT, &order.id.to_string(), "alice", "box", "dev-a", &sent_at]);
    let op = r#"{"changes":[{"field":["model"],"value":"gpt-5"},{"expected":{"tokens":100,"usd":5},"field":["caps","daily"]}],"kind":"set"}"#;
    assert_eq!(lines[6..], [op]);
    let value = json!({"b": [1, {"d": null, "c": "x\"y"}], "a": true});
    assert_eq!(canonical_json(&value), r#"{"a":true,"b":[1,{"c":"x\"y","d":null}]}"#);
    assert_eq!(set().describe(), r#"set model = "gpt-5", caps.daily removed"#);
    assert_eq!(AdminOp::Restart { now: true }.describe(), "restart now");
    let restart: AdminOp = serde_json::from_str(r#"{"kind":"restart"}"#).unwrap();
    assert_eq!(restart, AdminOp::Restart { now: false });
}

#[test]
fn a_daemon_carries_out_only_what_an_admin_signed() {
    let team = Team::new();
    let account = team.account();
    let order = team.order(AdminOp::Pause, team.jwt("alice", &[ADMIN_TAG], None));
    let stored_at = order.sent_at + TimeDelta::seconds(1);
    let at = |order: &AdminOrder, revoked: bool, stored_at| {
        order.verify(&order.subject(), [account.as_str()], |_| revoked, stored_at)
    };
    let check = |order: &AdminOrder| at(order, false, stored_at);
    let verified = check(&order).unwrap();
    assert_eq!((verified.id, verified.public_key.as_str()), (p("alice"), team.admin.public_key().as_str()));
    assert_eq!(order.signing_key(), Some(team.admin.public_key()));

    let mut tampered = order.clone();
    tampered.op = AdminOp::Retire;
    assert_matches!(check(&tampered), Err(OrderError::Credentials(JwtError::BadSignature(_))));
    let mut elsewhere = order.clone();
    elsewhere.machine = m("other");
    assert_matches!(check(&elsewhere), Err(OrderError::Credentials(JwtError::BadSignature(_))));
    let wrong = order.verify("ants.admin.other.dev-a", [account.as_str()], |_| false, stored_at);
    assert_matches!(wrong, Err(OrderError::WrongSubject(_)));

    let not_admin = team.order(AdminOp::Pause, team.jwt("alice", &[], None));
    assert_matches!(check(&not_admin), Err(OrderError::Credentials(JwtError::NotAdmin(_))));
    let other = Team::new();
    let foreign = other.order(AdminOp::Pause, other.jwt("alice", &[ADMIN_TAG], None));
    assert_matches!(check(&foreign), Err(OrderError::Credentials(JwtError::ForeignAccount(_))));
    let bobs = team.signed(AdminOrder { admin: p("bob"), ..order.clone() });
    assert_matches!(check(&bobs), Err(OrderError::NotTheAdmin { .. }));
    let key = order.verify(&order.subject(), [account.as_str()], |key| key == team.admin.public_key(), stored_at);
    assert_matches!(key, Err(OrderError::Revoked(_)));
    assert_matches!(at(&order, true, stored_at), Err(OrderError::Revoked(_)));

    assert!(at(&order, false, order.sent_at + TimeDelta::days(20)).is_ok(), "an order waits");
    let ahead = team.signed(AdminOrder { sent_at: stored_at + ORDER_CLOCK_SKEW + TimeDelta::seconds(1), ..order });
    assert_matches!(check(&ahead), Err(OrderError::Ahead(_)));

    let exp = Utc::now().timestamp() + 60;
    let expiring = team.order(AdminOp::Resume, team.jwt("alice", &[ADMIN_TAG], Some(exp)));
    let stored = expiring.sent_at + TimeDelta::seconds(1);
    assert!(at(&expiring, false, stored).is_ok(), "valid when stored");
    let expired = at(&expiring, false, stored + TimeDelta::days(3));
    assert_matches!(expired, Err(OrderError::Credentials(JwtError::Expired(_))), "stored after it expired");
}

/// An assistant's order for its human is signed by credentials naming the human, and signs a line naming it too, which
/// an admin's own order goes without, so its signature stays what it was.
#[test]
fn an_assistant_signs_an_order_for_its_human() {
    let team = Team::new();
    let account = team.account();
    let check = |order: &AdminOrder| {
        order.verify(&order.subject(), [account.as_str()], |_| false, order.sent_at + TimeDelta::seconds(1))
    };
    let own = AdminOrder::new(p("pa"), m("box"), p("dev-a"), AdminOp::Pause, team.jwt("pa", &[], None));
    let for_alice = team.signed(own.clone().on_behalf(Some(p("alice"))));
    let tagged = team.signed(AdminOrder {
        credentials: team.jwt("pa", &[&AssistantOf::tag(&p("alice"))], None),
        ..for_alice.clone()
    });
    assert_eq!(check(&tagged).unwrap().id, p("pa"));
    assert_matches!(check(&for_alice), Err(OrderError::Credentials(JwtError::NotAssistant { .. })));
    let for_bob = team.signed(AdminOrder { on_behalf_of: Some(p("bob")), ..tagged.clone() });
    assert_matches!(check(&for_bob), Err(OrderError::Credentials(JwtError::NotAssistant { .. })));
    let mut unsigned_claim = tagged.clone();
    unsigned_claim.signature = team.signed(AdminOrder { on_behalf_of: None, ..tagged.clone() }).signature;
    assert_matches!(check(&unsigned_claim), Err(OrderError::Credentials(JwtError::BadSignature(_))));
    let own = team.signed(AdminOrder { credentials: tagged.credentials.clone(), ..own });
    assert_matches!(check(&own), Err(OrderError::Credentials(_)), "an assistant is no admin");

    let lines = |order: &AdminOrder| String::from_utf8(order.signed()).unwrap().lines().count();
    assert_eq!((lines(&own), lines(&tagged)), (7, 8));
    assert_eq!(String::from_utf8(tagged.signed()).unwrap().lines().last(), Some("alice"));
    let json = serde_json::to_value(&own).unwrap();
    assert_eq!(json.get("on_behalf_of"), None);
    let outcome = AdminOutcome::of(&tagged, OrderState::Held, None, None);
    assert_eq!((outcome.state.as_str(), outcome.on_behalf_of), ("held", Some(p("alice"))));
    assert_eq!(held_orders_key(&m("box")), "box.held");
    assert_eq!(admin_outcome_of(&held_orders_key(&m("box"))), None);
}

#[test]
fn an_order_rule_covers_an_op_of_its_kind() {
    assert!(covers_order("order:*", &AdminOp::Retire));
    assert!(covers_order("order:pause", &AdminOp::Pause));
    assert!(covers_order("order:read_memories", &AdminOp::ReadMemories { identity: None }));
    for (rule, op) in [("order:resume", AdminOp::Pause), ("pause", AdminOp::Pause), ("execute:*", AdminOp::Pause)] {
        assert!(!covers_order(rule, &op), "{rule}");
    }
    let ops = [
        set(),
        AdminOp::Pause,
        AdminOp::Resume,
        AdminOp::Restart { now: true },
        AdminOp::Add { settings: Map::new(), assistant_of: None },
        AdminOp::Retire,
        AdminOp::ReadMemories { identity: None },
        AdminOp::ReadSettings,
        AdminOp::ReadSchedules,
        AdminOp::ReadMcp,
        AdminOp::McpSignIn { server: "github".to_owned(), callback: None },
        AdminOp::Clone { new: p("dev-b"), settings: Map::new() },
        AdminOp::ExportProvider { identity: "1f".to_owned() },
        AdminOp::ImportProvider {
            identity: "1f".to_owned(),
            payload: AdminPayload { id: Uuid::now_v7(), machine: m("box"), size: 2, sha256: "ab".to_owned() },
        },
        AdminOp::StopTrail { run: RunId::generate() },
        AdminOp::ResumeTrail { run: RunId::generate(), limits: None },
    ];
    for op in ops {
        assert_eq!(op.for_assistant(), ASSISTANT_ORDER_KINDS.contains(&op.kind()), "{}", op.kind());
        assert_eq!(serde_json::to_value(&op).unwrap()["kind"], op.kind());
    }
}

#[test]
fn an_outcome_names_what_was_ordered() {
    let team = Team::new();
    let order = team.order(AdminOp::ReadMemories { identity: None }, team.jwt("alice", &[ADMIN_TAG], None));
    let outcome = AdminOutcome::of(&order, OrderState::Applied, None, Some(json!({ "whoami": "x" })));
    assert_eq!(outcome.key(), order.outcome_key());
    let json = serde_json::to_value(&outcome).unwrap();
    assert_eq!((json["state"].as_str(), json["op"]["kind"].as_str()), (Some("applied"), Some("read_memories")));
    assert_eq!(serde_json::from_value::<AdminOutcome>(json).unwrap(), outcome);
    let unreadable = AdminOutcome::unreadable(order.id, m("box"), p("dev-a"), "malformed".to_owned());
    let json = serde_json::to_value(&unreadable).unwrap();
    assert_eq!(json.get("admin"), None);
    assert_eq!(json["reason"], "malformed");
}

#[test]
fn a_change_expects_what_the_settings_read_or_the_digest_of_a_secret() {
    let f = |path: &str| path.split('.').map(str::to_owned).collect::<Vec<_>>();
    let settings: Settings = serde_json::from_value(json!({
        "fields": { "model": "gpt-5", "env": { "TOKEN": "[redacted]", "RUST_LOG": "info" }, "caps": {} },
        "hidden": [{ "field": ["env"], "digest": "d1" }, { "field": ["env", "TOKEN"], "digest": "d2" }],
    }))
    .unwrap();
    let model = settings.change(f("model"), Some(json!("o3")));
    assert_eq!((model.expected, model.expected_digest), (Some(json!("gpt-5")), None));
    let token = settings.change(f("env.TOKEN"), None);
    assert_eq!((token.expected, token.expected_digest.as_deref()), (None, Some("d2")));
    assert_eq!(settings.change(f("env"), None).expected_digest.as_deref(), Some("d1"));
    let added = settings.change(f("env.TERM"), Some(json!("dumb")));
    assert_eq!((added.expected, added.expected_digest), (None, None), "absent: expected absent");
    assert_eq!(settings.get(&f("model.id")), None);
    let mut leaves: Vec<String> = settings.leaves().into_iter().map(|(field, _)| field.join(".")).collect();
    leaves.sort();
    assert_eq!(leaves, ["caps", "env.RUST_LOG", "env.TOKEN", "model"], "an empty table stands for itself");
}
