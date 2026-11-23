//! Tests of held orders: an order an assistant sent waits for its human to consent, and expires before the humans are
//! read.

use ants_proto::{AdminOp, AssistantGrant, Confirmations, PreAllow, UserId, confirmations_key, pre_allow_key};

use chrono::TimeDelta;

use super::*;
use crate::people::humans::TestDevice;
use ants_test_support::user;

fn grant(humans: &Registry, device: &TestDevice, assistant: Option<&str>) {
    let alice = user("alice");
    let mut grant = AssistantGrant::new(alice.clone(), assistant.map(user), Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
}

fn held(op: AdminOp) -> HeldOrder {
    let order =
        AdminOrder::new(user("pa"), "m1".parse().unwrap(), user("dev-a"), op, "").on_behalf(Some(user("alice")));
    HeldOrder { order, signer: "UPA".to_owned() }
}

fn answer(humans: &Registry, device: &TestDevice, held: &HeldOrder, answer: ActAnswer) {
    let (alice, order, now) = (user("alice"), &held.order, Utc::now());
    let act = Act::Order { machine: order.machine.clone(), ant: order.ant.clone(), id: order.id };
    let acts = Confirmations::answering(&[], &user("pa"), act, answer, now + ORDER_HOLD, now);
    let mut record = Confirmations::new(alice.clone(), acts, now, device.jwt(&alice));
    record.signature = device.sign(&record.signed());
    humans.put_record(&confirmations_key(&alice), Some(&serde_json::to_vec(&record).unwrap()));
}

fn pre_allow(humans: &Registry, device: &TestDevice, rules: &[&str]) {
    let alice = user("alice");
    let rules = rules.iter().map(|r| (*r).to_owned()).collect();
    let mut record = PreAllow::new(alice.clone(), user("pa"), rules, Utc::now(), device.jwt(&alice));
    record.signature = device.sign(&record.signed());
    humans.put_record(&pre_allow_key(&alice), Some(&serde_json::to_vec(&record).unwrap()));
}

/// An order waits for the human's answer or a rule of its kind, and is refused once denied, withdrawn or out of time.
#[test]
fn a_held_order_waits_for_its_human_to_consent() {
    let device = TestDevice::default();
    let humans = Registry::with(UserId::human(), &["alice"]).verifying_grants(Some(device.account()));
    let (pause, restart) = (held(AdminOp::Pause), held(AdminOp::Restart { now: false }));
    let now = Utc::now();
    grant(&humans, &device, Some("pa"));
    assert_eq!(consent(&humans, &pause, now), Consent::Waiting);

    pre_allow(&humans, &device, &["dev-a/order:pause", "dev-b/order:*", "*/execute:cargo test"]);
    assert_eq!(
        consent(&humans, &pause, now),
        Consent::Given { jwt: device.jwt(&user("alice")), how: "pre-allowed by order:pause".to_owned() }
    );
    assert_eq!(consent(&humans, &restart, now), Consent::Waiting, "a rule for another kind or ant");

    answer(&humans, &device, &restart, ActAnswer::Confirm);
    assert_eq!(
        consent(&humans, &restart, now),
        Consent::Given { jwt: device.jwt(&user("alice")), how: "confirmed".to_owned() }
    );
    answer(&humans, &device, &restart, ActAnswer::Deny);
    assert_eq!(consent(&humans, &restart, now), Consent::Refused("denied by alice".to_owned()));

    let late = now + ORDER_HOLD + TimeDelta::minutes(1);
    let read = held(AdminOp::ReadSettings);
    assert!(
        matches!(consent(&humans, &read, late), Consent::Refused(r) if r.contains("did not confirm it within 7 days"))
    );
    assert!(matches!(consent(&humans, &pause, late), Consent::Given { .. }), "a rule needs no confirmation in time");

    grant(&humans, &device, Some("pb"));
    for order in [&pause, &read] {
        assert_eq!(
            consent(&humans, order, now),
            Consent::Refused("withdrawn: the grant of alice no longer names pa".to_owned())
        );
    }
}

/// Before the humans' bucket is read, as while the hub has none, no grant stands, and an order is not refused as
/// withdrawn: it waits, and expires once out of time.
#[test]
fn a_held_order_expires_before_the_humans_are_read() {
    let humans = Registry::default();
    let pause = held(AdminOp::Pause);
    let now = Utc::now();
    assert_eq!(consent(&humans, &pause, now), Consent::Waiting);
    assert_eq!(
        consent(&humans, &pause, now + ORDER_HOLD + TimeDelta::minutes(1)),
        Consent::Refused("alice did not confirm it within 7 days".to_owned())
    );
}
