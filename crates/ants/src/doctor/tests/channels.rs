//! Tests of the doctor's channel check: members, messengers and owners that are neither ants of the bus nor humans.

use std::collections::BTreeSet;

use ants_proto::{ChannelEntry, Humans};

use super::super::channels::findings;
use super::*;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

#[test]
fn channels_naming_ants_of_the_bus_and_humans_are_sound_and_no_channels_too() {
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(p("dev-a"));
    ops.add_member(p("alice"));
    (ops.messenger, ops.owner) = (Some(p("dev-a")), Some(p("dev-a")));
    let checks = findings(&[ops], &BTreeSet::from([p("dev-a")]), &Humans::of([p("alice")]));
    assert_eq!(checks.len(), 1, "{checks:?}");
    assert_eq!(checks[0].status, Status::Ok, "{}", checks[0]);
    assert_eq!(checks[0].detail, "1 channel, its members, messenger and owner ants of the bus or humans");
    let checks = findings(&[], &BTreeSet::new(), &Humans::default());
    assert_eq!((checks[0].status, checks[0].detail.as_str()), (Status::Ok, "no channels"));
}

#[test]
fn a_member_messenger_or_owner_neither_ant_nor_human_is_warned_of_with_the_command_taking_it_out() {
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(p("dev-gone"));
    ops.add_member(p("bob"));
    ops.add_member(p("alice"));
    (ops.messenger, ops.owner) = (Some(p("dev-gone")), Some(p("dev-gone")));
    let checks = findings(&[ops], &BTreeSet::new(), &Humans::of([p("alice")]));
    let shown: Vec<(Status, &str, &[String])> =
        checks.iter().map(|c| (c.status, c.detail.as_str(), c.fix.as_slice())).collect();
    assert_eq!(
        shown,
        [
            (
                Status::Warn,
                "#ops holds dev-gone, neither an ant of the bus nor a human",
                &["ants channel remove ops dev-gone".to_owned()][..]
            ),
            (
                Status::Warn,
                "#ops holds bob, neither an ant of the bus nor a human",
                &["ants channel remove ops bob".to_owned()][..]
            ),
            (
                Status::Warn,
                "the messenger of #ops, dev-gone, is not an ant of the bus: humans' posts mentioning nobody go \
                 untriaged",
                &["ants channel set ops --default-messenger".to_owned()][..]
            ),
            (
                Status::Warn,
                "the owner of #ops, dev-gone, is not an ant of the bus",
                &["ants channel set ops --no-owner".to_owned()][..]
            ),
        ]
    );
}
