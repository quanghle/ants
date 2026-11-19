//! Property tests of the ids, subjects, keys and parsers of the wire contract.

use std::str::FromStr;
use std::time::Duration;

use ants_proto::{
    ChannelId, DeviceId, Envelope, Error, JsDomain, Machine, PermissionsVersion, Reaction, Role, ThreadId, ThreadTitle,
    UserId, Uuid, duration, subject,
};
use proptest::prelude::*;
use proptest::sample::Index;

/// A string any id built by `token_id!` takes: 1-64 of a-z, 0-9 and '-', starting with a letter or digit.
fn token() -> impl Strategy<Value = String> {
    "[a-z0-9][a-z0-9-]{0,63}"
}

/// A string a [`Machine`] takes: 1-64 of letters, digits, '-' and '_', starting with a letter or digit.
fn machine_name() -> impl Strategy<Value = String> {
    "[A-Za-z0-9][A-Za-z0-9_-]{0,63}"
}

fn user() -> impl Strategy<Value = UserId> {
    token().prop_map(|s| s.parse().unwrap())
}

fn thread() -> impl Strategy<Value = ThreadId> {
    token().prop_map(|s| s.parse().unwrap())
}

fn channel() -> impl Strategy<Value = ChannelId> {
    token().prop_map(|s| s.parse().unwrap())
}

fn reaction() -> impl Strategy<Value = Reaction> {
    token().prop_map(|s| s.parse().unwrap())
}

fn machine() -> impl Strategy<Value = Machine> {
    machine_name().prop_map(|s| s.parse().unwrap())
}

fn uuid() -> impl Strategy<Value = Uuid> {
    any::<u128>().prop_map(Uuid::from_u128)
}

/// Characters no id may hold: NATS subject metacharacters, whitespace and others outside both alphabets.
fn foreign_char() -> impl Strategy<Value = char> {
    prop::sample::select(vec!['.', '*', '>', ' ', '\t', '\n', '\r', '\0', '$', '/', ':', 'é', '🐜', '\u{a0}'])
}

/// `s` with `c` inserted at some position.
fn with_char(mut s: String, c: char, at: Index) -> String {
    s.insert(at.index(s.len() + 1), c);
    s
}

/// Strings no `token_id!` id takes.
fn invalid_token() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-z0-9][a-z0-9-]{64,100}",
        "-[a-z0-9-]{0,63}",
        (token(), foreign_char(), any::<Index>()).prop_map(|(s, c, at)| with_char(s, c, at)),
        (token(), prop::sample::select(vec!['A', 'Z', '_']), any::<Index>()).prop_map(|(s, c, at)| with_char(s, c, at)),
    ]
}

/// Strings no [`Machine`] takes.
fn invalid_machine() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[A-Za-z0-9][A-Za-z0-9_-]{64,100}",
        "[-_][A-Za-z0-9_-]{0,63}",
        (machine_name(), foreign_char(), any::<Index>()).prop_map(|(s, c, at)| with_char(s, c, at)),
    ]
}

/// What the `token_id!` ids take, as their error message states it.
fn is_token(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !s.starts_with('-')
}

/// What a [`Machine`] takes, as its error message states it.
fn is_machine_name(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        && s.starts_with(|c: char| c.is_ascii_alphanumeric())
}

/// Whether `s` is safe as one NATS subject token: not empty, no '.', no wildcard, no whitespace.
fn is_subject_token(s: &str) -> bool {
    !s.is_empty() && s != "*" && s != ">" && !s.contains(|c: char| c == '.' || c.is_whitespace())
}

fn tokens(subject: &str) -> Vec<String> {
    subject.split('.').map(str::to_owned).collect()
}

/// The same properties for every id `token_id!` defines.
macro_rules! token_id_properties {
    ($module:ident, $id:ty) => {
        mod $module {
            use super::*;

            proptest! {
                #[test]
                fn valid_tokens_round_trip(s in token()) {
                    let id: $id = s.parse().unwrap();
                    prop_assert_eq!(id.as_str(), s.as_str());
                    prop_assert_eq!(&id.to_string().parse::<$id>().unwrap(), &id);
                    prop_assert_eq!(&<$id>::try_from(s.clone()).unwrap(), &id);
                    let json = serde_json::to_string(&id).unwrap();
                    prop_assert_eq!(&json, &serde_json::to_string(&s).unwrap());
                    prop_assert_eq!(&serde_json::from_str::<$id>(&json).unwrap(), &id);
                    prop_assert_eq!(String::from(id), s);
                }

                #[test]
                fn invalid_tokens_are_rejected(s in invalid_token()) {
                    let rejected = matches!(s.parse::<$id>(), Err(Error::InvalidId { value, .. }) if value == s);
                    prop_assert!(rejected);
                    prop_assert!(<$id>::try_from(s.clone()).is_err());
                    prop_assert!(serde_json::from_str::<$id>(&serde_json::to_string(&s).unwrap()).is_err());
                }

                #[test]
                fn any_string_parses_as_the_rule_says(s in prop_oneof![any::<String>(), "[a-z0-9.*> _A-]{0,70}"]) {
                    let parsed = <$id>::from_str(&s);
                    prop_assert_eq!(parsed.is_ok(), is_token(&s));
                    if let Ok(id) = parsed {
                        prop_assert!(is_subject_token(id.as_str()));
                    }
                }
            }
        }
    };
}

token_id_properties!(user_id, UserId);
token_id_properties!(thread_id, ThreadId);
token_id_properties!(channel_id, ChannelId);
token_id_properties!(reaction_id, Reaction);
token_id_properties!(js_domain, JsDomain);

proptest! {
    #[test]
    fn machines_round_trip(s in machine_name()) {
        let machine: Machine = s.parse().unwrap();
        prop_assert_eq!(machine.as_str(), s.as_str());
        prop_assert_eq!(&machine.to_string().parse::<Machine>().unwrap(), &machine);
        prop_assert_eq!(&Machine::try_from(s.clone()).unwrap(), &machine);
        let json = serde_json::to_string(&machine).unwrap();
        prop_assert_eq!(&json, &serde_json::to_string(&s).unwrap());
        prop_assert_eq!(&serde_json::from_str::<Machine>(&json).unwrap(), &machine);
        prop_assert_eq!(String::from(machine), s);
    }

    #[test]
    fn invalid_machines_are_rejected(s in invalid_machine()) {
        prop_assert_eq!(s.parse::<Machine>(), Err(Error::InvalidMachine(s.clone())));
        prop_assert!(serde_json::from_str::<Machine>(&serde_json::to_string(&s).unwrap()).is_err());
    }

    #[test]
    fn any_string_parses_as_a_machine_as_the_rule_says(s in prop_oneof![any::<String>(), "[A-Za-z0-9.*> _-]{0,70}"]) {
        let parsed = s.parse::<Machine>();
        prop_assert_eq!(parsed.is_ok(), is_machine_name(&s));
        if let Ok(machine) = parsed {
            prop_assert!(is_subject_token(machine.as_str()));
        }
    }

    #[test]
    fn subjects_parse_back_to_their_parts(to in user(), from in user(), thread in thread(), channel in channel()) {
        prop_assert_eq!(subject::parse_inbox(&subject::inbox(&to, &from)), Some((to.clone(), from.clone())));
        prop_assert_eq!(subject::parse_notes(&subject::notes(&thread, &from)), Some((thread, from.clone())));
        prop_assert_eq!(subject::parse_chan(&subject::chan(&channel, &from)), Some((channel.clone(), from.clone())));
        let stored = format!("{}{from}", subject::stored_inbox(&to).strip_suffix('*').unwrap());
        prop_assert_eq!(subject::parse_stored_inbox(&to, &stored), Some(from.clone()));
        prop_assert_eq!(subject::parse_chanadm(&subject::chanadm(&channel, &from)), Some((channel, from)));
    }

    #[test]
    fn subjects_parse_only_as_their_own_kind(to in user(), from in user(), thread in thread(), channel in channel()) {
        let inbox = subject::inbox(&to, &from);
        let notes = subject::notes(&thread, &from);
        let chan = subject::chan(&channel, &from);
        let chanadm = subject::chanadm(&channel, &from);
        for other in [&notes, &chan, &chanadm] {
            prop_assert_eq!(subject::parse_inbox(other), None);
        }
        for other in [&inbox, &chan, &chanadm] {
            prop_assert_eq!(subject::parse_notes(other), None);
        }
        for other in [&inbox, &notes, &chanadm] {
            prop_assert_eq!(subject::parse_chan(other), None);
        }
        for other in [&inbox, &notes, &chan] {
            prop_assert_eq!(subject::parse_chanadm(other), None);
        }
    }

    #[test]
    fn stored_inbox_subjects_parse_only_for_their_user(user in user(), other in user(), from in user()) {
        let stored = format!("{}{from}", subject::stored_inbox(&other).strip_suffix('*').unwrap());
        prop_assert_eq!(subject::parse_stored_inbox(&user, &stored), (user == other).then_some(from));
    }

    #[test]
    fn parsed_subject_parts_are_single_tokens(
        s in prop_oneof![any::<String>(), "ants\\.(inbox|notes|chan|chanadm)\\.[a-z0-9.*> -]{0,40}"],
        user in user(),
    ) {
        let parts: Vec<String> = [
            subject::parse_inbox(&s).map(|(a, b)| [a.to_string(), b.to_string()]),
            subject::parse_notes(&s).map(|(a, b)| [a.to_string(), b.to_string()]),
            subject::parse_chan(&s).map(|(a, b)| [a.to_string(), b.to_string()]),
            subject::parse_chanadm(&s).map(|(a, b)| [a.to_string(), b.to_string()]),
        ]
        .into_iter()
        .flatten()
        .flatten()
        .collect();
        for part in &parts {
            prop_assert!(is_subject_token(part), "{part:?} from {s:?}");
        }
        if !parts.is_empty() {
            prop_assert_eq!(tokens(&s).len(), 4);
        }
        if let Some(from) = subject::parse_stored_inbox(&user, &s) {
            prop_assert_eq!(tokens(&s), vec!["_INBOX", user.as_str(), "stored", from.as_str()]);
        }
    }

    #[test]
    fn built_subjects_hold_their_ids_as_whole_tokens(
        ant in user(), other in user(), thread in thread(), channel in channel(), message in uuid(),
    ) {
        let (a, o, t, c) = (ant.as_str(), other.as_str(), thread.as_str(), channel.as_str());
        let m = message.to_string();
        let expected: Vec<(String, Vec<&str>)> = vec![
            (subject::inbox(&ant, &other), vec!["ants", "inbox", a, o]),
            (subject::notes(&thread, &ant), vec!["ants", "notes", t, a]),
            (subject::chan(&channel, &ant), vec!["ants", "chan", c, a]),
            (subject::chanadm(&channel, &ant), vec!["ants", "chanadm", c, a]),
            (subject::approvals(&ant), vec!["ants", "approvals", a]),
            (subject::approval_decision(&ant, &message), vec!["ants", "approvals", a, &m]),
            (subject::dead(&ant), vec!["ants", "dead", a]),
            (subject::enroll(&ant), vec!["ants", "enroll", a]),
            (subject::enroll_reply(&ant), vec!["ants", "enroll", a, "reply"]),
            (subject::antreq(&ant), vec!["ants", "antreq", a]),
            (subject::antreq_reply(&ant), vec!["ants", "antreq", a, "reply"]),
            (subject::reactions(&ant), vec!["ants", "reactions", a]),
            (subject::react(&ant), vec!["ants", "reactions", a, "react"]),
            (subject::reactions_reply(&ant), vec!["_INBOX", a, "reactions"]),
            (subject::save_trail(&ant), vec!["ants", "savetrail", a]),
            (subject::save_trail_reply(&ant), vec!["_INBOX", a, "savetrail"]),
            (subject::status(&ant, &thread), vec!["ants", "status", a, t]),
            (subject::cancel(&ant, &thread), vec!["ants", "cancel", a, t]),
            (subject::cancelled(&ant, &thread), vec!["ants", "cancelled", a, t]),
            (subject::queue(&ant), vec!["ants", "queue", a]),
            (subject::queued(&ant), vec!["ants", "queued", a]),
            (subject::colony(&ant), vec!["$KV", ants_proto::COLONY_BUCKET, a]),
            (subject::chain(&ant), vec!["$KV", ants_proto::CHAINS_BUCKET, a]),
            (subject::human_entry(&ant), vec!["$KV", ants_proto::HUMANS_BUCKET, a]),
            (subject::read_marker(&ant), vec!["$KV", ants_proto::READ_BUCKET, a]),
            (subject::channel_reads(&ant), vec!["$KV", ants_proto::READ_BUCKET, a, "chan"]),
            (ants_proto::channel_reads_key(&ant), vec![a, "chan"]),
            (subject::notify_prefs(&ant), vec!["$KV", ants_proto::NOTIFY_BUCKET, a]),
            (subject::notify_held(&ant), vec!["$KV", ants_proto::NOTIFY_BUCKET, "held", a]),
            (ants_proto::held_key(&ant), vec!["held", a]),
            (subject::channel_entry(&channel), vec!["$KV", ants_proto::CHANNELS_BUCKET, c]),
            (ants_proto::inbox_prefix(&ant), vec!["_INBOX", a]),
            (ants_proto::session_key(&ant, &thread), vec![a, t]),
            (ants_proto::thread_spend_key(&ant, &thread), vec![a, t, "spend"]),
            (ants_proto::channel_seen_key(&ant, &thread), vec![a, t, "seen"]),
            (ants_proto::escalation_deadlines_key(&ant), vec![a, "escalations", "deadlines"]),
            (ants_proto::message_mark_key(&ant, &message), vec![a, "msg", &m]),
            (ants_proto::unserved_key(&ant, &message), vec![a, "unserved", &m]),
            (ants_proto::reaction_key(&ant, &message, &"eyes".parse().unwrap()), vec![a, &m, "eyes"]),
            (ants_proto::link_key(&ant, &thread, &other), vec![a, t, o]),
            (ants_proto::link_outbox(&ant, &thread, &other), vec!["ants", "out", "links", a, t, o]),
            (ants_proto::owner_key(&thread, &other), vec![t, o]),
            (ants_proto::owner_outbox(&thread, &other), vec!["ants", "out", "owners", t, o]),
            (ants_proto::title_key(&thread), vec!["title", t]),
            (ants_proto::done_key(&thread), vec!["done", t]),
            (ants_proto::marks_key(&ant), vec!["marks", a]),
            (ants_proto::assisted_marks_key(&ant, &other), vec!["marks", a, o]),
            (ants_proto::assisted_reminders_key(&ant, &other), vec!["reminders", a, o]),
            (ants_proto::assisted_withdrawal_key(&ant, &message, &other), vec![a, "msg", &m, o]),
            (ants_proto::webhook_marker_key(&ant), vec![a, "webhook"]),
            (ants_proto::webhook_marker_subject(&ant), vec!["$KV", ants_proto::WEBHOOKS_BUCKET, a, "webhook"]),
            // Patterns: their wildcards stand only where these say.
            (subject::inbox_of(&ant), vec!["ants", "inbox", a, "*"]),
            (subject::inbox_from(&ant), vec!["ants", "inbox", "*", a]),
            (subject::stored_inbox(&ant), vec!["_INBOX", a, "stored", "*"]),
            (subject::notes_of(&thread), vec!["ants", "notes", t, "*"]),
            (subject::notes_from(&ant), vec!["ants", "notes", "*", a]),
            (subject::chan_room(&channel), vec!["ants", "chan", c, "*"]),
            (subject::chan_from(&ant), vec!["ants", "chan", "*", a]),
            (subject::chanadm_from(&ant), vec!["ants", "chanadm", "*", a]),
            (subject::status_of(&ant), vec!["ants", "status", a, "*"]),
            (subject::status_on(&thread), vec!["ants", "status", "*", t]),
            (subject::cancel_of(&ant), vec!["ants", "cancel", a, "*"]),
            (subject::cancelled_of(&ant), vec!["ants", "cancelled", a, "*"]),
            (subject::presence_of(&ant), vec!["$KV", ants_proto::PRESENCE_BUCKET, a, ">"]),
            (subject::devices_of(&ant), vec!["$KV", ants_proto::DEVICES_BUCKET, a, ">"]),
            (subject::revoked_devices(), vec!["$KV", ants_proto::DEVICES_BUCKET, "revoked", "*"]),
            (ants_proto::reactions_to(&message), vec!["*", &m, "*"]),
            (ants_proto::links_to(&thread), vec!["*", t, "*"]),
        ];
        for (built, parts) in expected {
            prop_assert_eq!(tokens(&built), parts);
        }
    }

    #[test]
    fn consumer_names_are_single_tokens_apart_from_their_spares(ant in user()) {
        let names = [
            ants_proto::inbox_consumer(&ant),
            ants_proto::notify_consumer(&ant),
            ants_proto::notes_consumer(&ant),
            ants_proto::chan_consumer(&ant),
            ants_proto::chan_reader(&ant),
            ants_proto::dead_reader(&ant),
            ants_proto::sent_reader(&ant),
        ];
        let prefixes = ["inbox-", "notify-", "notes-", "chan-", "chanread-", "deadread-", "sentread-"];
        for name in &names {
            prop_assert!(is_subject_token(name), "{name:?}");
            for spare in ants_proto::spare_readers(name) {
                prop_assert!(is_subject_token(&spare), "{spare:?}");
                let anyones = prefixes
                    .iter()
                    .any(|prefix| spare.strip_prefix(prefix).is_some_and(|rest| rest.parse::<UserId>().is_ok()));
                prop_assert!(!anyones, "spare {spare:?} is some user's own consumer");
            }
        }
    }

    #[test]
    fn machine_subjects_hold_the_machine_as_one_token(machine in machine(), human in user(), ant in user()) {
        let m = machine.as_str();
        prop_assert_eq!(tokens(&machine.api_prefix()), vec!["$JS", m, "API"]);
        prop_assert_eq!(tokens(&machine.deliver_prefix()), vec!["$JS", "S", m]);
        prop_assert_eq!(tokens(&machine.source_replies()), vec!["$JSC", "R", m, "*", ants_proto::OUTBOX_STREAM]);
        prop_assert_eq!(tokens(&machine.setup_inbox(&human)), vec!["_INBOX", human.as_str(), "leaf", m]);
        prop_assert_eq!(tokens(&subject::intake(&machine)), vec!["ants", "in", m, ">"]);
        for role in [Role::Human, Role::Ant] {
            for inbox in [true, false] {
                for (filter, destination) in machine.admitted(&ant, role, inbox) {
                    // The filter is the stored subject under the machine's intake and outbox prefixes, with each
                    // `*` numbered in turn as a placeholder of the destination.
                    let filter_tokens = tokens(&filter);
                    prop_assert_eq!(&filter_tokens[..5], &["ants", "in", m, "ants", "out"][..]);
                    let mut n = 0;
                    let expected: Vec<String> = filter_tokens[5..]
                        .iter()
                        .map(|token| if *token == "*" {
                            n += 1;
                            format!("{{{{wildcard({n})}}}}")
                        } else {
                            token.clone()
                        })
                        .collect();
                    prop_assert_eq!(destination, format!("ants.{}", expected.join(".")));
                }
            }
            // The chunks a user writes, of any sender for a human, under the same handles.
            let (filter, destination) = machine.admitted_files(&ant, role);
            let handles = filter.strip_prefix(&format!("ants.in.{m}.ants.out.files.")).unwrap_or_default();
            prop_assert_eq!(handles, if role.is_human() { ">".to_owned() } else { format!("{ant}.>") });
            prop_assert_eq!(destination, format!("$O.ants_files.C.{handles}"));
        }
    }

    #[test]
    fn outboxes_take_only_buffered_kinds(s in prop_oneof![any::<String>(), "ants\\.[a-z]{0,8}\\.[a-z.*>]{0,20}"]) {
        let buffered = ["ants.inbox.", "ants.notes.", "ants.chan.", "ants.dead."].iter().any(|p| s.starts_with(p));
        let outbox = subject::outbox(&s);
        prop_assert_eq!(outbox.is_some(), buffered);
        if let Some(outbox) = outbox {
            prop_assert_eq!(outbox, format!("ants.out.{}", &s["ants.".len()..]));
        }
    }

    #[test]
    fn renewable_names_are_single_tokens(
        name in prop_oneof![any::<String>(), "(leaf-)?[A-Za-z0-9.*> _-]{0,70}"],
        user in user(),
        machine in machine(),
    ) {
        if subject::renewable_name(&name) {
            prop_assert!(is_subject_token(&name), "{name:?}");
            prop_assert_eq!(tokens(&subject::renew(&name)), vec!["ants", "renew", name.as_str()]);
        }
        prop_assert!(subject::renewable_name(user.as_str()));
        let leaf = format!("leaf-{machine}");
        prop_assert!(subject::renewable_name(&leaf));
    }

    #[test]
    fn envelopes_check_their_own_subject(thread in thread(), from in user(), to in user(), text in ".{0,20}") {
        let envelope = Envelope::on_thread(thread, from.clone(), to.clone(), text);
        prop_assert_eq!(subject::parse_inbox(&envelope.subject()), Some((to.clone(), from.clone())));
        prop_assert!(envelope.check_subject(&envelope.subject()).is_ok());
        prop_assert_eq!(envelope.check_subject(&subject::inbox(&from, &to)).is_ok(), from == to);
    }

    #[test]
    fn reaction_keys_round_trip(by in user(), message in uuid(), reaction in reaction()) {
        let key = ants_proto::reaction_key(&by, &message, &reaction);
        prop_assert_eq!(ants_proto::parse_reaction_key(&key), Some((by, message, reaction)));
    }

    #[test]
    fn link_keys_round_trip_on_their_thread_only(from in user(), thread in thread(), other in thread(), to in user()) {
        let key = ants_proto::link_key(&from, &thread, &to);
        prop_assert_eq!(ants_proto::parse_link_key(&key, &thread), Some((from.clone(), to.clone())));
        prop_assert_eq!(ants_proto::parse_link_key(&key, &other), (other == thread).then_some((from, to)));
    }

    #[test]
    fn key_parsers_take_only_whole_keys(s in prop_oneof![any::<String>(), "[a-z0-9.*>-]{0,60}"], thread in thread()) {
        if let Some((by, message, reaction)) = ants_proto::parse_reaction_key(&s) {
            prop_assert_eq!(tokens(&s).len(), 3);
            prop_assert!(is_subject_token(by.as_str()) && is_subject_token(reaction.as_str()));
            prop_assert_eq!(
                ants_proto::parse_reaction_key(&ants_proto::reaction_key(&by, &message, &reaction)),
                Some((by, message, reaction))
            );
        }
        if let Some((from, to)) = ants_proto::parse_link_key(&s, &thread) {
            prop_assert_eq!(ants_proto::link_key(&from, &thread, &to), s);
        }
    }

    #[test]
    fn device_ids_round_trip(id in uuid(), others in prop::collection::vec(".{0,20}", 0..4)) {
        let device: DeviceId = id.to_string().parse().unwrap();
        prop_assert_eq!(device.to_string(), id.to_string());
        prop_assert_eq!(device.to_string().parse::<DeviceId>().unwrap(), device);
        let tag = device.tag();
        let tags = others.iter().map(String::as_str).filter(|t| !t.starts_with(DeviceId::TAG)).chain([tag.as_str()]);
        prop_assert_eq!(DeviceId::from_tags(tags), Some(device));
    }

    #[test]
    fn permissions_versions_round_trip(version in any::<u32>()) {
        let version = PermissionsVersion(version);
        prop_assert_eq!(PermissionsVersion::from_tags([version.tag().as_str()]), Some(version));
    }

    #[test]
    fn tag_parsers_never_panic(tags in prop::collection::vec(prop_oneof![any::<String>(), "ants-(device|perms):.{0,40}"], 0..4)) {
        let _ = DeviceId::from_tags(tags.iter().map(String::as_str));
        let _ = PermissionsVersion::from_tags(tags.iter().map(String::as_str));
    }

    #[test]
    fn mentions_are_found_once_in_order(
        ids in prop::collection::vec(user(), 0..6),
        opens in prop::collection::vec(prop::sample::select(vec!["", "(", "[", "{", "<", "\"", "'", "`"]), 6),
        ends in prop::collection::vec(prop::sample::select(vec!["", ",", ".", ":", ";", "!", "?", ")"]), 6),
        gap in prop::sample::select(vec![" ", "\n", "\t", "  "]),
    ) {
        let text = ids
            .iter()
            .zip(&opens)
            .zip(&ends)
            .map(|((id, open), end)| format!("{open}@{id}{end}"))
            .collect::<Vec<_>>()
            .join(gap);
        let mut expected: Vec<UserId> = Vec::new();
        for id in &ids {
            if !expected.contains(id) {
                expected.push(id.clone());
            }
        }
        prop_assert_eq!(ants_proto::parse_mentions(&text), expected);
    }

    #[test]
    fn glued_mentions_are_none(id in user(), before in "[a-zA-Z0-9_@.é]", after in "[A-Z_@é]|\\.[a-z0-9]") {
        prop_assert_eq!(ants_proto::parse_mentions(&format!("{before}@{id}")), Vec::<UserId>::new());
        prop_assert_eq!(ants_proto::parse_mentions(&format!("@{id}{after}")), Vec::<UserId>::new());
    }

    #[test]
    fn mentions_are_in_the_text(text in prop_oneof![any::<String>(), "[ @a-z.,(é🐜-]{0,40}"]) {
        let mentions = ants_proto::parse_mentions(&text);
        for (i, id) in mentions.iter().enumerate() {
            let mention = format!("@{id}");
            prop_assert!(text.contains(&mention));
            prop_assert!(!mentions[..i].contains(id));
        }
    }

    #[test]
    fn titles_are_trimmed_single_lines(
        title in prop_oneof![any::<String>(), "[ \ta-z\n\u{2028}\u{2029}]{0,130}"],
        thread in thread(),
        by in user(),
    ) {
        match ThreadTitle::new(thread.clone(), &title, by.clone()) {
            Ok(set) => {
                prop_assert_eq!(set.title.as_str(), title.trim());
                prop_assert!((1..=ants_proto::MAX_TITLE).contains(&set.title.chars().count()));
                let breaks_line = |c: char| c.is_control() || matches!(c, '\u{2028}' | '\u{2029}');
                prop_assert!(!set.title.chars().any(breaks_line));
                prop_assert_eq!(ThreadTitle::new(thread, &set.title, by).unwrap().title, set.title);
            }
            Err(error) => prop_assert_eq!(error, Error::InvalidTitle(title)),
        }
    }

    #[test]
    fn durations_format_and_parse_back(secs in any::<u64>(), nanos in 0..1_000_000_000u32) {
        let formatted = duration::format(Duration::new(secs, nanos));
        prop_assert_eq!(duration::parse(&formatted), Ok(Duration::from_secs(secs)));
        prop_assert!(formatted.chars().all(|c| c.is_ascii_digit() || "dhms".contains(c)));
    }

    #[test]
    fn durations_add_up_their_units(
        counts in prop::collection::vec(prop::option::of(0..10_000_000u64), 4),
    ) {
        let units = [('d', 86_400), ('h', 3_600), ('m', 60), ('s', 1)];
        let text: String =
            counts.iter().zip(units).filter_map(|(n, (unit, _))| n.map(|n| format!("{n}{unit}"))).collect();
        let secs: u64 = counts.iter().zip(units).map(|(n, (_, size))| n.unwrap_or(0) * size).sum();
        if text.is_empty() {
            prop_assert!(duration::parse(&text).is_err());
        } else {
            prop_assert_eq!(duration::parse(&text), Ok(Duration::from_secs(secs)));
        }
    }

    #[test]
    fn durations_with_units_out_of_order_are_rejected(
        (a, b) in (0..4usize).prop_flat_map(|a| (Just(a), 0..=a)),
        x in 0..1000u64,
        y in 0..1000u64,
    ) {
        let units = ['d', 'h', 'm', 's'];
        let text = format!("{x}{}{y}{}", units[a], units[b]);
        prop_assert!(duration::parse(&text).is_err(), "{text:?}");
    }

    #[test]
    fn durations_parse_only_digits_and_units(text in prop_oneof![any::<String>(), "[0-9dhms +.-]{0,30}"]) {
        if let Ok(parsed) = duration::parse(&text) {
            prop_assert!(text.chars().all(|c| c.is_ascii_digit() || "dhms".contains(c)), "{text:?}");
            prop_assert_eq!(duration::parse(&duration::format(parsed)), Ok(parsed));
        }
    }
}
