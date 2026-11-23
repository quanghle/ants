//! What web credentials allow ([`Permissions::for_web`]): those of a human's device that is a page of the web client,
//! marked by the user JWT tag [`WEB_TAG`], with the rights the page's views use and no other of a human's.

use super::{Permissions, kv_stream};
use crate::{
    HUMANS_BUCKET, OWNERS_BUCKET, READ_BUCKET, Role, STREAM, UserId, assistant_grant_key, inbox_prefix, subject,
};
#[cfg(doc)]
use crate::{PRESENCE_BUCKET, ReadMarker};

/// The user JWT tag of web credentials: a human's device that is a page of the web client, with the rights of
/// [`Permissions::for_web`] and never the admin role ([`crate::ADMIN_TAG`]).
pub const WEB_TAG: &str = "ants-web";

/// Whether a user JWT's `tags` carry [`WEB_TAG`], in either case.
pub fn is_web<'a>(tags: impl IntoIterator<Item = &'a str>) -> bool {
    tags.into_iter().any(|tag| tag.eq_ignore_ascii_case(WEB_TAG))
}

impl Permissions {
    /// What the web credentials of `human` allow, the same for every human: what the views of the web client send
    /// and read, each by a request of core NATS, and nothing else of a human's rights ([`Permissions::for_user`] in
    /// [`Role::Human`]).
    ///
    /// It sends on every inbox as itself ([`subject::inbox_from`]) and under no other sender; records the owner of
    /// the threads it messages an ant on ([`OWNERS_BUCKET`]); reads the stream's messages by direct gets naming no
    /// subject, in batches; reads and puts, by compare-and-set, its own [`ReadMarker`] and no channel reads; puts its
    /// own devices' presence ([`PRESENCE_BUCKET`]) and reads none; reads and puts its own entry among the humans
    /// ([`HUMANS_BUCKET`]), as a human's first connection does, and reads its own assistant grant there, which names
    /// whom an unaddressed message goes to, writing none; and asks for its own renewal. It hears its reply inboxes,
    /// the messages to it and those sent under its name, every ant's status and the answers to its renewal.
    ///
    /// It gets no stream info, creates, updates and purges no stream or bucket, deletes no message, creates no
    /// consumer, sets up no leaf outbox and publishes in none, writes no channel's, other human's or machine's
    /// record, and sends no admin order. A view added to the page adds the rights it needs here, under a new
    /// [`super::PERMISSIONS_VERSION`].
    pub fn for_web(human: &UserId) -> Self {
        let (read, humans) = (kv_stream(READ_BUCKET), kv_stream(HUMANS_BUCKET));
        let marker = subject::read_marker(human);
        let entry = subject::human_entry(human);
        let publish = vec![
            subject::inbox_from(human),
            subject::kv(OWNERS_BUCKET, "*.*"),
            format!("$JS.API.DIRECT.GET.{STREAM}"),
            format!("$JS.API.DIRECT.GET.{read}.{marker}"),
            marker,
            subject::presence_of(human),
            format!("$JS.API.DIRECT.GET.{humans}.{entry}"),
            entry,
            subject::kv_get(HUMANS_BUCKET, assistant_grant_key(human)),
            subject::renew(human.as_str()),
        ];
        let subscribe = vec![
            format!("{}.>", inbox_prefix(human)),
            subject::inbox_of(human),
            subject::inbox_from(human),
            subject::ALL_STATUS.to_owned(),
            subject::renew_reply(human.as_str()),
        ];
        Self { publish, subscribe }
    }

    /// What the credentials of a device of `human` allow: a web device's ([`Permissions::for_web`]) when `web`, else
    /// a human's ([`Permissions::for_user`] in [`Role::Human`]).
    pub fn for_human_device(human: &UserId, web: bool) -> Self {
        if web { Self::for_web(human) } else { Self::for_user(human, Role::Human) }
    }
}

#[cfg(test)]
mod tests {
    //! Tests of web credentials' rights: the views' requests and nothing else of a human's.

    use super::super::covers;
    use super::*;
    use crate::{JsDomain, Machine, Role, ThreadId, p};

    #[test]
    fn web_credentials_allow_what_the_page_s_views_use_and_nothing_else() {
        let alice = p("alice");
        let web = Permissions::for_web(&alice);
        assert_eq!(
            web.publish,
            [
                "ants.inbox.*.alice",
                "$KV.ants_owners.*.*",
                "$JS.API.DIRECT.GET.ANTS",
                "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice",
                "$KV.ants_read.alice",
                "$KV.ants_presence.alice.>",
                "$JS.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice",
                "$KV.ants_humans.alice",
                "$JS.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice.assistant",
                "ants.renew.alice",
            ]
        );
        assert_eq!(
            web.subscribe,
            ["_INBOX.alice.>", "ants.inbox.alice.>", "ants.inbox.*.alice", "ants.status.*.*", "ants.renew.alice.reply"]
        );
        let human = Permissions::for_user(&alice, Role::Human);
        for right in &web.publish {
            assert!(human.publish.iter().any(|have| covers(have, right)), "{right} is no right of a human's");
        }
        for right in &web.subscribe {
            assert!(human.subscribe.iter().any(|have| covers(have, right)), "{right} is no right of a human's");
        }
        assert!(!crate::writes_human_entries(web.publish.iter().map(String::as_str)));

        let (m1, thread): (Machine, ThreadId) = ("m1".parse().unwrap(), "t1".parse().unwrap());
        let (dev_a, bob) = (p("dev-a"), p("bob"));
        let refused = [
            "$JS.API.INFO".to_owned(),
            "$JS.API.STREAM.INFO.ANTS".to_owned(),
            "$JS.API.STREAM.CREATE.ANTS".to_owned(),
            "$JS.API.STREAM.UPDATE.ANTS".to_owned(),
            "$JS.API.STREAM.DELETE.ANTS".to_owned(),
            "$JS.API.STREAM.PURGE.ANTS".to_owned(),
            "$JS.API.STREAM.MSG.DELETE.ANTS".to_owned(),
            "$JS.API.STREAM.MSG.GET.ANTS".to_owned(),
            "$JS.API.CONSUMER.CREATE.ANTS".to_owned(),
            "$JS.API.CONSUMER.CREATE.ANTS.x.ants.inbox.alice.>".to_owned(),
            "$JS.API.STREAM.CREATE.KV_ants_read".to_owned(),
            "$JS.API.STREAM.UPDATE.KV_ants_humans".to_owned(),
            "$JS.API.STREAM.PURGE.KV_ants_presence".to_owned(),
            format!("$JS.API.STREAM.CREATE.{}", crate::OUTBOX_STREAM),
            format!("$JS.m1.API.STREAM.CREATE.{}", crate::OUTBOX_STREAM),
            format!("$JS.API.STREAM.UPDATE.{}", crate::INTAKE_STREAM),
            subject::outbox(&subject::inbox(&dev_a, &alice)).unwrap(),
            crate::owner_outbox(&thread, &dev_a),
            subject::channel_entry(&"ops".parse().unwrap()),
            subject::human_entry(&bob),
            format!("$KV.{HUMANS_BUCKET}.{}", assistant_grant_key(&alice)),
            subject::machine_record(&m1),
            subject::admin_order(&m1, &dev_a),
            subject::inbox(&dev_a, &bob),
            subject::webhook_inbox(&dev_a, &alice),
            subject::notes(&thread, &alice),
            subject::chan(&"ops".parse().unwrap(), &alice),
            subject::read_marker(&bob),
            subject::channel_reads(&alice),
            format!("$JS.API.DIRECT.GET.KV_ants_read.{}", subject::read_marker(&bob)),
            format!("$KV.{}.bob.d1.inbox", crate::PRESENCE_BUCKET),
            subject::approval_decision(&dev_a, &crate::Uuid::now_v7()),
            subject::cancel(&dev_a, &thread),
            subject::renew("bob"),
            subject::renew("leaf-m1"),
        ];
        for subject in &refused {
            assert!(!web.publish.iter().any(|have| covers(have, subject)), "{subject} is allowed");
        }
        for subject in [subject::ALL, ">", "_INBOX.bob.>", "ants.inbox.bob.dev-a", "$JS.API.>", "ants.renew.bob.reply"]
        {
            assert!(!web.subscribe.iter().any(|have| covers(have, subject)), "hears {subject}");
        }
    }

    #[test]
    fn web_credentials_are_the_same_for_every_human_and_take_a_hub_s_domain() {
        let renamed = |p: Permissions| -> Vec<String> {
            p.publish.into_iter().chain(p.subscribe).map(|s| s.replace("bob", "alice")).collect()
        };
        assert_eq!(renamed(Permissions::for_web(&p("bob"))), renamed(Permissions::for_web(&p("alice"))));

        let hub: JsDomain = "hub".parse().unwrap();
        let plain = Permissions::for_web(&p("alice"));
        let in_domain = plain.clone().with_js_domain(Some(&hub));
        assert_eq!(in_domain.subscribe, plain.subscribe);
        let added: Vec<&str> = in_domain.publish[plain.publish.len()..].iter().map(String::as_str).collect();
        assert_eq!(
            added,
            [
                "$JS.hub.API.$KV.ants_owners.*.*",
                "$JS.hub.API.DIRECT.GET.ANTS",
                "$JS.hub.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice",
                "$JS.hub.API.$KV.ants_read.alice",
                "$JS.hub.API.$KV.ants_presence.alice.>",
                "$JS.hub.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice",
                "$JS.hub.API.$KV.ants_humans.alice",
                "$JS.hub.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice.assistant",
            ]
        );
    }

    #[test]
    fn only_the_web_tag_marks_web_credentials() {
        assert!(is_web(["ants-perms:1", "ants-web"]));
        assert!(is_web(["ANTS-WEB"]));
        assert!(!is_web(["ants-web:x", "ants-webhook", "ants-device:x"]));
    }
}
