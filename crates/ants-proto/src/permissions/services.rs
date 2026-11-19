//! What the credentials of the team's services allow: the channel manager's ([`Permissions::for_channel_manager`]),
//! and those `ants enroll`, `ants signer`, `ants auth revoke` and `ants auth push` issue themselves from the account
//! key.

use super::{Permissions, kv_stream, ordered_consumer_subjects};
#[cfg(doc)]
use crate::{AntRequest, MACHINES_BUCKET, ReactionsRequest, SaveTrailRequest, channel_manager_serving_key};
use crate::{
    CHANNELS_BUCKET, COLONY_BUCKET, DEVICES_BUCKET, ENROLL_INBOX, FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET,
    INTAKE_STREAM, LINKS_BUCKET, REACTIONS_BUCKET, STREAM, THREADS_BUCKET, TRAILS_BUCKET, UserId, chan_consumer,
    channel_manager_serving_subject, done_reader, inbox_consumer, inbox_prefix, subject,
};

impl Permissions {
    /// What the credentials of the channel manager (`ants channel serve`) allow: hearing every channel management
    /// request, reading the colony and the humans, the assistants' records and the revocations (to check what an
    /// assistant asks for its human), reading and writing the channels bucket, posting in any channel
    /// as [`UserId::channel_manager`], and putting the version it runs ([`channel_manager_serving_key`]). Its JWT also
    /// lets it answer each request it hears once, on the request's reply subject, so it needs no right to publish on
    /// any ant's reply inboxes. It asks for its own renewal ([`subject::renew`]) and hears the answer.
    pub fn for_channel_manager() -> Self {
        let user = UserId::channel_manager();
        let (colony, channels, humans) =
            (kv_stream(COLONY_BUCKET), kv_stream(CHANNELS_BUCKET), kv_stream(HUMANS_BUCKET));
        let mut publish = vec![
            subject::chan_from(&user),
            format!("$KV.{CHANNELS_BUCKET}.>"),
            channel_manager_serving_subject(),
            // Finding JetStream on connecting, as an ant does, without the stream's info.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(&user)),
        ];
        for stream in [&colony, &channels, &humans] {
            publish.extend([format!("$JS.API.STREAM.INFO.{stream}"), format!("$JS.API.DIRECT.GET.{stream}.>")]);
            publish.extend(ordered_consumer_subjects(stream));
        }
        // Get of the assistants' records and of the revocations, to check a request an assistant makes for its human.
        let devices = kv_stream(DEVICES_BUCKET);
        publish.extend([
            format!("$JS.API.DIRECT.GET.{devices}.{}", subject::assistant_records()),
            format!("$JS.API.DIRECT.GET.{devices}.{}", subject::revoked_devices()),
        ]);
        publish.push(subject::renew(user.as_str()));
        let subscribe = vec![
            subject::ALL_CHANADM.to_owned(),
            format!("{}.>", inbox_prefix(&user)),
            subject::renew_reply(user.as_str()),
        ];
        Self { publish, subscribe }
    }

    /// What `ants enroll` and `ants signer` allow themselves on the machine holding the account key: hearing every
    /// enrollment, renewal and ant request ([`AntRequest`]) and answering it, asking the humans about an ant request
    /// as an approval request of its manager and reading their decision, and admitting an ant new to a leaf node into
    /// the hub's stream, links bucket and attachments' store from the leaf's outbox, as `ants leaf outbox` does, with
    /// the replies of its JetStream requests under [`ENROLL_INBOX`].
    pub fn for_enroll_approver() -> Self {
        let links = kv_stream(LINKS_BUCKET);
        Self {
            publish: vec![
                subject::ALL_ENROLL_REPLIES.to_owned(),
                subject::ALL_RENEW_REPLIES.to_owned(),
                subject::ALL_ANT_REPLIES.to_owned(),
                // A manager's ant request outside the templates' policy goes to the humans as an approval request
                // of the manager's, whose decision it reads back as the ant would.
                subject::ALL_APPROVAL_REQUESTS.to_owned(),
                format!("$JS.API.DIRECT.GET.{STREAM}.{}", subject::ALL_APPROVAL_DECISIONS),
                format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.INFO.{STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{STREAM}"),
                format!("$JS.API.STREAM.INFO.{links}"),
                format!("$JS.API.STREAM.UPDATE.{links}"),
                format!("$JS.API.STREAM.INFO.{FILES_STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{FILES_STREAM}"),
            ],
            subscribe: vec![
                subject::ALL_ENROLL_REQUESTS.to_owned(),
                subject::ALL_RENEW_REQUESTS.to_owned(),
                subject::ALL_ANT_REQUESTS.to_owned(),
                format!("{ENROLL_INBOX}.>"),
            ],
        }
    }

    /// What `ants signer` also allows itself, on the machine holding the account key, to answer ants' reactions
    /// requests ([`ReactionsRequest`]): hearing every request for the reactions to a message
    /// ([`subject::ALL_REACTIONS_REQUESTS`]) and to change an ant's own ([`subject::ALL_REACT_REQUESTS`]); reading the
    /// stream back by batched direct gets, from its info, to find whether the asking ant reads the message; reading
    /// every reaction, singly and through ordered consumers; putting and deleting any user's reaction, which only it
    /// does for an ant, once it found the ant reads the message; and answering on the ant's own
    /// [`subject::reactions_reply`]. The replies of its JetStream requests come under [`ENROLL_INBOX`], as those of
    /// [`Permissions::for_enroll_approver`], with which it is issued.
    pub fn for_reactions_responder() -> Self {
        let reactions = kv_stream(REACTIONS_BUCKET);
        let mut publish = vec![
            format!("$JS.API.STREAM.INFO.{STREAM}"),
            format!("$JS.API.DIRECT.GET.{STREAM}"),
            format!("$JS.API.DIRECT.GET.{reactions}.>"),
            format!("$KV.{REACTIONS_BUCKET}.>"),
            subject::ALL_REACTIONS_REPLIES.to_owned(),
        ];
        publish.extend(ordered_consumer_subjects(&reactions));
        Self {
            publish,
            subscribe: vec![
                subject::ALL_REACTIONS_REQUESTS.to_owned(),
                subject::ALL_REACT_REQUESTS.to_owned(),
                format!("{ENROLL_INBOX}.>"),
            ],
        }
    }

    /// What `ants signer` also allows itself, on the machine holding the account key, to save and remove trails for
    /// ants ([`SaveTrailRequest`]): hearing every such request ([`subject::ALL_SAVE_TRAIL_REQUESTS`]); reading the saved
    /// trails by direct gets naming no subject, as every user does; putting and deleting any saved trail, which only it
    /// does for an ant, once it found the trail absent or saved by the ant; and answering on the ant's own
    /// [`subject::save_trail_reply`]. The replies of its JetStream requests come under [`ENROLL_INBOX`], as those of
    /// [`Permissions::for_enroll_approver`], with which it is issued.
    pub fn for_trail_keeper() -> Self {
        Self {
            publish: vec![
                format!("$JS.API.DIRECT.GET.{}", kv_stream(TRAILS_BUCKET)),
                format!("$KV.{TRAILS_BUCKET}.*"),
                subject::ALL_SAVE_TRAIL_REPLIES.to_owned(),
            ],
            subscribe: vec![subject::ALL_SAVE_TRAIL_REQUESTS.to_owned(), format!("{ENROLL_INBOX}.>")],
        }
    }

    /// What the short-lived credentials `ants auth revoke` and `ants auth push` issue themselves from the account key
    /// allow, to say in [`DEVICES_BUCKET`] which keys of humans' credentials are revoked: creating the bucket, reading
    /// and putting the revocation records ([`subject::revoked_devices`]) and nothing else of it, with the replies of
    /// its JetStream requests under [`ENROLL_INBOX`]. No other credentials put a revocation record. They also delete
    /// the identity record and shared settings of an ant revoked whole ([`IDENTITIES_BUCKET`]), first reading the
    /// identity record and preserving its growth by sense id.
    pub fn for_device_revoker() -> Self {
        let devices = kv_stream(DEVICES_BUCKET);
        let revoked = subject::revoked_devices();
        let identities = kv_stream(IDENTITIES_BUCKET);
        let records = format!("$KV.{IDENTITIES_BUCKET}.*");
        let growth = subject::all_revoked_growth();
        Self {
            publish: vec![
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.INFO.{devices}"),
                format!("$JS.API.STREAM.CREATE.{devices}"),
                format!("$JS.API.DIRECT.GET.{devices}.{revoked}"),
                revoked,
                format!("$JS.API.DIRECT.GET.{identities}.{records}"),
                format!("$JS.API.DIRECT.GET.{identities}.{growth}"),
                records,
                growth,
                subject::all_shared_settings(),
            ],
            subscribe: vec![format!("{ENROLL_INBOX}.>")],
        }
    }

    /// What retirement cleanup allows on the hub: checking one ant's colony entry and deleting only its channel
    /// and done consumers, never its inbox consumer.
    pub fn for_retired_consumers(ant: &UserId) -> Self {
        let colony = kv_stream(COLONY_BUCKET);
        let threads = kv_stream(THREADS_BUCKET);
        Self {
            publish: vec![
                format!("$JS.API.DIRECT.GET.{colony}.$KV.{COLONY_BUCKET}.{ant}"),
                format!("$JS.API.CONSUMER.DELETE.{STREAM}.{}", chan_consumer(ant)),
                format!("$JS.API.CONSUMER.DELETE.{threads}.{}", done_reader(ant)),
            ],
            subscribe: vec![format!("{ENROLL_INBOX}.>")],
        }
    }

    /// What `ants signer` also allows itself, on the machine holding the account key, to say where that key is: putting
    /// the [`AccountHolder`](crate::AccountHolder) record in [`MACHINES_BUCKET`] ([`subject::account_holder`]), no
    /// other key of the bucket, with the replies of its JetStream requests under [`ENROLL_INBOX`], as those of
    /// [`Permissions::for_enroll_approver`], with which it is issued.
    pub fn for_account_holder() -> Self {
        Self { publish: vec![subject::account_holder()], subscribe: vec![format!("{ENROLL_INBOX}.>")] }
    }

    /// What `ants signer` allows itself on top of [`Permissions::for_enroll_approver`]: hearing every registration
    /// request and answering it. Leaf uplinks never carry these, as registration happens on the hub only.
    pub fn for_registrar() -> Self {
        Self {
            publish: vec![subject::ALL_LOGIN_REPLIES.to_owned()],
            subscribe: vec![subject::ALL_LOGIN_REQUESTS.to_owned()],
        }
    }
}
