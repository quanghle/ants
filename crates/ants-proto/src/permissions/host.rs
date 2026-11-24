//! What the host credentials of a machine allow ([`Permissions::for_host`]), with which the machine's daemon makes one
//! connection besides its ants'.

use super::{Permissions, ack_subjects, kv_stream, ordered_consumer_subjects};
#[cfg(doc)]
use crate::admin_outcome_key;
use crate::{
    ACCOUNT_HOLDER_KEY, AdminPayload, COLONY_BUCKET, DEVICES_BUCKET, FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET,
    MACHINES_BUCKET, Machine, SENSE_STREAM, STREAM, admin_consumer, host_inbox_prefix, subject,
};

impl Permissions {
    /// What the host credentials of `machine` allow (`ants auth host`), with which the machine's daemon makes one
    /// connection besides its ants': putting the machine's own record in [`MACHINES_BUCKET`] ([`subject::machine_record`]),
    /// and taking the admin orders to the machine's ants from the stream through its own consumer
    /// ([`admin_consumer`]), whose filter, in the subject, is pinned to the machine's orders ([`subject::admin_orders`]),
    /// and acking them, after recording the outcome of each beside the machine's record ([`admin_outcome_key`]), which it
    /// reads back by direct gets, and looking up the revocation records of the credentials that signed them
    /// ([`subject::revoked_devices`]). It reads and writes every ant's shared settings in [`IDENTITIES_BUCKET`]
    /// ([`subject::all_shared_settings`]), creating those of the machine's ants and applying admin orders to them; any
    /// machine's host credentials may write any ant's, but daemons serve only a record signed with the ant's own
    /// credentials ([`SharedSettings::verify`](crate::SharedSettings::verify)), which only its machines hold, for the
    /// bucket's incarnation, whose marker it reads and never writes ([`subject::identities_incarnation`]). It hears its
    /// own replies under [`host_inbox_prefix`], apart from every user's. Carrying out an order to add or retire an ant,
    /// it asks `ants signer` with an enrollment request carrying the order ([`subject::enroll`],
    /// [`EnrollRequest::order`](crate::EnrollRequest::order)) and hears the answers ([`subject::ALL_ENROLL_REPLIES`]),
    /// and reads the colony by direct gets to see a retired ant stopped. It reads and watches [`HUMANS_BUCKET`], for
    /// the humans' consent to the orders their assistants sent for them. It keeps the event logs of its ants' sense
    /// identities in [`SENSE_STREAM`]: it creates the stream when missing, its create answering with the stream's info
    /// as well, appends to any identity's subject ([`subject::ALL_SENSE`]), each event expecting the subject's last
    /// sequence, and reads them by direct gets, the last of a subject by its subject or a batch from a sequence by the
    /// request. Readers verify each event as signed with the ant's own credentials and chained to the one before. It
    /// stores and reads the payloads of the machine's admin orders in [`FILES_STREAM`] ([`AdminPayload::chunks_of`]):
    /// the exports of its ants' providers. It reads the record of the machine holding the account key
    /// ([`subject::account_holder`]) for the version `ants signer` runs, which `auto_update = "install"` installs up to.
    /// Nothing else: no other machine's record or outcomes, no ant's identity record, no message to or from an ant. No
    /// ant gets any of it.
    pub fn for_host(machine: &Machine) -> Self {
        let record = subject::machine_record(machine);
        let consumer = admin_consumer(machine);
        let outcomes = subject::admin_outcomes(machine);
        let mut publish = vec![
            record,
            format!("$JS.API.DIRECT.GET.{}.{outcomes}", kv_stream(MACHINES_BUCKET)),
            outcomes,
            format!("$JS.API.DIRECT.GET.{}.{}", kv_stream(DEVICES_BUCKET), subject::revoked_devices()),
            format!("$JS.API.DIRECT.GET.{}.{}", kv_stream(IDENTITIES_BUCKET), subject::all_shared_settings()),
            format!("$JS.API.DIRECT.GET.{}.{}", kv_stream(IDENTITIES_BUCKET), subject::identities_incarnation()),
            subject::all_shared_settings(),
            // Finding JetStream on connecting, as an ant does with its inbox consumer, without the stream's info.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", subject::admin_orders(machine)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
            subject::kv_get(COLONY_BUCKET, "*"),
            subject::ALL_ENROLL_REQUESTS.to_owned(),
            format!("$JS.API.STREAM.CREATE.{SENSE_STREAM}"),
            subject::ALL_SENSE.to_owned(),
            format!("$JS.API.DIRECT.GET.{SENSE_STREAM}"),
            format!("$JS.API.DIRECT.GET.{SENSE_STREAM}.{}", subject::ALL_SENSE),
            AdminPayload::chunks_of(machine),
            format!("$JS.API.DIRECT.GET.{FILES_STREAM}.{}", AdminPayload::chunks_of(machine)),
            subject::kv_get(MACHINES_BUCKET, ACCOUNT_HOLDER_KEY),
        ];
        let humans = kv_stream(HUMANS_BUCKET);
        publish.extend([format!("$JS.API.STREAM.INFO.{humans}"), format!("$JS.API.DIRECT.GET.{humans}.>")]);
        publish.extend(ordered_consumer_subjects(&humans));
        publish.extend(ack_subjects(STREAM, &consumer));
        let subscribe = vec![format!("{}.>", host_inbox_prefix(machine)), subject::ALL_ENROLL_REPLIES.to_owned()];
        Self { publish, subscribe }
    }
}
