//! Tests of the permissions each kind of credentials carries, one submodule per behaviour: renewals and ant requests,
//! JetStream domains, leaves and outboxes, assistants, webhooks and observers, and every bucket and subject's readers
//! and writers.

use chrono::Utc;

use super::*;
use crate::{
    ANTS_VERSION, AdminPayload, AntReply, AntRequest, AskedAnt, CancelReply, CancelRequest, ChainEntry, ChannelEntry,
    ChannelId, ChannelOp, DeviceId, ENROLL_INBOX, FILES_STREAM, HumanEntry, Humans, JsDomain, MAX_CHANNEL_DESCRIPTION,
    Machine, QueueRequest, Refusal, Role, SENSE_STREAM, SPARE_READERS, STREAM, Serving, ThreadId, UserId, Uuid,
    assisted_owner_key, assisted_withdrawal_key, p, spare_readers, subject,
};

mod assistants;
mod channels;
mod credentials;
mod domains;
mod humans;
mod leaves;
mod stores;
mod streams;
mod trails;
mod webhooks;
