//! Every key's latest value in a KV bucket, read with batched direct gets of the bucket's stream rather than a
//! consumer: what the listings of machines, identities and the trails' records are read from.

use std::collections::HashMap;

use ants_client::requests::Position;
use ants_daemon::bus::relayed::{Gets, GetsError, Got};
use ants_proto::subject;
use anyhow::{Context as _, Result, bail};
use async_nats::{StatusCode, jetstream};
use chrono::{DateTime, Utc};

/// The key and value of each live entry of `bucket`, a KV bucket, on `filter`, a subject under `$KV.<bucket>.`, by
/// batched direct gets of the bucket's stream, which keeps one message per key, in the order they were last put: each
/// key as its writer last put it. A message a stream relayed is skipped, leaving the key as its writer put it before,
/// when the bucket keeps it, as the trails' buckets do.
pub(crate) async fn latest_in(js: &jetstream::Context, bucket: &str, filter: &str) -> Result<Vec<(String, Vec<u8>)>> {
    Ok(latest_after(js, bucket, filter, 1).await?.0)
}

/// [`latest_in`], with when each entry was put.
pub(crate) async fn latest_timed_in(
    js: &jetstream::Context,
    bucket: &str,
    filter: &str,
) -> Result<Vec<(String, Vec<u8>, DateTime<Utc>)>> {
    Ok(timed_after(js, bucket, filter, 1).await?.0)
}

/// [`latest_in`] from the stream's sequence `from` on, with the sequence to read from next for the entries put since.
pub(crate) async fn latest_after(
    js: &jetstream::Context,
    bucket: &str,
    filter: &str,
    from: u64,
) -> Result<(Vec<(String, Vec<u8>)>, u64)> {
    let (entries, seq) = timed_after(js, bucket, filter, from).await?;
    Ok((entries.into_iter().map(|(key, value, _)| (key, value)).collect(), seq))
}

/// [`latest_after`], with when each entry was put.
async fn timed_after(
    js: &jetstream::Context,
    bucket: &str,
    filter: &str,
    from: u64,
) -> Result<(Vec<(String, Vec<u8>, DateTime<Utc>)>, u64)> {
    let mut gets = Gets::open(js, &subject::kv_stream(bucket), filter, Position::from_seq(from)).await?;
    let mut last = Entries::new();
    loop {
        let stored = match gets.next().await {
            Ok(Got::End) => return Ok((in_put_order(last), gets.at().seq)),
            Ok(Got::More) => continue,
            Ok(Got::Relayed { subject, .. }) => {
                tracing::warn!(%subject, "skipping an entry a stream relayed");
                continue;
            }
            Ok(Got::Stored(stored)) => stored,
            Err(GetsError::Send(e)) => {
                return Err(e).with_context(|| format!("reading {bucket} (`ants init` creates it)"));
            }
            Err(GetsError::Unanswered) => bail!("no answer reading {bucket} (`ants init` creates it)"),
            Err(GetsError::Status { status: StatusCode::NO_RESPONDERS, .. }) => {
                bail!("no {bucket} bucket: `ants init` creates it")
            }
            Err(GetsError::Status { status, description }) => bail!("reading {bucket}: {status} {description:?}"),
            Err(GetsError::Malformed(why)) => bail!(why),
        };
        let deleted = deleted(&stored.headers);
        if let Some(key) = subject::kv_key(bucket, &stored.subject) {
            let value = (!deleted).then_some((stored.payload, stored.stored_at));
            last.insert(key.to_owned(), (stored.seq, value));
        }
    }
}

/// Whether the `headers` a writer stored a KV entry with mark it deleted or purged: by the first `KV-Operation`, as
/// the daemons and `ants-client` read it, which a put may carry too.
fn deleted(headers: &[(String, String)]) -> bool {
    let operation = headers.iter().find(|(name, _)| name == "KV-Operation");
    operation.is_some_and(|(_, operation)| ["DEL", "PURGE"].contains(&operation.as_str()))
}

/// What a writer last put under each key, by the sequence of that message, deleted keys left out, in the order put.
fn in_put_order(last: Entries) -> Vec<(String, Vec<u8>, DateTime<Utc>)> {
    let mut entries: Vec<_> =
        last.into_iter().filter_map(|(key, (seq, value))| value.map(|(value, at)| (seq, key, value, at))).collect();
    entries.sort_unstable_by_key(|(seq, ..)| *seq);
    entries.into_iter().map(|(_, key, value, at)| (key, value, at)).collect()
}

/// Each key's last message a writer put: its sequence, and its value and when it was put, None for a delete.
type Entries = HashMap<String, (u64, Option<(Vec<u8>, DateTime<Utc>)>)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_is_deleted_by_its_first_kv_operation_of_del_or_purge_alone() {
        let headers = |all: &[(&str, &str)]| -> Vec<(String, String)> {
            all.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect()
        };
        assert!(deleted(&headers(&[("KV-Operation", "DEL")])) && deleted(&headers(&[("KV-Operation", "PURGE")])));
        assert!(!deleted(&headers(&[])) && !deleted(&headers(&[("Nats-TTL", "5")])));
        assert!(!deleted(&headers(&[("KV-Operation", "PUT")])), "a put naming its operation");
        assert!(!deleted(&headers(&[("KV-Operation", "PUT"), ("KV-Operation", "DEL")])), "the first value counts");
        assert!(!deleted(&headers(&[("kv-operation", "DEL")])), "the exact name");
    }
}
