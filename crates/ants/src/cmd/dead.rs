//! `ants dead`: the dead letters of ants.

use std::net::SocketAddr;
use std::sync::Arc;

use ants_daemon::delivery::dead;
use ants_daemon::monitoring::metrics;
use ants_proto::DeadLetter;
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;
use crate::messages::dead::{dropped, replayed, stream};
use crate::serve::serve_metrics;
use crate::text::excerpt;

#[derive(Subcommand, Debug)]
pub(crate) enum DeadCommand {
    /// List dead letters, oldest first.
    List {
        /// One JSON object per dead letter, the `DeadLetter` with its own sequence as `seq`.
        #[arg(long)]
        json: bool,
    },
    /// Send a dead letter's message to its inbox again, as it was, and remove the dead letter.
    Replay {
        #[arg(add = ArgValueCandidates::new(completions::dead_letters))]
        seq: u64,
    },
    /// Remove a dead letter without replaying it.
    Drop {
        #[arg(add = ArgValueCandidates::new(completions::dead_letters))]
        seq: u64,
    },
    /// Keep dead-lettering, until Ctrl-C, the messages JetStream stops delivering without the daemon having stored
    /// them as dead letters, and print each one. `ants inbox --follow` does the same while it runs.
    Watch {
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        /// One JSON object per dead letter stored, as `ants dead list --json` prints it.
        #[arg(long)]
        json: bool,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: DeadCommand) -> Result<()> {
    let (client, js) = nats.connect("ants-cli").await?;
    match command {
        DeadCommand::List { json } => {
            let letters = dead::list(&stream(&js).await?).await?;
            if letters.is_empty() && !json {
                println!("no dead letters");
            }
            for (seq, letter) in &letters {
                println!("{}", if json { dead_json(*seq, letter) } else { dead_row(*seq, letter) });
            }
        }
        DeadCommand::Replay { seq } => println!("{}", replayed(&js, seq).await?),
        DeadCommand::Drop { seq } => println!("{}", dropped(&js, seq).await?),
        DeadCommand::Watch { metrics, json } => {
            let backstop = Arc::new(metrics::Backstop::new(nats.me.id.clone(), client.clone()));
            let _metrics = serve_metrics(metrics, backstop).await?;
            let print = |seq, letter: &DeadLetter| {
                println!("{}", if json { dead_json(seq, letter) } else { dead_row(seq, letter) });
            };
            let watch = dead::watch(&client, &js, &nats.me.id, print);
            tokio::select! {
                watched = watch => watched?,
                _ = tokio::signal::ctrl_c() => {}
            }
        }
    }
    Ok(())
}

/// One line of `ants dead list --json`: the dead letter, with its own sequence in the stream, which `ants dead replay`
/// and `drop` take, as `seq`.
fn dead_json(seq: u64, letter: &DeadLetter) -> String {
    let mut json = serde_json::to_value(letter).unwrap_or_default();
    if let serde_json::Value::Object(fields) = &mut json {
        fields.insert("seq".to_owned(), seq.into());
    }
    json.to_string()
}

/// One line of `ants dead list`: sequence, ant, thread, sender, deliveries, time, error and the start of the text, or
/// the size of a sealed message's ciphertext.
fn dead_row(seq: u64, letter: &DeadLetter) -> String {
    let time = letter.died_at.format("%Y-%m-%d %H:%M:%S");
    let (thread, from, text) = match &letter.envelope {
        Some(e) => {
            let text = e.sealed.as_deref().map_or_else(|| e.text.clone(), ants_client::sealed::size_note);
            (e.thread_id.as_str(), e.from.as_str(), text)
        }
        None => ("-", "-", letter.payload.clone().unwrap_or_default()),
    };
    format!(
        "{seq}\t{}\t{thread}\t{from}\t{}x\t{time}\t{}\t{}",
        letter.ant,
        letter.deliveries,
        excerpt(&letter.error, 60),
        excerpt(&text, 60)
    )
}

#[cfg(test)]
mod tests {
    use ants_proto::Envelope;

    use super::*;

    #[test]
    fn dead_rows_show_the_message_or_the_bad_payload() {
        let mut ask = Envelope::new(ants_test_support::human(), "dev-a".parse().unwrap(), "build it\nand test it");
        ask.thread_id = "t1".parse().unwrap();
        let letter = DeadLetter::of_envelope("dev-a".parse().unwrap(), 3, 5, "harness crashed", ask);
        let row = dead_row(12, &letter);
        assert!(row.starts_with("12\tdev-a\tt1\thuman\t5x\t"), "{row}");
        assert!(row.ends_with("\tharness crashed\tbuild it…"), "{row}");

        let letter = DeadLetter::of_payload("dev-a".parse().unwrap(), 4, 1, "not an envelope", b"{oops");
        assert!(dead_row(13, &letter).ends_with("\tnot an envelope\t{oops"));

        let json: serde_json::Value = serde_json::from_str(&dead_json(12, &letter)).unwrap();
        assert_eq!((json["seq"].as_u64(), json["error"].as_str()), (Some(12), Some("not an envelope")));
        assert_eq!(json["payload"], "{oops");

        let junk = crate::test_support::hostile::HOSTILE;
        let letter = DeadLetter::of_payload("dev-a".parse().unwrap(), 4, 1, junk, junk.as_bytes());
        let row = dead_row(14, &letter);
        let fields: Vec<&str> = row.split('\t').collect();
        assert_eq!(fields.len(), 8, "{row:?}");
        for field in fields {
            crate::test_support::hostile::assert_printable(&format!("{field} spoof"));
        }
    }
}
