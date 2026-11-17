//! Fixtures the tests of the full-screen views share: a model, channel posts, tool calls and images.

use std::io::Cursor;
use std::time::Duration;

use ants_proto::{ChannelPost, HumanEntry, Humans, ReadMarker, StatusEvent, StatusUpdate, ThreadId, Uuid, subject};
use image::{DynamicImage, ImageFormat, RgbImage};
use ratatui_image::picker::Picker;
use ratatui_image::protocol::Protocol;

use super::images::encode;
use super::model::Model;
use crate::live::State;
use crate::test_support::live::{at, message, user};
use crate::thread::{Entry, Kind, Parser, ToolCall};

/// The model of alice, with bob a human too and nothing read yet.
pub(super) fn model() -> Model {
    let mut m = Model::new(State::new(user("alice"), Duration::from_hours(24)), "hub:4222".to_owned());
    m.data.marker = Some(ReadMarker::default());
    let humans = ["alice", "bob"].map(|h| (h, HumanEntry::new(user(h))));
    m.data.humans = Some(Humans::from_entries(humans));
    m
}

/// The entry of `post`, stored at sequence `seq`.
pub(super) fn post(seq: u64, post: &ChannelPost) -> Entry {
    let payload = serde_json::to_vec(post).unwrap();
    let subject = subject::chan(&post.channel, &post.from);
    Parser::default().entry(seq, at(seq.cast_signed()), &subject, &payload).unwrap()
}

/// What `ant` tells of its tool call `id` on `thread` at second `secs`, in `status`.
pub(super) fn tool(
    thread: &ThreadId,
    ant: &str,
    id: &str,
    title: Option<&str>,
    status: &str,
    secs: i64,
) -> StatusEvent {
    let update = StatusUpdate::Tool {
        id: id.to_owned(),
        title: title.map(str::to_owned),
        tool_kind: Some("execute".to_owned()),
        status: Some(status.to_owned()),
        paths: (status == "in_progress").then(|| vec![format!("src/{id}.rs")]),
    };
    StatusEvent { at: at(secs), ..StatusEvent::new(user(ant), thread.clone(), update) }
}

/// The entry of a call stored on `ant`'s timeline for `message`, started at second `started`.
pub(super) fn stored(
    seq: u64,
    thread: &ThreadId,
    ant: &str,
    message: Uuid,
    id: &str,
    status: &str,
    started: i64,
) -> Entry {
    let mut entry = self::message(seq, thread, ant, "alice", "cargo test");
    entry.kind = Kind::Tool;
    entry.id = None;
    entry.to = None;
    entry.stored_at = at(started);
    entry.tool = Some(ToolCall {
        call_id: id.to_owned(),
        message,
        tool_kind: Some("execute".to_owned()),
        status: Some(status.to_owned()),
        duration_ms: Some(3100),
        paths: vec!["Cargo.toml".to_owned()],
    });
    entry
}

/// A PNG `width` by `height` pixels.
pub(super) fn png(width: u32, height: u32) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    let image = DynamicImage::from(RgbImage::from_pixel(width, height, image::Rgb([200, 40, 40])));
    image.write_to(&mut bytes, ImageFormat::Png).unwrap();
    bytes.into_inner()
}

/// The image in `bytes` encoded as halfblocks, at 10 by 20 pixels a cell.
pub(super) fn halfblocks(bytes: &[u8]) -> Protocol {
    encode(&Picker::halfblocks(), bytes).unwrap()
}
