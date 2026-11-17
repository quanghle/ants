//! `ants spend`: what ants spent, against the caps of the config file.

use std::path::PathBuf;

use anyhow::Result;

use crate::connection::Connection;
use crate::spend;

pub(crate) async fn run(
    conn: &Connection<'_>,
    select: &spend::Select,
    config: Option<PathBuf>,
    json: bool,
) -> Result<()> {
    let path = match config {
        Some(path) => path,
        None => ants_daemon::config::default_path()?,
    };
    let caps = ants_daemon::config::Summary::load(&path)?.caps;
    spend::show(&conn.nats()?, select, &caps, json).await
}
