use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ATTACHMENT_CHUNK, FILES_BUCKET, Machine};

#[cfg(doc)]
use crate::{AdminOp, FILES_STREAM};

/// The first token of the chunk subjects of admin payloads in [`FILES_STREAM`], which no ant's name matches.
pub const ADMIN_PAYLOADS: &str = "_admin";

/// The most bytes an [`AdminPayload`] holds.
pub const MAX_ADMIN_PAYLOAD: u64 = 64 * 1024 * 1024;

/// Bytes an admin order or its outcome carries by reference, too large for either: the export of an ant's provider
/// ([`AdminOp::ExportProvider`], [`AdminOp::ImportProvider`]). They are stored in [`FILES_STREAM`] as chunks under
/// `$O.ants_files.C._admin.<id>.<machine>.<n>`, where `machine` is the machine of the order: its daemon stores an
/// export for the admin and reads an import the admin stored for it. Humans store and read any, a daemon only those of
/// its own machine, no ant any; like attachments, they expire with the stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminPayload {
    pub id: Uuid,
    /// The machine of the order it goes with, whose daemon alone of all reads or stores it.
    pub machine: Machine,
    pub size: u64,
    /// The SHA-256 of the bytes, in lowercase hex.
    pub sha256: String,
}

impl AdminPayload {
    /// The number of chunks it is stored in: one at least.
    pub fn chunks(&self) -> u64 {
        self.size.div_ceil(ATTACHMENT_CHUNK as u64).max(1)
    }

    /// The subjects of the chunks of every payload `machine` stores.
    pub fn chunks_of(machine: &Machine) -> String {
        format!("$O.{FILES_BUCKET}.C.{ADMIN_PAYLOADS}.*.{machine}.>")
    }

    /// Where chunk `n` (from 0) is stored in [`FILES_STREAM`].
    pub fn chunk_subject(&self, n: u64) -> String {
        format!("$O.{FILES_BUCKET}.C.{ADMIN_PAYLOADS}.{}.{}.{n}", self.id.simple(), self.machine)
    }
}

/// The `result` of an applied [`AdminOp::ExportProvider`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProviderExport {
    /// The name of the ant's provider; none when the ant has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Where the export is stored, a [`crate::provider::Export`] as JSON; none when the provider exports nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<AdminPayload>,
}
