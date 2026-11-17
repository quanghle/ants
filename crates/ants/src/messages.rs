//! Messages as the CLI's human sends and reads them: the text composed, the files attached, forwarding, the status
//! line while replies are awaited, the inbox, and reactions followed live.

pub(crate) mod attachments;
pub(crate) mod compose;
pub(crate) mod forward;
pub(crate) mod inbox;
pub(crate) mod progress;
pub(crate) mod reacted;
