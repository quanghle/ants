//! The team's humans as the CLI sees them: their registry and the CLI's own human, their devices' encryption keys and
//! the messages sealed for them, their assistants and the acts done on their behalf, where they read and what they
//! hold back of their notifications.

pub(crate) mod assistant;
pub(crate) mod behalf;
pub(crate) mod devices;
pub(crate) mod humans;
pub(crate) mod presence;
pub(crate) mod quiet;
pub(crate) mod sealed;
