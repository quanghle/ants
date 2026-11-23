//! What holds an ant's messages back or turns its harness away: token and cost caps, provider limits, a signed-out
//! harness, a harness failing to start, the machine out of open files, low memory and prompts outgrowing the context window, and the fallbacks an
//! ant fails over to meanwhile.

pub(crate) mod breaker;
pub mod caps;
pub(crate) mod fallback;
pub mod files;
pub(crate) mod overflow;
pub(crate) mod pressure;
pub(crate) mod ratelimit;
pub(crate) mod signin;
