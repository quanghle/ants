//! Credentials and secrets: checking the credentials a process uses, the OS credential store holding their seeds,
//! sealing messages between humans, and redacting secrets from what leaves the daemon.

pub mod creds;
pub mod keystore;
pub mod redact;
pub mod sealing;
