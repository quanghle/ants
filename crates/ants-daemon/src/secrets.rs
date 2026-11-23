//! Credentials and secrets: checking the credentials a process uses, the OS credential store holding their seeds,
//! sealing messages between humans, and redacting secrets from what leaves the daemon.

pub mod creds;
pub mod keystore;
pub mod redact;
pub mod sealing;

/// Whether two secrets are equal, told in time independent of where they differ.
pub fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}
