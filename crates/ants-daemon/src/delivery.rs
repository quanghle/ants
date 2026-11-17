//! Messages on their way to an ant beyond its inbox loop: the markers of attempts at them, the dead letters it gave up
//! on, those waiting for it, and the word to their senders when no daemon serves it.

pub mod attempts;
pub mod dead;
pub mod queue;
pub mod unserved;
