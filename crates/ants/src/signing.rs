//! What `ants signer` signs on the machine holding the account key: the ants manager ants add and retire, and the
//! devices humans register with a GitHub sign-in, with the bindings of their names to GitHub users.

pub(crate) mod binding;
pub(crate) mod github;
pub(crate) mod manager;
pub(crate) mod registrar;
pub(crate) mod retire;
pub(crate) mod signer;
