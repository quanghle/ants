//! Tests of the command bar: what its behaviours share, and the behaviours, one submodule each.

use ants_proto::{ChannelEntry, Questions, StatusEvent, StatusUpdate};

use super::bar::Status;
use super::*;
use crate::live;
use crate::messages::{attachments, forward};
use crate::test_support::live::{at, message, request, user};
use crate::tui::acts::Change;
use crate::tui::exec::{Done, Outcome};
use crate::tui::reads::Read;
use crate::tui::test_support::{model, typed};
use crate::tui::{nav, thread};

mod attach;
mod notes;
mod quoting;
mod send;
mod slash;
