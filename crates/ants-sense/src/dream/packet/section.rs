//! A section of a packet within a share of its budget ([`Section`]): a sage's earlier findings
//! ([`super::track_record()`]), the reviews a sage made of other ants ([`super::reviewing()`]) and the verdicts on the
//! last dream's findings. It keeps its entries in order while they fit its share, telling how many it left out, and its
//! heading and tally always: for room it shrinks to them before the identity is cut to titles.

/// A section of a packet within a share of its budget.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Section {
    /// Its heading and what it holds.
    pub head: String,
    /// Its entries, those kept first.
    pub entries: Vec<String>,
    /// What stays however many entries are left out: their tally, or that there are none.
    pub tally: String,
    /// What an entry is, as the line telling how many were left out names one, such as `earlier finding`.
    pub noun: &'static str,
}

impl Section {
    /// Whether it lists no entry.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Its text, keeping the first `kept` entries.
    pub fn text(&self, kept: usize) -> String {
        let mut text = self.head.clone();
        for entry in self.entries.iter().take(kept) {
            text.push_str(entry);
        }
        text.push_str(&self.left_out(kept));
        text.push_str(&self.tally);
        text
    }

    /// The most entries it keeps within `bytes`; none when its heading and tally alone take more.
    pub(super) fn within(&self, bytes: usize) -> usize {
        let mut kept = 0;
        let mut taken = self.head.len() + self.tally.len();
        for entry in &self.entries {
            if taken + entry.len() + self.left_out(kept + 1).len() > bytes {
                break;
            }
            taken += entry.len();
            kept += 1;
        }
        kept
    }

    fn left_out(&self, kept: usize) -> String {
        match self.entries.len().saturating_sub(kept) {
            0 => String::new(),
            1 => format!("\n1 {} left out for room.\n", self.noun),
            left => format!("\n{left} {}s left out for room.\n", self.noun),
        }
    }
}
