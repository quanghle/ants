//! The channel view's tabs over its threads: all of them but the archived ones, those in each state, and the archived
//! ones, each with its count; the tab chosen kept for each channel on the device.

use std::collections::HashMap;

use ants_proto::{ChannelId, ThreadId};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use serde::{Deserialize, Deserializer, Serialize};

use crate::tui::model::Data;
use crate::tui::style;
use crate::tui::work::ThreadState;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tab {
    /// Every thread but the archived ones.
    #[default]
    All,
    WaitingOnMe,
    Working,
    WaitingOnAnt,
    Stuck,
    Idle,
    Done,
    /// The threads the human archived or `auto_archive` left idle, with no message since, as
    /// `ants channel read --archived` prints them.
    Archived,
}

impl Tab {
    /// The tabs, in the order `←`/`→` step through them.
    pub const ALL: [Self; 8] = [
        Self::All,
        Self::WaitingOnMe,
        Self::Working,
        Self::WaitingOnAnt,
        Self::Stuck,
        Self::Idle,
        Self::Done,
        Self::Archived,
    ];

    fn of(state: ThreadState) -> Self {
        match state {
            ThreadState::WaitingOnMe => Self::WaitingOnMe,
            ThreadState::Working => Self::Working,
            ThreadState::WaitingOnAnt => Self::WaitingOnAnt,
            ThreadState::Stuck => Self::Stuck,
            ThreadState::Idle => Self::Idle,
            ThreadState::Done => Self::Done,
        }
    }

    fn state(self) -> Option<ThreadState> {
        ThreadState::ALL.into_iter().find(|s| Self::of(*s) == self)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Archived => "archived",
            tab => tab.state().map_or("", ThreadState::label),
        }
    }

    /// The tab `by` tabs on, round.
    pub fn moved(self, by: isize) -> Self {
        let at = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        let len = Self::ALL.len().cast_signed();
        Self::ALL[(at.cast_signed() + by).rem_euclid(len).cast_unsigned()]
    }

    /// Whether the tab lists `thread`.
    pub fn lists(self, thread: &ThreadId, data: &Data) -> bool {
        let archived = data.live.hidden(thread);
        match self {
            Self::All => !archived,
            Self::Archived => archived,
            tab => !archived && Some(data.state(thread)) == tab.state(),
        }
    }
}

/// How many of `threads` each tab lists.
pub fn counts<'t>(threads: impl IntoIterator<Item = &'t ThreadId>, data: &Data) -> HashMap<Tab, usize> {
    let mut counts = HashMap::new();
    for thread in threads {
        let tabs = if data.live.hidden(thread) {
            [Some(Tab::Archived), None]
        } else {
            [Some(Tab::All), Some(Tab::of(data.state(thread)))]
        };
        for tab in tabs.into_iter().flatten() {
            *counts.entry(tab).or_default() += 1;
        }
    }
    counts
}

/// The tabs in a line at most `width` wide, `chosen` highlighted: `‹ all N · waiting on me N · … ›`, leaving out those
/// listing nothing but `all` and the one chosen when they all do not fit.
pub fn line(chosen: Tab, counts: &HashMap<Tab, usize>, width: usize, focused: bool) -> Line<'static> {
    let count = |tab: &Tab| counts.get(tab).copied().unwrap_or(0);
    let item = |tab: &Tab| format!("{} {}", tab.label(), count(tab));
    let wide = |tabs: &[Tab]| tabs.iter().map(|t| item(t).chars().count() + 3).sum::<usize>() + 1;
    let mut tabs = Tab::ALL.to_vec();
    if wide(&tabs) > width {
        tabs.retain(|t| *t == Tab::All || *t == chosen || count(t) > 0);
    }
    let mut spans = vec![Span::styled("‹ ", style::dim())];
    for (at, tab) in tabs.iter().enumerate() {
        if at > 0 {
            spans.push(Span::styled(" · ", style::dim()));
        }
        let style = match tab.state() {
            _ if *tab == chosen => style::highlight(focused),
            Some(state) if count(tab) > 0 => Style::new().fg(state.color()),
            None if count(tab) > 0 => Style::new(),
            _ => style::dim(),
        };
        spans.push(Span::styled(item(tab), style));
    }
    spans.push(Span::styled(" ›", style::dim()));
    Line::from(spans)
}

/// Keep `tab` as the one chosen in `channel` among those `kept`: `all`, the default, by keeping none.
pub fn keep(kept: &mut HashMap<ChannelId, Tab>, channel: ChannelId, tab: Tab) {
    if tab == Tab::default() {
        kept.remove(&channel);
    } else {
        kept.insert(channel, tab);
    }
}

/// The tabs kept for each channel, but for those an older or newer `ants chat` kept that this one does not know.
pub fn kept<'de, D: Deserializer<'de>>(from: D) -> Result<HashMap<ChannelId, Tab>, D::Error> {
    let kept = HashMap::<String, serde_json::Value>::deserialize(from)?;
    Ok(kept.into_iter().filter_map(|(c, t)| Some((c.parse().ok()?, serde_json::from_value(t).ok()?))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::catchup::{STATE_FILE, keep as keep_state, load};

    #[test]
    fn the_device_keeps_the_tab_chosen_in_each_channel_but_for_those_it_does_not_know() {
        let dir = ants_test_support::scratch_dir("channel-tabs");
        let path = dir.join(STATE_FILE);
        let (ops, dev): (ChannelId, ChannelId) = ("ops".parse().unwrap(), "dev".parse().unwrap());
        keep_state(&path, |kept| keep(&mut kept.channel_tabs, ops.clone(), Tab::WaitingOnMe)).unwrap();
        keep_state(&path, |kept| keep(&mut kept.channel_tabs, dev.clone(), Tab::Archived)).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(r#""ops": "waiting-on-me""#), "{text}");
        keep_state(&path, |kept| keep(&mut kept.channel_tabs, dev.clone(), Tab::All)).unwrap();
        assert_eq!(load(&path).channel_tabs, HashMap::from([(ops.clone(), Tab::WaitingOnMe)]));
        let kept = r#"{"tools": "none", "channel_tabs": {"ops": "stuck", "dev": "later", "not a channel": "idle"}}"#;
        std::fs::write(&path, kept).unwrap();
        let kept = load(&path);
        assert_eq!(kept.channel_tabs, HashMap::from([(ops, Tab::Stuck)]));
        assert_eq!(kept.tools, crate::tui::timeline::Tools::None, "the rest kept all the same");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
