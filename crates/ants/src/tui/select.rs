//! A list's selection kept on the same item as items arrive, leave or move: by the item's key, and once the item
//! leaves on the item now where it was. Drawing the list pins the item drawn selected, so a key acts on the item the
//! human saw selected even when items arrived meanwhile.

use std::cell::{Cell, RefCell};

#[derive(Debug)]
pub struct Selection<K> {
    /// The selected item's key, once one was drawn or moved to.
    key: RefCell<Option<K>>,
    /// Where it was last seen.
    at: Cell<usize>,
}

impl<K> Default for Selection<K> {
    fn default() -> Self {
        Self { key: RefCell::new(None), at: Cell::new(0) }
    }
}

impl<K: PartialEq + Clone> Selection<K> {
    /// Where the selected item is among `keys`, those of the items listed: where its key is, else where it was, the
    /// last item at most. Selects the item there from now on.
    pub fn at(&self, keys: &[K]) -> usize {
        let Some(last) = keys.len().checked_sub(1) else { return 0 };
        let found = self.key.borrow().as_ref().and_then(|key| keys.iter().position(|k| k == key));
        let at = found.unwrap_or_else(|| self.at.get().min(last));
        self.pin(keys, at);
        at
    }

    /// Select the item at `to` among `keys`, the last one at most.
    pub fn select(&mut self, keys: &[K], to: usize) {
        if let Some(last) = keys.len().checked_sub(1) {
            self.pin(keys, to.min(last));
        }
    }

    fn pin(&self, keys: &[K], at: usize) {
        self.at.set(at);
        self.key.replace(keys.get(at).cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_selection_follows_its_item_and_stays_where_it_was_once_the_item_leaves() {
        let mut selection = Selection::default();
        assert_eq!(selection.at(&["b", "c"]), 0, "the first item until a key moves");
        assert_eq!(selection.at(&["a", "b", "c"]), 1, "the item drawn selected stays selected as one arrives above");
        selection.select(&["a", "b", "c"], 2);
        assert_eq!(selection.at(&["c", "a", "b"]), 0, "it follows its item as items move");
        assert_eq!(selection.at(&["a", "b", "d"]), 0, "gone, the item now where it was");
        assert_eq!(selection.at(&["x", "a", "b", "d"]), 1, "and that item from then on");
        selection.select(&["x", "a", "b", "d"], usize::MAX);
        assert_eq!(selection.at(&["x", "a"]), 1, "the last item at most");
        assert_eq!(selection.at(&[]), 0);
    }
}
