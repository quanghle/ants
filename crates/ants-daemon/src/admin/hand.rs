//! The orders a machine's daemon holds at once ([`super::IN_HAND`]), keyed by the stream sequence the order was stored
//! at, and which of them to decide when: an order waits while an order stored before it, in hand, is to one of its
//! ants, so that the orders to one ant are decided in the order they were stored; orders to other ants go on.
//!
//! An order just taken is unchecked until every order the consumer has delivered and not had acked is known to be in
//! hand ([`Hand::checked`]): until then an earlier order to the same ant may be on its way again, after a delivery to a
//! pull request that went silent, or after its outcome could not be recorded.

use std::collections::BTreeMap;

/// The orders in hand, `M` the message each came in.
#[derive(Debug)]
pub(super) struct Hand<M> {
    slots: BTreeMap<u64, Slot<M>>,
}

#[derive(Debug)]
struct Slot<M> {
    /// The ants the order is to: the ant of its subject, and the clone a clone order makes.
    ants: Vec<String>,
    /// The message, until its order is being decided.
    msg: Option<M>,
    checked: bool,
}

impl<M> Default for Hand<M> {
    fn default() -> Self {
        Self { slots: BTreeMap::new() }
    }
}

impl<M> Hand<M> {
    /// Hold `msg`, the order stored at `seq` to `ants`, unchecked. A delivery again of an order in hand takes the place
    /// of the earlier one while the order waits, and is dropped while it is being decided, whose ack acks it.
    pub(super) fn arrived(&mut self, seq: u64, ants: Vec<String>, msg: M) {
        match self.slots.get_mut(&seq) {
            Some(slot) => {
                if slot.msg.is_some() {
                    slot.msg = Some(msg);
                }
            }
            None => {
                self.slots.insert(seq, Slot { ants, msg: Some(msg), checked: false });
            }
        }
    }

    /// Whether an order in hand waits to be checked.
    pub(super) fn unchecked(&self) -> bool {
        self.slots.values().any(|slot| !slot.checked)
    }

    /// Note every order in hand checked: no earlier order is on its way.
    pub(super) fn checked(&mut self) {
        for slot in self.slots.values_mut() {
            slot.checked = true;
        }
    }

    /// How many orders are in hand.
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    /// The orders to decide now, with their stream sequences, taken out of their slots: those checked, waiting, and to
    /// none of the ants of an order in hand stored before them.
    pub(super) fn ready(&mut self) -> Vec<(u64, M)> {
        let mut busy: Vec<&str> = Vec::new();
        let mut ready = Vec::new();
        for (seq, slot) in &mut self.slots {
            let free = !slot.ants.iter().any(|ant| busy.contains(&ant.as_str()));
            if free
                && slot.checked
                && let Some(msg) = slot.msg.take()
            {
                ready.push((*seq, msg));
            }
            busy.extend(slot.ants.iter().map(String::as_str));
        }
        ready
    }

    /// Drop the order stored at `seq`, decided.
    pub(super) fn done(&mut self, seq: u64) {
        self.slots.remove(&seq);
    }

    /// The messages of the orders waiting, checked or not.
    pub(super) fn waiting(&self) -> impl Iterator<Item = &M> {
        self.slots.values().filter_map(|slot| slot.msg.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ants(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn orders_to_one_ant_go_in_stream_order_and_others_go_on() {
        let mut hand = Hand::default();
        hand.arrived(3, ants(&["dev-a", "dev-c"]), "clone dev-a as dev-c");
        hand.arrived(4, ants(&["dev-b"]), "pause dev-b");
        assert!(hand.ready().is_empty(), "nothing before it is checked");
        hand.checked();
        assert_eq!(hand.ready(), [(3, "clone dev-a as dev-c"), (4, "pause dev-b")]);

        hand.arrived(5, ants(&["dev-a"]), "resume dev-a");
        hand.arrived(6, ants(&["dev-c"]), "pause dev-c");
        hand.arrived(7, ants(&["dev-b"]), "read dev-b");
        hand.checked();
        assert!(!hand.unchecked(), "{hand:?}");
        assert!(hand.ready().is_empty(), "behind the clone and the pause");
        hand.done(4);
        assert_eq!(hand.ready(), [(7, "read dev-b")]);
        hand.done(3);
        assert_eq!(hand.ready(), [(5, "resume dev-a"), (6, "pause dev-c")]);
        assert_eq!(hand.len(), 3);
    }

    #[test]
    fn an_order_delivered_again_waits_before_those_stored_after_it() {
        let mut hand = Hand::default();
        hand.arrived(9, ants(&["dev-a"]), "retire dev-a");
        hand.arrived(8, ants(&["dev-a"]), "pause dev-a, delivered again");
        hand.arrived(9, ants(&["dev-a"]), "retire dev-a, delivered again");
        hand.checked();
        assert_eq!(hand.ready(), [(8, "pause dev-a, delivered again")]);
        hand.arrived(8, ants(&["dev-a"]), "pause dev-a, a third time");
        assert_eq!(hand.waiting().collect::<Vec<_>>(), [&"retire dev-a, delivered again"], "being decided");
        assert!(!hand.unchecked());
        hand.done(8);
        assert_eq!(hand.ready(), [(9, "retire dev-a, delivered again")]);
        hand.done(9);
        assert_eq!(hand.len(), 0);
    }
}
