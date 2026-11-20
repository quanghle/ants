//! A confirmation a view's key asked for, which `y` next gives: the view takes it out on its next key, any key but the
//! one confirming dropping it.

#[derive(Debug)]
pub struct Armed<T>(Option<T>);

impl<T> Default for Armed<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<T> Armed<T> {
    /// Ask to confirm `what`, or for none nothing.
    pub fn arm(&mut self, what: Option<T>) {
        self.0 = what;
    }

    /// What is asked to confirm, while it waits.
    pub fn get(&self) -> Option<&T> {
        self.0.as_ref()
    }

    /// Take what is asked to confirm, for the key taken now to confirm or drop it.
    pub fn take(&mut self) -> Option<T> {
        self.0.take()
    }

    /// Drop the confirmation asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.take().is_some()
    }
}
