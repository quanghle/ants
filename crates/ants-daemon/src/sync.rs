//! One way to take a std lock across the workspace: a poisoned lock is taken as is.
//!
//! Release builds abort on panic (`panic = "abort"`), so no lock is ever poisoned there. In debug builds and tests the
//! panic that poisoned a lock is the failure to report: later users carry on with the data as the panicking holder
//! left it, rather than piling a `PoisonError` panic on top.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// [`Mutex::lock`], ignoring poisoning.
pub trait MutexExt<T: ?Sized> {
    fn locked(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> MutexExt<T> for Mutex<T> {
    fn locked(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// [`RwLock::read`] and [`RwLock::write`], ignoring poisoning.
pub trait RwLockExt<T: ?Sized> {
    fn read_locked(&self) -> RwLockReadGuard<'_, T>;
    fn write_locked(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    fn read_locked(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_locked(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}
