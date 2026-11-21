//! Synchronization primitives, swapped for loom's under `--cfg loom` so
//! model tests can explore every interleaving.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;
#[cfg(loom)]
pub(crate) use loom::sync::{Mutex, RwLock};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;
#[cfg(not(loom))]
pub(crate) use std::sync::{Mutex, RwLock};
