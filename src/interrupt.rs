/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Graceful shutdown on Ctrl-C / SIGTERM for long-running import/export runs.
//!
//! Without this, killing a run mid-batch (e.g. a `kill <pid>` on a stuck
//! `--switch`) terminates it immediately, possibly abandoning a batch whose
//! target-side writes already succeeded but whose local id-cache write
//! hadn't happened yet -- exactly the case `--assume-not-deleted-in-destination`
//! on a later run would then risk recreating as a duplicate. Installing this
//! handler turns that signal into a flag: long-running loops check it between
//! batches, stop submitting new work, and let whatever's already in flight
//! finish (including its id-cache write) before exiting.

use std::sync::atomic::{AtomicBool, Ordering};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Installs the signal handler. Safe to call more than once (e.g. in tests);
/// only the first call's handler takes effect, later ones are ignored.
pub fn install() {
    let _ = ctrlc::set_handler(|| {
        INTERRUPTED.store(true, Ordering::SeqCst);
    });
}

/// Whether a shutdown has been requested.
pub fn requested() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}
