/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::imap::client::{ConnectMode, ImapClient};
use crate::imap::command;
use crate::imap::error::ImapError;
use crate::imap::name::encode_mailbox_name_with;
use crate::imap::response::Untagged;
use crate::imap::retry::{BackoffState, Disposition, RetryPolicy, classify};
use crate::imap::transport::Connector;
use crate::logging::Logger;

use super::coordinator::{Endpoint, ImapAuth, authenticate_client};
use super::fetch::FetchAttrs;

pub const HARD_CAP: usize = 8;

/// Default for [`ByteBudget`]: fetched bodies that may wait for the (single) SQLite writer.
pub const DEFAULT_BUFFER_MB: usize = 256;

/// Budget in bytes for message bodies fetched by the worker connections but not yet stored.
///
/// The fetch connections are usually faster than the one thread writing into SQLite (hashing,
/// indexing, disk). Without a limit the difference piles up in an unbounded channel: importing an
/// 82 GB archive mailbox grew to 31 GB of memory and was killed by the OOM killer.
///
/// A worker calls [`acquire`](Self::acquire) with the size of a message it just received and
/// blocks while the budget is used up, the writer calls [`release`](Self::release) once the
/// message is stored. A single message larger than the whole budget is still admitted when
/// nothing else is in flight, so the memory is bounded by `max(limit, largest message)` plus the
/// message each connection is currently receiving.
pub struct ByteBudget {
    limit: usize,
    state: Mutex<BudgetState>,
    freed: Condvar,
}

#[derive(Default)]
struct BudgetState {
    in_flight: usize,
    peak: usize,
    aborted: bool,
}

impl ByteBudget {
    pub fn new(limit: usize) -> Arc<ByteBudget> {
        Arc::new(ByteBudget {
            limit: limit.max(1),
            state: Mutex::new(BudgetState::default()),
            freed: Condvar::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BudgetState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn acquire(&self, n: usize) {
        let mut st = self.lock();
        while !st.aborted && st.in_flight > 0 && st.in_flight + n > self.limit {
            st = self.freed.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        st.in_flight += n;
        st.peak = st.peak.max(st.in_flight);
    }

    pub fn release(&self, n: usize) {
        let mut st = self.lock();
        st.in_flight = st.in_flight.saturating_sub(n);
        drop(st);
        self.freed.notify_all();
    }

    /// The consumer stopped (error, interrupt): never block a worker again until [`reset`](Self::reset),
    /// otherwise the workers would wait forever for releases that do not come and `shutdown()` would hang.
    pub fn abort(&self) {
        self.lock().aborted = true;
        self.freed.notify_all();
    }

    /// Start of a folder: forget what a previous, aborted folder left behind.
    pub fn reset(&self) {
        let mut st = self.lock();
        st.in_flight = 0;
        st.aborted = false;
        drop(st);
        self.freed.notify_all();
    }

    pub fn in_flight(&self) -> usize {
        self.lock().in_flight
    }

    /// Highest number of bytes waiting at the same time since the budget was created.
    pub fn peak(&self) -> usize {
        self.lock().peak
    }
}

/// `VANDELAY_IMAP_BUFFER_MB`, default [`DEFAULT_BUFFER_MB`], in bytes.
pub fn buffer_limit_from_env() -> usize {
    std::env::var("VANDELAY_IMAP_BUFFER_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(DEFAULT_BUFFER_MB)
        .saturating_mul(1024 * 1024)
}

pub struct FetchJob {
    pub folder: String,
    pub uidvalidity: u32,
    pub uids: Vec<u32>,
}

pub enum FetchEvent {
    Item {
        folder: String,
        uidvalidity: u32,
        attrs: FetchAttrs,
        /// Bytes taken from the [`ByteBudget`], give them back with [`WorkerPool::release`] once stored.
        cost: usize,
    },
    ChunkDone {
        folder: String,
        uidvalidity: u32,
        uids_requested: Vec<u32>,
        outcome: Result<(), ImapError>,
    },
}

pub struct WorkerArgs {
    pub connector: Arc<Connector>,
    pub endpoint: Arc<Endpoint>,
    pub mode: ConnectMode,
    pub auth: ImapAuth,
    pub compress: bool,
    pub policy: RetryPolicy,
    pub backoff: BackoffState,
    pub logger: Logger,
}

pub struct WorkerPool {
    job_tx: Sender<FetchJob>,
    event_rx: Receiver<FetchEvent>,
    handles: Vec<thread::JoinHandle<()>>,
    budget: Arc<ByteBudget>,
}

impl WorkerPool {
    pub fn start(
        args: WorkerArgs,
        pool_size: usize,
        buffer_limit: usize,
    ) -> Result<WorkerPool, ImapError> {
        let size = pool_size.clamp(1, HARD_CAP);
        let (job_tx, job_rx) = unbounded::<FetchJob>();
        let (event_tx, event_rx) = unbounded::<FetchEvent>();
        let mut handles = Vec::with_capacity(size);
        let args = Arc::new(args);
        let budget = ByteBudget::new(buffer_limit);

        for _ in 0..size {
            let args = args.clone();
            let job_rx = job_rx.clone();
            let event_tx = event_tx.clone();
            let budget = budget.clone();
            let handle = thread::spawn(move || {
                worker_loop(args, job_rx, event_tx, budget);
            });
            handles.push(handle);
        }

        Ok(WorkerPool {
            job_tx,
            event_rx,
            handles,
            budget,
        })
    }

    /// Give the bytes of a stored [`FetchEvent::Item`] back to the budget.
    pub fn release(&self, cost: usize) {
        self.budget.release(cost);
    }

    /// Call before the first job of a folder.
    pub fn begin_folder(&self) {
        self.budget.reset();
    }

    /// Call when a folder is left before all its events were consumed.
    pub fn abort_folder(&self) {
        self.budget.abort();
    }

    pub fn peak_buffered(&self) -> usize {
        self.budget.peak()
    }

    pub fn submit(&self, job: FetchJob) {
        let _ = self.job_tx.send(job);
    }

    pub fn recv(&self) -> Result<FetchEvent, crossbeam_channel::RecvError> {
        self.event_rx.recv()
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<FetchEvent, crossbeam_channel::RecvTimeoutError> {
        self.event_rx.recv_timeout(timeout)
    }

    pub fn shutdown(self) {
        drop(self.job_tx);
        for h in self.handles {
            let _ = h.join();
        }
    }
}

fn worker_loop(
    args: Arc<WorkerArgs>,
    job_rx: Receiver<FetchJob>,
    event_tx: Sender<FetchEvent>,
    budget: Arc<ByteBudget>,
) {
    let mut client: Option<ImapClient> = None;
    let mut current_folder: Option<String> = None;
    while let Ok(job) = job_rx.recv() {
        let job_folder = job.folder.clone();
        let job_uv = job.uidvalidity;
        let job_uids = job.uids.clone();
        let event_tx_for_job = event_tx.clone();
        let outcome = match catch_unwind(AssertUnwindSafe(|| {
            run_job_with_retry(
                &args,
                &mut client,
                &mut current_folder,
                &job,
                &event_tx_for_job,
                &budget,
            )
        })) {
            Ok(r) => r,
            Err(_) => {
                client = None;
                current_folder = None;
                Err(ImapError::Protocol("worker thread panicked".into()))
            }
        };
        let _ = event_tx.send(FetchEvent::ChunkDone {
            folder: job_folder,
            uidvalidity: job_uv,
            uids_requested: job_uids,
            outcome,
        });
    }
}

fn run_job_with_retry(
    args: &WorkerArgs,
    client_slot: &mut Option<ImapClient>,
    current_folder: &mut Option<String>,
    job: &FetchJob,
    event_tx: &Sender<FetchEvent>,
    budget: &ByteBudget,
) -> Result<(), ImapError> {
    let mut transient_attempts: u32 = 0;
    let mut transport_attempts: u32 = 0;
    loop {
        if client_slot.is_none() {
            match connect_and_auth(args) {
                Ok(c) => {
                    *client_slot = Some(c);
                    *current_folder = None;
                }
                Err(e) => {
                    let disp = classify(&e);
                    if disp == Disposition::TransportDrop
                        && transport_attempts < args.policy.max_retries
                    {
                        transport_attempts += 1;
                        std::thread::sleep(args.backoff.transport_delay(transport_attempts));
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        let Some(client) = client_slot.as_mut() else {
            return Err(ImapError::Protocol(
                "worker pool: client slot empty after connect".into(),
            ));
        };
        match run_one_job(client, current_folder, job, event_tx, budget) {
            Ok(()) => {
                args.backoff.reset();
                return Ok(());
            }
            Err(e) => match classify(&e) {
                Disposition::TransportDrop => {
                    *client_slot = None;
                    *current_folder = None;
                    if transport_attempts >= args.policy.max_retries {
                        return Err(e);
                    }
                    transport_attempts += 1;
                    std::thread::sleep(args.backoff.transport_delay(transport_attempts));
                }
                Disposition::Transient => {
                    if transient_attempts >= args.policy.max_retries {
                        return Err(e);
                    }
                    transient_attempts += 1;
                    std::thread::sleep(args.backoff.next_shared_delay());
                }
                _ => return Err(e),
            },
        }
    }
}

fn connect_and_auth(args: &WorkerArgs) -> Result<ImapClient, ImapError> {
    let mut client = ImapClient::connect(
        &args.connector,
        &args.endpoint.host,
        args.endpoint.port,
        args.mode,
        args.logger,
    )?;
    authenticate_client(&mut client, &args.auth)
        .map_err(|e| ImapError::AuthFailed(e.to_string()))?;
    let _ = client.refresh_capabilities();
    if args.compress && client.has_capability("COMPRESS=DEFLATE") {
        client.compress_deflate()?;
    }
    if client.has_capability("ENABLE") && client.has_capability("UTF8=ACCEPT") {
        let _ = client.enable(&["UTF8=ACCEPT"]);
    }
    Ok(client)
}

fn run_one_job(
    client: &mut ImapClient,
    current_folder: &mut Option<String>,
    job: &FetchJob,
    event_tx: &Sender<FetchEvent>,
    budget: &ByteBudget,
) -> Result<(), ImapError> {
    if current_folder.as_deref() != Some(job.folder.as_str()) {
        let wire = encode_mailbox_name_with(&job.folder, client.utf8_accept());
        client.run_collect(&command::select(&wire))?;
        *current_folder = Some(job.folder.clone());
    }
    let set = command::format_uid_set(&job.uids, true);
    let folder = job.folder.clone();
    let uv = job.uidvalidity;
    client.run_streamed(
        &command::uid_fetch(
            &set,
            &["UID", "FLAGS", "INTERNALDATE", "RFC822.SIZE", "BODY.PEEK[]"],
        ),
        |u| {
            if let Untagged::Fetch { .. } = &u
                && let Some(attrs) = super::fetch::extract(&u)
            {
                // wait here, with the message in hand but not yet queued, while the writer is behind
                let cost = attrs.body.as_ref().map_or(0, Vec::len);
                budget.acquire(cost);
                if event_tx
                    .send(FetchEvent::Item {
                        folder: folder.clone(),
                        uidvalidity: uv,
                        attrs,
                        cost,
                    })
                    .is_err()
                {
                    budget.release(cost);
                }
            }
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn acquire_below_the_limit_does_not_block() {
        let b = ByteBudget::new(100);
        b.acquire(40);
        b.acquire(60);
        assert_eq!(b.in_flight(), 100);
        b.release(100);
        assert_eq!(b.in_flight(), 0);
    }

    #[test]
    fn a_message_larger_than_the_whole_budget_is_admitted_when_nothing_is_in_flight() {
        let b = ByteBudget::new(10);
        b.acquire(1_000); // must not wait forever
        assert_eq!(b.in_flight(), 1_000);
        assert_eq!(b.peak(), 1_000);
    }

    #[test]
    fn acquire_blocks_until_the_writer_released_enough() {
        let b = ByteBudget::new(100);
        b.acquire(80);
        let done = Arc::new(AtomicUsize::new(0));
        let (b2, d2) = (b.clone(), done.clone());
        let t = thread::spawn(move || {
            b2.acquire(50); // 80 + 50 > 100: has to wait
            d2.store(1, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(150));
        assert_eq!(done.load(Ordering::SeqCst), 0, "must still be blocked");
        b.release(80);
        t.join().unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 1);
        assert_eq!(b.in_flight(), 50);
    }

    #[test]
    fn abort_releases_blocked_workers_and_reset_restores_the_limit() {
        let b = ByteBudget::new(10);
        b.acquire(10);
        let b2 = b.clone();
        let t = thread::spawn(move || b2.acquire(10));
        thread::sleep(Duration::from_millis(100));
        b.abort();
        t.join().unwrap(); // would hang forever without abort()
        b.acquire(5); // aborted: never blocks
        b.reset();
        assert_eq!(b.in_flight(), 0);
        b.acquire(10);
        let b3 = b.clone();
        let t = thread::spawn(move || b3.acquire(1));
        thread::sleep(Duration::from_millis(100));
        assert!(!t.is_finished(), "limit applies again after reset()");
        b.release(10);
        t.join().unwrap();
    }

    #[test]
    fn slow_consumer_cannot_make_the_buffer_grow_beyond_the_limit() {
        // 4 fast "fetch connections" x 200 messages of 1 MiB against one slow writer, budget 8 MiB
        const MIB: usize = 1024 * 1024;
        let budget = ByteBudget::new(8 * MIB);
        let (tx, rx) = unbounded::<usize>();
        let producers: Vec<_> = (0..4)
            .map(|_| {
                let (b, tx) = (budget.clone(), tx.clone());
                thread::spawn(move || {
                    for _ in 0..200 {
                        b.acquire(MIB);
                        tx.send(MIB).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        let mut stored = 0usize;
        while let Ok(n) = rx.recv() {
            if stored % 50 == 0 {
                thread::sleep(Duration::from_millis(2)); // the writer is behind
            }
            stored += 1;
            budget.release(n);
        }
        for p in producers {
            p.join().unwrap();
        }
        assert_eq!(stored, 800);
        assert_eq!(budget.in_flight(), 0);
        assert!(
            budget.peak() <= 8 * MIB,
            "peak {} MiB over the 8 MiB budget",
            budget.peak() / MIB
        );
    }

    #[test]
    fn buffer_limit_from_env_default() {
        // the variable is not set in the test environment
        if std::env::var("VANDELAY_IMAP_BUFFER_MB").is_err() {
            assert_eq!(buffer_limit_from_env(), DEFAULT_BUFFER_MB * 1024 * 1024);
        }
    }
}
