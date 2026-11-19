/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use super::error::ImapError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    TransportDrop,
    Transient,
    Permanent,
    PerMessageRecoverable,
    PerFolderPermanent,
    FeatureNegotiation,
    AuthFailed,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_retries: u32,
}

impl RetryPolicy {
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

const SHARED_BASE_MS: u64 = 1_000;
const SHARED_CAP_MS: u64 = 30_000;
const TRANSPORT_BASE_MS: u64 = 500;
const TRANSPORT_CAP_MS: u64 = 30_000;
const LEVEL_CAP: u32 = 16;

#[derive(Default, Clone)]
pub struct BackoffState {
    inner: Arc<BackoffInner>,
}

#[derive(Default)]
struct BackoffInner {
    level: AtomicU32,
    transient_retries: AtomicU32,
    transport_retries: AtomicU32,
}

impl BackoffState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        self.inner.level.store(0, Ordering::Relaxed);
    }

    pub fn current_level(&self) -> u32 {
        self.inner.level.load(Ordering::Relaxed)
    }

    pub fn transient_retries(&self) -> u32 {
        self.inner.transient_retries.load(Ordering::Relaxed)
    }

    pub fn transport_retries(&self) -> u32 {
        self.inner.transport_retries.load(Ordering::Relaxed)
    }

    pub fn total_retries(&self) -> u64 {
        self.transient_retries() as u64 + self.transport_retries() as u64
    }

    pub fn next_shared_delay(&self) -> Duration {
        let prev = self.inner.level.fetch_add(1, Ordering::Relaxed);
        self.inner.transient_retries.fetch_add(1, Ordering::Relaxed);
        let n = prev.saturating_add(1).min(LEVEL_CAP);
        let shift = n.saturating_sub(1).min(15);
        let scaled = SHARED_BASE_MS.saturating_mul(1u64 << shift);
        let scaled = scaled.min(SHARED_CAP_MS);
        let half = scaled / 2;
        Duration::from_millis(jitter_in_range(half, scaled))
    }

    pub fn transport_delay(&self, attempt: u32) -> Duration {
        self.inner.transport_retries.fetch_add(1, Ordering::Relaxed);
        let n = attempt.min(LEVEL_CAP);
        let shift = n.saturating_sub(1).min(15);
        let scaled = TRANSPORT_BASE_MS.saturating_mul(1u64 << shift);
        let scaled = scaled.min(TRANSPORT_CAP_MS);
        Duration::from_millis(jitter_in_range(0, scaled))
    }
}

fn jitter_in_range(lo: u64, hi: u64) -> u64 {
    if hi <= lo {
        return lo;
    }
    let span = hi - lo;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    lo + (nanos % span)
}

pub fn is_negotiation_failure(err: &ImapError) -> bool {
    match err {
        ImapError::Bad(_) => true,
        ImapError::No(no) => !no.is_transient(),
        ImapError::Unsupported(_) => true,
        _ => false,
    }
}

pub fn classify(err: &ImapError) -> Disposition {
    match err {
        ImapError::Io(_) => Disposition::TransportDrop,
        ImapError::Tls(_) => Disposition::Permanent,
        ImapError::Disconnected => Disposition::TransportDrop,
        ImapError::Bye(_) => Disposition::TransportDrop,
        ImapError::AuthFailed(_) => Disposition::AuthFailed,
        ImapError::Bad(_) => Disposition::FeatureNegotiation,
        ImapError::No(no) => {
            if no.is_auth_failed() {
                Disposition::AuthFailed
            } else if no.is_transient() {
                Disposition::Transient
            } else {
                Disposition::Permanent
            }
        }
        ImapError::Protocol(_) => Disposition::Permanent,
        ImapError::Parse(_) => Disposition::Permanent,
        ImapError::Unsupported(_) => Disposition::Permanent,
    }
}

pub fn folder_disposition(err: &ImapError) -> Disposition {
    let base = classify(err);
    if matches!(
        base,
        Disposition::Permanent | Disposition::FeatureNegotiation
    ) {
        Disposition::PerFolderPermanent
    } else {
        base
    }
}

pub fn message_disposition(err: &ImapError) -> Disposition {
    let base = classify(err);
    if matches!(
        base,
        Disposition::Permanent | Disposition::FeatureNegotiation
    ) {
        Disposition::PerMessageRecoverable
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::NoError;
    use super::*;
    use std::io::{self, ErrorKind};

    #[test]
    fn transport_errors_are_transport_drop() {
        for k in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::BrokenPipe,
            ErrorKind::UnexpectedEof,
            ErrorKind::TimedOut,
        ] {
            let e = ImapError::Io(io::Error::new(k, "x"));
            assert_eq!(classify(&e), Disposition::TransportDrop, "{k:?}");
        }
    }

    #[test]
    fn bye_is_transport_drop() {
        assert_eq!(
            classify(&ImapError::Bye("going away".into())),
            Disposition::TransportDrop
        );
    }

    #[test]
    fn auth_failed_classified_separately() {
        let e = ImapError::AuthFailed("bad creds".into());
        assert_eq!(classify(&e), Disposition::AuthFailed);
        let e = ImapError::No(NoError::new("x", Some("AUTHENTICATIONFAILED".into())));
        assert_eq!(classify(&e), Disposition::AuthFailed);
    }

    #[test]
    fn transient_no_text_marked_transient() {
        let e = ImapError::No(NoError::new("Try again later", None));
        assert_eq!(classify(&e), Disposition::Transient);
        let e = ImapError::No(NoError::new("user over quota", None));
        assert_eq!(classify(&e), Disposition::Transient);
    }

    #[test]
    fn non_transient_no_is_permanent() {
        let e = ImapError::No(NoError::new("mailbox does not exist", None));
        assert_eq!(classify(&e), Disposition::Permanent);
    }

    #[test]
    fn bad_is_feature_negotiation() {
        let e = ImapError::Bad("unrecognised command".into());
        assert_eq!(classify(&e), Disposition::FeatureNegotiation);
    }

    #[test]
    fn protocol_and_parse_are_permanent() {
        assert_eq!(
            classify(&ImapError::Protocol("x".into())),
            Disposition::Permanent
        );
        assert_eq!(
            classify(&ImapError::Parse("x".into())),
            Disposition::Permanent
        );
    }

    #[test]
    fn folder_disposition_lifts_permanent_to_per_folder() {
        let e = ImapError::No(NoError::new("no such mailbox", None));
        assert_eq!(folder_disposition(&e), Disposition::PerFolderPermanent);
    }

    #[test]
    fn folder_disposition_keeps_transient_class() {
        let e = ImapError::No(NoError::new("try again", None));
        assert_eq!(folder_disposition(&e), Disposition::Transient);
    }

    #[test]
    fn message_disposition_demotes_permanent_to_per_message() {
        let e = ImapError::Parse("bad fetch".into());
        assert_eq!(message_disposition(&e), Disposition::PerMessageRecoverable);
    }

    #[test]
    fn shared_delay_grows_with_consecutive_throttles() {
        let s = BackoffState::new();
        let d1 = s.next_shared_delay();
        let d3 = {
            let _ = s.next_shared_delay();
            s.next_shared_delay()
        };
        assert!(d1 <= Duration::from_millis(SHARED_BASE_MS));
        assert!(
            d3 >= d1,
            "expected non-decreasing delay, got {d1:?} -> {d3:?}"
        );
        assert!(d3 <= Duration::from_millis(SHARED_CAP_MS));
    }

    #[test]
    fn shared_delay_caps_at_cap_ms() {
        let s = BackoffState::new();
        for _ in 0..40 {
            let _ = s.next_shared_delay();
        }
        let d = s.next_shared_delay();
        assert!(d <= Duration::from_millis(SHARED_CAP_MS));
    }

    #[test]
    fn reset_clears_level() {
        let s = BackoffState::new();
        let _ = s.next_shared_delay();
        let _ = s.next_shared_delay();
        assert!(s.current_level() > 0);
        s.reset();
        assert_eq!(s.current_level(), 0);
    }

    #[test]
    fn transport_delay_does_not_touch_shared_level() {
        let s = BackoffState::new();
        let _ = s.transport_delay(1);
        let _ = s.transport_delay(2);
        let _ = s.transport_delay(3);
        assert_eq!(s.current_level(), 0, "transport delays are local");
    }

    #[test]
    fn transport_delay_bounded() {
        let s = BackoffState::new();
        let d1 = s.transport_delay(1);
        let d6 = s.transport_delay(6);
        assert!(d1 <= Duration::from_millis(TRANSPORT_BASE_MS));
        assert!(d6 <= Duration::from_millis(TRANSPORT_CAP_MS));
    }

    #[test]
    fn shared_state_is_cloneable_and_shared() {
        let a = BackoffState::new();
        let b = a.clone();
        let _ = a.next_shared_delay();
        assert_eq!(a.current_level(), b.current_level());
    }

    #[test]
    fn is_negotiation_failure_matches_bad_unsupported_and_nontransient_no() {
        assert!(is_negotiation_failure(&ImapError::Bad("x".into())));
        assert!(is_negotiation_failure(&ImapError::Unsupported("x".into())));
        assert!(is_negotiation_failure(&ImapError::No(NoError::new(
            "no such mailbox",
            None
        ))));
    }

    #[test]
    fn is_negotiation_failure_excludes_transient_no_and_others() {
        assert!(!is_negotiation_failure(&ImapError::No(NoError::new(
            "try again later",
            None
        ))));
        assert!(!is_negotiation_failure(&ImapError::Bye("x".into())));
        assert!(!is_negotiation_failure(&ImapError::Disconnected));
    }

    #[test]
    fn transient_retries_counter_increments_on_shared_delay() {
        let s = BackoffState::new();
        assert_eq!(s.transient_retries(), 0);
        let _ = s.next_shared_delay();
        let _ = s.next_shared_delay();
        assert_eq!(s.transient_retries(), 2);
    }

    #[test]
    fn transport_retries_counter_increments_on_transport_delay() {
        let s = BackoffState::new();
        assert_eq!(s.transport_retries(), 0);
        let _ = s.transport_delay(1);
        let _ = s.transport_delay(2);
        assert_eq!(s.transport_retries(), 2);
    }

    #[test]
    fn nonexistent_mailbox_with_temp_in_its_name_is_permanent_not_transient() {
        let e = ImapError::No(NoError::new(
            "Mailbox doesn't exist: INBOX/Wiederherge  29-temp-1",
            Some("NONEXISTENT".into()),
        ));
        assert_eq!(classify(&e), Disposition::Permanent);
    }

    #[test]
    fn unavailable_and_overquota_are_transient() {
        for code in ["UNAVAILABLE", "OVERQUOTA"] {
            let e = ImapError::No(NoError::new("busy", Some(code.into())));
            assert_eq!(classify(&e), Disposition::Transient, "{code}");
        }
    }
}
