/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ImapError {
    #[error("transport error: {0}")]
    Io(#[from] io::Error),

    #[error("tls error: {0}")]
    Tls(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("response parse error: {0}")]
    Parse(String),

    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("server said BAD: {0}")]
    Bad(String),

    #[error("server said NO: {0}")]
    No(NoError),

    #[error("server closed connection: {0}")]
    Bye(String),

    #[error("connection is no longer usable")]
    Disconnected,

    #[error("unsupported by server: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone)]
pub struct NoError {
    pub text: String,
    pub code: Option<String>,
}

impl NoError {
    pub fn new(text: impl Into<String>, code: Option<String>) -> Self {
        Self {
            text: text.into(),
            code,
        }
    }

    /// Whether retrying the same command later can succeed.
    ///
    /// The response code (RFC 5530) decides first: `NONEXISTENT`, `NOPERM` and the like never
    /// get better by waiting, `UNAVAILABLE`, `OVERQUOTA`, `LIMIT` and `INUSE` do. Only when the
    /// server sent no (known) code the text is searched for markers. That text often contains the
    /// mailbox name (`Mailbox doesn't exist: INBOX/temp-1`), so the markers must be specific:
    /// a bare "temp" matched every folder called Temp, Templates or `...temp-N` and made vandelay
    /// retry (5 times, up to 30 s each) a command which can never succeed.
    pub fn is_transient(&self) -> bool {
        if let Some(code) = &self.code {
            match code.to_ascii_uppercase().as_str() {
                "UNAVAILABLE" | "OVERQUOTA" | "LIMIT" | "INUSE" => return true,
                "NONEXISTENT"
                | "ALREADYEXISTS"
                | "NOPERM"
                | "CANNOT"
                | "CLIENTBUG"
                | "CORRUPTION"
                | "EXPUNGEISSUED"
                | "TOOBIG"
                | "NOTSAVED"
                | "UNKNOWN-CTE"
                | "HASCHILDREN"
                | "INUSE-PERMANENT"
                | "AUTHENTICATIONFAILED"
                | "AUTHORIZATIONFAILED"
                | "PRIVACYREQUIRED"
                | "EXPIRED"
                | "CONTACTADMIN"
                | "NOPERMANENTFLAGS" => return false,
                _ => {}
            }
        }
        let lower = self.text.to_ascii_lowercase();
        // "does not exist" echoes the mailbox name and can never succeed later, whatever the name contains
        if ["doesn't exist", "does not exist", "no such", "not found"]
            .iter()
            .any(|m| lower.contains(m))
        {
            return false;
        }
        const TRANSIENT_MARKERS: &[&str] = &[
            "temporar", // temporary, temporarily
            "try again",
            "try later",
            "unavailable",
            "over quota",
            "overquota",
            "throttle",
            "rate limit",
            "too many",
        ];
        TRANSIENT_MARKERS.iter().any(|m| lower.contains(m))
    }

    pub fn is_auth_failed(&self) -> bool {
        matches!(
            self.code.as_deref(),
            Some("AUTHENTICATIONFAILED") | Some("PRIVACYREQUIRED") | Some("AUTHORIZATIONFAILED")
        )
    }
}

impl fmt::Display for NoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.code {
            Some(code) => write!(f, "[{code}] {}", self.text),
            None => f.write_str(&self.text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_markers_detected_case_insensitively() {
        for s in [
            "Temporary failure",
            "Try again later",
            "Server unavailable",
            "OVER QUOTA",
            "user is rate limited",
            "Too many connections",
        ] {
            assert!(
                NoError::new(s, None).is_transient(),
                "should be transient: {s}"
            );
        }
    }

    #[test]
    fn non_transient_markers_not_flagged() {
        for s in [
            "Mailbox does not exist",
            "Permission denied",
            "Invalid argument",
        ] {
            assert!(
                !NoError::new(s, None).is_transient(),
                "should NOT be transient: {s}"
            );
        }
    }

    #[test]
    fn mailbox_name_containing_temp_is_not_a_transient_marker() {
        // seen on a Dovecot account with thousands of "...temp-N" placeholder folders: GETACL on a
        // \Noselect placeholder answers NO [NONEXISTENT] and echoes the name; it was retried forever
        for name in [
            "Wiederherge  29-temp-1",
            "Temp",
            "Templates",
            "INBOX/temporary stuff",
            "Tempo",
        ] {
            let text = format!("Mailbox doesn't exist: INBOX/{name}");
            assert!(
                !NoError::new(text.clone(), Some("NONEXISTENT".into())).is_transient(),
                "with code: {text}"
            );
            assert!(
                !NoError::new(text.clone(), None).is_transient(),
                "without code: {text}"
            );
        }
    }

    #[test]
    fn permanent_code_wins_over_transient_looking_text() {
        for code in [
            "NONEXISTENT",
            "NOPERM",
            "CANNOT",
            "ALREADYEXISTS",
            "TOOBIG",
            "nonexistent",
        ] {
            assert!(
                !NoError::new(
                    "Server temporarily unavailable, try again later",
                    Some(code.into())
                )
                .is_transient(),
                "{code}"
            );
        }
    }

    #[test]
    fn transient_codes_are_transient_whatever_the_text() {
        for code in ["UNAVAILABLE", "OVERQUOTA", "LIMIT", "INUSE", "unavailable"] {
            assert!(
                NoError::new("failed", Some(code.into())).is_transient(),
                "{code}"
            );
        }
    }

    #[test]
    fn does_not_exist_text_is_permanent_even_without_code() {
        for s in [
            "Mailbox does not exist",
            "no such mailbox: Temporary",
            "Folder not found: try again",
        ] {
            assert!(!NoError::new(s, None).is_transient(), "{s}");
        }
    }

    #[test]
    fn unknown_code_falls_back_to_the_text() {
        assert!(
            NoError::new("Temporary failure, try again", Some("SOMETHING".into())).is_transient()
        );
        assert!(!NoError::new("Permission denied", Some("SOMETHING".into())).is_transient());
    }

    #[test]
    fn temporary_wording_is_still_transient() {
        for s in [
            "Temporary failure",
            "temporarily unavailable",
            "Temp error, try again later",
        ] {
            // the third one has no "temporar" but "try again"
            assert!(NoError::new(s, None).is_transient(), "{s}");
        }
    }

    #[test]
    fn auth_failed_codes_detected() {
        assert!(NoError::new("x", Some("AUTHENTICATIONFAILED".into())).is_auth_failed());
        assert!(NoError::new("x", Some("PRIVACYREQUIRED".into())).is_auth_failed());
        assert!(!NoError::new("x", Some("ALERT".into())).is_auth_failed());
        assert!(!NoError::new("x", None).is_auth_failed());
    }
}
