/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashSet;

use crate::imap::client::ImapClient;
use crate::imap::command;
use crate::imap::error::ImapError;
use crate::imap::response::Untagged;
use crate::imap::retry::is_negotiation_failure;

pub fn select_uids(client: &mut ImapClient) -> Result<Vec<u32>, ImapError> {
    if client.has_capability("ESEARCH") {
        match client.run_collect(command::uid_search_esearch_all()) {
            Ok(resp) => {
                for u in &resp.untagged {
                    if let Untagged::Esearch { all, .. } = u {
                        return Ok(all.clone());
                    }
                }
            }
            Err(e) if is_negotiation_failure(&e) => {}
            Err(e) => return Err(e),
        }
    }
    match client.run_collect(command::uid_search_all()) {
        Ok(resp) => {
            for u in &resp.untagged {
                if let Untagged::Search(uids) = u {
                    return Ok(uids.clone());
                }
            }
            Ok(Vec::new())
        }
        Err(e) if is_negotiation_failure(&e) => uids_via_fetch(client),
        Err(e) => Err(e),
    }
}

fn uids_via_fetch(client: &mut ImapClient) -> Result<Vec<u32>, ImapError> {
    let resp = client.run_collect(command::uid_fetch_all_uids())?;
    let mut out = Vec::new();
    for u in resp.untagged {
        if let Untagged::Fetch { items, .. } = u {
            for (n, v) in items {
                if n == "UID"
                    && let Some(num) = v.as_number()
                {
                    out.push(num as u32);
                }
            }
        }
    }
    Ok(out)
}

/// Largest message that fits into the archive: SQLite's default limit for a
/// single BLOB is 1,000,000,000 bytes (SQLITE_MAX_LENGTH), keep a little slack.
pub const MAX_MESSAGE_BYTES: u64 = 999_000_000;

/// Return the `(uid, size)` of those of `uids` whose RFC822.SIZE exceeds
/// [`MAX_MESSAGE_BYTES`], asking the server for sizes only (no bodies).
pub fn oversize_uids(
    client: &mut ImapClient,
    uids: &[u32],
) -> Result<Vec<(u32, u64)>, ImapError> {
    let mut out = Vec::new();
    for chunk in uids.chunks(1000) {
        let set = chunk
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let resp = client.run_collect(&command::uid_fetch(&set, &["UID", "RFC822.SIZE"]))?;
        for u in &resp.untagged {
            if let Some(attrs) = super::fetch::extract(u)
                && let (Some(uid), Some(size)) = (attrs.uid, attrs.size)
                && size > MAX_MESSAGE_BYTES
            {
                out.push((uid, size));
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Default)]
pub struct UidDiff {
    pub new: Vec<u32>,
    pub vanished: Vec<u32>,
    pub present: Vec<u32>,
}

pub fn diff_uids(local: &[u32], server: &[u32]) -> UidDiff {
    let local_set: HashSet<u32> = local.iter().copied().collect();
    let server_set: HashSet<u32> = server.iter().copied().collect();
    let mut new = Vec::new();
    let mut present = Vec::new();
    for &uid in server {
        if local_set.contains(&uid) {
            present.push(uid);
        } else {
            new.push(uid);
        }
    }
    let mut vanished: Vec<u32> = local
        .iter()
        .copied()
        .filter(|u| !server_set.contains(u))
        .collect();
    new.sort_unstable();
    new.dedup();
    vanished.sort_unstable();
    vanished.dedup();
    present.sort_unstable();
    present.dedup();
    UidDiff {
        new,
        vanished,
        present,
    }
}

pub fn chunks(uids: &[u32], chunk_size: usize) -> Vec<&[u32]> {
    if chunk_size == 0 {
        return vec![uids];
    }
    uids.chunks(chunk_size).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_basic() {
        let local = vec![1, 2, 5];
        let server = vec![2, 3, 5, 7];
        let d = diff_uids(&local, &server);
        assert_eq!(d.new, vec![3, 7]);
        assert_eq!(d.vanished, vec![1]);
        assert_eq!(d.present, vec![2, 5]);
    }

    #[test]
    fn diff_empty_local_is_all_new() {
        let d = diff_uids(&[], &[1, 2, 3]);
        assert_eq!(d.new, vec![1, 2, 3]);
        assert!(d.vanished.is_empty());
        assert!(d.present.is_empty());
    }

    #[test]
    fn diff_empty_server_is_all_vanished() {
        let d = diff_uids(&[1, 2, 3], &[]);
        assert_eq!(d.vanished, vec![1, 2, 3]);
        assert!(d.new.is_empty());
        assert!(d.present.is_empty());
    }

    #[test]
    fn diff_dedups_inputs() {
        let d = diff_uids(&[1, 1, 2, 2], &[2, 2, 3, 3]);
        assert_eq!(d.new, vec![3]);
        assert_eq!(d.vanished, vec![1]);
        assert_eq!(d.present, vec![2]);
    }

    #[test]
    fn chunks_splits_evenly() {
        let v: Vec<u32> = (1..=10).collect();
        let c = chunks(&v, 3);
        assert_eq!(c.len(), 4);
        assert_eq!(c[0], &[1, 2, 3][..]);
        assert_eq!(c[3], &[10][..]);
    }

    #[test]
    fn chunks_zero_returns_whole() {
        let v: Vec<u32> = vec![1, 2, 3];
        let c = chunks(&v, 0);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0], &v[..]);
    }
}
