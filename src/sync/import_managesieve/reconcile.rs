/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, params};

use crate::db;
use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresentOutcome {
    Unchanged,
    ActiveOnly,
    ContentUpdated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    New {
        name: String,
        active: bool,
    },
    Present {
        name: String,
        local_id: i64,
        outcome: PresentOutcome,
        active: bool,
    },
    Vanished {
        name: String,
        local_id: i64,
    },
}

pub struct Plan {
    pub actions: Vec<Action>,
}

pub fn plan(
    server: &[(String, bool, [u8; 32])],
    local: &HashMap<String, i64>,
    local_hashes: &HashMap<i64, [u8; 32]>,
    local_active: &HashMap<i64, bool>,
) -> Plan {
    let mut actions = Vec::new();
    let mut server_names: HashSet<&str> = HashSet::new();
    for (name, active, hash) in server {
        server_names.insert(name.as_str());
        match local.get(name) {
            None => actions.push(Action::New {
                name: name.clone(),
                active: *active,
            }),
            Some(local_id) => {
                let cur_hash = local_hashes.get(local_id);
                let cur_active = local_active.get(local_id).copied().unwrap_or(false);
                let outcome = if cur_hash != Some(hash) {
                    PresentOutcome::ContentUpdated
                } else if cur_active != *active {
                    PresentOutcome::ActiveOnly
                } else {
                    PresentOutcome::Unchanged
                };
                actions.push(Action::Present {
                    name: name.clone(),
                    local_id: *local_id,
                    outcome,
                    active: *active,
                });
            }
        }
    }
    for (name, local_id) in local {
        if !server_names.contains(name.as_str()) {
            actions.push(Action::Vanished {
                name: name.clone(),
                local_id: *local_id,
            });
        }
    }
    Plan { actions }
}

pub type LocalState = (HashMap<i64, [u8; 32]>, HashMap<i64, bool>);

pub fn load_local_state(conn: &Connection, source_id: i64) -> Result<LocalState, Error> {
    let mut stmt = conn.prepare(
        "SELECT s.id, b.hash, s.is_active
         FROM sieve_scripts s JOIN blobs b ON b.id = s.blob_id
         JOIN sync_id_managesieve m ON m.local_id = s.id
         WHERE m.source_id = ?1",
    )?;
    let rows = stmt.query_map(params![source_id], |row| {
        let id: i64 = row.get(0)?;
        let hash: Vec<u8> = row.get(1)?;
        let active: i64 = row.get(2)?;
        Ok((id, hash, active != 0))
    })?;
    let mut hashes = HashMap::new();
    let mut active = HashMap::new();
    for r in rows {
        let (id, hash, a) = r?;
        if hash.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&hash);
            hashes.insert(id, arr);
        }
        active.insert(id, a);
    }
    Ok((hashes, active))
}

pub fn apply_new(
    tx: &rusqlite::Transaction<'_>,
    source_id: i64,
    name: &str,
    bytes: &[u8],
) -> Result<i64, Error> {
    let blob_id = db::blobs::intern_blob(tx, bytes)?;
    // `sieve_scripts.name` is globally unique in the archive, but the
    // id-mapping this action is based on ("New" = not yet mapped) is scoped
    // to `source_id`. After --allow-source-change re-points the archive at a
    // new source, a script of this name can already exist from the old one;
    // inserting again would hit the UNIQUE constraint. Adopting the existing
    // row instead keeps a "same account, new connection details" rerun
    // convergent rather than failing.
    let existing_id: Option<i64> = tx
        .query_row(
            "SELECT id FROM sieve_scripts WHERE name = ?1",
            params![name],
            |row| row.get(0),
        )
        .optional()?;
    let local_id = if let Some(id) = existing_id {
        tx.execute(
            "UPDATE sieve_scripts SET blob_id = ?1 WHERE id = ?2",
            params![blob_id, id],
        )?;
        id
    } else {
        tx.execute(
            "INSERT INTO sieve_scripts (name, is_active, blob_id) VALUES (?1, 0, ?2)",
            params![name, blob_id],
        )?;
        tx.last_insert_rowid()
    };
    db::managesieve_ids::insert(tx, source_id, name, local_id)?;
    Ok(local_id)
}

pub fn apply_content_update(
    tx: &rusqlite::Transaction<'_>,
    local_id: i64,
    bytes: &[u8],
) -> Result<(), Error> {
    let blob_id = db::blobs::intern_blob(tx, bytes)?;
    tx.execute(
        "UPDATE sieve_scripts SET blob_id = ?1 WHERE id = ?2",
        params![blob_id, local_id],
    )?;
    Ok(())
}

pub fn apply_delete(
    tx: &rusqlite::Transaction<'_>,
    source_id: i64,
    name: &str,
    local_id: i64,
) -> Result<(), Error> {
    tx.execute("DELETE FROM sieve_scripts WHERE id = ?1", params![local_id])?;
    db::managesieve_ids::delete(tx, source_id, name)?;
    Ok(())
}

pub fn apply_active_assignment(
    conn: &mut Connection,
    new_active_local: Option<i64>,
) -> Result<(), Error> {
    let tx = conn.transaction()?;
    let existing_active: Option<i64> = tx
        .query_row(
            "SELECT id FROM sieve_scripts WHERE is_active = 1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if existing_active != new_active_local {
        if existing_active.is_some() {
            tx.execute(
                "UPDATE sieve_scripts SET is_active = 0 WHERE is_active = 1",
                [],
            )?;
        }
        if let Some(id) = new_active_local {
            tx.execute(
                "UPDATE sieve_scripts SET is_active = 1 WHERE id = ?1",
                params![id],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init;
    use crate::db::sources::{SourceKey, upsert_source};
    use blake3::Hasher;

    fn hash(b: &[u8]) -> [u8; 32] {
        let mut h = Hasher::new();
        h.update(b);
        h.finalize().into()
    }

    fn mem() -> (Connection, i64) {
        let c = Connection::open_in_memory().unwrap();
        init::apply_schema(&c).unwrap();
        let sid = upsert_source(
            &c,
            &SourceKey {
                kind: "managesieve".to_owned(),
                session_url: "sieve://h:4190".to_owned(),
                account_id: "a".to_owned(),
            },
            None,
            "a",
        )
        .unwrap();
        (c, sid)
    }

    #[test]
    fn plan_classifies_new_present_unchanged_and_vanished() {
        let h_v = hash(b"v");
        let h_w = hash(b"w");
        let server: Vec<(String, bool, [u8; 32])> = vec![
            ("a".into(), false, h_v),
            ("b".into(), true, h_w),
            ("c".into(), false, h_v),
        ];
        let mut local: HashMap<String, i64> = HashMap::new();
        local.insert("b".into(), 2);
        local.insert("c".into(), 3);
        local.insert("d".into(), 4);
        let mut lh: HashMap<i64, [u8; 32]> = HashMap::new();
        lh.insert(2, h_w);
        lh.insert(3, h_v);
        lh.insert(4, h_v);
        let mut la: HashMap<i64, bool> = HashMap::new();
        la.insert(2, true);
        la.insert(3, false);
        la.insert(4, false);
        let p = plan(&server, &local, &lh, &la);
        let mut new_count = 0;
        let mut unchanged = 0;
        let mut content_updated = 0;
        let mut active_only = 0;
        let mut vanished = 0;
        for a in p.actions {
            match a {
                Action::New { .. } => new_count += 1,
                Action::Present {
                    outcome: PresentOutcome::Unchanged,
                    ..
                } => unchanged += 1,
                Action::Present {
                    outcome: PresentOutcome::ContentUpdated,
                    ..
                } => content_updated += 1,
                Action::Present {
                    outcome: PresentOutcome::ActiveOnly,
                    ..
                } => active_only += 1,
                Action::Vanished { .. } => vanished += 1,
            }
        }
        assert_eq!(new_count, 1);
        assert_eq!(unchanged, 2);
        assert_eq!(content_updated, 0);
        assert_eq!(active_only, 0);
        assert_eq!(vanished, 1);
    }

    #[test]
    fn plan_flags_content_update_when_hash_differs() {
        let h_v = hash(b"v");
        let h_w = hash(b"w");
        let server: Vec<(String, bool, [u8; 32])> = vec![("a".into(), false, h_w)];
        let mut local: HashMap<String, i64> = HashMap::new();
        local.insert("a".into(), 1);
        let mut lh = HashMap::new();
        lh.insert(1, h_v);
        let mut la = HashMap::new();
        la.insert(1, false);
        let p = plan(&server, &local, &lh, &la);
        assert!(matches!(
            p.actions[0],
            Action::Present {
                outcome: PresentOutcome::ContentUpdated,
                ..
            }
        ));
    }

    #[test]
    fn plan_flags_active_only_when_only_flag_differs() {
        let h = hash(b"same");
        let server: Vec<(String, bool, [u8; 32])> = vec![("a".into(), true, h)];
        let mut local: HashMap<String, i64> = HashMap::new();
        local.insert("a".into(), 1);
        let mut lh = HashMap::new();
        lh.insert(1, h);
        let mut la = HashMap::new();
        la.insert(1, false);
        let p = plan(&server, &local, &lh, &la);
        assert!(matches!(
            p.actions[0],
            Action::Present {
                outcome: PresentOutcome::ActiveOnly,
                ..
            }
        ));
    }

    #[test]
    fn apply_new_inserts_script_and_id_mapping() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id = apply_new(&tx, sid, "vacation", b"require[\"x\"];").unwrap();
        tx.commit().unwrap();
        assert!(id > 0);
        let local = db::managesieve_ids::local_for(&c, sid, "vacation")
            .unwrap()
            .unwrap();
        assert_eq!(local, id);
        let count: i64 = c
            .query_row("SELECT count(*) FROM sieve_scripts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn apply_new_adopts_existing_row_left_by_a_different_source() {
        let (mut c, old_sid) = mem();
        let tx = c.transaction().unwrap();
        let old_id = apply_new(&tx, old_sid, "mail", b"require [\"fileinto\"];\n").unwrap();
        tx.commit().unwrap();

        // Simulate --allow-source-change: a new source row, unrelated to the
        // old one, with no id-mapping of its own yet for "mail".
        let new_sid = db::sources::upsert_source(
            &c,
            &db::sources::SourceKey {
                kind: "managesieve".to_owned(),
                session_url: "sieve://new-host:4190".to_owned(),
                account_id: "a".to_owned(),
            },
            None,
            "a",
        )
        .unwrap();
        assert_ne!(old_sid, new_sid);

        let tx = c.transaction().unwrap();
        let id = apply_new(&tx, new_sid, "mail", b"require [\"vacation\"];\n").unwrap();
        tx.commit().unwrap();

        assert_eq!(id, old_id, "must reuse the existing row, not duplicate it");
        let count: i64 = c
            .query_row("SELECT count(*) FROM sieve_scripts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "no duplicate row under the new source");
        let local = db::managesieve_ids::local_for(&c, new_sid, "mail")
            .unwrap()
            .unwrap();
        assert_eq!(local, old_id);
        let blob: Vec<u8> = c
            .query_row(
                "SELECT b.data FROM sieve_scripts s JOIN blobs b ON b.id = s.blob_id WHERE s.id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(blob, b"require [\"vacation\"];\n");
    }

    #[test]
    fn apply_content_update_swaps_blob_pointer() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id = apply_new(&tx, sid, "vacation", b"v1").unwrap();
        tx.commit().unwrap();
        let tx = c.transaction().unwrap();
        apply_content_update(&tx, id, b"v2").unwrap();
        tx.commit().unwrap();
        let blob: Vec<u8> = c
            .query_row(
                "SELECT b.data FROM sieve_scripts s JOIN blobs b ON b.id=s.blob_id WHERE s.id=?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(blob, b"v2");
    }

    #[test]
    fn apply_active_assignment_swaps_active_in_one_transaction() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id_a = apply_new(&tx, sid, "a", b"x").unwrap();
        let id_b = apply_new(&tx, sid, "b", b"y").unwrap();
        tx.commit().unwrap();
        apply_active_assignment(&mut c, Some(id_a)).unwrap();
        let cur: Option<i64> = c
            .query_row(
                "SELECT id FROM sieve_scripts WHERE is_active = 1",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(cur, Some(id_a));
        apply_active_assignment(&mut c, Some(id_b)).unwrap();
        let cur: Option<i64> = c
            .query_row(
                "SELECT id FROM sieve_scripts WHERE is_active = 1",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(cur, Some(id_b));
        apply_active_assignment(&mut c, None).unwrap();
        let cur: Option<i64> = c
            .query_row(
                "SELECT id FROM sieve_scripts WHERE is_active = 1",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(cur, None);
    }

    #[test]
    fn apply_delete_clears_active_before_destroy() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id = apply_new(&tx, sid, "a", b"x").unwrap();
        tx.commit().unwrap();
        apply_active_assignment(&mut c, Some(id)).unwrap();
        let tx = c.transaction().unwrap();
        apply_delete(&tx, sid, "a", id).unwrap();
        tx.commit().unwrap();
        let count: i64 = c
            .query_row("SELECT count(*) FROM sieve_scripts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert!(
            db::managesieve_ids::local_for(&c, sid, "a")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn apply_new_dedups_identical_script_bytes_at_blob_level() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id_a = apply_new(&tx, sid, "a", b"require [\"fileinto\"];\n").unwrap();
        let id_b = apply_new(&tx, sid, "b", b"require [\"fileinto\"];\n").unwrap();
        tx.commit().unwrap();
        let blob_a: i64 = c
            .query_row(
                "SELECT blob_id FROM sieve_scripts WHERE id = ?1",
                params![id_a],
                |r| r.get(0),
            )
            .unwrap();
        let blob_b: i64 = c
            .query_row(
                "SELECT blob_id FROM sieve_scripts WHERE id = ?1",
                params![id_b],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(blob_a, blob_b, "identical bytes must share a blob row");
        let blob_count: i64 = c
            .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blob_count, 1, "blob table must contain a single row");
    }

    #[test]
    fn apply_content_update_orphans_old_blob_for_gc() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id = apply_new(&tx, sid, "a", b"v1").unwrap();
        tx.commit().unwrap();
        let tx = c.transaction().unwrap();
        apply_content_update(&tx, id, b"v2").unwrap();
        tx.commit().unwrap();
        let blob_count_before_gc: i64 = c
            .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blob_count_before_gc, 2, "old blob still present pre-GC");
        let tx = c.unchecked_transaction().unwrap();
        crate::db::blobs::gc_orphan_blobs(&tx).unwrap();
        tx.commit().unwrap();
        let blob_count_after_gc: i64 = c
            .query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blob_count_after_gc, 1, "orphan blob reclaimed by GC");
    }

    #[test]
    fn partial_unique_index_rejects_two_active_rows() {
        let (mut c, sid) = mem();
        let tx = c.transaction().unwrap();
        let id_a = apply_new(&tx, sid, "a", b"x").unwrap();
        let id_b = apply_new(&tx, sid, "b", b"y").unwrap();
        tx.commit().unwrap();
        c.execute(
            "UPDATE sieve_scripts SET is_active = 1 WHERE id = ?1",
            params![id_a],
        )
        .unwrap();
        let err = c
            .execute(
                "UPDATE sieve_scripts SET is_active = 1 WHERE id = ?1",
                params![id_b],
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("UNIQUE") || msg.contains("unique"),
            "expected unique-constraint failure, got {msg}"
        );
    }
}
