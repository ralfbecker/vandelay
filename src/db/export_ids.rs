/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::types::ObjectType;

/// Gets or creates the target row for `(session_url, account_id)`, so the id
/// cache below never mixes mappings from two different export targets.
pub fn ensure_target(
    conn: &Connection,
    session_url: &str,
    account_id: &str,
) -> Result<i64, rusqlite::Error> {
    conn.execute(
        "INSERT INTO export_targets (session_url, account_id) VALUES (?1, ?2)
         ON CONFLICT (session_url, account_id) DO NOTHING",
        params![session_url, account_id],
    )?;
    conn.query_row(
        "SELECT id FROM export_targets WHERE session_url = ?1 AND account_id = ?2",
        params![session_url, account_id],
        |row| row.get(0),
    )
}

/// Every cached local id -> target JMAP id mapping for `ty` on this target.
pub fn all_for_type(
    conn: &Connection,
    target_id: i64,
    ty: ObjectType,
) -> Result<HashMap<i64, String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT local_id, jmap_id FROM export_target_ids WHERE target_id = ?1 AND type_name = ?2",
    )?;
    let rows = stmt.query_map(params![target_id, ty.jmap_name()], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map = HashMap::new();
    for r in rows {
        let (local_id, jmap_id) = r?;
        map.insert(local_id, jmap_id);
    }
    Ok(map)
}

pub fn upsert(
    conn: &Connection,
    target_id: i64,
    ty: ObjectType,
    local_id: i64,
    jmap_id: &str,
) -> Result<(), rusqlite::Error> {
    conn.execute(
        "INSERT INTO export_target_ids (target_id, type_name, local_id, jmap_id)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (target_id, type_name, local_id) DO UPDATE SET jmap_id = excluded.jmap_id",
        params![target_id, ty.jmap_name(), local_id, jmap_id],
    )?;
    Ok(())
}

/// Bulk variant of [`upsert`], one transaction for the whole batch a
/// reconcile pass collected.
pub fn upsert_many(
    conn: &Connection,
    target_id: i64,
    ty: ObjectType,
    pairs: &[(i64, String)],
) -> Result<(), rusqlite::Error> {
    if pairs.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for (local_id, jmap_id) in pairs {
        upsert(&tx, target_id, ty, *local_id, jmap_id)?;
    }
    tx.commit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init::apply_schema(&c).unwrap();
        c
    }

    #[test]
    fn ensure_target_is_idempotent_and_returns_stable_id() {
        let c = mem();
        let id1 = ensure_target(&c, "https://a/jmap", "w").unwrap();
        let id2 = ensure_target(&c, "https://a/jmap", "w").unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_targets_get_different_ids() {
        let c = mem();
        let a = ensure_target(&c, "https://a/jmap", "w").unwrap();
        let b = ensure_target(&c, "https://b/jmap", "w").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn upsert_many_roundtrips_and_updates_existing_rows() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        upsert_many(
            &c,
            t,
            ObjectType::Email,
            &[(1, "e1".to_owned()), (2, "e2".to_owned())],
        )
        .unwrap();
        let map = all_for_type(&c, t, ObjectType::Email).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&1), Some(&"e1".to_owned()));

        upsert(&c, t, ObjectType::Email, 1, "e1-new").unwrap();
        let map = all_for_type(&c, t, ObjectType::Email).unwrap();
        assert_eq!(map.get(&1), Some(&"e1-new".to_owned()));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn cache_is_scoped_per_target() {
        let c = mem();
        let a = ensure_target(&c, "https://a/jmap", "w").unwrap();
        let b = ensure_target(&c, "https://b/jmap", "w").unwrap();
        upsert(&c, a, ObjectType::Email, 1, "on-a").unwrap();
        assert!(all_for_type(&c, b, ObjectType::Email).unwrap().is_empty());
    }

    #[test]
    fn cache_is_scoped_per_type() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        upsert(&c, t, ObjectType::Email, 1, "email-1").unwrap();
        assert!(all_for_type(&c, t, ObjectType::Mailbox).unwrap().is_empty());
    }
}
