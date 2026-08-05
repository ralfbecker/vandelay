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

/// Drops cache rows for `local_ids`, e.g. once the target has confirmed the
/// objects they pointed at are actually gone, so a future reconcile stops
/// treating them as still cached.
pub fn delete_many(
    conn: &Connection,
    target_id: i64,
    ty: ObjectType,
    local_ids: &[i64],
) -> Result<(), rusqlite::Error> {
    if local_ids.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for local_id in local_ids {
        tx.execute(
            "DELETE FROM export_target_ids
             WHERE target_id = ?1 AND type_name = ?2 AND local_id = ?3",
            params![target_id, ty.jmap_name(), local_id],
        )?;
    }
    tx.commit()
}

/// The target's Email `state` token as of the end of the last export that
/// completed cleanly, or `None` if there isn't one (never exported before,
/// or the last attempt didn't finish cleanly — see [`set_email_state`]).
pub fn get_email_state(conn: &Connection, target_id: i64) -> Result<Option<String>, rusqlite::Error> {
    conn.query_row(
        "SELECT email_state FROM export_targets WHERE id = ?1",
        params![target_id],
        |row| row.get(0),
    )
}

/// Sets (or, with `None`, clears) the target's persisted Email state token.
/// Callers clear it the instant they decide to rely on a match, before
/// doing anything else, and only set a fresh token once the run that relied
/// on it finishes cleanly — so a run that crashes in between leaves `NULL`
/// rather than a token whose validity can no longer be vouched for.
pub fn set_email_state(
    conn: &Connection,
    target_id: i64,
    state: Option<&str>,
) -> Result<(), rusqlite::Error> {
    conn.execute(
        "UPDATE export_targets SET email_state = ?1 WHERE id = ?2",
        params![state, target_id],
    )?;
    Ok(())
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
    fn delete_many_removes_only_the_named_rows() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        upsert_many(
            &c,
            t,
            ObjectType::Email,
            &[
                (1, "e1".to_owned()),
                (2, "e2".to_owned()),
                (3, "e3".to_owned()),
            ],
        )
        .unwrap();
        delete_many(&c, t, ObjectType::Email, &[2]).unwrap();
        let map = all_for_type(&c, t, ObjectType::Email).unwrap();
        assert_eq!(map.len(), 2);
        assert!(!map.contains_key(&2));
        assert!(map.contains_key(&1) && map.contains_key(&3));
    }

    #[test]
    fn delete_many_with_empty_ids_is_a_no_op() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        upsert(&c, t, ObjectType::Email, 1, "e1").unwrap();
        delete_many(&c, t, ObjectType::Email, &[]).unwrap();
        assert_eq!(all_for_type(&c, t, ObjectType::Email).unwrap().len(), 1);
    }

    #[test]
    fn email_state_defaults_to_none_and_roundtrips() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        assert_eq!(get_email_state(&c, t).unwrap(), None);

        set_email_state(&c, t, Some("s1")).unwrap();
        assert_eq!(get_email_state(&c, t).unwrap(), Some("s1".to_owned()));

        set_email_state(&c, t, Some("s2")).unwrap();
        assert_eq!(get_email_state(&c, t).unwrap(), Some("s2".to_owned()));

        set_email_state(&c, t, None).unwrap();
        assert_eq!(get_email_state(&c, t).unwrap(), None);
    }

    #[test]
    fn email_state_is_scoped_per_target() {
        let c = mem();
        let a = ensure_target(&c, "https://a/jmap", "w").unwrap();
        let b = ensure_target(&c, "https://b/jmap", "w").unwrap();
        set_email_state(&c, a, Some("only-a")).unwrap();
        assert_eq!(get_email_state(&c, a).unwrap(), Some("only-a".to_owned()));
        assert_eq!(get_email_state(&c, b).unwrap(), None);
    }

    #[test]
    fn cache_is_scoped_per_type() {
        let c = mem();
        let t = ensure_target(&c, "https://a/jmap", "w").unwrap();
        upsert(&c, t, ObjectType::Email, 1, "email-1").unwrap();
        assert!(all_for_type(&c, t, ObjectType::Mailbox).unwrap().is_empty());
    }
}
