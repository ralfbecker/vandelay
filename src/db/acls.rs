/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use rusqlite::{Connection, params};

/// Replaces all ACL entries for a mailbox wholesale. GETACL always returns
/// the complete current ACL (never a diff), so a rerun is a plain
/// delete-then-reinsert rather than a merge.
pub fn replace_for_mailbox(
    conn: &Connection,
    mailbox_id: i64,
    entries: &[(String, String)],
) -> Result<(), rusqlite::Error> {
    conn.execute(
        "DELETE FROM mailbox_acls WHERE mailbox_id = ?1",
        params![mailbox_id],
    )?;
    for (identifier, rights) in entries {
        conn.execute(
            "INSERT INTO mailbox_acls (mailbox_id, identifier, rights) VALUES (?1, ?2, ?3)",
            params![mailbox_id, identifier, rights],
        )?;
    }
    Ok(())
}

pub fn for_mailbox(
    conn: &Connection,
    mailbox_id: i64,
) -> Result<Vec<(String, String)>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT identifier, rights FROM mailbox_acls WHERE mailbox_id = ?1 ORDER BY identifier",
    )?;
    let rows = stmt.query_map(params![mailbox_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect()
}

/// Every ACL row across the archive, for export (which needs to resolve
/// identifiers to target principals once across all mailboxes, not per
/// mailbox).
pub fn all(conn: &Connection) -> Result<Vec<(i64, String, String)>, rusqlite::Error> {
    let mut stmt =
        conn.prepare("SELECT mailbox_id, identifier, rights FROM mailbox_acls ORDER BY mailbox_id, identifier")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    rows.collect()
}

pub fn count(conn: &Connection) -> Result<i64, rusqlite::Error> {
    conn.query_row("SELECT count(*) FROM mailbox_acls", [], |r| r.get(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        // apply_schema() alone doesn't touch pragmas; the real open() path
        // (src/db/init.rs apply_pragmas) always turns this on, and the
        // cascade-delete test below only means something with it enabled.
        c.pragma_update(None, "foreign_keys", "ON").unwrap();
        init::apply_schema(&c).unwrap();
        c
    }

    fn insert_mailbox(c: &Connection, name: &str) -> i64 {
        c.execute(
            "INSERT INTO mailboxes (name, parent_id, sort_order, is_subscribed) \
             VALUES (?1, NULL, 0, 1)",
            params![name],
        )
        .unwrap();
        c.last_insert_rowid()
    }

    #[test]
    fn replace_then_read_back_roundtrips() {
        let c = mem();
        let mailbox = insert_mailbox(&c, "INBOX");
        replace_for_mailbox(
            &c,
            mailbox,
            &[
                ("jdoe@example.com".to_owned(), "lrswikta".to_owned()),
                ("anyone".to_owned(), "lr".to_owned()),
            ],
        )
        .unwrap();
        let rows = for_mailbox(&c, mailbox).unwrap();
        assert_eq!(
            rows,
            vec![
                ("anyone".to_owned(), "lr".to_owned()),
                ("jdoe@example.com".to_owned(), "lrswikta".to_owned()),
            ]
        );
    }

    #[test]
    fn replace_is_wholesale_not_a_merge() {
        let c = mem();
        let mailbox = insert_mailbox(&c, "INBOX");
        replace_for_mailbox(
            &c,
            mailbox,
            &[
                ("jdoe@example.com".to_owned(), "lrswikta".to_owned()),
                ("anyone".to_owned(), "lr".to_owned()),
            ],
        )
        .unwrap();
        // Second GETACL response: rights shrunk, "anyone" entry gone.
        replace_for_mailbox(&c, mailbox, &[("jdoe@example.com".to_owned(), "lr".to_owned())])
            .unwrap();
        assert_eq!(
            for_mailbox(&c, mailbox).unwrap(),
            vec![("jdoe@example.com".to_owned(), "lr".to_owned())]
        );
    }

    #[test]
    fn negative_rights_prefix_survives_in_identifier() {
        let c = mem();
        let mailbox = insert_mailbox(&c, "Shared/Team");
        replace_for_mailbox(&c, mailbox, &[("-anyone".to_owned(), "lrs".to_owned())]).unwrap();
        assert_eq!(
            for_mailbox(&c, mailbox).unwrap(),
            vec![("-anyone".to_owned(), "lrs".to_owned())]
        );
    }

    #[test]
    fn deleting_mailbox_cascades_to_its_acls() {
        let c = mem();
        let mailbox = insert_mailbox(&c, "INBOX");
        replace_for_mailbox(&c, mailbox, &[("jdoe@example.com".to_owned(), "lr".to_owned())])
            .unwrap();
        assert_eq!(count(&c).unwrap(), 1);
        c.execute("DELETE FROM mailboxes WHERE id = ?1", params![mailbox])
            .unwrap();
        assert_eq!(count(&c).unwrap(), 0, "ON DELETE CASCADE should have fired");
    }

    #[test]
    fn count_reflects_rows_across_mailboxes() {
        let c = mem();
        let a = insert_mailbox(&c, "INBOX");
        let b = insert_mailbox(&c, "Archive");
        replace_for_mailbox(&c, a, &[("jdoe@example.com".to_owned(), "lr".to_owned())]).unwrap();
        replace_for_mailbox(
            &c,
            b,
            &[
                ("jdoe@example.com".to_owned(), "lr".to_owned()),
                ("anyone".to_owned(), "l".to_owned()),
            ],
        )
        .unwrap();
        assert_eq!(count(&c).unwrap(), 3);
    }

    #[test]
    fn all_lists_every_row_across_mailboxes() {
        let c = mem();
        let a = insert_mailbox(&c, "INBOX");
        let b = insert_mailbox(&c, "Archive");
        replace_for_mailbox(&c, a, &[("jdoe@example.com".to_owned(), "lr".to_owned())]).unwrap();
        replace_for_mailbox(&c, b, &[("anyone".to_owned(), "l".to_owned())]).unwrap();
        assert_eq!(
            all(&c).unwrap(),
            vec![
                (a, "jdoe@example.com".to_owned(), "lr".to_owned()),
                (b, "anyone".to_owned(), "l".to_owned()),
            ]
        );
    }
}
