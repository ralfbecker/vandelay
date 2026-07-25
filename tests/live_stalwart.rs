/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod integration;
mod seeder;

use std::path::PathBuf;

use integration::stalwart::shared as shared_stalwart;
use rusqlite::Connection;
use serde_json::{Value, json};
use vandelay::db;
use vandelay::imap::client::{ConnectMode, ImapClient};
use vandelay::imap::transport::Connector;
use vandelay::jmap::account::{self, AccountSelector};
use vandelay::jmap::http::{Auth, HttpClient, RetryPolicy};
use vandelay::jmap::request::Request;
use vandelay::jmap::session::Session;
use vandelay::logging::Logger;
use vandelay::sync::import_imap::{ImapAuth, ImapImportConfig};
use vandelay::sync::{self, CommonConfig, ConnectConfig, ExportConfig};
use vandelay::types::ObjectType;

fn admin_client() -> HttpClient {
    HttpClient::new(
        Auth::Basic {
            user: seeder::ADMIN_USER.into(),
            password: seeder::ADMIN_PASSWORD.into(),
        },
        RetryPolicy::new(5),
        true,
    )
}

#[test]
#[ignore = "requires Docker"]
fn session_discovery_and_admin_principal_resolution() {
    let stalwart = shared_stalwart();
    let fx = seeder::provision(stalwart.base_url()).expect("provision");
    let client = admin_client();

    let session =
        Session::discover(&client, &fx.base_url).expect("session discovery via .well-known");
    assert!(
        session.api_url.starts_with("https://") || session.api_url.starts_with("http://"),
        "apiUrl should be absolute: {}",
        session.api_url
    );
    let limits = session.core_limits().expect("core limits present");
    assert!(limits.max_objects_in_get >= 1);
    assert!(limits.max_concurrent_requests >= 1);
    assert!(
        !session.accounts.is_empty(),
        "authenticated admin session must enumerate accounts"
    );

    assert_eq!(fx.domain, seeder::DOMAIN);
    assert!(
        !fx.domain_id.is_empty(),
        "seeder should have ensured a domain id"
    );
    assert_eq!(
        fx.admin_login,
        (
            seeder::ADMIN_USER.to_owned(),
            seeder::ADMIN_PASSWORD.to_owned()
        )
    );

    let target = fx.account("test1").expect("test1 seeded");
    assert!(
        !target.admin_role,
        "test1 must be a regular user, not admin"
    );
    let seeded = target.seeded.as_ref().expect("test1 seed stats");
    assert!(seeded.emails > 0, "test1 should be seeded with emails");
    assert!(
        seeded.mailboxes_created >= 7,
        "test1 layout has at least 7 mailboxes"
    );
    assert!(
        seeded.file_nodes >= 9,
        "test1 layout has at least 9 file nodes"
    );
    assert!(
        seeded.contacts > 0,
        "test1 should be seeded with at least one contact"
    );
    assert!(
        seeded.events > 0,
        "test1 should be seeded with at least one event"
    );
    assert!(
        seeded.address_books > 0,
        "test1 layout requests an extra address book"
    );
    assert!(
        seeded.calendars > 0,
        "test1 layout requests an extra calendar"
    );
    assert!(seeded.identity, "test1 layout requests a custom identity");
    assert_eq!(
        seeded.sieve_active,
        Some(true),
        "test1 layout activates a sieve script"
    );

    let resolved = account::resolve(
        &AccountSelector::Name(target.email.clone()),
        &session,
        &client,
    )
    .expect("admin principal resolution");
    assert_eq!(
        resolved, target.account_id,
        "{} must resolve to the seeded account id {}",
        target.email, target.account_id
    );

    seeder::teardown(stalwart.base_url()).expect("teardown");
}

fn tmp_archive(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "vandelay-live-acl-{tag}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

fn common(archive: &std::path::Path) -> CommonConfig {
    CommonConfig {
        archive: archive.to_path_buf(),
        threads: 1,
        dry_run: false,
        max_retries: 3,
        allow_invalid_certs: true,
        logger: Logger::from_flags(false, 0),
    }
}

/// This is the test that actually retires the risk flagged during planning:
/// the IMAP-char -> internal Acl -> JMAP MailboxRight mapping in
/// `src/sync/export/acl.rs` was derived by reading Stalwart's own source,
/// not from a live server. Round-tripping a real SETACL through a real
/// GETACL, a real `import imap --acl`, and a real `export --acl` against an
/// actual (current `latest`) Stalwart container, then reading back the
/// `shareWith` Stalwart itself computed, confirms the mapping still matches
/// reality rather than a snapshot of the source read during planning.
#[test]
#[ignore = "requires Docker"]
fn acl_round_trips_through_import_and_export() {
    let stalwart = shared_stalwart();
    let fx = seeder::provision(stalwart.base_url()).expect("provision");
    let src = fx.account("test1").expect("test1 seeded");
    let dst = fx.account("test4").expect("test4 seeded");

    // Grant test4 lookup+read on test1's INBOX over real IMAP ACL, the way
    // an admin or the user themselves would via a real IMAP client.
    let connector = Connector::new(true).expect("connector");
    let mut imap = ImapClient::connect(
        &connector,
        &stalwart.host,
        stalwart.imaps_port,
        ConnectMode::ImplicitTls,
        Logger::from_flags(false, 0),
    )
    .expect("imap connect");
    imap.login(&src.email, &src.password).expect("imap login");
    imap.run_collect(&format!("SETACL INBOX {} lr", dst.email))
        .expect("SETACL");
    let getacl = imap.run_collect("GETACL INBOX").expect("GETACL");
    assert!(
        getacl
            .untagged
            .iter()
            .any(|u| format!("{u:?}").contains(&dst.email)),
        "server should reflect the grant just made: {:?}",
        getacl.untagged
    );
    imap.logout().ok();

    // Real `import imap --acl`, restricted to INBOX to keep this fast.
    let archive = tmp_archive("roundtrip");
    let import_cfg = ImapImportConfig {
        url: format!("imaps://{}:{}", stalwart.host, stalwart.imaps_port),
        auth: ImapAuth::Basic {
            user: src.email.clone(),
            password: src.password.clone(),
        },
        allow_cleartext: false,
        compress: false,
        include: Vec::new(),
        exclude: Vec::new(),
        exclude_special: Vec::new(),
        folder: vec!["INBOX".to_owned()],
        subscribed_only: false,
        automap: true,
        include_deleted: false,
        fetch_batch: 256,
        imap_connections: 1,
        allow_source_change: false,
        acl: true,
    };
    let import_summary =
        sync::import_imap::run(common(&archive), import_cfg).expect("imap import");
    assert!(
        !import_summary.any_failed(),
        "import had failures: {import_summary:?}"
    );

    let conn = Connection::open(&archive).unwrap();
    let mailbox_id: i64 = conn
        .query_row("SELECT id FROM mailboxes WHERE name = 'INBOX'", [], |r| {
            r.get(0)
        })
        .expect("INBOX row");
    let rows = db::acls::for_mailbox(&conn, mailbox_id).expect("acl rows");
    // Stalwart also reports an implicit full-rights entry for the mailbox
    // owner (test1) alongside the one we granted, and doesn't necessarily
    // preserve SETACL's character order in its GETACL reply — compare the
    // grant we made as a character set, not a literal string.
    let (_, dst_rights) = rows
        .iter()
        .find(|(id, _)| *id == dst.email)
        .expect("dst.email should have a GETACL entry after SETACL");
    let mut got: Vec<char> = dst_rights.chars().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec!['l', 'r'],
        "GETACL round trip should capture exactly the {{l,r}} rights SETACL granted, got {dst_rights:?}"
    );
    drop(conn);

    // Real `export --acl` of just the mailbox, to a different account.
    let export_cfg = ExportConfig {
        connect: ConnectConfig {
            url: fx.base_url.clone(),
            auth: Auth::Basic {
                user: dst.email.clone(),
                password: dst.password.clone(),
            },
            account: AccountSelector::Id(dst.account_id.clone()),
        },
        objects: Some(vec![ObjectType::Mailbox]),
        prune: false,
        yes: false,
        acl: true,
    };
    let export_summary = sync::export::run(common(&archive), export_cfg).expect("export");
    assert!(
        !export_summary.any_failed(),
        "export had failures: {export_summary:?}"
    );
    let acl_counts = export_summary
        .per_type
        .iter()
        .find(|(k, _)| *k == "mailbox_acl")
        .expect("mailbox_acl counts present");
    assert_eq!(acl_counts.1.updated, 1, "the shareWith push should succeed");
    assert_eq!(acl_counts.1.failed, 0, "counts={acl_counts:?}");

    // Read back what Stalwart itself computed for shareWith, straight from
    // the target's own JMAP Mailbox/get, using the real Session/HttpClient
    // primitives (not vandelay's export internals) so this is an
    // independent check.
    let client = HttpClient::new(
        Auth::Basic {
            user: dst.email.clone(),
            password: dst.password.clone(),
        },
        RetryPolicy::new(3),
        true,
    );
    let session = Session::discover(&client, &fx.base_url).expect("session discovery");
    // shareWith is defined by the mail:share capability extension; a plain
    // Mailbox/get that only declares urn:ietf:params:jmap:mail (what the
    // generic get_all() helper does) doesn't get it back even though it's
    // set server-side, matching why src/sync/export/acl.rs's own read of
    // the current shareWith has to declare it explicitly via
    // Request::require(). Do the same here for this independent check.
    let mut req = Request::new();
    req.call(
        "Mailbox/get",
        json!({ "accountId": dst.account_id, "ids": null, "properties": ["id", "role", "shareWith"] }),
        "g",
    );
    req.require("urn:ietf:params:jmap:mail:share");
    let resp = req.send(&client, &session.api_url).expect("Mailbox/get");
    let mr = resp.first().expect("method response");
    let list = mr
        .args
        .get("list")
        .and_then(Value::as_array)
        .expect("list array");
    let inbox = list
        .iter()
        .find(|m| m.get("role").and_then(Value::as_str) == Some("inbox"))
        .expect("target should have an inbox mailbox after export");
    let share_with = inbox
        .get("shareWith")
        .and_then(Value::as_object)
        .expect("shareWith should be present after --acl export");

    // Two entries land in shareWith: test4's explicit "lr" grant, and
    // test1's own implicit owner entry (GETACL on the source reported it
    // too, alongside the grant we made — vandelay captures and migrates
    // ACL data verbatim, it doesn't try to distinguish "owner" from
    // "explicit grant"). Tell them apart by mayShare, which only the
    // owner's richer rights ("rliteswkxpa", includes 'a') would grant.
    assert_eq!(
        share_with.len(),
        2,
        "test4's grant plus test1's own owner entry: {share_with:?}"
    );
    let grant = share_with
        .values()
        .map(|v| v.as_object().expect("rights object"))
        .find(|rights| rights.get("mayShare") == Some(&Value::Bool(false)))
        .expect("the non-owner (test4) grant should be present");
    // "lr": l -> Acl::Read, r -> Acl::ReadItems; mayReadItems needs both.
    assert_eq!(grant.get("mayReadItems"), Some(&Value::Bool(true)));
    assert_eq!(grant.get("mayAddItems"), Some(&Value::Bool(false)));
    assert_eq!(grant.get("mayDelete"), Some(&Value::Bool(false)));
    assert_eq!(
        grant.get("mayRename"),
        Some(&Value::Bool(false)),
        "mayRename can never be derived from IMAP ACL data"
    );

    let _ = std::fs::remove_file(&archive);
    seeder::teardown(stalwart.base_url()).expect("teardown");
}
