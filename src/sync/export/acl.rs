/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use crate::db;
use crate::jmap::error::JmapError;
use crate::jmap::request::{Request, SetRequest, check_method_error, set_call};
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::TargetResolver;
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

use super::{Maps, Net};

const MAIL_SHARE_URN: &str = "urn:ietf:params:jmap:mail:share";
const PRINCIPALS_URN: &str = "urn:ietf:params:jmap:principals";

/// Internal rights bitset mirroring Stalwart's `types::acl::Acl` (the subset
/// relevant to Mailbox sharing). Derived directly from Stalwart's own source
/// (`imap-proto`'s `impl From<Rights> for Acl`, `jmap-proto`'s
/// `impl JmapRight for MailboxRight`), not guessed: an IMAP ACL right maps
/// to one or more of these, and a JMAP `MailboxRight` is granted iff ALL of
/// its required `Acl` variants are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Acl {
    Read,
    ReadItems,
    ModifyItems,
    AddItems,
    RemoveItems,
    CreateChild,
    Delete,
    Submit,
    Share,
}

fn imap_char_to_acl(c: char) -> Option<Acl> {
    match c {
        'l' => Some(Acl::Read),
        'r' => Some(Acl::ReadItems),
        's' | 'w' => Some(Acl::ModifyItems),
        'i' => Some(Acl::AddItems),
        'p' => Some(Acl::Submit),
        'k' => Some(Acl::CreateChild),
        'x' => Some(Acl::Delete),
        't' | 'e' => Some(Acl::RemoveItems),
        'a' => Some(Acl::Share),
        // Legacy RFC2086 c/d and anything else: no equivalent, ignored.
        _ => None,
    }
}

fn acl_set_from_rights(rights: &str) -> HashSet<Acl> {
    rights.chars().filter_map(imap_char_to_acl).collect()
}

/// JMAP MailboxRight -> the Acl variants ALL of which must be present to
/// grant it. `mayRename` requires `Acl::Modify`, which no IMAP right ever
/// produces, so it has no entry here: it can never be granted from IMAP ACL
/// data and is always reported as `false`.
const RIGHT_TABLE: &[(&str, &[Acl])] = &[
    ("mayReadItems", &[Acl::Read, Acl::ReadItems]),
    ("mayAddItems", &[Acl::AddItems]),
    ("mayRemoveItems", &[Acl::RemoveItems]),
    ("maySetSeen", &[Acl::ModifyItems]),
    ("maySetKeywords", &[Acl::ModifyItems]),
    ("mayCreateChild", &[Acl::CreateChild]),
    ("maySubmit", &[Acl::Submit]),
    ("mayDelete", &[Acl::Delete]),
    ("mayShare", &[Acl::Share]),
];

fn mailbox_rights(acls: &HashSet<Acl>) -> Map<String, Value> {
    let mut obj = Map::with_capacity(RIGHT_TABLE.len() + 1);
    obj.insert("mayRename".to_owned(), Value::Bool(false));
    for (name, required) in RIGHT_TABLE {
        let granted = required.iter().all(|a| acls.contains(a));
        obj.insert((*name).to_owned(), Value::Bool(granted));
    }
    obj
}

/// Exports local `mailbox_acls` rows as JMAP Mailbox `shareWith`, merging
/// with whatever the target mailbox already has (vandelay's entries win on
/// conflicting identifiers; identifiers unknown to vandelay are left alone)
/// rather than replacing wholesale, since the target may have shares
/// granted natively that the archive knows nothing about.
pub fn export(ctx: &Context, net: &Net, maps: &Maps, logger: &Logger) -> TypeCounts {
    let mut counts = TypeCounts::default();

    if ctx.dry_run() {
        logger.warn("dry-run: --acl export skipped (not reflected in the dry-run summary)");
        return counts;
    }

    if !net.session.supports(&net.account, MAIL_SHARE_URN)
        || !net.session.capabilities.contains_key(PRINCIPALS_URN)
    {
        logger.warn(&format!(
            "target does not support {MAIL_SHARE_URN} / {PRINCIPALS_URN}; --acl export skipped"
        ));
        return counts;
    }

    let rows = match db::acls::all(&ctx.conn) {
        Ok(r) => r,
        Err(e) => {
            logger.warn(&format!("--acl export: reading local ACLs failed: {e}"));
            return counts;
        }
    };
    if rows.is_empty() {
        return counts;
    }

    // Group by target mailbox id; a mailbox with no resolved target (not in
    // scope for this export run) is left untouched.
    let mut by_mailbox: HashMap<JmapId, Vec<(String, String)>> = HashMap::new();
    for (mailbox_id, identifier, rights) in rows {
        if let Some(target) = maps.target(ObjectType::Mailbox, mailbox_id) {
            by_mailbox
                .entry(target)
                .or_default()
                .push((identifier, rights));
        }
    }
    if by_mailbox.is_empty() {
        return counts;
    }

    let identifiers: Vec<String> = by_mailbox
        .values()
        .flat_map(|entries| entries.iter().map(|(id, _)| id.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let principals = match resolve_principals(net, &identifiers) {
        Ok(p) => p,
        Err(e) => {
            logger.warn(&format!("--acl export: principal resolution failed: {e}"));
            return counts;
        }
    };
    for identifier in &identifiers {
        if principals.get(identifier).cloned().flatten().is_none() {
            logger.warn(&format!(
                "--acl export: identifier {identifier:?} did not resolve to a principal \
                 on the target; its ACL entries are skipped"
            ));
        }
    }

    let mailbox_ids: Vec<JmapId> = by_mailbox.keys().cloned().collect();
    let current = match current_share_with(net, &mailbox_ids) {
        Ok(c) => c,
        Err(e) => {
            logger.warn(&format!(
                "--acl export: reading current shareWith failed, proceeding as if unshared: {e}"
            ));
            HashMap::new()
        }
    };

    let mut update = Map::new();
    for (target_id, entries) in &by_mailbox {
        let mut merged = current
            .get(target_id)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for (identifier, rights) in entries {
            let Some(Some(principal_id)) = principals.get(identifier) else {
                continue;
            };
            let acls = acl_set_from_rights(rights);
            merged.insert(
                principal_id.0.clone(),
                Value::Object(mailbox_rights(&acls)),
            );
        }
        update.insert(
            target_id.0.clone(),
            json!({ "shareWith": Value::Object(merged) }),
        );
    }

    match set_call(
        &net.client,
        &net.api,
        &net.account,
        "Mailbox",
        SetRequest {
            update: Some(Value::Object(update)),
            extra_using: &[MAIL_SHARE_URN],
            ..Default::default()
        },
        &net.limits,
    ) {
        Ok(outcome) => {
            counts.updated += outcome.updated.len() as u64;
            for (id, err) in &outcome.not_updated {
                logger.warn(&format!("--acl export: mailbox {id} not updated: {err}"));
                counts.failed += 1;
            }
        }
        Err(e) => {
            logger.warn(&format!("--acl export: Mailbox/set failed: {e}"));
            counts.failed += 1;
        }
    }

    counts
}

/// Resolves identifiers (assumed to be target account emails, matching how
/// Stalwart's own IMAP ACL identifiers work) to JMAP Principal ids, in one
/// HTTP round trip: N `Principal/query` + `Principal/get` backreferenced
/// call-pairs packed into a single `Request`, chunked to respect
/// `maxCallsInRequest`. An identifier with no match is left as `None`
/// rather than failing the whole batch.
fn resolve_principals(
    net: &Net,
    identifiers: &[String],
) -> Result<HashMap<String, Option<JmapId>>, JmapError> {
    let mut out: HashMap<String, Option<JmapId>> =
        identifiers.iter().map(|id| (id.clone(), None)).collect();
    if identifiers.is_empty() {
        return Ok(out);
    }

    let pairs_per_request = (net.limits.max_calls_in_request.max(2) / 2).max(1) as usize;
    for chunk in identifiers.chunks(pairs_per_request) {
        let mut req = Request::new();
        for (i, identifier) in chunk.iter().enumerate() {
            req.call(
                "Principal/query",
                json!({ "filter": { "email": identifier } }),
                format!("q{i}"),
            );
            req.call(
                "Principal/get",
                json!({
                    "#ids": { "resultOf": format!("q{i}"), "name": "Principal/query", "path": "/ids" },
                    "properties": ["id", "email"]
                }),
                format!("g{i}"),
            );
        }
        let resp = req.send(&net.client, &net.api)?;
        for (i, identifier) in chunk.iter().enumerate() {
            let Ok(get) = resp.by_call_id(&format!("g{i}")) else {
                continue;
            };
            if check_method_error(get).is_err() {
                continue;
            }
            if let Some(id) = get
                .args
                .get("list")
                .and_then(Value::as_array)
                .and_then(|list| list.first())
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
            {
                out.insert(identifier.clone(), Some(JmapId(id.to_owned())));
            }
        }
    }
    Ok(out)
}

/// Current `shareWith` for each target mailbox, so export can merge into it
/// rather than replace it outright.
fn current_share_with(
    net: &Net,
    mailbox_ids: &[JmapId],
) -> Result<HashMap<JmapId, Value>, JmapError> {
    if mailbox_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut req = Request::new();
    req.call(
        "Mailbox/get",
        json!({
            "accountId": net.account,
            "ids": mailbox_ids.iter().map(|i| i.0.clone()).collect::<Vec<_>>(),
            "properties": ["id", "shareWith"],
        }),
        "g",
    );
    req.require(MAIL_SHARE_URN);
    let resp = req.send(&net.client, &net.api)?;
    let mr = resp.first()?;
    check_method_error(mr)?;
    let list = mr
        .args
        .get("list")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(list
        .into_iter()
        .filter_map(|v| {
            let id = v.get("id").and_then(Value::as_str)?.to_owned();
            Some((
                JmapId(id),
                v.get("shareWith").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rights_are_granted_only_when_all_required_acls_present() {
        // "lrswikta": l,r,s,w,i,k,t,a all present (no 'p','x').
        let acls = acl_set_from_rights("lrswikta");
        let rights = mailbox_rights(&acls);
        assert_eq!(rights["mayReadItems"], Value::Bool(true)); // needs l+r
        assert_eq!(rights["mayAddItems"], Value::Bool(true)); // i
        assert_eq!(rights["maySetSeen"], Value::Bool(true)); // s or w
        assert_eq!(rights["maySetKeywords"], Value::Bool(true)); // s or w
        assert_eq!(rights["mayCreateChild"], Value::Bool(true)); // k
        assert_eq!(rights["mayRemoveItems"], Value::Bool(true)); // t
        assert_eq!(rights["mayShare"], Value::Bool(true)); // a
        assert_eq!(rights["maySubmit"], Value::Bool(false)); // no 'p'
        assert_eq!(rights["mayDelete"], Value::Bool(false)); // no 'x'
        // mayRename can never be derived from IMAP ACL data.
        assert_eq!(rights["mayRename"], Value::Bool(false));
    }

    #[test]
    fn partial_read_rights_do_not_grant_may_read_items() {
        // Only 'l' (lookup), missing 'r' (read): mayReadItems needs both.
        let acls = acl_set_from_rights("l");
        let rights = mailbox_rights(&acls);
        assert_eq!(rights["mayReadItems"], Value::Bool(false));
    }

    #[test]
    fn unmapped_legacy_chars_are_ignored() {
        let acls = acl_set_from_rights("rc d");
        assert_eq!(acls, HashSet::from([Acl::ReadItems]));
    }
}
