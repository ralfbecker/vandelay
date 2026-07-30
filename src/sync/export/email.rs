/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use super::common::{get_objects_parallel, jid, target_query_get};
use super::{Maps, Net, Plan, email_batch};
use crate::db;
use crate::error::Error;
use crate::jmap::blobxfer;
use crate::jmap::error::JmapError;
use crate::jmap::request::{Request, check_method_error};
use crate::jmap::session::Limits;
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{EMAIL_SELECT, EmailRow, TargetResolver, row_to_email};
use crate::sync::pool::{Pool, effective_workers};
use crate::sync::keys::{EmailIndex, EmailKey, email_index, email_keys, index_from_json};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

fn server_index(v: &Value) -> EmailIndex {
    let arr = |k: &str| {
        v.get(k)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.get("email").and_then(Value::as_str).map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let mids: Vec<String> = v
        .get("messageId")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    email_index(
        &mids,
        &arr("from"),
        v.get("subject").and_then(Value::as_str).unwrap_or(""),
        v.get("sentAt").and_then(Value::as_str).unwrap_or(""),
        &arr("to"),
    )
}

/// The batched path creates blobs through a regular method call, so it never
/// touches the upload endpoint and only `maxConcurrentRequests` applies. The
/// per-message path uses both and is bound by the smaller of the two.
fn import_workers(threads: usize, limits: &Limits, batched: bool) -> usize {
    let by_requests = effective_workers(threads, limits, false);
    if batched {
        by_requests
    } else {
        by_requests.min(effective_workers(threads, limits, true))
    }
}

struct ImportJob {
    cid: String,
    blob_local_id: i64,
    bytes: Vec<u8>,
    mids: Map<String, Value>,
    keywords: Map<String, Value>,
    received_at: String,
    hint: String,
}

struct ImportResult {
    cid: String,
    hint: String,
    outcome: Result<SingleImport, JmapError>,
}

type BlobCache = Mutex<HashMap<i64, JmapId>>;

fn load_local(ctx: &Context) -> Result<Vec<(i64, EmailRow)>, Error> {
    let mut stmt = ctx
        .conn
        .prepare(EMAIL_SELECT)
        .map_err(|e| Error::Partial(e.to_string()))?;
    stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        Ok((id, row_to_email(row)))
    })
    .and_then(|m| m.collect::<Result<Vec<_>, _>>())
    .map_err(|e| Error::Partial(e.to_string()))?
    .into_iter()
    .map(|(id, r)| Ok((id, r.map_err(Error::from)?)))
    .collect::<Result<_, Error>>()
}

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<Plan, Error> {
    let ty = ObjectType::Email;
    let local = load_local(ctx)?;

    let target_row = db::export_ids::ensure_target(&ctx.conn, &net.api, &net.account)
        .map_err(|e| Error::Partial(e.to_string()))?;
    let cached = db::export_ids::all_for_type(&ctx.conn, target_row, ty)
        .map_err(|e| Error::Partial(e.to_string()))?;

    if !local.is_empty() && local.iter().all(|(id, _)| cached.contains_key(id)) {
        match try_cached(net, &local, &cached, ctx.common.threads, counts)? {
            Some(plan) => return Ok(plan),
            None => logger.warn(
                "Email: cached target ids are stale (target changed since last export); \
                 rebuilding the full match instead of trusting the cache",
            ),
        }
    }

    let target_min = target_query_get(net, ty, Some(&["messageId"]), ctx.common.threads)
        .map_err(Error::from)?;
    let mut indices: Vec<EmailIndex> = target_min.iter().map(server_index).collect();

    let fallback_ids: Vec<JmapId> = target_min
        .iter()
        .zip(indices.iter())
        .filter(|(_, i)| i.mids.is_empty())
        .filter_map(|(v, _)| jid(v).map(JmapId))
        .collect();
    if !fallback_ids.is_empty() {
        let got = get_objects_parallel(
            net,
            ty,
            &fallback_ids,
            Some(&["messageId", "from", "subject", "sentAt", "to"]),
            ctx.common.threads,
        )
        .map_err(Error::from)?;
        let by_id: HashMap<String, &Value> = got
            .iter()
            .filter_map(|v| jid(v).map(|i| (i, v)))
            .collect();
        for (v, slot) in target_min.iter().zip(indices.iter_mut()) {
            if let Some(full) = jid(v).and_then(|i| by_id.get(&i)) {
                *slot = server_index(full);
            }
        }
    }
    // A HashMap instead of a bare HashSet of keys, so a local-key match can
    // recover which target id it matched and cache it for the next run.
    let target_map: HashMap<EmailKey, String> = target_min
        .iter()
        .zip(email_keys(&indices))
        .filter_map(|(v, key)| jid(v).map(|id| (key, id)))
        .collect();

    let local_indices: Vec<EmailIndex> = local
        .iter()
        .map(|(_, r)| index_from_json(&r.message_match))
        .collect();
    let local_keys = email_keys(&local_indices);

    // Servers cap in-flight requests per user, so the cost driver is request
    // count. With RFC 9404 one request carries a whole batch of blobs.
    let batched = email_batch::supports_blob_upload(&net.session);
    let workers = import_workers(ctx.common.threads, &net.limits, batched);
    let (batch_count, batch_bytes) = if batched {
        let pending = local_keys
            .iter()
            .filter(|k| !target_map.contains_key(*k))
            .count();
        email_batch::batch_limits(&net.limits, pending, workers)
    } else {
        (1, usize::MAX)
    };
    let cache: Arc<BlobCache> = Arc::new(Mutex::new(HashMap::new()));
    let pool: Pool<Vec<ImportJob>, Vec<ImportResult>> = Pool::new(workers, {
        let net = Arc::new(net.clone());
        let cache = cache.clone();
        move |jobs: Vec<ImportJob>| run_batch(&net, &cache, jobs)
    });
    let window = workers * 2;
    let mut in_flight = 0usize;
    let mut batch: Vec<ImportJob> = Vec::new();
    let mut batch_encoded = 0usize;
    let mut to_cache: Vec<(i64, String)> = Vec::new();

    for (i, key) in local_keys.iter().enumerate() {
        if let Some(target_jmap_id) = target_map.get(key) {
            counts.skipped += 1;
            crate::progress::advance(1);
            to_cache.push((local[i].0, target_jmap_id.clone()));
            continue;
        }
        let (local_id, row) = &local[i];
        let job = match prepare_job(ctx, maps, *local_id, row, counts, logger) {
            Some(j) => j,
            None => {
                crate::progress::advance(1);
                continue;
            }
        };
        if net.dry_run {
            counts.created += 1;
            crate::progress::advance(1);
            continue;
        }
        batch_encoded += email_batch::encoded_len(job.bytes.len());
        batch.push(job);
        if batch.len() >= batch_count || batch_encoded >= batch_bytes {
            submit_batch(
                &pool,
                &mut batch,
                &mut batch_encoded,
                &mut in_flight,
                window,
                counts,
                logger,
                &mut to_cache,
            );
        }
    }
    submit_batch(
        &pool,
        &mut batch,
        &mut batch_encoded,
        &mut in_flight,
        window,
        counts,
        logger,
        &mut to_cache,
    );
    for batch in pool.finish() {
        for res in batch {
            account(res, counts, logger, &mut to_cache);
        }
    }

    if !net.dry_run {
        db::export_ids::upsert_many(&ctx.conn, target_row, ty, &to_cache)
            .map_err(|e| Error::Partial(e.to_string()))?;
    }

    Ok(Plan::default())
}

/// Verifies every cached target id is still present, via a batched existence
/// check bounded by the number of *local* rows rather than the target's
/// whole size. Returns `Ok(None)` if any cached id turns out stale, so the
/// caller falls back to the full rebuild rather than silently trusting a
/// cache that may no longer reflect the target's real state.
fn try_cached(
    net: &Net,
    local: &[(i64, EmailRow)],
    cached: &HashMap<i64, String>,
    threads: usize,
    counts: &mut TypeCounts,
) -> Result<Option<Plan>, Error> {
    let ids: Vec<JmapId> = local
        .iter()
        .map(|(local_id, _)| JmapId(cached[local_id].clone()))
        .collect();
    let found = get_objects_parallel(net, ObjectType::Email, &ids, Some(&[]), threads)
        .map_err(Error::from)?;
    if found.len() != ids.len() {
        return Ok(None);
    }
    for _ in local {
        counts.skipped += 1;
        crate::progress::advance(1);
    }
    Ok(Some(Plan::default()))
}

type BatchPool = Pool<Vec<ImportJob>, Vec<ImportResult>>;

/// Hands the accumulated batch to the pool, then drains one completed batch
/// once the submission window is full so memory stays bounded.
#[allow(clippy::too_many_arguments)]
fn submit_batch(
    pool: &BatchPool,
    batch: &mut Vec<ImportJob>,
    encoded: &mut usize,
    in_flight: &mut usize,
    window: usize,
    counts: &mut TypeCounts,
    logger: &Logger,
    to_cache: &mut Vec<(i64, String)>,
) {
    if batch.is_empty() {
        return;
    }
    pool.submit(std::mem::take(batch));
    *encoded = 0;
    *in_flight += 1;
    if *in_flight >= window
        && let Ok(done) = pool.results().recv()
    {
        *in_flight -= 1;
        for res in done {
            account(res, counts, logger, to_cache);
        }
    }
}

fn one_result(net: &Net, cache: &BlobCache, job: ImportJob) -> ImportResult {
    let outcome = run_import(net, cache, &job);
    ImportResult {
        cid: job.cid,
        hint: job.hint,
        outcome,
    }
}

fn per_message(net: &Net, cache: &BlobCache, jobs: Vec<ImportJob>) -> Vec<ImportResult> {
    jobs.into_iter()
        .map(|job| one_result(net, cache, job))
        .collect()
}

/// One `Blob/upload` for the whole batch, then one `Email/import`. Any
/// batch-level failure degrades to the per-message path rather than failing
/// every message in the chunk.
fn run_batch(net: &Net, cache: &BlobCache, jobs: Vec<ImportJob>) -> Vec<ImportResult> {
    if jobs.len() < 2 {
        return per_message(net, cache, jobs);
    }
    let items: Vec<(String, &[u8])> = jobs
        .iter()
        .map(|j| (j.cid.clone(), j.bytes.as_slice()))
        .collect();
    let blobs = match email_batch::upload_batch(
        &net.client,
        &net.api,
        &net.account,
        &net.limits,
        &items,
    ) {
        Ok(b) => b,
        Err(_) => return per_message(net, cache, jobs),
    };

    // `Blob/upload` reports an exhausted upload quota as a method-level
    // `overQuota` inside a 200 response, which no retry layer sees. The upload
    // endpoint reports the same condition as HTTP 429, which the client retries
    // with backoff, so those messages go back to the per-message path.
    let (throttled, uploaded): (Vec<ImportJob>, Vec<ImportJob>) = jobs
        .into_iter()
        .partition(|j| email_batch::is_over_quota(blobs.get(&j.cid)));
    if uploaded.is_empty() {
        return per_message(net, cache, throttled);
    }

    let mut results = match import_uploaded(net, &uploaded, &blobs) {
        Some(r) => r,
        None => {
            let mut all = uploaded;
            all.extend(throttled);
            return per_message(net, cache, all);
        }
    };

    // A staged blob can expire between the upload and the import when the
    // server throttles in between. Those go back through the per-message path,
    // which re-uploads before retrying.
    let stale: HashSet<String> = results
        .iter()
        .filter(|r| is_blob_not_found(r))
        .map(|r| r.cid.clone())
        .collect();
    let mut redo = throttled;
    if !stale.is_empty() {
        results.retain(|r| !stale.contains(&r.cid));
        redo.extend(uploaded.into_iter().filter(|j| stale.contains(&j.cid)));
    }
    results.extend(per_message(net, cache, redo));
    results
}

fn is_blob_not_found(res: &ImportResult) -> bool {
    matches!(
        &res.outcome,
        Ok(SingleImport::NotCreated { error_type, .. }) if error_type == "blobNotFound"
    )
}

/// Imports every successfully uploaded blob of a batch in one call. Returns
/// `None` if the import request itself failed, so the caller can retry per
/// message.
fn import_uploaded(
    net: &Net,
    jobs: &[ImportJob],
    blobs: &HashMap<String, email_batch::BlobOutcome>,
) -> Option<Vec<ImportResult>> {
    let mut emails = Map::new();
    for job in jobs {
        if let Some(email_batch::BlobOutcome::Created(blob)) = blobs.get(&job.cid) {
            emails.insert(
                job.cid.clone(),
                import_item(
                    blob.0.clone(),
                    job.mids.clone(),
                    job.keywords.clone(),
                    &job.received_at,
                ),
            );
        }
    }
    let imported = if emails.is_empty() {
        HashMap::new()
    } else {
        send_batch_import(net, emails).ok()?
    };
    Some(
        jobs.iter()
            .map(|job| ImportResult {
                cid: job.cid.clone(),
                hint: job.hint.clone(),
                outcome: batch_outcome(&job.cid, blobs, &imported),
            })
            .collect(),
    )
}

fn batch_outcome(
    cid: &str,
    blobs: &HashMap<String, email_batch::BlobOutcome>,
    imported: &HashMap<String, SingleImport>,
) -> Result<SingleImport, JmapError> {
    match blobs.get(cid) {
        Some(email_batch::BlobOutcome::Failed { error_type, detail }) => {
            Ok(SingleImport::NotCreated {
                error_type: error_type.clone(),
                detail: format!("Blob/upload failed: {detail}"),
            })
        }
        _ => Ok(imported.get(cid).cloned().unwrap_or(SingleImport::NotCreated {
            error_type: String::new(),
            detail: format!("Email/import returned no result for {cid}"),
        })),
    }
}

fn prepare_job(
    ctx: &Context,
    maps: &Maps,
    local_id: i64,
    row: &EmailRow,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Option<ImportJob> {
    let cid = format!("e{local_id}");
    let mids = match build_mailbox_ids(row, maps) {
        Some(m) => m,
        None => {
            logger.warn(&format!(
                "Email/import {cid} ({}) skipped: mailbox not on target",
                blob_hint(row, None)
            ));
            counts.failed += 1;
            return None;
        }
    };
    let bytes = match db::blobs::blob_bytes(&ctx.conn, row.blob_local_id) {
        Ok(Some(b)) => b,
        Ok(None) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob upload failed: blob local id {} missing",
                blob_hint(row, None),
                row.blob_local_id
            ));
            counts.failed += 1;
            return None;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob read failed: {e}",
                blob_hint(row, None)
            ));
            counts.failed += 1;
            return None;
        }
    };
    let hint = blob_hint(row, Some(bytes.len() as u64));
    Some(ImportJob {
        cid,
        blob_local_id: row.blob_local_id,
        bytes,
        mids,
        keywords: build_keywords(row),
        received_at: row.received_at.clone(),
        hint,
    })
}

fn run_import(net: &Net, cache: &BlobCache, job: &ImportJob) -> Result<SingleImport, JmapError> {
    let blob = upload_cached(net, cache, job.blob_local_id, &job.bytes)?;
    let item = import_item(
        blob.0.clone(),
        job.mids.clone(),
        job.keywords.clone(),
        &job.received_at,
    );
    match send_single_import(net, &job.cid, item)? {
        SingleImport::NotCreated { ref error_type, .. } if error_type == "blobNotFound" => {
            invalidate(cache, job.blob_local_id, &blob);
            let blob = upload_cached(net, cache, job.blob_local_id, &job.bytes)?;
            let item = import_item(
                blob.0,
                job.mids.clone(),
                job.keywords.clone(),
                &job.received_at,
            );
            send_single_import(net, &job.cid, item)
        }
        other => Ok(other),
    }
}

fn upload_cached(
    net: &Net,
    cache: &BlobCache,
    local_id: i64,
    bytes: &[u8],
) -> Result<JmapId, JmapError> {
    if let Some(id) = cache.lock().unwrap().get(&local_id) {
        return Ok(id.clone());
    }
    let id = blobxfer::upload_bytes(
        &net.client,
        &net.session,
        &net.account,
        "message/rfc822",
        bytes,
    )?;
    cache.lock().unwrap().insert(local_id, id.clone());
    Ok(id)
}

fn invalidate(cache: &BlobCache, local_id: i64, stale: &JmapId) {
    let mut c = cache.lock().unwrap();
    if c.get(&local_id) == Some(stale) {
        c.remove(&local_id);
    }
}

fn account(
    res: ImportResult,
    counts: &mut TypeCounts,
    logger: &Logger,
    to_cache: &mut Vec<(i64, String)>,
) {
    crate::progress::advance(1);
    match res.outcome {
        Ok(SingleImport::Created(jmap_id)) => {
            counts.created += 1;
            if let Some(local_id) = res.cid.strip_prefix('e').and_then(|s| s.parse::<i64>().ok())
            {
                to_cache.push((local_id, jmap_id));
            }
        }
        Ok(SingleImport::Skipped) => counts.skipped += 1,
        Ok(SingleImport::NotCreated { detail, .. }) => {
            logger.warn(&format!(
                "Email/import {} ({}) failed: {detail}",
                res.cid, res.hint
            ));
            counts.failed += 1;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {} ({}) send failed: {e}{}",
                res.cid,
                res.hint,
                size_note(&e)
            ));
            counts.failed += 1;
        }
    }
}

fn build_mailbox_ids(row: &EmailRow, maps: &Maps) -> Option<Map<String, Value>> {
    let mut mids = Map::new();
    for ml in &row.mailbox_locals {
        let t = maps.target(ObjectType::Mailbox, *ml)?;
        mids.insert(t.0, Value::Bool(true));
    }
    Some(mids)
}

fn build_keywords(row: &EmailRow) -> Map<String, Value> {
    let mut kw = Map::new();
    for k in &row.keywords {
        kw.insert(k.clone(), Value::Bool(true));
    }
    kw
}

fn blob_hint(row: &EmailRow, len: Option<u64>) -> String {
    let idx = index_from_json(&row.message_match);
    let mut s = match idx.mids.first() {
        Some(mid) => format!("message-id <{mid}>"),
        None => "no message-id".to_owned(),
    };
    if let Some(len) = len {
        use std::fmt::Write;
        let _ = write!(s, ", {}", crate::inspect::format_bytes(len));
    }
    s
}

fn size_note(e: &JmapError) -> &'static str {
    if matches!(
        e,
        JmapError::RequestTooLarge | JmapError::SingleObjectTooLarge(_)
    ) {
        "; exceeds the target server size limit, so this message is skipped and re-running will not migrate it"
    } else {
        ""
    }
}

fn import_item(
    blob: String,
    mids: Map<String, Value>,
    kw: Map<String, Value>,
    received_at: &str,
) -> Value {
    json!({
        "blobId": blob,
        "mailboxIds": Value::Object(mids),
        "keywords": Value::Object(kw),
        "receivedAt": received_at,
    })
}

#[derive(Clone)]
enum SingleImport {
    Created(String),
    Skipped,
    NotCreated { error_type: String, detail: String },
}

/// Imports a whole batch in one `Email/import` call and reports each creation
/// id separately, so one bad message cannot fail its neighbours.
fn send_batch_import(
    net: &Net,
    emails: Map<String, Value>,
) -> Result<HashMap<String, SingleImport>, JmapError> {
    let cids: Vec<String> = emails.keys().cloned().collect();
    let mut req = Request::new();
    req.call(
        "Email/import",
        json!({ "accountId": net.account, "emails": Value::Object(emails) }),
        "i",
    );
    req.fits(&net.limits)?;
    let resp = req.send(&net.client, &net.api)?;
    let mr = resp.first()?;
    check_method_error(mr)?;
    let created = mr.args.get("created").and_then(Value::as_object);
    let not_created = mr.args.get("notCreated").and_then(Value::as_object);

    let mut out = HashMap::with_capacity(cids.len());
    for cid in cids {
        if let Some(id) = created
            .and_then(|c| c.get(&cid))
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str)
        {
            out.insert(cid, SingleImport::Created(id.to_owned()));
            continue;
        }
        let err = not_created.and_then(|nc| nc.get(&cid));
        let error_type = err
            .and_then(|e| e.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let entry = if error_type == "alreadyExists" {
            SingleImport::Skipped
        } else {
            SingleImport::NotCreated {
                detail: match err {
                    Some(e) => e.to_string(),
                    None => format!("Email/import returned no result for {cid}"),
                },
                error_type,
            }
        };
        out.insert(cid, entry);
    }
    Ok(out)
}

fn send_single_import(net: &Net, cid: &str, item: Value) -> Result<SingleImport, JmapError> {
    let mut emails = Map::new();
    emails.insert(cid.to_owned(), item);
    let mut req = Request::new();
    req.call(
        "Email/import",
        json!({ "accountId": net.account, "emails": Value::Object(emails) }),
        "i",
    );
    req.fits(&net.limits)?;
    let resp = req.send(&net.client, &net.api)?;
    let mr = resp.first()?;
    check_method_error(mr)?;
    if let Some(err) = mr
        .args
        .get("notCreated")
        .and_then(Value::as_object)
        .and_then(|nc| nc.get(cid))
    {
        let error_type = err
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if error_type == "alreadyExists" {
            return Ok(SingleImport::Skipped);
        }
        return Ok(SingleImport::NotCreated {
            error_type,
            detail: err.to_string(),
        });
    }
    if let Some(id) = mr
        .args
        .get("created")
        .and_then(Value::as_object)
        .and_then(|c| c.get(cid))
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
    {
        return Ok(SingleImport::Created(id.to_owned()));
    }
    Ok(SingleImport::NotCreated {
        error_type: String::new(),
        detail: format!("Email/import returned neither created nor notCreated for {cid}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_workers_capped_by_both_request_and_upload_limits() {
        let limits = Limits {
            max_objects_in_get: 500,
            max_objects_in_set: 500,
            max_calls_in_request: 16,
            max_concurrent_requests: 8,
            max_concurrent_upload: 2,
            max_size_request: 10_000_000,
            max_size_upload: 50_000_000,
        };
        assert_eq!(import_workers(16, &limits, false), 2);
        assert_eq!(import_workers(1, &limits, false), 1);
        assert_eq!(import_workers(0, &limits, false), 1);
    }

    #[test]
    fn batched_import_workers_ignore_the_upload_limit() {
        let limits = Limits {
            max_objects_in_get: 500,
            max_objects_in_set: 500,
            max_calls_in_request: 16,
            max_concurrent_requests: 16,
            max_concurrent_upload: 2,
            max_size_request: 10_000_000,
            max_size_upload: 50_000_000,
        };
        assert_eq!(import_workers(16, &limits, true), 16);
        assert_eq!(import_workers(16, &limits, false), 2);
        assert_eq!(import_workers(4, &limits, true), 4);
    }
}
