/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use serde_json::{Map, Value, json};

use super::common::{get_objects_parallel, jid};
use super::{Maps, Net, Plan, email_batch};
use crate::db;
use crate::error::Error;
use crate::jmap::blobxfer;
use crate::jmap::error::JmapError;
use crate::jmap::request::{Request, check_method_error, get_state, query_ids_filtered};
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

/// Below this a chunk is never split further, whatever it returns: an error
/// is propagated (the run fails rather than silently under-counting what's
/// already on the target), a suspicious 0-result count is accepted as real.
const MIN_CHUNK: Duration = Duration::days(31);

/// Full-rebuild target enumeration for `reconcile`'s slow path
/// (`target_query_get`'s old caller here), scoped by calendar year instead
/// of one unfiltered account-wide `Email/query`. An unfiltered query forces
/// the server's search backend to sort/traverse every email in the account
/// just to paginate, which trips Meilisearch's `maxTotalHits` cap on
/// accounts with large single mailboxes (e.g. a mailing-list archive
/// folder) -- see the memory note `stalwart_email_query_index_bug` for the
/// related bug this was first noticed alongside. Chunking by year keeps
/// each query's matched set bounded by what that year actually holds, and
/// `query_range_adaptive` splits a chunk further (down to `MIN_CHUNK`) if it
/// still errors or looks suspiciously empty.
fn target_email_min(
    net: &Net,
    local: &[(i64, EmailRow)],
    threads: usize,
    logger: &Logger,
) -> Result<Vec<Value>, JmapError> {
    let ids = target_email_ids_chunked(net, local, logger)?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    get_objects_parallel(
        net,
        ObjectType::Email,
        &ids,
        Some(&["messageId"]),
        threads,
        |_| {},
    )
}

/// The local archive's earliest and latest `receivedAt` year, used to bound
/// the per-year chunks below. `None` if no local row has a parseable date
/// (e.g. an empty archive), in which case there's nothing to chunk by.
fn local_year_range(local: &[(i64, EmailRow)]) -> Option<(i32, i32)> {
    local
        .iter()
        .filter_map(|(_, r)| DateTime::parse_from_rfc3339(&r.received_at).ok())
        .map(|dt| dt.year())
        .fold(None, |acc: Option<(i32, i32)>, y| {
            Some(acc.map_or((y, y), |(lo, hi)| (lo.min(y), hi.max(y))))
        })
}

fn year_start(year: i32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().expect("valid calendar year")
}

fn target_email_ids_chunked(
    net: &Net,
    local: &[(i64, EmailRow)],
    logger: &Logger,
) -> Result<Vec<JmapId>, JmapError> {
    let Some((min_year, max_year)) = local_year_range(local) else {
        return Ok(Vec::new());
    };
    let mut ids = Vec::new();
    for year in min_year..=max_year {
        // The oldest and newest chunk are left open-ended on their outer
        // side, so target-side mail outside the local archive's exact date
        // span (a stray pre-migration message, or one that landed on the
        // target after the archive snapshot) is still found rather than
        // silently excluded from the dedup match.
        let after = (year != min_year).then(|| year_start(year));
        let before = (year != max_year).then(|| year_start(year + 1));
        query_range_adaptive(net, after, before, logger, &mut ids)?;
    }
    Ok(ids)
}

fn now_utc() -> DateTime<Utc> {
    DateTime::<Utc>::from(std::time::SystemTime::now())
}

fn effective_bounds(after: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> (DateTime<Utc>, DateTime<Utc>) {
    (after.unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap()), before.unwrap_or_else(now_utc))
}

fn is_min_chunk(after: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> bool {
    let (lo, hi) = effective_bounds(after, before);
    hi - lo <= MIN_CHUNK
}

fn date_filter(after: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> Value {
    let mut m = Map::new();
    if let Some(a) = after {
        m.insert("after".to_owned(), Value::String(a.to_rfc3339()));
    }
    if let Some(b) = before {
        m.insert("before".to_owned(), Value::String(b.to_rfc3339()));
    }
    Value::Object(m)
}

fn describe_range(after: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> String {
    format!(
        "{}..{}",
        after.map(|d| d.to_rfc3339()).unwrap_or_else(|| "-inf".to_owned()),
        before.map(|d| d.to_rfc3339()).unwrap_or_else(|| "now".to_owned()),
    )
}

/// Queries one date-range chunk of the target's Email ids, splitting it in
/// half and recursing when the chunk itself looks untrustworthy: an
/// `Email/query` error (logged either way, so a maxTotalHits-shaped error
/// text can be confirmed even though its exact client-visible form isn't
/// pinned down yet), or -- only when `--prune` is active, since that's the
/// mode where trusting a false "nothing here" most directly risks acting on
/// wrong data -- a suspicious 0-result count. Below `MIN_CHUNK` neither
/// condition splits further: an error propagates (the run fails instead of
/// silently under-counting) and a 0-result count is accepted as real.
fn query_range_adaptive(
    net: &Net,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    logger: &Logger,
    out: &mut Vec<JmapId>,
) -> Result<(), JmapError> {
    let filter = date_filter(after, before);
    let result = query_ids_filtered(
        &net.client,
        &net.api,
        &net.account,
        "Email",
        &net.limits,
        &filter,
        |n| crate::progress::advance(n as u64),
    );
    let floor = is_min_chunk(after, before);
    match result {
        Ok(ids) => {
            if ids.is_empty() && net.prune && !floor {
                logger.warn(&format!(
                    "Email: target query for {} returned 0 results with --prune set; \
                     splitting to double-check before trusting it",
                    describe_range(after, before)
                ));
                return split_and_recurse(net, after, before, logger, out);
            }
            out.extend(ids);
            Ok(())
        }
        Err(e) => {
            logger.warn(&format!(
                "Email: target query for {} failed: {e}",
                describe_range(after, before)
            ));
            if floor {
                Err(e)
            } else {
                split_and_recurse(net, after, before, logger, out)
            }
        }
    }
}

fn split_and_recurse(
    net: &Net,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    logger: &Logger,
    out: &mut Vec<JmapId>,
) -> Result<(), JmapError> {
    let (lo, hi) = effective_bounds(after, before);
    let mid = lo + (hi - lo) / 2;
    query_range_adaptive(net, after, Some(mid), logger, out)?;
    query_range_adaptive(net, Some(mid), before, logger, out)
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

    // Import never revives a local id, so any cache row whose local id is no
    // longer in the archive means that message was deleted at the source
    // since the last export. Detecting this is a pure local lookup against
    // our own cache, independent of (and cheaper than) whichever path below
    // confirms the *surviving* cache entries against the target.
    let (prune_candidates, prune_local_ids) = source_deleted(&local, &cached);

    if !local.is_empty() {
        if let Some(mut plan) = try_state_proven_fast_path(
            ctx, net, maps, target_row, ty, &local, &cached, counts, logger,
        )? {
            plan.prune_candidates = prune_candidates;
            plan.prune_local_ids = prune_local_ids;
            return Ok(plan);
        }

        if net.assume_not_deleted_in_destination {
            // Unlike the state-proven path above, this is an unverified
            // assertion: cached rows are trusted outright and anything
            // without a cache entry is created directly with no
            // target-side dedup search at all — unlike the full rebuild
            // below, whose entire purpose is catching objects the cache
            // doesn't know about. That's an accepted trade-off: a message
            // created by a prior run that crashed before its id was cached
            // would be recreated here as a duplicate. `create_missing`
            // writes cache rows incrementally (per completed batch, not
            // once at the very end) specifically to bound how much a crash
            // mid-run can lose to a handful of in-flight batches rather
            // than the whole run.
            trust_cache_and_create(ctx, net, maps, target_row, ty, &local, &cached, counts, logger)?;
            record_state_for_next_run(ctx, net, target_row, logger);
            return Ok(Plan {
                prune_candidates,
                prune_local_ids,
                ..Plan::default()
            });
        }
    }

    if !local.is_empty() && local.iter().all(|(id, _)| cached.contains_key(id)) {
        match try_cached(net, &local, &cached, ctx.common.threads, counts)? {
            Some(mut plan) => {
                plan.prune_candidates = prune_candidates;
                plan.prune_local_ids = prune_local_ids;
                record_state_for_next_run(ctx, net, target_row, logger);
                return Ok(plan);
            }
            None => {
                // try_cached's existence check already advanced progress
                // speculatively for the ids it verified before finding a
                // stale one; the full rebuild below re-walks every local row
                // and advances again, so undo those first or the total
                // roughly doubles.
                crate::progress::reset(0);
                logger.warn(
                    "Email: cached target ids are stale (target changed since last export); \
                     rebuilding the full match instead of trusting the cache",
                );
            }
        }
    }

    let target_min = target_email_min(net, &local, ctx.common.threads, logger).map_err(Error::from)?;
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
            |_| {},
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

    let mut matched_to_cache: Vec<(i64, String)> = Vec::new();
    let mut to_create: Vec<(i64, &EmailRow)> = Vec::new();
    for (i, key) in local_keys.iter().enumerate() {
        if let Some(target_jmap_id) = target_map.get(key) {
            counts.skipped += 1;
            crate::progress::advance(1);
            matched_to_cache.push((local[i].0, target_jmap_id.clone()));
        } else {
            to_create.push((local[i].0, &local[i].1));
        }
    }
    create_missing(ctx, net, maps, target_row, ty, &to_create, counts, logger)?;

    if !net.dry_run {
        db::export_ids::upsert_many(&ctx.conn, target_row, ty, &matched_to_cache)
            .map_err(|e| Error::Partial(e.to_string()))?;
    }
    record_state_for_next_run(ctx, net, target_row, logger);

    Ok(Plan {
        prune_candidates,
        prune_local_ids,
        ..Plan::default()
    })
}

/// If the target's Email `state` token hasn't moved since the end of the
/// last export that finished cleanly, this is provably (not just assumed)
/// safe: every cached id is still valid, and every local row *not* yet
/// cached can't be on the target either — it would have had to move the
/// state to get there. That makes it strictly safer than
/// `--assume-not-deleted-in-destination`, which asserts the same thing
/// without proof, so it's tried first and needs no flag.
///
/// Returns `Ok(None)` whenever there's no persisted state, it doesn't match
/// the target's current one, or fetching it fails, so the caller falls
/// through to its normal (slower but unconditionally safe) strategy.
#[allow(clippy::too_many_arguments)]
fn try_state_proven_fast_path(
    ctx: &Context,
    net: &Net,
    maps: &Maps,
    target_row: i64,
    ty: ObjectType,
    local: &[(i64, EmailRow)],
    cached: &HashMap<i64, String>,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<Option<Plan>, Error> {
    let persisted = db::export_ids::get_email_state(&ctx.conn, target_row)
        .map_err(|e| Error::Partial(e.to_string()))?;
    let Some(persisted) = persisted else {
        return Ok(None);
    };
    let current = match get_state(&net.client, &net.api, &net.account, "Email") {
        Ok(s) => s,
        Err(e) => {
            logger.warn(&format!(
                "Email: fetching target state failed, falling back to the default strategy: {e}"
            ));
            return Ok(None);
        }
    };
    if current.as_deref() != Some(persisted.as_str()) {
        return Ok(None);
    }

    // Proven safe for exactly this run. Clear immediately, before relying on
    // it for anything, so a crash anywhere below leaves NULL behind: the
    // next run then correctly falls back to the default strategy instead of
    // re-trusting a token whose validity this run can no longer vouch for.
    if !net.dry_run {
        db::export_ids::set_email_state(&ctx.conn, target_row, None)
            .map_err(|e| Error::Partial(e.to_string()))?;
    }

    trust_cache_and_create(ctx, net, maps, target_row, ty, local, cached, counts, logger)?;
    record_state_for_next_run(ctx, net, target_row, logger);

    Ok(Some(Plan::default()))
}

/// Captures the target's current Email state and persists it so a future
/// run's [`try_state_proven_fast_path`] can use it, once this run reaches
/// this point without error. Best-effort: failing to record it only means
/// the next run won't get to skip its target checks, not that this run's
/// own work is lost, so it's logged and swallowed rather than propagated.
fn record_state_for_next_run(ctx: &Context, net: &Net, target_row: i64, logger: &Logger) {
    if net.dry_run {
        return;
    }
    match get_state(&net.client, &net.api, &net.account, "Email") {
        Ok(Some(state)) => {
            if let Err(e) = db::export_ids::set_email_state(&ctx.conn, target_row, Some(&state)) {
                logger.warn(&format!("Email: recording target state for next run failed: {e}"));
            }
        }
        Ok(None) => {}
        Err(e) => logger.warn(&format!(
            "Email: fetching target state for next run failed: {e}"
        )),
    }
}

/// Trusts every cached row outright and creates everything else directly,
/// with no target-side dedup search. Shared by the state-proven fast path
/// (where this is provably safe) and `--assume-not-deleted-in-destination`
/// (where it's an accepted, documented risk).
#[allow(clippy::too_many_arguments)]
fn trust_cache_and_create(
    ctx: &Context,
    net: &Net,
    maps: &Maps,
    target_row: i64,
    ty: ObjectType,
    local: &[(i64, EmailRow)],
    cached: &HashMap<i64, String>,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<(), Error> {
    let mut to_create: Vec<(i64, &EmailRow)> = Vec::new();
    for (local_id, row) in local {
        if cached.contains_key(local_id) {
            counts.skipped += 1;
            crate::progress::advance(1);
        } else {
            to_create.push((*local_id, row));
        }
    }
    create_missing(ctx, net, maps, target_row, ty, &to_create, counts, logger)
}

/// Creates every row in `to_create` on the target, batching blob uploads and
/// `Email/import` calls as usual. Creation results are cached incrementally
/// (one completed batch at a time, via `submit_batch`/`flush_results`)
/// rather than accumulated for a single write at the end of the run: for a
/// reconcile spanning hundreds of thousands of messages, that bounds how
/// much a crash mid-run can lose to whatever batches were still in flight.
#[allow(clippy::too_many_arguments)]
fn create_missing(
    ctx: &Context,
    net: &Net,
    maps: &Maps,
    target_row: i64,
    ty: ObjectType,
    to_create: &[(i64, &EmailRow)],
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<(), Error> {
    if to_create.is_empty() {
        return Ok(());
    }

    // Servers cap in-flight requests per user, so the cost driver is request
    // count. With RFC 9404 one request carries a whole batch of blobs.
    let batched = email_batch::supports_blob_upload(&net.session);
    let workers = import_workers(ctx.common.threads, &net.limits, batched);
    let (batch_count, batch_bytes) = if batched {
        email_batch::batch_limits(&net.limits, to_create.len(), workers)
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
    let mut interrupted = false;

    for (local_id, row) in to_create {
        // Stop submitting *new* batches once interrupted, but still fall
        // through to the same submit/finish/flush below as a normal
        // completion -- so whatever's already in flight gets its results
        // cached before this returns, rather than dropped.
        if crate::interrupt::requested() {
            interrupted = true;
            break;
        }
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
                ctx,
                target_row,
                ty,
                &pool,
                &mut batch,
                &mut batch_encoded,
                &mut in_flight,
                window,
                counts,
                logger,
            )?;
        }
    }
    submit_batch(
        ctx,
        target_row,
        ty,
        &pool,
        &mut batch,
        &mut batch_encoded,
        &mut in_flight,
        window,
        counts,
        logger,
    )?;
    for batch in pool.finish() {
        flush_results(ctx, target_row, ty, batch, counts, logger)?;
    }
    if interrupted {
        return Err(Error::Interrupted);
    }
    Ok(())
}

/// Cache rows whose local id no longer has a matching row in `local`,
/// returned as parallel `(target jmap ids, local ids)` vectors suitable for
/// [`Plan::prune_candidates`] / [`Plan::prune_local_ids`].
fn source_deleted(
    local: &[(i64, EmailRow)],
    cached: &HashMap<i64, String>,
) -> (Vec<String>, Vec<i64>) {
    let local_ids: HashSet<i64> = local.iter().map(|(id, _)| *id).collect();
    cached
        .iter()
        .filter(|(local_id, _)| !local_ids.contains(local_id))
        .map(|(local_id, jmap_id)| (jmap_id.clone(), *local_id))
        .unzip()
}

/// Drops `export_target_ids` rows for the local ids behind whichever of
/// `plan.prune_candidates` the target just confirmed as destroyed. Ids the
/// target reported as not destroyed keep their cache row, so they resurface
/// as candidates again on the next export instead of being forgotten after
/// a failed attempt.
pub fn forget_destroyed(
    ctx: &Context,
    net: &Net,
    plan: &Plan,
    destroyed: &[String],
) -> Result<(), Error> {
    if destroyed.is_empty() || plan.prune_local_ids.is_empty() {
        return Ok(());
    }
    let destroyed: HashSet<&str> = destroyed.iter().map(String::as_str).collect();
    let local_ids: Vec<i64> = plan
        .prune_candidates
        .iter()
        .zip(&plan.prune_local_ids)
        .filter(|(jmap_id, _)| destroyed.contains(jmap_id.as_str()))
        .map(|(_, local_id)| *local_id)
        .collect();
    if local_ids.is_empty() {
        return Ok(());
    }
    let target_row = db::export_ids::ensure_target(&ctx.conn, &net.api, &net.account)
        .map_err(|e| Error::Partial(e.to_string()))?;
    db::export_ids::delete_many(&ctx.conn, target_row, ObjectType::Email, &local_ids)
        .map_err(|e| Error::Partial(e.to_string()))
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
    let found = get_objects_parallel(net, ObjectType::Email, &ids, Some(&[]), threads, |n| {
        crate::progress::advance(n as u64);
    })
    .map_err(Error::from)?;
    if found.len() != ids.len() {
        return Ok(None);
    }
    for _ in local {
        counts.skipped += 1;
    }
    Ok(Some(Plan::default()))
}

type BatchPool = Pool<Vec<ImportJob>, Vec<ImportResult>>;

/// Hands the accumulated batch to the pool, then drains and persists one
/// completed batch once the submission window is full so memory stays
/// bounded.
#[allow(clippy::too_many_arguments)]
fn submit_batch(
    ctx: &Context,
    target_row: i64,
    ty: ObjectType,
    pool: &BatchPool,
    batch: &mut Vec<ImportJob>,
    encoded: &mut usize,
    in_flight: &mut usize,
    window: usize,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<(), Error> {
    if batch.is_empty() {
        return Ok(());
    }
    pool.submit(std::mem::take(batch));
    *encoded = 0;
    *in_flight += 1;
    if *in_flight >= window
        && let Ok(done) = pool.results().recv()
    {
        *in_flight -= 1;
        flush_results(ctx, target_row, ty, done, counts, logger)?;
    }
    Ok(())
}

/// Accounts one completed batch and immediately persists whatever target ids
/// it produced, instead of accumulating results for a single write at the
/// end of a reconcile that may run for hours.
fn flush_results(
    ctx: &Context,
    target_row: i64,
    ty: ObjectType,
    done: Vec<ImportResult>,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<(), Error> {
    let mut to_cache = Vec::new();
    for res in done {
        account(res, counts, logger, &mut to_cache);
    }
    if to_cache.is_empty() {
        return Ok(());
    }
    db::export_ids::upsert_many(&ctx.conn, target_row, ty, &to_cache)
        .map_err(|e| Error::Partial(e.to_string()))
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
    // server throttles in between, or the import can fail with a transient
    // serverUnavailable. Both go back through the per-message path, which
    // re-uploads (blobNotFound) or just retries the same import once
    // (serverUnavailable) before giving up -- see run_import.
    let retry: HashSet<String> = results
        .iter()
        .filter(|r| is_retryable_not_created(r))
        .map(|r| r.cid.clone())
        .collect();
    let mut redo = throttled;
    if !retry.is_empty() {
        results.retain(|r| !retry.contains(&r.cid));
        redo.extend(uploaded.into_iter().filter(|j| retry.contains(&j.cid)));
    }
    results.extend(per_message(net, cache, redo));
    results
}

fn is_retryable_not_created(res: &ImportResult) -> bool {
    matches!(
        &res.outcome,
        Ok(SingleImport::NotCreated { error_type, .. })
            if error_type == "blobNotFound" || error_type == "serverUnavailable"
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
        // Transient server-side condition, not a stale blob reference -- no
        // re-upload needed, just retry the same import once.
        SingleImport::NotCreated { ref error_type, .. } if error_type == "serverUnavailable" => {
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
        // The source blob itself is not a valid RFC 5322 message -- no retry
        // or reimport will ever fix that, and the only real remedy (deleting
        // it) has the same end state as leaving it out, so this is not
        // treated as a run failure.
        Ok(SingleImport::NotCreated { error_type, detail }) if error_type == "invalidEmail" => {
            logger.warn(&format!(
                "Email/import {} ({}) skipped: not a valid RFC 5322 message: {detail}",
                res.cid, res.hint
            ));
            counts.skipped += 1;
        }
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

    fn not_created(error_type: &str) -> ImportResult {
        ImportResult {
            cid: "e1".to_string(),
            hint: "no message-id, 2 B".to_string(),
            outcome: Ok(SingleImport::NotCreated {
                error_type: error_type.to_string(),
                detail: "Blob does not contain a valid RFC 5322 message.".to_string(),
            }),
        }
    }

    #[test]
    fn invalid_email_is_skipped_not_failed() {
        let mut counts = TypeCounts::default();
        let logger = Logger::new(0);
        let mut to_cache = Vec::new();
        account(not_created("invalidEmail"), &mut counts, &logger, &mut to_cache);
        assert_eq!(counts.skipped, 1);
        assert_eq!(counts.failed, 0);
    }

    #[test]
    fn other_not_created_reasons_still_count_as_failed() {
        let mut counts = TypeCounts::default();
        let logger = Logger::new(0);
        let mut to_cache = Vec::new();
        account(
            not_created("someOtherError"),
            &mut counts,
            &logger,
            &mut to_cache,
        );
        assert_eq!(counts.skipped, 0);
        assert_eq!(counts.failed, 1);
    }

    #[test]
    fn blob_not_found_and_server_unavailable_are_retryable() {
        assert!(is_retryable_not_created(&not_created("blobNotFound")));
        assert!(is_retryable_not_created(&not_created("serverUnavailable")));
    }

    #[test]
    fn other_not_created_reasons_are_not_retryable() {
        assert!(!is_retryable_not_created(&not_created("invalidEmail")));
        assert!(!is_retryable_not_created(&not_created("someOtherError")));
    }

    fn row_at(received_at: &str) -> (i64, EmailRow) {
        (
            0,
            EmailRow {
                blob_local_id: 0,
                received_at: received_at.to_string(),
                mailbox_locals: Vec::new(),
                keywords: Vec::new(),
                message_match: String::new(),
            },
        )
    }

    #[test]
    fn local_year_range_spans_earliest_to_latest() {
        let local = vec![
            row_at("2024-06-01T00:00:00Z"),
            row_at("2022-01-15T00:00:00Z"),
            row_at("2026-08-18T19:43:57Z"),
        ];
        assert_eq!(local_year_range(&local), Some((2022, 2026)));
    }

    #[test]
    fn local_year_range_ignores_unparseable_dates() {
        let local = vec![row_at("not-a-date"), row_at("2025-01-01T00:00:00Z")];
        assert_eq!(local_year_range(&local), Some((2025, 2025)));
    }

    #[test]
    fn local_year_range_empty_local_is_none() {
        assert_eq!(local_year_range(&[]), None);
    }

    #[test]
    fn date_filter_omits_open_ended_bounds() {
        let both = date_filter(Some(year_start(2025)), Some(year_start(2026)));
        assert_eq!(both["after"], "2025-01-01T00:00:00+00:00");
        assert_eq!(both["before"], "2026-01-01T00:00:00+00:00");

        let oldest_chunk = date_filter(None, Some(year_start(2023)));
        assert!(oldest_chunk.get("after").is_none());
        assert_eq!(oldest_chunk["before"], "2023-01-01T00:00:00+00:00");

        let newest_chunk = date_filter(Some(year_start(2026)), None);
        assert!(newest_chunk.get("before").is_none());
    }

    #[test]
    fn a_full_year_is_not_a_min_chunk() {
        assert!(!is_min_chunk(Some(year_start(2025)), Some(year_start(2026))));
    }

    #[test]
    fn a_month_wide_range_is_a_min_chunk() {
        let start = year_start(2025);
        assert!(is_min_chunk(Some(start), Some(start + Duration::days(20))));
    }

    #[test]
    fn splitting_a_year_lands_on_two_halves_that_cover_it_exactly() {
        let (lo, hi) = effective_bounds(Some(year_start(2025)), Some(year_start(2026)));
        let mid = lo + (hi - lo) / 2;
        assert!(mid > lo && mid < hi);
    }
}
