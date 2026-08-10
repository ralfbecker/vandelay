/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use clap::Parser;

use vandelay::cli::{Action, Cli};
use vandelay::error::Error;
use vandelay::inspect;
use vandelay::sync::{self, Summary};

fn main() {
    let code = run();
    std::process::exit(code);
}

fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            return if err.use_stderr() { 1 } else { 0 };
        }
    };

    let action = match cli.resolve() {
        Ok(action) => action,
        Err(err) => return fail(&err),
    };

    let result = match action {
        Action::Import(common, config) => {
            let logger = common.logger;
            sync::import_jmap::run(common, config).map(|s| (s, logger))
        }
        Action::ImportImap(common, config) => {
            let logger = common.logger;
            sync::import_imap::run(common, config).map(|s| (s, logger))
        }
        Action::ImportDav(common, config) => {
            let logger = common.logger;
            sync::import_dav::run(common, config).map(|s| (s, logger))
        }
        Action::ImportManageSieve(common, config) => {
            let logger = common.logger;
            sync::import_managesieve::run(common, config).map(|s| (s, logger))
        }
        Action::ImportMaildir(common, config) => {
            let logger = common.logger;
            sync::import_maildir::run(common, config).map(|s| (s, logger))
        }
        Action::ImportTakeout(common, config) => {
            let logger = common.logger;
            sync::import_takeout::run(common, config).map(|s| (s, logger))
        }
        Action::ImportExchangeEws(common, config) => {
            let logger = common.logger;
            sync::import_exchange_ews::run(common, config).map(|s| (s, logger))
        }
        Action::ImportExchangeGraph(common, config) => {
            let logger = common.logger;
            sync::import_exchange_graph::run(common, config).map(|s| (s, logger))
        }
        Action::Export(common, config) => {
            let logger = common.logger;
            sync::export::run(common, config).map(|s| (s, logger))
        }
        Action::Inspect(config) => {
            return match inspect::run(config) {
                Ok(()) => 0,
                Err(err) => fail(&err),
            };
        }
    };

    match result {
        Ok((summary, logger)) => {
            report(&summary);
            if summary.any_failed() {
                logger.error("some objects failed; the archive is consistent and resumable");
                5
            } else {
                0
            }
        }
        Err(err) => fail(&err),
    }
}

fn fail(err: &Error) -> i32 {
    eprintln!("error: {err}");
    err.exit_code()
}

fn report(summary: &Summary) {
    for (type_name, counts) in &summary.per_type {
        println!(
            "{type_name}: created={} fetched={} updated={} deleted={} skipped={} failed={}",
            counts.created,
            counts.fetched,
            counts.updated,
            counts.deleted,
            counts.skipped,
            counts.failed
        );
    }
    if summary.retries_observed > 0 {
        println!(
            "retries: {} ({} after a server Retry-After/backoff wait)",
            summary.retries_observed, summary.retry_after_sleeps
        );
    }
}
