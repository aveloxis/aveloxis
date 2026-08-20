// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — one-shot backfill for pull_requests.meta_head_id/
// meta_base_id (the 2026-08-19 fill audit found them 100% dark:
// processStagedPR computed both PKs since inception and discarded
// them; SetPRMetaLinks now persists them forward). This backfill is
// the v0.27.15 msg_ref_metadata class: 100% derivable from local data
// (a PK-join against pull_request_meta's UNIQUE (pull_request_id,
// head_or_base)), zero API calls, self-disabling via the IS NULL
// predicates, keyset windows over the driven table's PK so interrupted
// runs resume (the v0.26.6 lesson — never LIMIT-rescan loops).
package db

import (
	"context"
	"log/slog"
)

const prMetaLinkBoundsSQL = `SELECT COALESCE(MAX(pull_request_id), 0) FROM aveloxis_data.pull_requests`

const prMetaLinkHeadSQL = `
	UPDATE aveloxis_data.pull_requests p
	SET meta_head_id = m.pr_meta_id
	FROM aveloxis_data.pull_request_meta m
	WHERE m.pull_request_id = p.pull_request_id
	  AND m.head_or_base = 'head'
	  AND p.meta_head_id IS NULL
	  AND p.pull_request_id > $1 AND p.pull_request_id <= $2`

const prMetaLinkBaseSQL = `
	UPDATE aveloxis_data.pull_requests p
	SET meta_base_id = m.pr_meta_id
	FROM aveloxis_data.pull_request_meta m
	WHERE m.pull_request_id = p.pull_request_id
	  AND m.head_or_base = 'base'
	  AND p.meta_base_id IS NULL
	  AND p.pull_request_id > $1 AND p.pull_request_id <= $2`

// ensurePRMetaLinks backfills both link columns. ~21 windows per side
// at the house 1M window size on the production fleet (21.3M PRs).
// No DISTINCT needed: uq (pull_request_id, head_or_base) guarantees at
// most one meta row per side. Errors land in the fail-closed collector
// (v0.19.4).
func ensurePRMetaLinks(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	if err := runKeysetWindows(ctx, pg, logger,
		"v0.27.104 backfill pull_requests.meta_head_id",
		prMetaLinkBoundsSQL, prMetaLinkHeadSQL); err != nil {
		logger.Error("migration step failed", "label", "v0.27.104 backfill meta_head_id", "error", err)
		*errs = append(*errs, err)
	}
	if err := runKeysetWindows(ctx, pg, logger,
		"v0.27.104 backfill pull_requests.meta_base_id",
		prMetaLinkBoundsSQL, prMetaLinkBaseSQL); err != nil {
		logger.Error("migration step failed", "label", "v0.27.104 backfill meta_base_id", "error", err)
		*errs = append(*errs, err)
	}
}
