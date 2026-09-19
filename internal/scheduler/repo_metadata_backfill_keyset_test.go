// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// repo_metadata_backfill_keyset_test.go — v0.29.56. The backfill paged
// with "candidates LIMIT N" and never stamped a failure, so every repo
// that cannot be fetched (deleted, renamed, blocked) stayed at the head of
// the ordered candidate set and was re-fetched on every later page. In two
// hours of the 2026-09-17 chaoss.tv log the failures grew 197 → 238 → 279
// across pages, each one costing an API call and a second of pacing on
// every page after the first. Pages advance by a keyset cursor over
// repo_id instead (the bulk-backfill rule).

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestMetadataBackfillCursorAdvancesPastFailures(t *testing.T) {
	targets := []db.RepoMetadataBackfillTarget{{RepoID: 10}, {RepoID: 42}, {RepoID: 7}}
	if got := metadataBackfillCursor(targets, 0); got != 42 {
		t.Errorf("cursor = %d, want the highest repo_id in the page (42), so a failing repo is not re-fetched", got)
	}
	// An empty page leaves the cursor alone.
	if got := metadataBackfillCursor(nil, 99); got != 99 {
		t.Errorf("cursor = %d, want 99", got)
	}
}

func TestReposNeedingMetadataBackfillIsKeyset(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/repo_metadata.go"),
		"func (s *PostgresStore) ReposNeedingMetadataBackfill(")
	sql := srctest.NormalizeWS(body)
	if !strings.Contains(sql, "repo_id > $1") {
		t.Error("the candidate query must window on the cursor (repo_id > $1); a plain LIMIT re-serves every repo that failed")
	}
	if !strings.Contains(sql, "ORDER BY repo_id") {
		t.Error("the window needs a deterministic order")
	}
}
