// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.17: the contributor breadth worker pre-fix only marked a
// contributor as "processed" by inserting rows into
// contributor_repo. A contributor whose /users/{login}/events
// returned zero events left contributor_repo empty for them
// forever, and GetContributorsForBreadth's NULLS-FIRST ordering
// kept selecting them on every cycle — the worker spun on
// dead-end contributors and never reached the other 1.4M.
//
// The fix adds an unconditional per-contributor attempt timestamp
// (cntrb_last_breadth_at) that mirrors the existing
// cntrb_last_enriched_at / cntrb_last_search_attempted_at pattern.
// MarkBreadthAttempted updates it after EVERY attempt regardless
// of whether events were found, and GetContributorsForBreadth
// filters by it with a configurable cooldown.

func TestSchemaHasCntrbLastBreadthAtColumn(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "cntrb_last_breadth_at") {
		t.Error("schema.sql must declare cntrb_last_breadth_at on aveloxis_data.contributors — pre-v0.20.17 the breadth worker had no per-contributor 'I tried, found nothing' signal and reprocessed dead-end contributors forever")
	}
}

func TestMigrateAddsCntrbLastBreadthAtColumn(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	needle := `addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.contributors", "cntrb_last_breadth_at"`
	if !strings.Contains(string(data), needle) {
		t.Errorf("migrate.go must call addColumnIfMissing for aveloxis_data.contributors.cntrb_last_breadth_at — operators upgrading from <v0.20.17 need the column added automatically")
	}
}

func TestMarkBreadthAttemptedExists(t *testing.T) {
	data, err := os.ReadFile("breadth_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "func (s *PostgresStore) MarkBreadthAttempted(") {
		t.Error("MarkBreadthAttempted store method must exist — the worker calls this after EVERY contributor attempt regardless of whether events were found, so contributors with zero public events still exit the unprocessed-queue")
	}
}

func TestGetContributorsForBreadthFiltersByCooldown(t *testing.T) {
	data, err := os.ReadFile("breadth_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate GetContributorsForBreadth's body.
	idx := strings.Index(src, "func (s *PostgresStore) GetContributorsForBreadth(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsForBreadth")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of GetContributorsForBreadth")
	}
	body := tail[:1+endRel]

	if !strings.Contains(body, "cntrb_last_breadth_at") {
		t.Error("GetContributorsForBreadth must filter by cntrb_last_breadth_at so the cooldown semantics actually exclude recently-attempted contributors. Pre-v0.20.17 the query JOINed contributor_repo, which left zero-event contributors permanently at the front of the queue.")
	}
	// The query must exclude rows whose attempt is within the
	// cooldown window. Acceptable shapes: "IS NULL OR <" or
	// "NOT EXISTS"/COALESCE patterns. Pin via the cooldown
	// parameter being part of the SQL.
	if !strings.Contains(body, "INTERVAL") && !strings.Contains(body, "interval") &&
		!strings.Contains(body, "NOW() -") {
		t.Error("GetContributorsForBreadth must use a time-window filter (INTERVAL or NOW() - ...) so contributors recently attempted are excluded from the next batch")
	}
}
