// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// The P-4 deletions (v0.29.98) left name bans behind ("no MakeQueuedReposDue",
// "no ListQueue", …); a name ban is escaped by a rename. These pin the
// OPERATION each deleted function performed (stage review B4; the
// users.email writer ban is the model).

// TestBreadthStampIsBatched: every write of contributors.cntrb_last_breadth_at
// stamps a SET of ids (ANY($n::uuid[])), never one row per statement — the
// 18,000-statements-per-cycle shape MarkBreadthAttempted had.
func TestBreadthStampIsBatched(t *testing.T) {
	stmts := sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 100))
	writers := 0
	for _, st := range sqlscan.FindWrites(stmts, "aveloxis_data.contributors") {
		if !st.WritesColumn("aveloxis_data.contributors", "cntrb_last_breadth_at") {
			continue
		}
		writers++
		if !regexp.MustCompile(`ANY\(\$\d+::uuid\[\]\)`).MatchString(st.SQL) {
			t.Errorf("%s stamps cntrb_last_breadth_at one row at a time:\n%s", st.File, st.SQL)
		}
	}
	srctest.MinCount(t, "cntrb_last_breadth_at writers", writers, 1)
}

// TestDueAtIsNeverForcedFleetWide: a function that sets collection_queue.due_at
// to NOW() addresses rows it owns — a repository (a boost, a completed job,
// a re-queue on conflict) or a worker's locks (crash recovery, parking) —
// never "every queued row" (MakeQueuedReposDue selected by status alone).
// The startup realignment computes due_at from last_collected per row. At
// function level, because CompleteJob's statement is concatenated and a
// statement scan sees only its first literal.
func TestDueAtIsNeverForcedFleetWide(t *testing.T) {
	setsNow := regexp.MustCompile(`(?i)due_at\s*=\s*(?:CASE[^,]*)?NOW\(\)`)
	owns := regexp.MustCompile(`(?is)(\bWHERE\b.*\b(repo_id\s*=|locked_by)|ON\s+CONFLICT\s*\(\s*repo_id\s*\))`)
	writers := 0
	for name, src := range srctest.PackageFiles(t, "internal/db", 100) {
		for fn, part := range srctest.FuncParts(t, srctest.StripGoComments(src)) {
			if !strings.Contains(part, "collection_queue") || !setsNow.MatchString(part) {
				continue
			}
			writers++
			if !owns.MatchString(part) {
				t.Errorf("%s %s sets collection_queue.due_at = NOW() for rows it does not own (no repo_id or locked_by predicate): a fleet-wide force-due", name, fn)
			}
		}
	}
	srctest.MinCount(t, "functions forcing due_at = NOW()", writers, 3)
}

// TestSchemaStampReadersKeepTheError: the stamp is read through
// schemaVersionProbe; no reader discards its error (the lossy
// GetSchemaVersion mapped every failure to "" = unstamped, SR-5) — neither
// with `_` nor in another costume (review R2-3: `if err != nil { return
// "" }`): a caller that does not return an error itself hands it to a
// sink — the verdict renderer or a log attribute — and never returns an
// empty stamp.
func TestSchemaStampReadersKeepTheError(t *testing.T) {
	discard := regexp.MustCompile(`(?:\b\w+\s*,\s*_|_)\s*:?=\s*\w+\.schemaVersionProbe\(`)
	// The RESULT list (review R3-2: `error` as a parameter is not a way out).
	returnsError := regexp.MustCompile(`\)\s*(?:error|\([^)]*\berror\b[^)]*\))\s*\{`)
	sink := regexp.MustCompile(`logSchemaVersionCheck\(|"error",\s*err\b`)
	callers := 0
	for name, src := range srctest.PackageFiles(t, "internal/db", 100) {
		for fn, part := range srctest.FuncParts(t, srctest.StripGoComments(src)) {
			if !strings.Contains(part, ".schemaVersionProbe(") || fn == "schemaVersionProbe" {
				continue
			}
			callers++
			if discard.MatchString(part) {
				t.Errorf("%s %s reads the schema stamp and discards the probe's error (a failed read is not \"unstamped\", SR-5)", name, fn)
			}
			if !returnsError.MatchString(part) {
				if !sink.MatchString(part) {
					t.Errorf("%s %s reads the schema stamp, returns no error and hands the probe's error to no sink (logSchemaVersionCheck or a log attribute): a failed read vanishes", name, fn)
				}
				if strings.Contains(part, `return ""`) {
					t.Errorf("%s %s returns an empty stamp — the lossy reader in another costume", name, fn)
				}
			}
		}
	}
	srctest.MinCount(t, "schemaVersionProbe callers", callers, 4)
}

// TestQueueListingsAreBounded: every ordered SELECT that lists queue ROWS
// (status and due_at in its select list) carries a LIMIT — the unbounded
// ListQueue (every job of the fleet in one answer) must not return under
// any name or in any file. An ordered id list or an aggregate digest is
// not a job listing.
func TestQueueListingsAreBounded(t *testing.T) {
	ordered, listings := 0, 0
	for _, st := range sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 100)) {
		up := strings.ToUpper(st.SQL)
		from := strings.Index(up, "FROM AVELOXIS_OPS.COLLECTION_QUEUE")
		if from < 0 || !strings.Contains(up, "ORDER BY") {
			continue
		}
		ordered++ // the denominator: every ordered read of the queue examined
		selectList := up[:from]
		if !strings.Contains(selectList, "STATUS") || !strings.Contains(selectList, "DUE_AT") {
			continue
		}
		listings++
		if !strings.Contains(up, "LIMIT") {
			t.Errorf("an ordered listing of collection_queue has no LIMIT:\n%s", st.SQL)
		}
	}
	// Today: the page (ListQueuePage) is the one job listing; the claim
	// selects an id in a subquery, the digest aggregates, the re-warm lists ids.
	srctest.MinCount(t, "ordered reads of collection_queue examined", ordered, 3)
	srctest.MinCount(t, "job listings among them", listings, 1)
}
