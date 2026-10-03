// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestInsertVulnerabilityBatchOwnsOneTransaction — PR #226 review
// 5402909422: the rows and the repository's cache stamp commit as one unit.
// A pool batch is already one implicit transaction in pgx v5 (one Sync in
// the extended modes; one multi-statement query under the simple protocol —
// verified in the whole-branch review), so the explicit transaction states
// the invariant in the writer rather than inheriting it from the wire
// protocol (the ReplaceScancodeSnapshot shape).
func TestInsertVulnerabilityBatchOwnsOneTransaction(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/vulnerability_store.go"), "func (s *PostgresStore) InsertVulnerabilityBatch("))
	for _, needle := range []string{"s.pool.Begin(ctx)", "tx.Rollback(ctx)", "tx.SendBatch(ctx, batch)", "tx.Commit(ctx)"} {
		if !strings.Contains(body, needle) {
			t.Errorf("InsertVulnerabilityBatch must send its batch inside one explicit transaction — missing %q", needle)
		}
	}
	if strings.Contains(body, "s.pool.SendBatch(") {
		t.Error("InsertVulnerabilityBatch must not send the batch on the pool (atomic only in some exec modes)")
	}
}

// TestRetriedBatchesAreBuiltPerAttempt — review 8 on PR #226: a *pgx.Batch
// sent once keeps each statement's description from the connection that
// prepared it (QueryExecModeCacheStatement); a withRetry attempt on another
// connection then skips the prepare and fails with 26000 "prepared statement
// does not exist" instead of healing the deadlock it retries. Every store
// function that retries a batch builds the batch inside the retried closure.
func TestRetriedBatchesAreBuiltPerAttempt(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 20)
	examined := 0
	for name, src := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src = srctest.StripGoComments(src)
		for _, part := range strings.Split(src, "\nfunc ")[1:] {
			w := strings.Index(part, "withRetry(")
			b := strings.Index(part, "pgx.Batch{}")
			if w < 0 || b < 0 {
				continue
			}
			examined++
			if b < w {
				t.Errorf("%s: %s builds its pgx.Batch before withRetry — build it inside the retried closure",
					name, strings.SplitN(part, "\n", 2)[0])
			}
		}
	}
	srctest.MinCount(t, "store functions that retry a batch", examined, 2)
}

// TestOneVulnerabilityUpsertSpelling — PR #226 review 5403078495: the
// single-row writer is the batch writer with one row. A second upsert
// spelling moved no cache stamp (and could drift from the first).
func TestOneVulnerabilityUpsertSpelling(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/vulnerability_store.go"))
	single := srctest.FuncBody(t, src, "func (s *PostgresStore) InsertVulnerability(")
	if !strings.Contains(single, "s.InsertVulnerabilityBatch(") || strings.Contains(single, "INSERT INTO") {
		t.Error("InsertVulnerability must delegate to InsertVulnerabilityBatch, not carry its own upsert")
	}
	if n := strings.Count(src, "INSERT INTO aveloxis_data.repo_deps_vulnerabilities"); n != 1 {
		t.Errorf("one repo_deps_vulnerabilities upsert spelling expected, found %d", n)
	}
}
