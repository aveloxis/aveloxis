// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestInsertVulnerabilityBatchOwnsOneTransaction — PR #226 review
// 5402909422: the rows and the repository's cache stamp must commit as one
// unit whatever the pool's exec mode. A pool.SendBatch is one implicit
// transaction only under the extended protocol's single Sync; under
// QueryExecModeSimpleProtocol (which postgres.go's own comment names as the
// fallback behind a transaction-mode pooler) each statement commits on its
// own, so the stamp could commit without the rows or the reverse. The batch
// runs inside an explicit transaction (the ReplaceScancodeSnapshot shape).
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
