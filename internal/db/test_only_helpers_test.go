// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/spdx"
)

// Helpers production never calls (v0.29.98, stage 0.5 P-4): moved out of
// the production files so `staticcheck -checks U1000 -tests=false`
// (scripts/unused-ratchet.sh) sees only the deliberate seams.

// isOSILicense judges a RAW stored license (tests and ad-hoc callers; the
// table badges judge their already-normalized key with licenseKeyIsOSI): the
// license, normalized, judged against the
// SPDX list's isOsiApproved by internal/spdx.OSIApproved (worklist 53). An OR
// is approved when any option is, an AND when every term is, and X WITH an
// exception follows X. The v0.28.1 " AND " split and the hand-kept allowlist
// it read are gone: the allowlist disagreed with SPDX in both directions.
func isOSILicense(license string) bool {
	return spdx.OSIApproved(NormalizeLicenseToSPDX(license))
}

// repoGoneAt reads the gone stamp directly; nil = reachable / never
// probed. The former store method GetRepoGoneAt had no caller: the prelim
// and gone-recheck paths write, clear and read the column in their own
// statements.
func repoGoneAt(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) *time.Time {
	t.Helper()
	var ts *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_gone_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	return ts
}

// stampOf reads the schema stamp with its error arm kept (SR-5); a failed
// read fails the test instead of reading as "unstamped".
func stampOf(ctx context.Context, t testing.TB, store *PostgresStore) string {
	t.Helper()
	v, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("read the schema stamp: %v", err)
	}
	return v
}
