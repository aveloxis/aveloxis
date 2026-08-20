// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.106 — Copilot review round on PR #184 (all 8 findings verified;
// 7 taken, 1 declined as a deliberate Augur-parity behavior). This file
// pins the db-side fixes.
package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// Finding 1 (the severe one): login-only UserRefs (userID == 0 — GitLab
// group owners, GraphQL Bot actors) all shared the resolver cache key
// {platform, 0}, so after the first resolved, EVERY subsequent login-only
// ref returned the FIRST one's cntrb_id — collapsing distinct identities
// onto one contributor. The identities lookup had the same collapse via
// a (platform, 0) probe. Post-fix, userID==0 refs key by login and skip
// the identities probe.
func TestLoginOnlyRefsResolveDistinctly(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE 'avpr184-%'`)
	})

	r := NewContributorResolver(store)
	id1, err := r.Resolve(ctx, 2, 0, "avpr184-group-alpha", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := r.Resolve(ctx, 2, 0, "avpr184-group-beta", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatalf("two DIFFERENT login-only refs collapsed onto one contributor (%s) — the {platform, 0} cache-key bug", id1)
	}
	// Same login twice → same contributor (the cache still works).
	id1b, err := r.Resolve(ctx, 2, 0, "avpr184-group-alpha", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	if id1b != id1 {
		t.Fatalf("re-resolving the same login-only ref must be stable: %s vs %s", id1b, id1)
	}

	// v0.27.108 (round 3): NO identity row may exist for these refs —
	// the pre-fix step-3 INSERT funneled every login-only ref into one
	// shared (platform, 0) identity row (login churning per observation,
	// cntrb_id pinned to the first contributor).
	var zeroRows int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributor_identities
		WHERE platform_user_id = 0 AND cntrb_id::text IN ($1, $2)`, id1, id2).Scan(&zeroRows); err != nil {
		t.Fatal(err)
	}
	if zeroRows != 0 {
		t.Fatalf("login-only refs created %d (platform, 0) identity rows — identity rows are keyed by platform_user_id, which these refs don't have", zeroRows)
	}
}

// Finding 5: the rename-heal must route through UpdateRepoURLs (plural —
// which rewrites the child tables' stored issue/PR/release URLs carrying
// the old owner/repo path), and must only fall through to the insert on
// an actual 23505 uniqueness race; any other update failure propagates
// (falling through would recreate the very duplicate the heal prevents).
func TestRenameHealUsesUpdateRepoURLsAndGatesFallthrough(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")
	if !strings.Contains(body, "UpdateRepoURLs(") {
		t.Error("the rename-heal must call UpdateRepoURLs (plural) so child-table URLs heal too")
	}
	// The fallthrough gate: a pgErr 23505 check must guard the heal's
	// error path (transient failures must NOT proceed to insert).
	i := strings.Index(body, "FindRepoByPlatformRepoID(")
	if i < 0 {
		t.Fatal("heal branch missing")
	}
	region := body[i:]
	if j := strings.Index(region, "var id int64"); j > 0 {
		region = region[:j]
	}
	if !strings.Contains(region, `"23505"`) {
		t.Error("the heal's URL-update failure path must gate the insert fallthrough on SQLSTATE 23505 specifically")
	}
}

// Finding 6: the column-writer tripwire must scope its writer search to
// INSERT/UPDATE statements that name the audited table — a column
// mentioned only in a comment or a SELECT-only reader must NOT count as
// having a writer.
func TestColumnTripwireScopesToWriteStatements(t *testing.T) {
	src, err := os.ReadFile("column_writer_tripwire_test.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// The table-scoped statement matcher (regex form INSERT\s+INTO|UPDATE
	// anchored to aveloxis_data.<table>).
	if !strings.Contains(s, `INSERT\s+INTO|UPDATE`) || !strings.Contains(s, "aveloxis_data") {
		t.Error("tripwire must scope matches to INSERT/UPDATE statements naming the audited table")
	}
	if !strings.Contains(s, "commentRe") {
		t.Error("tripwire must strip SQL comments so commented mentions can't satisfy the writer check")
	}
}
