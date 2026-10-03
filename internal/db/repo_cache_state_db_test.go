// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestRepoCacheStates — v0.29.73: the state the API's repository-page cache
// is validated against. Every writer the repository page can observe moves
// it: the queue row (claim, CompleteJob, re-queue), the decoupled scancode
// worker's stamp and the vulnerability scan's stamp. A repository with no
// queue row still has a state (it exists); an unknown id is absent, never a
// zero state another id could share.
func TestRepoCacheStates(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	var a, b int64
	for i, id := range []*int64{&a, &b} {
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, 'r', '_avrcs', 1) RETURNING repo_id`, "https://github.com/_avrcs/r"+string(rune('a'+i))).Scan(id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2)`, a, b)
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, a, b)
	})
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, '2026-09-01T00:00:00Z')`, a); err != nil {
		t.Fatal(err)
	}
	const missing = int64(-1)
	read := func() map[int64]RepoCacheState {
		t.Helper()
		m, err := store.RepoCacheStates(ctx, []int64{a, b, missing})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	s0 := read()
	if _, ok := s0[missing]; ok {
		t.Error("an unknown repository id must be absent from the map")
	}
	if _, ok := s0[b]; !ok {
		t.Error("a repository without a queue row must still have a state")
	}
	if s0[a].Collecting {
		t.Error("a queued repository is not collecting")
	}
	if s0[a].Fingerprint() == s0[b].Fingerprint() {
		t.Error("two repositories with different queue rows must not share a fingerprint")
	}

	step := func(name, sql string, id int64, wantCollecting bool) {
		t.Helper()
		before := read()[id].Fingerprint()
		if _, err := store.pool.Exec(ctx, sql, id); err != nil {
			t.Fatal(err)
		}
		after := read()[id]
		if after.Fingerprint() == before {
			t.Errorf("%s must change the fingerprint", name)
		}
		if after.Collecting != wantCollecting {
			t.Errorf("%s: Collecting = %t, want %t", name, after.Collecting, wantCollecting)
		}
	}
	step("a claim (status collecting, updated_at stamped)",
		`UPDATE aveloxis_ops.collection_queue SET status = 'collecting', updated_at = NOW() WHERE repo_id = $1`, a, true)
	step("a completed job (status queued, last_collected advanced)",
		`UPDATE aveloxis_ops.collection_queue SET status = 'queued', last_collected = NOW(), updated_at = NOW() WHERE repo_id = $1`, a, false)
	step("a scancode run", `UPDATE aveloxis_data.repos SET scancode_last_run = NOW() WHERE repo_id = $1`, a, false)
	step("a vulnerability scan", `UPDATE aveloxis_data.repos SET vuln_scan_last_run = NOW() WHERE repo_id = $1`, a, false)

	// A heartbeat stamps only locked_at; the fingerprint must not churn.
	before := read()[a].Fingerprint()
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET locked_at = NOW() WHERE repo_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	if read()[a].Fingerprint() != before {
		t.Error("a heartbeat (locked_at only) must not change the fingerprint")
	}

	if m, err := store.RepoCacheStates(ctx, nil); err != nil || len(m) != 0 {
		t.Errorf("no ids: got %v, %v; want an empty map", m, err)
	}
}

// TestRepoCacheStateSeesWritersOutsideTheJob — review round 1, finding 2:
// `aveloxis run-scorecard` (ReplaceScorecard) and `heal-vulnerabilities
// --rescore-only` (UpdateCVSSScoreForVector) rewrite what the repository page
// shows outside any collection job. The writers themselves stamp the queue
// row (SR-18: the owning layer enforces, so every caller is covered); a
// rescore that changes nothing stamps nothing.
func TestRepoCacheStateSeesWritersOutsideTheJob(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	var a, other int64
	for i, id := range []*int64{&a, &other} {
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, 'r', '_avrcsw', 1) RETURNING repo_id`, "https://github.com/_avrcsw/r"+string(rune('a'+i))).Scan(id); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected, updated_at)
			VALUES ($1, '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')`, *id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id IN ($1, $2)`, a, other)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id IN ($1, $2)`, a, other)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id IN ($1, $2)`, a, other)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2)`, a, other)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, a, other)
	})
	fp := func(id int64) string {
		t.Helper()
		m, err := store.RepoCacheStates(ctx, []int64{id})
		if err != nil {
			t.Fatal(err)
		}
		return m[id].Fingerprint()
	}

	before, otherBefore := fp(a), fp(other)
	if _, err := store.ReplaceScorecard(ctx, a, "remote", []ScorecardRow{{Name: "Code-Review", Score: "8"}},
		func(string, bool) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if fp(a) == before {
		t.Error("a scorecard written outside the collection job must change the repository's cache state")
	}
	if fp(other) != otherBefore {
		t.Error("another repository's scorecard state must not move")
	}

	const vector = "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.repo_deps_vulnerabilities (repo_id, vuln_id, package_name, cvss_vector, cvss_score)
		VALUES ($1, 'GHSA-avrcsw', 'pkg', $2, 0)`, a, vector); err != nil {
		t.Fatal(err)
	}
	before, otherBefore = fp(a), fp(other)
	n, err := store.UpdateCVSSScoreForVector(ctx, vector, 9.8)
	if err != nil || n < 1 {
		t.Fatalf("rescore: %d rows, %v", n, err)
	}
	if fp(a) == before {
		t.Error("a CVSS rescore of a repository's findings must change its cache state")
	}
	if fp(other) != otherBefore {
		t.Error("a repository without the rescored vector must not move")
	}
	before = fp(a)
	if n, err := store.UpdateCVSSScoreForVector(ctx, vector, 9.8); err != nil || n != 0 {
		t.Fatalf("an unchanged rescore: %d rows, %v", n, err)
	}
	if fp(a) != before {
		t.Error("a rescore that changes nothing must not move the cache state")
	}
}
