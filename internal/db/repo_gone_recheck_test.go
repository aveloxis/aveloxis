// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.7 — the gone-repo recheck cadence. A 404/410 dequeues a repo,
// so no collection cycle ever probes it again; before this release the
// only way a re-publicized repository came back was an operator
// running mark-gone-repos by hand. repos.repo_gone_checked_at is the
// per-row cadence marker the scheduler's recheck ticker claims on.

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestGoneCheckedColumnDeclaredAndMigrated(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	if !strings.Contains(schema, "repo_gone_checked_at    TIMESTAMPTZ") {
		t.Error("schema.sql must declare repos.repo_gone_checked_at TIMESTAMPTZ")
	}
	src := readSourceFile(t, "migrate.go")
	if !strings.Contains(src, `"aveloxis_data.repos", "repo_gone_checked_at", "TIMESTAMPTZ"`) {
		t.Error("migrate.go must addColumnIfMissing repos.repo_gone_checked_at for existing fleets")
	}
}

// The sideline probe IS a check: MarkRepoGone stamps checked_at in the
// same single statement so a freshly-gone repo waits a full cadence
// before its first recheck, and the statement count pin
// (TestMarkRepoGoneIsSingleStatement) still holds.
func TestMarkRepoGoneStampsCheckedAt(t *testing.T) {
	src := readSourceFile(t, "repo_gone.go")
	i := strings.Index(src, "func (s *PostgresStore) MarkRepoGone(")
	if i < 0 {
		t.Fatal("MarkRepoGone missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "repo_gone_checked_at = NOW()") {
		t.Error("MarkRepoGone must stamp repo_gone_checked_at in the same UPDATE")
	}
}

// Candidate shape: gone-stamped rows only, oldest check first with
// never-checked first, bounded by the caller's batch. The cadence is a
// parameter (the config accessor's value), never a literal here.
func TestGoneRecheckCandidatesShape(t *testing.T) {
	src := readSourceFile(t, "repo_gone.go")
	i := strings.Index(src, "func (s *PostgresStore) GetGoneRecheckCandidates(")
	if i < 0 {
		t.Fatal("GetGoneRecheckCandidates missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	for _, needle := range []string{
		"repo_gone_at IS NOT NULL",
		"repo_gone_checked_at IS NULL OR",
		"ORDER BY r.repo_gone_checked_at NULLS FIRST",
		"LIMIT $2",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("GetGoneRecheckCandidates must contain %q", needle)
		}
	}
	// The checked stamp only ever lands on a gone-stamped row: a
	// resurrected repo must not carry a fresh check stamp that would
	// delay its NEXT gone→recheck cycle if it goes gone again.
	j := strings.Index(src, "func (s *PostgresStore) MarkRepoGoneChecked(")
	if j < 0 {
		t.Fatal("MarkRepoGoneChecked missing")
	}
	if !strings.Contains(src[j:], "AND repo_gone_at IS NOT NULL") {
		t.Error("MarkRepoGoneChecked must be guarded to gone-stamped rows")
	}
}

// End-to-end against a live database: the claim window, the stamp,
// and the non-gone exclusion.
func TestGoneRecheckCandidatesEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const slug = "_avgonerecheck"
	cleanup := func() {
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	mk := func(name string) int64 {
		id, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitHub,
			GitURL:   "https://github.com/" + slug + "/" + name,
			Owner:    slug, Name: name,
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	gone := mk("gone")
	alive := mk("alive")
	if err := store.MarkRepoGone(ctx, gone); err != nil {
		t.Fatal(err)
	}
	ids := func(cs []GoneProbeCandidate) map[int64]bool {
		m := map[int64]bool{}
		for _, c := range cs {
			m[c.RepoID] = true
		}
		return m
	}
	// Just stamped gone → checked now → NOT due under a 28-day cadence.
	got, err := store.GetGoneRecheckCandidates(ctx, 28*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if ids(got)[gone] {
		t.Error("a repo stamped gone moments ago must not be due for recheck (MarkRepoGone counts as a check)")
	}
	if ids(got)[alive] {
		t.Error("a reachable repo must never be a recheck candidate")
	}
	// Age the check stamp past the cadence → due.
	if _, err := store.Pool().Exec(ctx,
		`UPDATE aveloxis_data.repos SET repo_gone_checked_at = NOW() - interval '30 days' WHERE repo_id = $1`, gone); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetGoneRecheckCandidates(ctx, 28*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !ids(got)[gone] {
		t.Fatal("a gone repo whose last check is older than the cadence must be due")
	}
	if !got[0].GoneStamped {
		t.Error("candidates must report GoneStamped = true")
	}
	// A NULL stamp (pre-v0.29.7 rows) is due FIRST.
	if _, err := store.Pool().Exec(ctx,
		`UPDATE aveloxis_data.repos SET repo_gone_checked_at = NULL WHERE repo_id = $1`, gone); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetGoneRecheckCandidates(ctx, 28*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !ids(got)[gone] {
		t.Fatal("a never-checked gone repo must be due")
	}
	// The stamp closes the window again.
	if err := store.MarkRepoGoneChecked(ctx, gone); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetGoneRecheckCandidates(ctx, 28*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if ids(got)[gone] {
		t.Error("MarkRepoGoneChecked must take the repo out of the due set")
	}
	// The stamp is a no-op on a reachable row.
	if err := store.MarkRepoGoneChecked(ctx, alive); err != nil {
		t.Fatal(err)
	}
	var ts *time.Time
	if err := store.Pool().QueryRow(ctx,
		`SELECT repo_gone_checked_at FROM aveloxis_data.repos WHERE repo_id = $1`, alive).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	if ts != nil {
		t.Error("MarkRepoGoneChecked must not stamp a row that is not gone")
	}
	// Resurrection clears the gone stamp; the row leaves the candidate set.
	if err := store.ResurrectRepo(ctx, gone, 10); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetGoneRecheckCandidates(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if ids(got)[gone] {
		t.Error("a resurrected repo must not be a recheck candidate")
	}
	if err := store.Pool().QueryRow(ctx,
		`SELECT repo_gone_checked_at FROM aveloxis_data.repos WHERE repo_id = $1`, gone).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	if ts != nil {
		t.Error("ResurrectRepo must clear repo_gone_checked_at with the gone state (a reachable repo carries no check stamp)")
	}
}
