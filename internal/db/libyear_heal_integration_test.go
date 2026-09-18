// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 — the behavioural tier for HealUnknownLibyear. The predicate and
// the keyset walk only exist against a real database, so this is where they
// are actually exercised: the dry run must change nothing, --apply must NULL
// exactly the uncomputable rows, and a second run must be a no-op.
//
// The rows that matter are the ones no registry answer can ever fix: a
// dependency declared with NO pinned version has no release date to measure
// age from. Rows WITH a pinned version are left alone even when their date
// is missing, because v0.29.56 fixed the resolvers behind most of them and
// re-analysis fills real dates in — NULLing them here would erase rows the
// next collection pass is about to answer properly.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestHealUnknownLibyearIntegration(t *testing.T) {
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
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	// A repo of our own, cleaned up regardless of outcome.
	const repoGit = "https://example.invalid/_avheal/libyear-heal"
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'libyear-heal', '_avheal', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name
		RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("clear fixtures: %v", err)
	}

	// name, current_version, current_release_date, latest_release_date, libyear
	type seed struct {
		name    string
		version string
		curDate string
		latDate string
		libyear *float64
	}
	num := func(f float64) *float64 { return &f }
	seeds := []seed{
		// UNCOMPUTABLE: no pinned version. These are the target.
		{"unpinned-zero", "", "", "2024-01-01T00:00:00Z", num(0)},
		{"unpinned-nonzero", "", "", "", num(1.5)},
		// COMPUTABLE, and correct: must survive untouched.
		{"pinned-real", "1.0.0", "2022-01-01T00:00:00Z", "2024-01-01T00:00:00Z", num(2)},
		{"pinned-genuine-zero", "1.0.0", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", num(0)},
		// PINNED but dateless: deliberately NOT healed — re-analysis owns it.
		{"pinned-no-date", "1.0.0", "", "2024-01-01T00:00:00Z", num(0)},
		// Already NULL: nothing to do, and it must not be counted as work.
		{"already-null", "", "", "", nil},
	}
	for _, s := range seeds {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear
			  (repo_id, name, package_manager, current_version, current_release_date, latest_release_date, libyear)
			VALUES ($1, $2, 'pypi', $3, $4, $5, $6)`,
			repoID, s.name, s.version, s.curDate, s.latDate, s.libyear); err != nil {
			t.Fatalf("seed %s: %v", s.name, err)
		}
	}

	libyearOf := func(name string) (val *float64) {
		if err := store.pool.QueryRow(ctx,
			`SELECT libyear FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = $2`,
			repoID, name).Scan(&val); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return val
	}

	// A DRY RUN must change nothing at all.
	candidates, updated, err := store.HealUnknownLibyear(ctx, false)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if updated != 0 {
		t.Errorf("dry run reported %d rows updated, want 0 — the preview must not write", updated)
	}
	if candidates < 2 {
		t.Errorf("dry run found %d candidates, want at least the 2 unpinned rows", candidates)
	}
	for _, name := range []string{"unpinned-zero", "unpinned-nonzero"} {
		if libyearOf(name) == nil {
			t.Errorf("%s was NULLed by a DRY RUN", name)
		}
	}

	// --apply NULLs exactly the uncomputable rows.
	_, updated, err = store.HealUnknownLibyear(ctx, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if updated < 2 {
		t.Errorf("apply updated %d rows, want at least the 2 unpinned ones", updated)
	}
	for _, name := range []string{"unpinned-zero", "unpinned-nonzero"} {
		if libyearOf(name) != nil {
			t.Errorf("%s still carries a libyear after --apply — it names no version, so it can never be computed", name)
		}
	}
	// Everything computable is untouched, including a GENUINE zero: that is
	// the whole distinction this release exists to preserve.
	for name, want := range map[string]float64{"pinned-real": 2, "pinned-genuine-zero": 0, "pinned-no-date": 0} {
		got := libyearOf(name)
		if got == nil {
			t.Errorf("%s was NULLed — only rows with no pinned version may be", name)
			continue
		}
		if *got != want {
			t.Errorf("%s libyear = %v, want %v", name, *got, want)
		}
	}

	// Idempotent: a second apply finds nothing left to do for this repo.
	before := candidates
	candidates2, updated2, err := store.HealUnknownLibyear(ctx, true)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if candidates2 >= before {
		t.Errorf("second run still sees %d candidates (first saw %d) — the heal did not converge", candidates2, before)
	}
	_ = updated2
}
