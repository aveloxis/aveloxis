// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.68 (worklist 69) — the startup metadata backfill re-fetched the
// same ~5,600 repositories at every serve start (3–4 h each time on
// production: processed=5617/5636/5597/5588/5527/5537 across six starts,
// 546 of 594 FetchRepoInfo failures the same 404s every time). The
// candidate query selected rows whose description AND language are empty,
// and nothing recorded that a repo had been asked: a repo whose forge
// answer really is empty was written back empty and stayed a candidate
// forever, and a 404 was retried forever. Each answer (the caller decides:
// metadata written, or a definitive not-found) is now stamped and the query
// skips rows stamped within the cooldown.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestMetadataBackfillSkipsReposAskedWithinCooldown(t *testing.T) {
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

	// Three candidates by every other clause: one never asked, one whose
	// forge answer was empty (written back empty, then stamped), one that
	// 404'd (stamped, nothing written).
	names := []string{"never", "empty", "gone"}
	ids := map[string]int64{}
	for _, n := range names {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, '_avmetacool', 1)
			ON CONFLICT (repo_git) DO UPDATE SET repo_description = '', primary_language = '',
			    metadata_backfill_attempted_at = NULL
			RETURNING repo_id`, "https://github.com/_avmetacool/"+n, n).Scan(&id); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
		ids[n] = id
	}
	t.Cleanup(func() {
		for _, id := range ids {
			_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	candidates := func(cooldown time.Duration) map[int64]bool {
		t.Helper()
		after := ids["never"]
		for _, id := range ids {
			if id < after {
				after = id
			}
		}
		got, err := store.ReposNeedingMetadataBackfill(ctx, after-1, 500, cooldown)
		if err != nil {
			t.Fatalf("candidate query: %v", err)
		}
		seen := map[int64]bool{}
		for _, c := range got {
			seen[c.RepoID] = true
		}
		return seen
	}

	const cooldown = 24 * time.Hour
	before := candidates(cooldown)
	for _, n := range names {
		if !before[ids[n]] {
			t.Fatalf("precondition: %s (repo %d) must be a candidate before any attempt", n, ids[n])
		}
	}

	// The empty answer: UpdateRepoMetadata writes the forge's empty values
	// back, exactly as the backfill does on success.
	if err := store.UpdateRepoMetadata(ctx, ids["empty"], "", "", nil, false, "", "", time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"empty", "gone"} {
		if err := store.MarkMetadataBackfillAttempted(ctx, ids[n]); err != nil {
			t.Fatalf("mark %s: %v", n, err)
		}
	}

	after := candidates(cooldown)
	if !after[ids["never"]] {
		t.Error("a never-asked repo must stay a candidate")
	}
	if after[ids["empty"]] {
		t.Error("a repo asked within the cooldown whose forge answer was empty is still a candidate — it is re-fetched at every serve start forever")
	}
	if after[ids["gone"]] {
		t.Error("a repo asked within the cooldown that 404'd is still a candidate — the same 404 is retried at every serve start forever")
	}

	// Once the cooldown has passed the repo is asked again: the stamp defers,
	// it never retires a repo for good.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET metadata_backfill_attempted_at = NOW() - make_interval(secs => $2) WHERE repo_id = $1`,
		ids["gone"], (cooldown + time.Hour).Seconds()); err != nil {
		t.Fatal(err)
	}
	if !candidates(cooldown)[ids["gone"]] {
		t.Error("a repo last asked longer ago than the cooldown must be a candidate again")
	}
	// The cooldown is the caller's: a shorter one re-admits the fresher stamp.
	if !candidates(time.Nanosecond)[ids["empty"]] {
		t.Error("the cooldown argument must drive the filter (a near-zero cooldown re-admits a just-stamped repo)")
	}
}

// The stamp must land on the row, and only on that row.
func TestMarkMetadataBackfillAttemptedStampsOneRow(t *testing.T) {
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
	for _, x := range []struct {
		n  string
		id *int64
	}{{"a", &a}, {"b", &b}} {
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, '_avmetastamp', 2)
			ON CONFLICT (repo_git) DO UPDATE SET metadata_backfill_attempted_at = NULL
			RETURNING repo_id`, "https://gitlab.com/_avmetastamp/"+x.n, x.n).Scan(x.id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, a, b)
	})

	if err := store.MarkMetadataBackfillAttempted(ctx, a); err != nil {
		t.Fatal(err)
	}
	var stampedA, stampedB bool
	if err := store.pool.QueryRow(ctx, `
		SELECT
		  COALESCE((SELECT metadata_backfill_attempted_at > NOW() - interval '1 minute' FROM aveloxis_data.repos WHERE repo_id = $1), FALSE),
		  (SELECT metadata_backfill_attempted_at IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $2)`,
		a, b).Scan(&stampedA, &stampedB); err != nil {
		t.Fatal(err)
	}
	if !stampedA {
		t.Error("MarkMetadataBackfillAttempted must stamp the repo's metadata_backfill_attempted_at with the current time")
	}
	if stampedB {
		t.Error("MarkMetadataBackfillAttempted stamped a repo it was not given")
	}
}

// Existing fleets no-op schema.sql's CREATE TABLE, so the column reaches them
// only through migrate.go's ALTER guard (SR-8); a fresh database gets it from
// schema.sql. Both halves, with comments stripped so prose cannot satisfy it.
func TestMetadataBackfillStampColumnDeclaredAndMigrated(t *testing.T) {
	if !strings.Contains(readSourceFile(t, "schema.sql"), "\n    metadata_backfill_attempted_at TIMESTAMPTZ,") {
		t.Error("schema.sql must declare repos.metadata_backfill_attempted_at TIMESTAMPTZ")
	}
	src := srctest.StripGoComments(readSourceFile(t, "migrate.go"))
	if !strings.Contains(src, `addColumnIfMissing(ctx, pg, logger, errs, "aveloxis_data.repos", "metadata_backfill_attempted_at", "TIMESTAMPTZ")`) {
		t.Error("migrate.go must addColumnIfMissing repos.metadata_backfill_attempted_at for existing fleets")
	}
}
