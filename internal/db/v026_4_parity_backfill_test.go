// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.26.4 backfill tests for the two SQL-derivable GraphQL parity
// columns. Incremental cycles are since-filtered, so the forward fix
// alone would leave every historical row dark forever (operator
// correction, 2026-07-09: "that would only happen if I did a full
// recollect, and that is not the default").

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestV0264ParityBackfillMigrationsRegistered(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, needle := range []string{
		"v0.26.4 backfill pull_requests.pr_diff_url from pr_html_url",
		"pr_diff_url = pr_html_url || '.diff'",
		"COALESCE(pr_diff_url, '') = ''", // idempotency guard
		"v0.26.4 backfill pull_request_meta.meta_label from pull_request_repo",
		"split_part(r.pr_repo_full_name, '/', 1) || ':' || m.meta_ref",
		"r.pr_repo_meta_id = m.pr_meta_id",
		"r.pr_repo_head_or_base = m.head_or_base",
		"COALESCE(m.meta_label, '') = ''", // idempotency guard
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("migrate.go must contain %q — the v0.26.4 backfill for "+
				"historical rows the since-filtered incremental cycles never revisit", needle)
		}
	}
}

func TestV0264ParityBackfillEndToEnd(t *testing.T) {
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
	// t.Cleanup, not defer: function defers run BEFORE t.Cleanup
	// callbacks, and the seed-row cleanup below needs the pool alive.
	// LIFO ordering makes this close run AFTER the deletes.
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}

	// Seed a repo + a PR with a dark diff_url + a meta/repo pair with a
	// dark label — the exact shape GraphQL collection produced before
	// v0.26.4.
	var repoID int64
	err = store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/aveloxis-it/parity-backfill', 'aveloxis-it', 'parity-backfill', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_owner = EXCLUDED.repo_owner
		RETURNING repo_id`).Scan(&repoID)
	if err != nil {
		t.Fatal(err)
	}
	var prID int64
	err = store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, pr_html_url, pr_diff_url)
		VALUES ($1, 990001, 42, 'https://github.com/aveloxis-it/parity-backfill/pull/42', '')
		RETURNING pull_request_id`, repoID).Scan(&prID)
	if err != nil {
		t.Fatal(err)
	}
	var metaID int64
	err = store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_request_meta (pull_request_id, repo_id, head_or_base, meta_label, meta_ref, meta_sha)
		VALUES ($1, $2, 'head', '', 'feature-branch', 'abc123')
		RETURNING pr_meta_id`, prID, repoID).Scan(&metaID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `
		INSERT INTO aveloxis_data.pull_request_repo (pr_repo_meta_id, pr_repo_head_or_base, pr_repo_full_name)
		VALUES ($1, 'head', 'forkowner/parity-backfill')`, metaID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id = $1`, metaID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.pull_request_meta WHERE pr_meta_id = $1`, metaID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.pull_requests WHERE pull_request_id = $1`, prID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Run migrations again — the backfill steps must fill both columns.
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("backfill migrate: %v", err)
	}

	var diffURL, metaLabel string
	if err := store.Pool().QueryRow(ctx,
		`SELECT pr_diff_url FROM aveloxis_data.pull_requests WHERE pull_request_id = $1`, prID).Scan(&diffURL); err != nil {
		t.Fatal(err)
	}
	if want := "https://github.com/aveloxis-it/parity-backfill/pull/42.diff"; diffURL != want {
		t.Errorf("pr_diff_url backfill: got %q want %q", diffURL, want)
	}
	if err := store.Pool().QueryRow(ctx,
		`SELECT meta_label FROM aveloxis_data.pull_request_meta WHERE pr_meta_id = $1`, metaID).Scan(&metaLabel); err != nil {
		t.Fatal(err)
	}
	if want := "forkowner:feature-branch"; metaLabel != want {
		t.Errorf("meta_label backfill: got %q want %q (owner from the PAIRED "+
			"pull_request_repo row, not the base repo)", metaLabel, want)
	}
}
