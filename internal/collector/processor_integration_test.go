// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL end-to-end test for the staged-collection write path
// (v0.25.38, tech-debt Action 2). Processor.ProcessRepo — the busiest
// write path in the system, turning staged JSON into relational rows —
// previously had ZERO behavioral coverage: every reference was a
// source-contract substring pin, the exact v0.21.0 failure mode
// (source and test agreeing on a wrong answer). This test drives
// stage → flush → process → relational rows against a real Postgres.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProcessorEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := db.RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Independent raw connection for seeding cleanup + assertions — the
	// test verifies DB state without trusting the store under test.
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	const slug = "_avproc_e2e"
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.issue_labels WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_data.releases WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_ops.staging WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = '` + slug + `_reporter')`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = '` + slug + `_reporter'`,
		} {
			raw.Exec(ctx, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "/Repo",
		Owner:    slug, Name: "Repo",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stage exactly what the staged collector stages: an issue envelope
	// (with a label and a reporter needing contributor resolution) and a
	// release.
	sw := db.NewStagingWriter(store, repoID, int16(model.PlatformGitHub), logger)
	closedAt := time.Now().Add(-time.Hour)
	if err := sw.Stage(ctx, EntityIssue, stagedIssue{
		Issue: model.Issue{
			PlatformID: 991100221,
			Number:     7,
			Title:      "processor e2e issue",
			State:      "closed",
			ReporterRef: model.UserRef{
				PlatformID: 991100222,
				Login:      slug + "_reporter",
			},
			CreatedAt: time.Now().Add(-48 * time.Hour),
			UpdatedAt: time.Now(),
			ClosedAt:  &closedAt,
		},
		Labels: []model.IssueLabel{{Text: "bug", Color: "ff0000"}},
	}); err != nil {
		t.Fatalf("stage issue: %v", err)
	}
	if err := sw.Stage(ctx, EntityRelease, model.Release{
		ID:      slug + "-rel-1",
		Name:    "v1.0.0",
		TagName: "v1.0.0",
	}); err != nil {
		t.Fatalf("stage release: %v", err)
	}
	if err := sw.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The write path under test.
	proc := NewProcessor(store, logger)
	if err := proc.ProcessRepo(ctx, repoID, int16(model.PlatformGitHub)); err != nil {
		t.Fatalf("ProcessRepo: %v", err)
	}

	// Issue landed with the right shape.
	var title, state string
	var reporterID *string
	if err := raw.QueryRow(ctx, `
		SELECT issue_title, issue_state, reporter_id::text
		FROM aveloxis_data.issues WHERE repo_id = $1 AND platform_issue_id = 991100221`,
		repoID).Scan(&title, &state, &reporterID); err != nil {
		t.Fatalf("issue row missing after ProcessRepo: %v", err)
	}
	if title != "processor e2e issue" || state != "closed" {
		t.Errorf("issue row = (%q, %q), want staged values", title, state)
	}

	// The reporter was RESOLVED into a contributor row and linked.
	if reporterID == nil {
		t.Fatal("reporter_id must be resolved — the resolveUser path is part of the write path")
	}
	var login string
	if err := raw.QueryRow(ctx,
		`SELECT cntrb_login FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`,
		*reporterID).Scan(&login); err != nil {
		t.Fatalf("resolved contributor row missing: %v", err)
	}
	if login != slug+"_reporter" {
		t.Errorf("resolved contributor login = %q, want %q", login, slug+"_reporter")
	}

	// The bundled label landed against the issue's DB id.
	var labelCount int
	raw.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.issue_labels il
		JOIN aveloxis_data.issues i ON i.issue_id = il.issue_id
		WHERE i.repo_id = $1 AND il.label_text = 'bug'`, repoID).Scan(&labelCount)
	if labelCount != 1 {
		t.Errorf("bundled issue label must land via the parent's DB id, got %d rows", labelCount)
	}

	// The release landed.
	var relCount int
	raw.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.releases WHERE repo_id = $1`, repoID).Scan(&relCount)
	if relCount != 1 {
		t.Errorf("release row count = %d, want 1", relCount)
	}

	// Staging rows are marked processed (idempotent drain contract).
	var unprocessed int
	raw.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.staging WHERE repo_id = $1 AND NOT processed`,
		repoID).Scan(&unprocessed)
	if unprocessed != 0 {
		t.Errorf("%d staging rows left unprocessed after ProcessRepo", unprocessed)
	}

	// Re-processing is a no-op (ON CONFLICT idempotency): same counts.
	if err := proc.ProcessRepo(ctx, repoID, int16(model.PlatformGitHub)); err != nil {
		t.Fatalf("second ProcessRepo: %v", err)
	}
	var issueCount int
	raw.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&issueCount)
	if issueCount != 1 {
		t.Errorf("re-process must not duplicate rows: %d issues", issueCount)
	}
}
