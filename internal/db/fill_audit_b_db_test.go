// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — Workstream B of the 2026-08-19 fill audit, db side:
// pull_request_repo.pr_cntrb_id (0 of 41.2M), pull_requests.meta_head_id/
// meta_base_id (100% dark, data local), repos.created_at (0%), plus the
// keyset backfills for the historical rows.
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

// --- source-contract pins -------------------------------------------------

func TestSetPRMetaLinksShape(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) SetPRMetaLinks(")
	// Fill-and-track with prefer-new + zero-guard: a zero id must leave
	// the column untouched; a real id may replace (meta row recreated
	// under a new PK).
	if !strings.Contains(body, "COALESCE(NULLIF($2::bigint, 0), meta_head_id)") ||
		!strings.Contains(body, "COALESCE(NULLIF($3::bigint, 0), meta_base_id)") {
		t.Error("SetPRMetaLinks must write meta ids via COALESCE(NULLIF($n::bigint,0), existing)")
	}
	if !strings.Contains(body, "IS DISTINCT FROM") {
		t.Error("SetPRMetaLinks needs the no-op guard so steady-state re-collection doesn't churn the row")
	}
}

func TestMigrationBackfillsPRMetaLinks(t *testing.T) {
	src, err := os.ReadFile("pr_meta_links.go")
	if err != nil {
		t.Fatalf("pr_meta_links.go must exist (the one-shot meta-link backfill): %v", err)
	}
	s := string(src)
	for _, needle := range []string{
		"meta_head_id IS NULL",
		"meta_base_id IS NULL",
		"m.head_or_base = 'head'",
		"m.head_or_base = 'base'",
		"runKeysetWindows",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("pr_meta_links.go missing %q (keyset-windowed, self-disabling backfill)", needle)
		}
	}
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "ensurePRMetaLinks(") {
		t.Error("RunMigrations must invoke ensurePRMetaLinks")
	}
}

func TestBackfillPRRepoOwnersShape(t *testing.T) {
	src, err := os.ReadFile("identity_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) BackfillPRRepoOwners(")
	for _, needle := range []string{
		"pr.pr_repo_id > $1 AND pr.pr_repo_id <= $2", // keyset windows (v0.26.6 lesson)
		// v0.27.123 (round 15): ambiguous owner logins stay NULL — the
		// sweep groups per target and keeps exactly-one-contributor groups
		// (the old unordered DISTINCT ON picked arbitrarily).
		"HAVING COUNT(DISTINCT c.cntrb_id) = 1",
		"COALESCE(c.cntrb_deleted, 0) = 0",
		"split_part(pr.pr_repo_full_name, '/', 1)",
		"pr.pr_cntrb_id IS NULL",
		"pr_repo_head_or_base", // the cheap meta-pair PK-join first pass
		// v0.27.110 (Copilot round 5): both passes GitHub-only — GitLab
		// rows heal forward-only (the login sweep + v0.26.5-era meta
		// attributions are platform-blind and could fabricate a GitHub
		// user onto a GitLab group row). Plus the GitLab-native
		// disqualifier on the bare-login fallback arm.
		"r.platform_id = 1",
		"COALESCE(c.gl_username, '') = ''",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("BackfillPRRepoOwners missing %q", needle)
		}
	}
}

func TestUpdateRepoMetadataWritesCreatedAt(t *testing.T) {
	src, err := os.ReadFile("repo_metadata.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// created_at is an immutable fact: fill-empty-only.
	if !strings.Contains(s, "created_at = COALESCE(repos.created_at,") {
		t.Error("UpdateRepoMetadata must write repos.created_at fill-empty-only")
	}
	if !strings.Contains(s, "updated_at = GREATEST(") {
		t.Error("UpdateRepoMetadata must refresh repos.updated_at from the forge's LastUpdated (nil-safe)")
	}
}

// --- live-DB end-to-end (gated on AVELOXIS_TEST_DB) ------------------------

func TestFillAuditBEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/avfillb/target-repo",
		Owner:    "avfillb", Name: "target-repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id IN (SELECT pr_meta_id FROM aveloxis_data.pull_request_meta WHERE repo_id=$1)`, repoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_request_meta WHERE repo_id=$1`, repoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id=$1`, repoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE 'avfillb%'`)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.repos WHERE repo_id=$1`, repoID)
	})

	prID, err := store.UpsertPullRequest(ctx, &model.PullRequest{
		RepoID: repoID, PlatformSrcID: 991001, Number: 1,
		Title: "b-wave pr", State: "open",
	})
	if err != nil {
		t.Fatal(err)
	}

	// --- meta links: forward path ---
	headID, err := store.UpsertPRMeta(ctx, &model.PullRequestMeta{
		PRID: prID, RepoID: repoID, HeadOrBase: "head", Ref: "feat", SHA: "aaa",
	})
	if err != nil {
		t.Fatal(err)
	}
	baseID, err := store.UpsertPRMeta(ctx, &model.PullRequestMeta{
		PRID: prID, RepoID: repoID, HeadOrBase: "base", Ref: "main", SHA: "bbb",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetPRMetaLinks(ctx, prID, headID, baseID); err != nil {
		t.Fatal(err)
	}
	var gotHead, gotBase int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(meta_head_id,0), COALESCE(meta_base_id,0) FROM aveloxis_data.pull_requests WHERE pull_request_id=$1`, prID).Scan(&gotHead, &gotBase); err != nil {
		t.Fatal(err)
	}
	if gotHead != headID || gotBase != baseID {
		t.Fatalf("meta links not stamped: got (%d,%d) want (%d,%d)", gotHead, gotBase, headID, baseID)
	}
	// zero ids leave values untouched
	if err := store.SetPRMetaLinks(ctx, prID, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(meta_head_id,0) FROM aveloxis_data.pull_requests WHERE pull_request_id=$1`, prID).Scan(&gotHead); err != nil {
		t.Fatal(err)
	}
	if gotHead != headID {
		t.Fatal("SetPRMetaLinks(0,0) must not clobber stamped ids")
	}

	// --- meta links: migration backfill on a row the forward path missed ---
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.pull_requests SET meta_head_id=NULL, meta_base_id=NULL WHERE pull_request_id=$1`, prID); err != nil {
		t.Fatal(err)
	}
	var errs []error
	ensurePRMetaLinks(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)), &errs)
	if len(errs) > 0 {
		t.Fatalf("ensurePRMetaLinks errors: %v", errs)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(meta_head_id,0), COALESCE(meta_base_id,0) FROM aveloxis_data.pull_requests WHERE pull_request_id=$1`, prID).Scan(&gotHead, &gotBase); err != nil {
		t.Fatal(err)
	}
	if gotHead != headID || gotBase != baseID {
		t.Fatalf("migration backfill missed: got (%d,%d) want (%d,%d)", gotHead, gotBase, headID, baseID)
	}

	// --- pr_cntrb_id: forward semantics + backfill ---
	// Contributor whose gh_login case-variant-matches the fork owner.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login)
		VALUES (gen_random_uuid(), 'avfillbowner', 'AvFillBOwner')
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	// Soft-deleted decoy with the same login spelling must never win.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_deleted)
		VALUES (gen_random_uuid(), 'avfillbowner-deleted', 'avfillbowner', 1)
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`); err != nil {
		t.Fatal(err)
	}

	// pr_repo rows: one with a matchable owner, one with empty full name
	// (deleted fork) that must stay NULL.
	if err := store.UpsertPRRepo(ctx, &model.PullRequestRepo{
		MetaID: headID, HeadOrBase: "head", SrcRepoID: 1,
		RepoName: "fork", RepoFullName: "avfillbowner/fork",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRRepo(ctx, &model.PullRequestRepo{
		MetaID: baseID, HeadOrBase: "base", SrcRepoID: 2,
		RepoName: "", RepoFullName: "",
	}); err != nil {
		t.Fatal(err)
	}

	// v0.27.123 (Copilot round 15, active): an AMBIGUOUS owner login —
	// two active contributors both LOWER-matching the owner segment —
	// must stay NULL. Pre-fix, unordered DISTINCT ON assigned one of
	// them arbitrarily.
	for _, row := range [][2]string{
		{"avfillbambig-a", "AvFillBAmbig"},
		{"avfillbambig-b", "avfillbambig"},
	} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login)
			VALUES (gen_random_uuid(), $1, $2)
			ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, row[0], row[1]); err != nil {
			t.Fatal(err)
		}
	}
	ambigPRID, err := store.UpsertPullRequest(ctx, &model.PullRequest{
		RepoID: repoID, PlatformSrcID: 991002, Number: 2,
		Title: "ambiguous-owner pr", State: "open",
	})
	if err != nil {
		t.Fatal(err)
	}
	ambigMetaID, err := store.UpsertPRMeta(ctx, &model.PullRequestMeta{
		PRID: ambigPRID, RepoID: repoID, HeadOrBase: "head", Ref: "amb", SHA: "ccc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRRepo(ctx, &model.PullRequestRepo{
		MetaID: ambigMetaID, HeadOrBase: "head", SrcRepoID: 3,
		RepoName: "fork", RepoFullName: "avfillbambig/fork",
	}); err != nil {
		t.Fatal(err)
	}

	// v0.27.123: execute BOTH rewritten dry-run compositions (they run
	// unwindowed SQL the real pass never exercises) — the unambiguous
	// fixture row must be counted while it is still NULL.
	dn, err := store.BackfillPRRepoOwners(ctx, 0, 0, true)
	if err != nil {
		t.Fatalf("pr_repo dry-run: %v", err)
	}
	if dn < 1 {
		t.Fatalf("pr_repo dry-run counted %d, want >= 1 (the unambiguous fixture row)", dn)
	}
	if _, err := store.BackfillPRMetaOwners(ctx, 0, 0, true); err != nil {
		t.Fatalf("pr_meta dry-run: %v", err)
	}

	n, err := store.BackfillPRRepoOwners(ctx, 1000000, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("backfill filled %d rows, want >= 1", n)
	}
	var cntrb *string
	var deleted int
	if err := store.pool.QueryRow(ctx, `
		SELECT pr.pr_cntrb_id::text, COALESCE(c.cntrb_deleted,0)
		FROM aveloxis_data.pull_request_repo pr
		JOIN aveloxis_data.contributors c ON c.cntrb_id = pr.pr_cntrb_id
		WHERE pr.pr_repo_meta_id=$1`, headID).Scan(&cntrb, &deleted); err != nil {
		t.Fatalf("head pr_repo row not filled: %v", err)
	}
	if deleted != 0 {
		t.Error("backfill picked the soft-deleted decoy")
	}
	if err := store.pool.QueryRow(ctx, `SELECT pr.pr_cntrb_id::text FROM aveloxis_data.pull_request_repo pr WHERE pr.pr_repo_meta_id=$1 AND pr.pr_cntrb_id IS NOT NULL`, baseID).Scan(&cntrb); err == nil {
		t.Error("deleted-fork row (empty full name) must stay NULL")
	}
	if err := store.pool.QueryRow(ctx, `SELECT pr.pr_cntrb_id::text FROM aveloxis_data.pull_request_repo pr WHERE pr.pr_repo_meta_id=$1 AND pr.pr_cntrb_id IS NOT NULL`, ambigMetaID).Scan(&cntrb); err == nil {
		t.Error("ambiguous owner login (two active contributors match) must stay NULL — the sweep may not pick arbitrarily")
	}
	// v0.27.110: a GITLAB-platform row with a perfectly matching owner
	// login must stay NULL — the backfill is GitHub-only (GitLab rows
	// heal forward via ID-qualified owner refs; the login table is
	// platform-blind and could hand a GitLab group a GitHub user).
	glRepoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitLab,
		GitURL:   "https://gitlab.com/avfillb/gl-target",
		Owner:    "avfillb", Name: "gl-target",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id IN (SELECT pr_meta_id FROM aveloxis_data.pull_request_meta WHERE repo_id=$1)`, glRepoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_request_meta WHERE repo_id=$1`, glRepoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id=$1`, glRepoID)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.repos WHERE repo_id=$1`, glRepoID)
	})
	glPRID, err := store.UpsertPullRequest(ctx, &model.PullRequest{
		RepoID: glRepoID, PlatformSrcID: 991003, Number: 1,
		Title: "gl pr", State: "open",
	})
	if err != nil {
		t.Fatal(err)
	}
	glMetaID, err := store.UpsertPRMeta(ctx, &model.PullRequestMeta{
		PRID: glPRID, RepoID: glRepoID, HeadOrBase: "head", Ref: "feat", SHA: "ccc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRRepo(ctx, &model.PullRequestRepo{
		MetaID: glMetaID, HeadOrBase: "head", SrcRepoID: 3,
		RepoName: "gl-fork", RepoFullName: "avfillbowner/gl-fork",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BackfillPRRepoOwners(ctx, 1000000, 0, false); err != nil {
		t.Fatal(err)
	}
	var glCntrb *string
	if err := store.pool.QueryRow(ctx, `SELECT pr_cntrb_id::text FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id=$1 AND pr_cntrb_id IS NOT NULL`, glMetaID).Scan(&glCntrb); err == nil {
		t.Error("GitLab-platform pr_repo row was backfilled by the GitHub-only login sweep — cross-platform fabrication")
	}

	// Idempotent: second run fills nothing new.
	n2, err := store.BackfillPRRepoOwners(ctx, 1000000, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("second backfill run must be a no-op, filled %d", n2)
	}
	// Forward prefer-new: a re-upsert with nil ContribID must not clobber.
	if err := store.UpsertPRRepo(ctx, &model.PullRequestRepo{
		MetaID: headID, HeadOrBase: "head", SrcRepoID: 1,
		RepoName: "fork", RepoFullName: "avfillbowner/fork",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT pr_cntrb_id::text FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id=$1 AND pr_cntrb_id IS NOT NULL`, headID).Scan(&cntrb); err != nil {
		t.Error("nil-ContribID re-upsert clobbered pr_cntrb_id")
	}

	// --- repos.created_at fill-empty-only via UpdateRepoMetadata ---
	created := time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := store.UpdateRepoMetadata(ctx, repoID, "d", "Go", nil, false, "", "", created, created); err != nil {
		t.Fatal(err)
	}
	var gotCreated time.Time
	if err := store.pool.QueryRow(ctx, `SELECT created_at FROM aveloxis_data.repos WHERE repo_id=$1`, repoID).Scan(&gotCreated); err != nil {
		t.Fatal(err)
	}
	if !gotCreated.Equal(created) {
		t.Fatalf("created_at not written: got %v", gotCreated)
	}
	// A later call with a different created_at must NOT overwrite.
	if err := store.UpdateRepoMetadata(ctx, repoID, "d", "Go", nil, false, "", "", created.AddDate(1, 0, 0), created); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT created_at FROM aveloxis_data.repos WHERE repo_id=$1`, repoID).Scan(&gotCreated); err != nil {
		t.Fatal(err)
	}
	if !gotCreated.Equal(created) {
		t.Fatal("created_at is immutable — fill-empty-only was violated")
	}
}
