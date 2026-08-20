// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — rename/transfer duplicate prevention at add time.
//
// The 2026-08-19 production audit of `aveloxis reconcile-repos` output
// proved that ALL 12 "consolidate (data-bearing dup)" pairs were genuine
// upstream renames or transfers (dio/eaigw → dio/ai-gateway,
// 18F/api.data.gov → GSA/api.data.gov, imr-framework/pypulseq →
// pulseq/pypulseq, ...): a repo added individually under its OLD name
// was later re-discovered by an org scan under its NEW name. URL-based
// dedup (FindRepoByURL + uq_repos_repo_git_ci) structurally cannot
// catch these — the two URLs are genuinely different strings. The only
// identity that survives a rename is the forge's numeric repository ID,
// which the schema has carried since inception (repos.platform_repo_id)
// but which was 0% populated on production (0 of 142,480 rows) and had
// zero readers.
//
// The fix: capture the forge ID everywhere it is free (org-scan listing
// JSON already contains `id`; Phase 0 FetchRepoInfo backfills the rest
// of the fleet), and make UpsertRepo a forge-ID-aware choke point — when
// the caller supplies PlatformID and the URL is untracked, a forge-ID
// hit means RENAME: heal the existing row's URL via UpdateRepoURL
// instead of minting a duplicate.
package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// --- source-contract pins -------------------------------------------------

func TestUpsertRepoHasForgeIDRenameHeal(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")

	// The heal branch must be gated on the caller actually supplying a
	// forge ID — callers without one (individual URL adds) must behave
	// exactly as before.
	if !strings.Contains(body, `r.PlatformID != ""`) {
		t.Error("UpsertRepo must gate the rename-heal branch on r.PlatformID being non-empty")
	}
	if !strings.Contains(body, "FindRepoByPlatformRepoID(") {
		t.Error("UpsertRepo must look up by (platform_id, platform_repo_id) before creating a row for an untracked URL")
	}
	// v0.27.106 (PR #184 review): the heal upgraded from the singular
	// UpdateRepoURL to UpdateRepoURLs — the PLURAL also rewrites the
	// child tables' stored issue/PR/release URLs carrying the old path.
	if !strings.Contains(body, "UpdateRepoURLs(") {
		t.Error("UpsertRepo's rename-heal must route through UpdateRepoURLs (repo_git + owner + name + child-table URLs)")
	}
}

// REVISED in v0.27.117 (Copilot round 11, active — real): the original
// prefer-NONEMPTY-INCOMING shape let a non-empty incoming ID OVERWRITE
// a different stored one, silently destroying the delete-and-recreate
// mismatch signal SetPlatformRepoIDIfEmpty and Phase 0 surface. The
// forge ID never changes for a given repo, so the correct shape is
// FILL-EMPTY-ONLY: prefer the STORED value; an id-less re-upsert still
// can't wipe a captured ID, and the first observation still fills.
func TestUpsertRepoConflictFillsPlatformRepoIDEmptyOnly(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")
	re := regexp.MustCompile(`platform_repo_id\s*=\s*COALESCE\(NULLIF\(repos\.platform_repo_id,\s*''\),\s*EXCLUDED\.platform_repo_id\)`)
	if !re.MatchString(body) {
		t.Error("UpsertRepo ON CONFLICT must set platform_repo_id FILL-EMPTY-ONLY (COALESCE(NULLIF(repos....,''), EXCLUDED....)) — an incoming ID must never overwrite a different stored one")
	}
}

// INVERTED in v0.27.116 (Copilot round 10, suppressed #1 — real): the
// original pin REQUIRED a plain schema.sql declaration, which violates
// the v0.27.98 rule — newly-introduced indexes are MIGRATION-ONLY in
// their introducing release, because the base schema DDL runs BEFORE
// the CONCURRENTLY helper, so a live fleet's first upgrade would
// block-build the index and turn ensureForgeIDIndex into a no-op.
// ensureForgeIDIndex owns BOTH fresh installs and upgrades
// (CONCURRENTLY on an empty table is instant — the v0.27.98 precedent).
func TestPlatformRepoIDIndexIsMigrationOnly(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Comment-stripped: the rule comment at the old declaration site
	// legitimately names the index; only real DDL counts.
	stripped := regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(string(src), "")
	if strings.Contains(stripped, "idx_repos_platform_repo_id") {
		t.Error("idx_repos_platform_repo_id must NOT be declared in schema.sql — migration-only (v0.27.98 rule); ensureForgeIDIndex owns fresh installs too")
	}
}

func TestMigrationCreatesPlatformRepoIDIndexConcurrently(t *testing.T) {
	combined := ""
	for _, f := range []string{"migrate.go", "repo_forge_id.go"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		combined += string(b)
	}
	if !strings.Contains(combined, "idx_repos_platform_repo_id") {
		t.Error("migration must create idx_repos_platform_repo_id for existing fleets")
	}
	if !strings.Contains(combined, "execCreateIndexConcurrently") {
		t.Error("the forge-ID index must build via execCreateIndexConcurrently (house pattern)")
	}
}

func TestUpdateRepoMetadataBackfillsPlatformRepoID(t *testing.T) {
	src, err := os.ReadFile("repo_metadata.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "platformRepoID string") {
		t.Error("UpdateRepoMetadata must take a platformRepoID parameter so Phase 0 backfills the fleet")
	}
	// v0.27.117 (Copilot round 11, active — real): FILL-EMPTY-ONLY, the
	// same flip as UpsertRepo — a "backfill" that overwrites a different
	// stored ID erases the delete-and-recreate mismatch signal. The
	// RETURNING + comparison make Phase 0 the fleet-wide DETECTOR.
	re := regexp.MustCompile(`platform_repo_id\s*=\s*COALESCE\(NULLIF\(repos\.platform_repo_id,\s*''\),\s*\$\d+\)`)
	if !re.MatchString(s) {
		t.Error("UpdateRepoMetadata must write platform_repo_id FILL-EMPTY-ONLY (prefer the stored value)")
	}
	if !strings.Contains(s, "RETURNING COALESCE(platform_repo_id, '')") {
		t.Error("UpdateRepoMetadata must RETURN the stored forge ID so Phase 0 detects delete-and-recreate mismatches fleet-wide")
	}
	if !strings.Contains(s, "storedForgeID != platformRepoID") {
		t.Error("UpdateRepoMetadata must compare stored vs observed forge ID and log the mismatch at ERROR")
	}
}

// extractFuncBody returns the source from the function declaration to the
// next top-level `func ` keyword (good enough for needle pins).
func extractFuncBody(t *testing.T, src, decl string) string {
	t.Helper()
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("declaration not found: %s", decl)
	}
	rest := src[i+len(decl):]
	j := strings.Index(rest, "\nfunc ")
	if j < 0 {
		return decl + rest
	}
	return decl + rest[:j]
}

// --- live-DB end-to-end (gated on AVELOXIS_TEST_DB) ------------------------

// TestRenameDedupAtAddTime reproduces the exact 2026-08-19 incident shape:
// a repo tracked under its old URL gets re-discovered by an org scan under
// its new URL with the same forge numeric ID. Pre-v0.27.102 this minted a
// duplicate row; post-fix the existing row's URL heals and no dup appears.
func TestRenameDedupAtAddTime(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const forgeID = "990017731"
	oldURL := "https://github.com/avrenametest/old-name-xyz"
	newURL := "https://github.com/avrenametest/new-name-xyz"
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = 'avrenametest')`)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = 'avrenametest')`)
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = 'avrenametest'`)
	})

	// 1. The individually-added era: repo enters under its old name WITH
	//    its forge ID captured (post-fix scans + Phase 0 both populate it).
	oldID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: oldURL,
		Owner: "avrenametest", Name: "old-name-xyz", PlatformID: forgeID,
	})
	if err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
	var storedForge string
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, oldID).Scan(&storedForge); err != nil {
		t.Fatal(err)
	}
	if storedForge != forgeID {
		t.Fatalf("platform_repo_id not stored on insert: got %q want %q", storedForge, forgeID)
	}

	// 1b. A child row carrying the OLD owner/repo path in its stored
	//     URL — the v0.27.111 transactional heal must rewrite it
	//     alongside repo_git (the round-6 active finding: child
	//     rewrites must never silently fail while repo_git changes).
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, html_url)
		VALUES ($1, 991701, 1, 'rename child', 'https://github.com/avrenametest/old-name-xyz/issues/1')`,
		oldID); err != nil {
		t.Fatal(err)
	}

	// 2. The rename: upstream renamed the repo; an org scan enumerates it
	//    under the NEW URL with the SAME forge ID.
	healedID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: newURL,
		Owner: "avrenametest", Name: "new-name-xyz", PlatformID: forgeID,
	})
	if err != nil {
		t.Fatalf("rename-era upsert: %v", err)
	}
	if healedID != oldID {
		t.Fatalf("rename minted a duplicate row: old=%d new=%d", oldID, healedID)
	}
	var gotURL, gotName string
	if err := store.pool.QueryRow(ctx, `SELECT repo_git, repo_name FROM aveloxis_data.repos WHERE repo_id = $1`, oldID).Scan(&gotURL, &gotName); err != nil {
		t.Fatal(err)
	}
	if gotURL != newURL {
		t.Errorf("repo_git not healed to the new URL: got %q want %q", gotURL, newURL)
	}
	if gotName != "new-name-xyz" {
		t.Errorf("repo_name not healed: got %q", gotName)
	}
	var childURL string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(html_url,'') FROM aveloxis_data.issues WHERE repo_id=$1 AND platform_issue_id=991701`, oldID).Scan(&childURL); err != nil {
		t.Fatal(err)
	}
	if childURL != "https://github.com/avrenametest/new-name-xyz/issues/1" {
		t.Errorf("child issue URL not rewritten by the transactional heal: got %q", childURL)
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_owner = 'avrenametest'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after rename re-discovery, got %d", count)
	}

	// 3. Idempotency: a second scan pass under the new URL returns the
	//    same row via the ordinary ON CONFLICT (repo_git) path.
	againID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: newURL,
		Owner: "avrenametest", Name: "new-name-xyz", PlatformID: forgeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if againID != oldID {
		t.Fatalf("re-scan returned a different row: %d vs %d", againID, oldID)
	}

	// 4. An id-less re-upsert (individual URL add) must NOT wipe the
	//    captured forge ID (prefer-nonempty contract).
	if _, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: newURL,
		Owner: "avrenametest", Name: "new-name-xyz",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, oldID).Scan(&storedForge); err != nil {
		t.Fatal(err)
	}
	if storedForge != forgeID {
		t.Fatalf("id-less re-upsert wiped platform_repo_id: got %q", storedForge)
	}

	// 5. SetPlatformRepoIDIfEmpty fills only empty — never overwrites.
	if err := store.SetPlatformRepoIDIfEmpty(ctx, oldID, "111"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, oldID).Scan(&storedForge); err != nil {
		t.Fatal(err)
	}
	if storedForge != forgeID {
		t.Fatalf("SetPlatformRepoIDIfEmpty overwrote a populated value: got %q", storedForge)
	}

	// 6. A different forge ID under an untracked URL creates a NEW row —
	//    the heal must never glue two genuinely different repos together.
	otherID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/avrenametest/unrelated-abc",
		Owner:    "avrenametest", Name: "unrelated-abc", PlatformID: "990017732",
	})
	if err != nil {
		t.Fatal(err)
	}
	if otherID == oldID {
		t.Fatal("distinct forge IDs must create distinct rows")
	}
}
