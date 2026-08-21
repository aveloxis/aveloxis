// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration tests for the v0.25.32 case-variant duplicate-repo
// prevention layer. Gated on AVELOXIS_TEST_DB (scratch DB only — see
// docs/guide/ci-cd.md for the local recipe). Covers:
//
//   - UpsertRepo resolving a case-variant URL to the existing row
//     (same repo_id, one row per LOWER(repo_git) key).
//   - FindRepoByURL preferring a byte-exact match on a pre-dedup DB
//     that holds both variants, and falling back deterministically.
//   - Generic git (platform 3) staying byte-exact on purpose.
//   - HealRepoCaseDrift correcting case drift and refusing to collide
//     with an existing occupant.
//   - The cross-user sharing contract: a second group adding a case
//     variant of an already-tracked repo links the SHARED repo_id.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func caseConnect(t *testing.T) (context.Context, *PostgresStore) {
	t.Helper()
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
	if store.GetSchemaVersion(ctx) != ToolVersion {
		if err := RunMigrations(ctx, store, logger); err != nil {
			t.Fatalf("RunMigrations: %v", err)
		}
	}
	return ctx, store
}

// cleanupCaseRepos removes every test repo whose URL carries the given
// slug, including queue/user_repos leftovers from prior runs.
func cleanupCaseRepos(ctx context.Context, t *testing.T, store *PostgresStore, slug string) {
	t.Helper()
	like := "%" + slug + "%"
	for _, sql := range []string{
		`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`,
	} {
		if _, err := store.pool.Exec(ctx, sql, like); err != nil {
			t.Logf("cleanup (non-fatal): %v", err)
		}
	}
}

func TestUpsertRepoCaseVariantResolvesToSameRow(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_upsert"
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })

	id1, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "_Org/Repo",
		Owner:    slug + "_Org", Name: "Repo",
	})
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	id2, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + strings.ToLower(slug+"_Org/Repo"),
		Owner:    strings.ToLower(slug + "_Org"), Name: "repo",
	})
	if err != nil {
		t.Fatalf("case-variant upsert: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("case-variant upsert created a second row: %d vs %d — the "+
			"resolveCaseVariantURL substitution is not firing", id1, id2)
	}

	var count int
	if err := store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repos WHERE LOWER(repo_git) = LOWER($1)`,
		"https://github.com/"+slug+"_Org/Repo").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 repos row for the LOWER key, got %d", count)
	}
}

func TestFindRepoByURLCaseFallbackAndExactPreference(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_find"
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })

	gid := defaultRepoGroup(ctx, t, store)
	dropCaseUniqueIndex(ctx, t, store)

	// Seed BOTH variants directly (bypassing UpsertRepo's resolution) —
	// exactly the pre-dedup production state.
	var upperID, lowerID int64
	upperURL := "https://github.com/" + slug + "_Org/Repo"
	lowerURL := strings.ToLower(upperURL)
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'Repo', $3) RETURNING repo_id`, gid, upperURL, slug+"_Org").Scan(&upperID); err != nil {
		t.Fatalf("seed upper: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'repo', $3) RETURNING repo_id`, gid, lowerURL, strings.ToLower(slug+"_Org")).Scan(&lowerID); err != nil {
		t.Fatalf("seed lower: %v", err)
	}

	// Byte-exact match wins when both variants exist.
	if got, err := store.FindRepoByURL(ctx, lowerURL); err != nil || got != lowerID {
		t.Fatalf("exact lower lookup = (%d, %v), want %d", got, err, lowerID)
	}
	if got, err := store.FindRepoByURL(ctx, upperURL); err != nil || got != upperID {
		t.Fatalf("exact upper lookup = (%d, %v), want %d", got, err, upperID)
	}

	// A THIRD casing matches case-insensitively with the deterministic
	// repo_id tiebreak (oldest row).
	mixed := "https://github.com/" + strings.ToUpper(slug) + "_ORG/REPO"
	if got, err := store.FindRepoByURL(ctx, mixed); err != nil || got != upperID {
		t.Fatalf("mixed-case lookup = (%d, %v), want oldest row %d", got, err, upperID)
	}
}

func TestGenericGitStaysByteExact(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_generic"
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })

	url := "https://selfhosted.example.org/" + slug + "_Team/Repo"
	id1, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGenericGit, GitURL: url,
		Owner: slug + "_Team", Name: "Repo",
	})
	if err != nil {
		t.Fatalf("generic upsert: %v", err)
	}
	id2, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGenericGit, GitURL: strings.ToLower(url),
		Owner: strings.ToLower(slug + "_Team"), Name: "repo",
	})
	if err != nil {
		t.Fatalf("generic case-variant upsert: %v", err)
	}
	if id1 == id2 {
		t.Fatal("generic-git case variants must stay DISTINCT rows — unknown hosts " +
			"may legitimately be case-sensitive (platform gate broke)")
	}
	if got, _ := store.FindRepoByURL(ctx, strings.ToUpper(url)); got != 0 {
		t.Fatalf("generic-git lookup must be byte-exact; case-variant lookup returned %d", got)
	}
}

func TestHealRepoCaseDriftEndToEnd(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_heal"
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })

	// Happy path: stored casing differs from canonical only by case.
	id, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "_wrong/CaSe",
		Owner:    slug + "_wrong", Name: "CaSe",
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical := slug + "_Wrong/Case"
	healed, err := store.HealRepoCaseDrift(ctx, id, canonical)
	if err != nil {
		t.Fatalf("heal: %v", err)
	}
	if !healed {
		t.Fatal("expected healed=true for a case-only drift")
	}
	var gitURL, owner, name string
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_git, repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_id = $1`,
		id).Scan(&gitURL, &owner, &name); err != nil {
		t.Fatal(err)
	}
	if gitURL != "https://github.com/"+canonical || owner != slug+"_Wrong" || name != "Case" {
		t.Fatalf("heal wrote (%q, %q, %q), want canonical spelling", gitURL, owner, name)
	}

	// Real rename (more than case) is refused.
	if healed, err := store.HealRepoCaseDrift(ctx, id, slug+"_Wrong/renamed-project"); err != nil || healed {
		t.Fatalf("real rename must be left to prelim: healed=%v err=%v", healed, err)
	}

	// Occupancy guard: another row already holds the canonical URL.
	// (Seeding a real duplicate pair requires the pre-dedup state.)
	dropCaseUniqueIndex(ctx, t, store)
	gid := defaultRepoGroup(ctx, t, store)
	var occupantID, driftedID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'Repo', $3) RETURNING repo_id`,
		gid, "https://github.com/"+slug+"_Occ/Repo", slug+"_Occ").Scan(&occupantID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'repo', $3) RETURNING repo_id`,
		gid, "https://github.com/"+strings.ToLower(slug+"_Occ/Repo"), strings.ToLower(slug+"_Occ")).Scan(&driftedID); err != nil {
		t.Fatal(err)
	}
	if healed, err := store.HealRepoCaseDrift(ctx, driftedID, slug+"_Occ/Repo"); err != nil || healed {
		t.Fatalf("heal into an occupied canonical URL must refuse (dedup-repos "+
			"territory): healed=%v err=%v", healed, err)
	}
}

// The cross-user sharing contract: when a second group adds a case
// variant of an already-tracked repo, the SHARED repo_id is linked into
// that group — no new repos row, no second queue row.
func TestAddRepoToGroupSharesRepoAcrossGroups(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avcase_share"
	cleanupCaseRepos(ctx, t, store, slug)
	cleanupSQL := func() {
		cleanupCaseRepos(ctx, t, store, slug)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name ILIKE $1`, "%"+slug+"%")
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, slug+"_user")
	}
	cleanupSQL()
	t.Cleanup(cleanupSQL)

	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, TRUE)
		RETURNING user_id`, slug+"_user").Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var groupA, groupB int64
	for name, dst := range map[string]*int64{slug + "_A": &groupA, slug + "_B": &groupB} {
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, 'approved')
			RETURNING group_id`, userID, name).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}

	canonicalURL := "https://github.com/" + slug + "_Org/Repo"
	if err := store.AddRepoToGroup(ctx, userID, groupA, canonicalURL); err != nil {
		t.Fatalf("group A add: %v", err)
	}
	// Second group adds the CASE VARIANT of the same repo.
	if err := store.AddRepoToGroup(ctx, userID, groupB, strings.ToLower(canonicalURL)); err != nil {
		t.Fatalf("group B case-variant add: %v", err)
	}

	var repoCount int
	if err := store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repos WHERE LOWER(repo_git) = LOWER($1)`,
		canonicalURL).Scan(&repoCount); err != nil {
		t.Fatal(err)
	}
	if repoCount != 1 {
		t.Fatalf("expected ONE shared repos row, got %d — the case variant created a duplicate", repoCount)
	}

	var linkedA, linkedB int64
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupA).Scan(&linkedA); err != nil {
		t.Fatalf("group A link: %v", err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_ops.user_repos WHERE group_id = $1`, groupB).Scan(&linkedB); err != nil {
		t.Fatalf("group B link: %v", err)
	}
	if linkedA != linkedB {
		t.Fatalf("groups link different repo_ids (%d vs %d) — data sharing broken", linkedA, linkedB)
	}

	var queueCount int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.collection_queue q
		JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
		WHERE LOWER(r.repo_git) = LOWER($1)`, canonicalURL).Scan(&queueCount); err != nil {
		t.Fatal(err)
	}
	if queueCount != 1 {
		t.Fatalf("expected exactly 1 queue row for the shared repo, got %d", queueCount)
	}
}

// dropCaseUniqueIndex simulates the pre-dedup fleet state: on a fleet
// that still HAS case-variant duplicates, uq_repos_repo_git_ci cannot
// exist yet (the migration skips it with a WARN until `aveloxis
// dedup-repos` drains). Tests that deliberately seed duplicate pairs
// must drop it first — on a clean scratch DB the previous RunMigrations
// created it, which is itself proof the backstop works. The next
// RunMigrations recreates it once the test's cleanup removes the pairs.
func dropCaseUniqueIndex(ctx context.Context, t *testing.T, store *PostgresStore) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, `DROP INDEX IF EXISTS aveloxis_data.uq_repos_repo_git_ci`); err != nil {
		t.Fatalf("drop uq_repos_repo_git_ci: %v", err)
	}
}

// defaultRepoGroup returns (creating if needed) the Default repo group
// used for direct-seed inserts.
func defaultRepoGroup(ctx context.Context, t *testing.T, store *PostgresStore) int64 {
	t.Helper()
	var gid int64
	err := store.pool.QueryRow(ctx,
		`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = 'Default'`).Scan(&gid)
	if err != nil {
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
			VALUES ('Default', 'Auto-created') RETURNING repo_group_id`).Scan(&gid); err != nil {
			t.Fatalf("create Default repo group: %v", err)
		}
	}
	return gid
}
