// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// generate-showcase (growth plan phase 4) — source-contract pins +
// the AVELOXIS_TEST_DB end-to-end run.

func showcaseSrc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("generate_showcase.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGenerateShowcaseCommandRegistered(t *testing.T) {
	b, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "generateShowcaseCmd(&cfgPath)") {
		t.Error("generate-showcase must be registered in main.go's AddCommand block")
	}
}

func TestGenerateShowcaseFlagsAndShape(t *testing.T) {
	src := showcaseSrc(t)
	for _, needle := range []string{
		`Use:   "generate-showcase"`,
		`"out"`, `"base-url"`, `"gui-root"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate_showcase.go must contain %s", needle)
		}
	}
}

func TestGenerateShowcaseDoesNotMigrate(t *testing.T) {
	// Comment-stripped scan (the v0.21.5 contract pin — the comment
	// EXPLAINING the rule would otherwise false-match it).
	src := showcaseSrc(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("generate-showcase must NOT call store.Migrate (v0.21.5: only serve and migrate run migrations)")
	}
}

func TestGenerateShowcaseAnonymousAndAtomic(t *testing.T) {
	src := showcaseSrc(t)
	// Anonymous reads: both collection reads pass userID 0.
	for _, needle := range []string{
		"ListCollections(ctx, 0)",
		"GetCollectionRepos(ctx, c.CollectionID, 0,",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate-showcase must query anonymously (userID 0): missing %q", needle)
		}
	}
	// Atomic writes + prune: tmp+rename, and stale slugs removed.
	for _, needle := range []string{"writeAtomic(", ".tmp", "os.Rename(", "os.Remove("} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate-showcase must write atomically and prune stale pages: missing %q", needle)
		}
	}
}

// TestGenerateShowcaseEndToEnd (AVELOXIS_TEST_DB): seed a collection
// with a group + repos + cached queue counts, generate into a temp
// dir, and assert the pages exist with the right content and ZERO
// user-data leakage; then delete the collection and prove the prune.
func TestGenerateShowcaseEndToEnd(t *testing.T) {
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
	pool := store.Pool()

	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collections WHERE name LIKE '_avshow%'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE name LIKE '_avshow%')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE '_avshow%'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avshow')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avshow'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '_avshow%'`)
	}
	clean()
	t.Cleanup(clean)

	suffix := time.Now().UnixNano()
	var adminID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, TRUE) RETURNING user_id`,
		fmt.Sprintf("_avshow_admin_%d", suffix)).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	var gid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name) VALUES ($1, '_avshow_grp') RETURNING group_id`, adminID).Scan(&gid); err != nil {
		t.Fatal(err)
	}
	mkRepo := func(name string, commits int64) int64 {
		var rid int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, '_avshow', $2, 1) RETURNING repo_id`,
			"https://github.com/_avshow/"+name, name).Scan(&rid); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_issues, last_prs, last_commits, last_collected)
			VALUES ($1, 'queued', 11, 7, $2, NOW())`, rid, commits); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, gid, rid); err != nil {
			t.Fatal(err)
		}
		return rid
	}
	mkRepo("alpha", 500)
	mkRepo("beta", 900)

	collID, err := store.CreateCollection(ctx, "_avshow Ecosystem <One>", "the showcase test set", 1, adminID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddGroupToCollection(ctx, collID, gid); err != nil {
		t.Fatal(err)
	}

	out := t.TempDir()
	sum, err := runGenerateShowcase(ctx, store, logger, showcaseOpts{
		OutDir: out, BaseURL: "https://aveloxis.io", GUIRoot: out, RepoCap: 100, PageSize: 100,
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if sum.Collections < 1 || !sum.Sitemap {
		t.Fatalf("summary: %+v", sum)
	}

	slug := "avshow-ecosystem-one"
	page, err := os.ReadFile(filepath.Join(out, slug+".html"))
	if err != nil {
		// Slug collision suffixing or other collections in the scratch
		// DB may shift the exact name — find it by content.
		entries, _ := os.ReadDir(out)
		t.Fatalf("expected %s.html (dir has %d entries): %v", slug, len(entries), err)
	}
	html := string(page)
	for _, needle := range []string{"_avshow Ecosystem", "beta", "900", "canonical", "showcase-login-cta"} {
		if !strings.Contains(html, needle) {
			t.Errorf("collection page missing %q", needle)
		}
	}
	// beta (900 commits) sorts before alpha (500) — commits desc.
	if strings.Index(html, "beta") > strings.Index(html, ">alpha<") && strings.Contains(html, ">alpha<") {
		t.Error("repos must order by commits desc (beta before alpha)")
	}
	// ZERO user-data leakage.
	lower := strings.ToLower(html)
	for _, banned := range []string{"_avshow_admin", "_avshow_grp", "login_name", "starred"} {
		if strings.Contains(lower, banned) {
			t.Errorf("public page leaked %q", banned)
		}
	}
	// Sitemap exists and carries the page.
	sm, err := os.ReadFile(filepath.Join(out, "sitemap.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sm), "/showcase/"+slug+".html") {
		t.Error("sitemap must include the generated showcase page")
	}

	// Prune: delete the collection, regenerate, page disappears.
	if err := store.DeleteCollection(ctx, collID); err != nil {
		t.Fatal(err)
	}
	sum2, err := runGenerateShowcase(ctx, store, logger, showcaseOpts{
		OutDir: out, BaseURL: "https://aveloxis.io", GUIRoot: out, RepoCap: 100, PageSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, slug+".html")); !os.IsNotExist(err) {
		t.Errorf("deleted collection's page must be pruned (pruned=%d)", sum2.Pruned)
	}
}
