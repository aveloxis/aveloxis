// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// reconcile-repos looks a redirect target up among the stored repositories
// to find the row to heal a renamed duplicate into. A row an older release
// stored under a page that is not a repository's is not such a row: the
// target is judged first and no winner is answered, so the stranded row is
// re-enqueued as for any untracked target (0.29.146, review rounds 3-4). A
// repository's URL is looked up as before.
func TestRenameWinnerIgnoresARowStoredUnderANonRepositoryURL(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	suffix := time.Now().UnixNano()
	insert := func(url, owner, name string) int64 {
		t.Helper()
		var id int64
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 3) RETURNING repo_id`, url, owner, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return id
	}
	page := fmt.Sprintf("https://git.example.invalid/_avwinner-%d", suffix)
	poisoned := insert(page, "", "")
	realURL := fmt.Sprintf("https://git.example.invalid/_avwinner-%d/repo", suffix)
	realID := insert(realURL, fmt.Sprintf("_avwinner-%d", suffix), "repo")

	if id, err := renameWinner(ctx, store, logger, 1, page+"/"); err != nil || id != 0 {
		t.Errorf("renameWinner(a page stored as row %d) = (%d, %v), want no winner", poisoned, id, err)
	}
	if !strings.Contains(logs.String(), "not a repository's URL") {
		t.Errorf("the refused target is not logged: %s", logs.String())
	}
	if id, err := renameWinner(ctx, store, logger, 1, realURL); err != nil || id != realID {
		t.Errorf("renameWinner(a stored repository's URL) = (%d, %v), want %d", id, err, realID)
	}
	// The premise: without the judgment the lookup does find the page's row.
	if id, err := store.FindRepoByURL(ctx, page+"/"); err != nil || id != poisoned {
		t.Fatalf("premise: FindRepoByURL(page) = (%d, %v), want the stored row %d", id, err, poisoned)
	}
}

// The one judgment reconcile-repos and mark-gone-repos share: a redirect
// target that is not a repository's URL is not a rename. Credentials and
// an over-long target are other refusals, handled where they always were.
func TestRedirectIsNotARename(t *testing.T) {
	for target, want := range map[string]bool{
		"https://gitlab.com/users/sign_in":                  true, // the forge's sign-in page
		"https://github.com/login/oauth":                    true,
		"https://github.com/chaoss/":                        true, // an owner's page
		"https://git.example.invalid/onesegment":            true,
		"ftp://github.com/chaoss/augur":                     true,
		"https://github.com/chaoss/augur":                   false,
		"https://gitlab.com/group/sub/project":              false,
		"https://git.example.invalid/owner/repo.git":        false,
		"https://user:pw@github.com/chaoss/augur":           false, // credentials: refused by its own rule
		"https://github.com/o/" + strings.Repeat("x", 9000): false, // over-long: its own rule
	} {
		if got := redirectIsNotARename(target); got != want {
			t.Errorf("redirectIsNotARename(%.60q) = %v, want %v", target, got, want)
		}
	}
}

// mark-gone-repos judges a redirect target before it looks it up, as
// reconcile-repos and prelim do (review round 6: it was the third site,
// and reported an archived row as "renamed onto a tracked repository" when
// the target was a sign-in page an older release had stored).
func TestMarkGoneReposJudgesARedirectTargetBeforeTheLookup(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageSource(t, "cmd/aveloxis", 1))
	if n := strings.Count(src, "store.FindRepoByURL(ctx, finalURL)"); n != 2 {
		t.Fatalf("%d lookups of a redirect target in cmd/aveloxis, want 2 (renameWinner's and mark-gone-repos'): a new one must judge the target first", n)
	}
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runMarkGoneRepos("))
	judge, lookup := strings.Index(body, "redirectIsNotARename(finalURL)"), strings.Index(body, "store.FindRepoByURL(ctx, finalURL)")
	if judge < 0 || lookup < judge {
		t.Errorf("mark-gone-repos calls redirectIsNotARename(finalURL) before store.FindRepoByURL(ctx, finalURL) (offsets %d, %d)", judge, lookup)
	}
	winner := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func renameWinner("))
	judge, lookup = strings.Index(winner, "redirectIsNotARename(finalURL)"), strings.Index(winner, "store.FindRepoByURL(ctx, finalURL)")
	if judge < 0 || lookup < judge {
		t.Errorf("renameWinner calls redirectIsNotARename(finalURL) before the lookup (offsets %d, %d)", judge, lookup)
	}
}
