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

// Every command that follows a redirect judges its target before looking
// it up among the stored repositories (review rounds 3, 4 and 6 each found
// one more site that did not). The judgment is the store's
// (db.RedirectTargetNotARepository, or db.RedirectLeftTheRepository
// built on it, which mark-gone-repos uses); here, the wiring: the files that probe
// a redirect, the lookups in them, and the judgment ahead of each.
func TestRedirectTargetsAreJudgedBeforeTheLookup(t *testing.T) {
	probing := map[string]int{}
	for _, file := range strings.Split(srctest.PackageSource(t, "cmd/aveloxis", 30), "// file: ")[1:] {
		name, body, _ := strings.Cut(file, "\n")
		body = srctest.StripGoComments(body)
		if strings.Contains(body, "ResolveRedirectTarget(") {
			probing[name] = strings.Count(body, ".FindRepoByURL(")
		}
	}
	// Two commands probe (mark-gone-repos and reconcile-repos), each with
	// one lookup; the functions holding them are checked by name below.
	if len(probing) != 2 {
		t.Errorf("files probing a redirect: %v, want 2 (a new one judges its target with db.RedirectTargetNotARepository or db.RedirectLeftTheRepository before any lookup, and is counted here)", probing)
	}
	for name, n := range probing {
		if n != 1 {
			t.Errorf("%s: %d repository lookups by URL, want 1, behind the judgment", name, n)
		}
	}
	for _, site := range []struct{ dir, fn, judge, lookup string }{
		{"cmd/aveloxis", "func runMarkGoneRepos(", "if needsTargetLookup(c, atOwnURL, targetNotARepo) {", ".FindRepoByURL("},
		{"cmd/aveloxis", "func needsTargetLookup(", "!targetNotARepo", ""},
		{"cmd/aveloxis", "func renameWinner(", "if refusal := db.RedirectTargetNotARepository(finalURL); refusal != nil {", ".FindRepoByURL("},
		{"internal/collector", "func RunPrelim(", "if refusal := db.RedirectTargetNotARepository(finalURL); refusal != nil {", ".FindRepoByURL("},
		// The gone stamp's clearers: a 2xx from a page that is not the
		// repository's clears nothing (round 7).
		{"internal/collector", "func RunPrelim(", "if statusCode >= 200 && statusCode < 300 && db.RedirectLeftTheRepository(repo.GitURL, finalURL) == nil {", ".ClearRepoGone("},
		{"internal/scheduler", "func (s *Scheduler) runGoneRecheck(", "case status >= 200 && status < 300 && db.RedirectLeftTheRepository(c.GitURL, finalURL) != nil:", ".ResurrectRepo("},
	} {
		body := srctest.StripGoComments(srctest.PackageFunc(t, site.dir, site.fn))
		judge := strings.Index(body, site.judge)
		if judge < 0 {
			t.Errorf("%s lacks the judgment `%s`", site.fn, site.judge)
			continue
		}
		if site.lookup != "" {
			if first := strings.Index(body, site.lookup); first < judge {
				t.Errorf("%s: the first %s (offset %d) must follow the judgment (offset %d)", site.fn, site.lookup, first, judge)
			}
		}
	}
}
