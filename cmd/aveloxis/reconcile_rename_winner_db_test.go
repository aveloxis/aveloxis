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
