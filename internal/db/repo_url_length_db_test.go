// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestRenameWritersRefuseAnOverLongURL — O9 (v0.29.71): UpsertRepo bounded
// repo_git at MaxAddURLBytes, but the two rename writers did not. Both now
// refuse before writing (SR-18: the store owns the column's limits), and
// the row keeps its URL.
func TestRenameWritersRefuseAnOverLongURL(t *testing.T) {
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
	const orig = "https://github.com/_avurllen/repo"
	var id int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'repo', '_avurllen', 1) RETURNING repo_id`, orig).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	long := "https://github.com/_avurllen/" + strings.Repeat("y", MaxAddURLBytes)
	atLimit := "https://github.com/_avurllen/" + strings.Repeat("z", MaxAddURLBytes-len("https://github.com/_avurllen/"))
	for name, write := range map[string]func(string) error{
		"UpdateRepoURL":  func(u string) error { return store.UpdateRepoURL(ctx, id, u) },
		"UpdateRepoURLs": func(u string) error { return store.UpdateRepoURLs(ctx, id, orig, u) },
	} {
		if err := write(long); !errors.Is(err, ErrURLTooLong) {
			t.Errorf("%s(%d bytes) = %v, want ErrURLTooLong", name, len(long), err)
		}
		var stored string
		_ = store.pool.QueryRow(ctx, `SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored)
		if stored != orig {
			t.Errorf("%s wrote the over-long URL", name)
		}
		// The boundary is inclusive: exactly MaxAddURLBytes is stored.
		if err := write(atLimit); err != nil {
			t.Errorf("%s at exactly MaxAddURLBytes = %v, want stored", name, err)
		}
		if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET repo_git = $2 WHERE repo_id = $1`, id, orig); err != nil {
			t.Fatal(err)
		}
	}
}
