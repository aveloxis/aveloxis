package db

// Tests for GetReposBatch. The method itself needs a live Postgres connection,
// so the integration-style tests skip unless AVELOXIS_TEST_DB is set. The
// source-contract tests pin the SQL shape and behavior that the monitor
// dashboard depends on (single query via ANY($1), map keyed by repo_id).

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

// TestGetReposBatchUsesArrayParam enforces that the batch lookup runs as a
// single query against the repos table with the ANY($1) idiom, not an
// in-loop SELECT. The monitor dashboard rebuild hinges on this — an
// accidental regression back to per-row lookups would reintroduce the
// N+1 slowdown this change is meant to fix.
func TestGetReposBatchUsesArrayParam(t *testing.T) {
	data, err := os.ReadFile("repos_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "func (s *PostgresStore) GetReposBatch") {
		t.Fatal("repos_batch.go must declare GetReposBatch on *PostgresStore")
	}

	idx := strings.Index(src, "func (s *PostgresStore) GetReposBatch")
	body := src[idx:]
	end := strings.Index(body, "\nfunc ")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "ANY($1)") {
		t.Error("GetReposBatch must use WHERE repo_id = ANY($1) for a single round-trip")
	}
	if !strings.Contains(body, "aveloxis_data.repos") {
		t.Error("GetReposBatch must query aveloxis_data.repos")
	}
	if !strings.Contains(body, "platform_id") {
		t.Error("GetReposBatch must select platform_id so callers can show Platform badge")
	}
	if !strings.Contains(body, "repo_owner") || !strings.Contains(body, "repo_name") || !strings.Contains(body, "repo_git") {
		t.Error("GetReposBatch must select repo_owner, repo_name, repo_git")
	}
}

// TestGetReposBatchEmptyInput guarantees the empty-input path returns a
// non-nil empty map and never hits the database. A nil map would force
// callers to nil-check; a DB call on empty input would waste a round-trip.
func TestGetReposBatchEmptyInput(t *testing.T) {
	// Uses a zero-value *PostgresStore; if the function touches s.pool
	// on the empty path it will nil-panic and the test fails loudly.
	s := &PostgresStore{}
	got, err := s.GetReposBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("nil input: err = %v, want nil", err)
	}
	if got == nil {
		t.Error("nil input: result map must be non-nil empty map, not nil")
	}
	if len(got) != 0 {
		t.Errorf("nil input: len = %d, want 0", len(got))
	}

	got2, err := s.GetReposBatch(context.Background(), []int64{})
	if err != nil {
		t.Fatalf("empty slice: err = %v, want nil", err)
	}
	if got2 == nil || len(got2) != 0 {
		t.Errorf("empty slice: got %v, want non-nil empty map", got2)
	}
}

// TestGetReposBatchIntegration is a live-DB round-trip covering the cases
// the monitor dashboard actually hits: one known ID, mix of known/unknown,
// and platform preservation across GitHub/GitLab/Generic. Skip unless
// AVELOXIS_TEST_DB is set to a scratch connection string.
func TestGetReposBatchIntegration(t *testing.T) {
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Insert three repos across all three platform IDs.
	tokens := []struct {
		owner, name, url string
		plat             model.Platform
	}{
		{"_avbatch", "a", "https://github.com/_avbatch/a", model.PlatformGitHub},
		{"_avbatch", "b", "https://gitlab.com/_avbatch/b", model.PlatformGitLab},
		{"_avbatch", "c", "https://gitsrv.example/_avbatch/c", model.PlatformGenericGit},
	}
	ids := make([]int64, 0, len(tokens))
	for _, tk := range tokens {
		id, err := store.UpsertRepo(ctx, &model.Repo{
			Owner: tk.owner, Name: tk.name, GitURL: tk.url, Platform: tk.plat,
		})
		if err != nil {
			t.Fatalf("insert %s: %v", tk.url, err)
		}
		ids = append(ids, id)
	}

	// Case 1: all known IDs returned.
	got, err := store.GetReposBatch(ctx, ids)
	if err != nil {
		t.Fatalf("GetReposBatch: %v", err)
	}
	if len(got) != len(ids) {
		t.Errorf("len = %d, want %d", len(got), len(ids))
	}
	for i, id := range ids {
		r, ok := got[id]
		if !ok {
			t.Errorf("id %d missing from result", id)
			continue
		}
		if r.Platform != tokens[i].plat {
			t.Errorf("id %d: platform = %v, want %v", id, r.Platform, tokens[i].plat)
		}
		if r.Owner != tokens[i].owner || r.Name != tokens[i].name {
			t.Errorf("id %d: owner/name = %s/%s, want %s/%s",
				id, r.Owner, r.Name, tokens[i].owner, tokens[i].name)
		}
	}

	// Case 2: mix of known + unknown. Unknowns must be absent, not error.
	mixed := append([]int64{}, ids...)
	mixed = append(mixed, -999999) // definitely doesn't exist
	got2, err := store.GetReposBatch(ctx, mixed)
	if err != nil {
		t.Fatalf("mixed input: %v", err)
	}
	if _, ok := got2[-999999]; ok {
		t.Error("unknown id must NOT appear in result map")
	}
	if len(got2) != len(ids) {
		t.Errorf("mixed: len = %d, want %d (only known ids)", len(got2), len(ids))
	}

	// Case 3: all-unknown IDs. Non-nil empty map, no error.
	got3, err := store.GetReposBatch(ctx, []int64{-1, -2, -3})
	if err != nil {
		t.Fatalf("unknown-only: %v", err)
	}
	if got3 == nil || len(got3) != 0 {
		t.Errorf("unknown-only: got %v, want non-nil empty map", got3)
	}
}
