// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): a stored GitLab key belongs to exactly
// one instance, tagged with the instance's web base in
// worker_oauth.instance_url. '' is the main instance — what every existing
// row means, and what Augur-imported keys are. A token keeps one row
// (UNIQUE (access_token, platform)): re-adding it under another instance
// moves it and reports where it was.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
)

func TestWorkerOAuthInstanceRoundTrip(t *testing.T) {
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
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	cleanup := func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token LIKE '_avkey_%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	const fd = "https://gitlab.freedesktop.invalid"
	for _, k := range []struct{ name, token, plat, inst string }{
		{"main", "_avkey_main", "gitlab", ""},
		{"fd", "_avkey_fd", "gitlab", fd},
		{"gh", "_avkey_gh", "github", ""},
	} {
		prev, existed, err := SaveAPIKey(ctx, store.Pool(), k.name, k.token, k.plat, k.inst)
		if err != nil {
			t.Fatalf("SaveAPIKey(%s): %v", k.token, err)
		}
		if existed || prev != "" {
			t.Errorf("SaveAPIKey(%s) on a new token reported (%q, existed=%v)", k.token, prev, existed)
		}
	}

	stored, err := LoadStoredAPIKeys(ctx, store.Pool(), "gitlab")
	if err != nil {
		t.Fatal(err)
	}
	byInst := map[string][]string{}
	for _, k := range stored {
		if len(k.Token) > 7 && k.Token[:7] == "_avkey_" {
			byInst[k.InstanceURL] = append(byInst[k.InstanceURL], k.Token)
			if k.OAuthID <= 0 {
				t.Errorf("LoadStoredAPIKeys(%s) carried no oauth_id", k.Token)
			}
		}
	}
	want := map[string][]string{"": {"_avkey_main"}, fd: {"_avkey_fd"}}
	if !reflect.DeepEqual(byInst, want) {
		t.Errorf("LoadStoredAPIKeys(gitlab) by instance = %v, want %v (github keys never included)", byInst, want)
	}

	// Moving a token to another instance keeps one row and reports the old tag.
	prev, existed, err := SaveAPIKey(ctx, store.Pool(), "moved", "_avkey_main", "gitlab", fd)
	if err != nil {
		t.Fatal(err)
	}
	if !existed || prev != "" {
		t.Errorf("moving a main-instance key reported (%q, existed=%v), want (\"\", true) — the move off the main instance must be visible", prev, existed)
	}
	prev, existed, err = SaveAPIKey(ctx, store.Pool(), "moved-back", "_avkey_main", "gitlab", "")
	if err != nil {
		t.Fatal(err)
	}
	if !existed || prev != fd {
		t.Errorf("moving the key back reported (%q, existed=%v), want (%q, true)", prev, existed, fd)
	}
	var rows int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.worker_oauth WHERE access_token = '_avkey_main'`).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("a re-added token must keep one row, got %d (%v)", rows, err)
	}
}
