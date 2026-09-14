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

	byInst, err := LoadAPIKeysByInstance(ctx, store.Pool(), "gitlab", false)
	if err != nil {
		t.Fatal(err)
	}
	onlyTest := func(m map[string][]string) map[string][]string {
		out := map[string][]string{}
		for inst, toks := range m {
			for _, tok := range toks {
				if len(tok) > 7 && tok[:7] == "_avkey_" {
					out[inst] = append(out[inst], tok)
				}
			}
		}
		return out
	}
	want := map[string][]string{"": {"_avkey_main"}, fd: {"_avkey_fd"}}
	if got := onlyTest(byInst); !reflect.DeepEqual(got, want) {
		t.Errorf("LoadAPIKeysByInstance(gitlab) = %v, want %v (github keys never included)", got, want)
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
