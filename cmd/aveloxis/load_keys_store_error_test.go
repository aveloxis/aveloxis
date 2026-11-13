// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestLoadKeysReturnsAStoreFailure pins the caller half of worklist item 55:
// loadKeys logged a LoadAPIKeys error and went on, so a failed key read
// ended as "no API keys configured" — sending the operator to add-key
// instead of to the database. Every caller is a collection or a one-shot
// that cannot run without keys, so the read's failure is the command's.
func TestLoadKeysReturnsAStoreFailure(t *testing.T) {
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
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = loadKeys(canceled, &config.Config{}, store, false, logger)
	if err == nil || strings.Contains(err.Error(), "no API keys configured") {
		t.Errorf("a failed key read = %v; want the store's failure, not \"no API keys configured\"", err)
	}
}

func TestKeyRefs(t *testing.T) {
	rows := []db.APIKeyRow{
		{ID: 4, Name: "ops", Token: "ghp_db", Table: "aveloxis_ops.worker_oauth"},
		{ID: 5, Name: "both", Token: "ghp_both", Table: "aveloxis_ops.worker_oauth"},
	}
	refs := keyRefs("github", []string{"ghp_cfg", "ghp_both"}, rows)
	want := map[string]string{
		"ghp_cfg":  "config file github.api_keys[0]",
		"ghp_db":   `aveloxis_ops.worker_oauth oauth_id=4 name="ops"`,
		"ghp_both": `config file github.api_keys[1]; aveloxis_ops.worker_oauth oauth_id=5 name="both"`,
	}
	if len(refs) != len(want) {
		t.Fatalf("refs = %v", refs)
	}
	for token, w := range want {
		if refs[token] != w {
			t.Errorf("ref of %s = %q, want %q", token, refs[token], w)
		}
		if strings.Contains(refs[token], token) {
			t.Errorf("the reference carries the token: %q", refs[token])
		}
	}
	if got := keyTokens(rows); len(got) != 2 || got[0] != "ghp_db" || got[1] != "ghp_both" {
		t.Errorf("keyTokens = %v", got)
	}
}

// End to end: a key from the config file and a key from the database reach
// the pool with their references, which is what the pool's log lines print.
func TestLoadKeysGivesThePoolEachKeysReference(t *testing.T) {
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
	dbToken := fmt.Sprintf("ghp_reftest%d", time.Now().UnixNano())
	var id int64
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform)
		VALUES ('ref test', $1, 'github') RETURNING oauth_id`, dbToken).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.worker_oauth WHERE oauth_id = $1`, id)
	})
	cfg := &config.Config{}
	cfg.GitHub.APIKeys = []string{"ghp_fromconfig"}
	gh, _, err := loadKeys(ctx, cfg, store, false, logger)
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := gh.Snapshot()
	refs := map[string]string{}
	for _, k := range snap {
		refs[k.Hash] = k.Ref
	}
	if got := refs[platform.TokenHash("ghp_fromconfig")]; got != "config file github.api_keys[0]" {
		t.Errorf("the config key's reference in the pool = %q", got)
	}
	if got, want := refs[platform.TokenHash(dbToken)], fmt.Sprintf(`aveloxis_ops.worker_oauth oauth_id=%d name="ref test"`, id); got != want {
		t.Errorf("the database key's reference in the pool = %q, want %q", got, want)
	}
}
