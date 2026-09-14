// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 Phase C (API-key administration): the admin API stores tokens and
// lists/removes them by id, but never reads a token back — the list and the
// delete return key_id and key_mask computed in SQL, which must equal the
// Go platform.KeyID / platform.MaskToken the key report uses, or the page
// could not match a stored row to the key serve loaded.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func adminKeysStore(t *testing.T) (context.Context, *PostgresStore) {
	t.Helper()
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
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token LIKE '_avadm_%'`)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.forge_key_reports WHERE reporter LIKE '_avadm_%'`)
	}
	cleanup()
	t.Cleanup(cleanup)
	return ctx, store
}

func TestAdminAPIKeysInsertListDelete(t *testing.T) {
	ctx, store := adminKeysStore(t)
	const fd = "https://gitlab.freedesktop.invalid"

	id, err := store.InsertAPIKey(ctx, "fd key", "_avadm_fd_0123456789", "gitlab", fd)
	if err != nil || id <= 0 {
		t.Fatalf("InsertAPIKey = (%d, %v)", id, err)
	}
	// The same token again: refused, naming where it is stored — the admin
	// path never moves a key (remove + add).
	_, err = store.InsertAPIKey(ctx, "again", "_avadm_fd_0123456789", "gitlab", "https://other.invalid")
	var exists *APIKeyExistsError
	if !errors.As(err, &exists) || !errors.Is(err, ErrAPIKeyExists) || exists.InstanceURL != fd || exists.OAuthID != id {
		t.Fatalf("duplicate InsertAPIKey = %v, want ErrAPIKeyExists naming %s (oauth_id %d)", err, fd, id)
	}
	// The same token on the other platform is a different key.
	if _, err := store.InsertAPIKey(ctx, "gh", "_avadm_fd_0123456789", "github", ""); err != nil {
		t.Fatalf("same token, other platform: %v", err)
	}
	if _, err := store.InsertAPIKey(ctx, "short", "_avadm_s", "github", ""); err != nil {
		t.Fatal(err)
	}

	list, err := store.ListAdminAPIKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]AdminAPIKey{}
	for _, k := range list {
		got[k.Platform+"|"+k.KeyMask+"|"+k.InstanceURL] = k
	}
	for _, tc := range []struct{ platform, token, inst string }{
		{"gitlab", "_avadm_fd_0123456789", fd},
		{"github", "_avadm_fd_0123456789", ""},
		{"github", "_avadm_s", ""},
	} {
		k, ok := got[tc.platform+"|"+platform.MaskToken(tc.token)+"|"+tc.inst]
		if !ok {
			t.Errorf("list is missing %s %s under %q: %v", tc.platform, platform.MaskToken(tc.token), tc.inst, got)
			continue
		}
		if k.KeyID != platform.KeyID(tc.token) {
			t.Errorf("SQL key_id %q != platform.KeyID %q for %s", k.KeyID, platform.KeyID(tc.token), tc.token)
		}
		if k.CreatedAt.IsZero() || k.OAuthID <= 0 {
			t.Errorf("list row %+v lacks created_at or oauth_id", k)
		}
	}
	// No listed field may hold a token.
	body, _ := json.Marshal(list)
	if strings.Contains(string(body), "_avadm_fd_0123456789") || strings.Contains(string(body), `"_avadm_s"`) {
		t.Fatalf("the admin key list leaked a token: %s", body)
	}

	removed, err := store.DeleteAPIKey(ctx, id)
	if err != nil || removed.KeyID != platform.KeyID("_avadm_fd_0123456789") || removed.InstanceURL != fd || removed.Platform != "gitlab" {
		t.Fatalf("DeleteAPIKey = (%+v, %v)", removed, err)
	}
	if _, err := store.DeleteAPIKey(ctx, id); !errors.Is(err, ErrAPIKeyNotFound) {
		t.Fatalf("second DeleteAPIKey = %v, want ErrAPIKeyNotFound", err)
	}
}

func TestLoadAugurAPIKeysWithoutAugurSchema(t *testing.T) {
	ctx, store := adminKeysStore(t)
	var exists bool
	if err := store.Pool().QueryRow(ctx, `SELECT to_regclass('augur_operations.worker_oauth') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Skip("scratch DB has augur_operations.worker_oauth")
	}
	toks, err := LoadAugurAPIKeys(ctx, store.Pool(), "github")
	if err != nil || toks != nil {
		t.Fatalf("LoadAugurAPIKeys without an Augur schema = (%v, %v), want (nil, nil)", toks, err)
	}
}

// A reporter upserts its own row with the database clock, and rows older
// than a day from any OTHER reporter are dropped in the same statement.
func TestForgeKeyReportsUpsertAndAge(t *testing.T) {
	ctx, store := adminKeysStore(t)
	if err := store.SaveForgeKeyReport(ctx, "_avadm_old", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.forge_key_reports SET reported_at = now() - interval '25 hours' WHERE reporter = '_avadm_old'`); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveForgeKeyReport(ctx, "_avadm_me", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveForgeKeyReport(ctx, "_avadm_me", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadForgeKeyReports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]ForgeKeyReportRow{}
	for _, r := range rows {
		if strings.HasPrefix(r.Reporter, "_avadm_") {
			seen[r.Reporter] = r
		}
	}
	if _, ok := seen["_avadm_old"]; ok {
		t.Error("a report older than a day from another reporter survived the next save")
	}
	me, ok := seen["_avadm_me"]
	if !ok || !strings.Contains(string(me.Report), `"n": 2`) && !strings.Contains(string(me.Report), `"n":2`) {
		t.Fatalf("own report = %+v, want the latest upsert", me)
	}
	if me.AgeSeconds < 0 || me.AgeSeconds > 60 {
		t.Errorf("AgeSeconds = %v, want a fresh report judged by the database clock", me.AgeSeconds)
	}
}
