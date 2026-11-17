// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLoadAPIKeysDistinguishesAMissingTableFromAFailure pins worklist item
// 55: LoadAPIKeys swallowed EVERY error from either key table, so a lost
// connection or a cancelled context read as "no keys configured" (and with
// --augur-keys a failed worker_oauth read silently returned the Augur keys).
// The swallow exists for one real case — the table (or its schema) does not
// exist yet, before migrate, or augur_operations absent — and only that case
// is "none"; every other failure is returned.
func TestLoadAPIKeysDistinguishesAMissingTableFromAFailure(t *testing.T) {
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

	// A missing table and a missing schema are "none".
	if keys, err := loadAPIKeysFrom(ctx, store.pool, "aveloxis_ops._av_no_such_table", "no_such_schema.worker_oauth", "github", true); err != nil || len(keys) != 0 {
		t.Errorf("missing tables = (%v, %v); want (none, nil)", keys, err)
	}
	// Anything else is the failure it is.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := LoadAPIKeys(canceled, store.pool, "github", false); err == nil {
		t.Error("a cancelled lookup = nil error; want the failure returned, not \"no keys\"")
	}
	if _, err := LoadAPIKeys(canceled, store.pool, "github", true); err == nil {
		t.Error("a cancelled lookup with --augur-keys = nil error; want the failure, not the Augur fallback")
	}
	// The real tables answer without error (possibly empty).
	if _, err := LoadAPIKeys(ctx, store.pool, "github", false); err != nil {
		t.Errorf("LoadAPIKeys on the migrated database: %v", err)
	}
}

// A loaded key carries its row: the log lines about a key name the table
// and oauth_id an operator deletes (0.29.146 — a token hash alone did not
// say which row was the dead one).
func TestLoadAPIKeysCarriesTheRow(t *testing.T) {
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

	token := fmt.Sprintf("glpat-rowtest-%d", time.Now().UnixNano())
	var id int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.worker_oauth (name, access_token, platform)
		VALUES ('row test', $1, 'gitlab') RETURNING oauth_id`, token).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.worker_oauth WHERE oauth_id = $1`, id)
	})
	rows, err := LoadAPIKeys(ctx, store.pool, "gitlab", false)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range rows {
		if r.Token != token {
			continue
		}
		found = true
		if r.ID != id || r.Name != "row test" || r.Table != "aveloxis_ops.worker_oauth" {
			t.Errorf("row = %+v; want oauth_id %d, name \"row test\", table aveloxis_ops.worker_oauth", APIKeyRow{ID: r.ID, Name: r.Name, Table: r.Table}, id)
		}
		if want := fmt.Sprintf(`aveloxis_ops.worker_oauth oauth_id=%d name="row test"`, id); r.Ref() != want {
			t.Errorf("Ref = %q, want %q", r.Ref(), want)
		}
	}
	if !found {
		t.Fatal("the inserted key was not loaded")
	}
}

func TestAPIKeyRowRef(t *testing.T) {
	for _, tc := range []struct {
		row  APIKeyRow
		want string
	}{
		{APIKeyRow{ID: 7, Name: "ops", Table: "aveloxis_ops.worker_oauth", Token: "ghp_secret"}, `aveloxis_ops.worker_oauth oauth_id=7 name="ops"`},
		{APIKeyRow{ID: 9, Table: "augur_operations.worker_oauth", Token: "ghp_secret"}, `augur_operations.worker_oauth oauth_id=9 name=""`},
		{APIKeyRow{ID: 3, Name: "a \"quoted\"\nname", Table: "aveloxis_ops.worker_oauth"}, `aveloxis_ops.worker_oauth oauth_id=3 name="a \"quoted\"\nname"`},
	} {
		got := tc.row.Ref()
		if got != tc.want {
			t.Errorf("Ref = %q, want %q", got, tc.want)
		}
		if tc.row.Token != "" && strings.Contains(got, tc.row.Token) {
			t.Errorf("Ref carries the token: %q", got)
		}
	}
}
