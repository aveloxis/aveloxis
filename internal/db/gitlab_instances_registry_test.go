// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): every GitLab instance has a platform_id
// registered on aveloxis_data.platforms under its web base URL
// (platform_instance_url). 2 is the historical instance — stamped once from
// the main instance's web base and never changed; other instances get
// 100–199 in first-seen order. Ids are never deleted or reused, config order
// does not matter, and an api_url change touches nothing here.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func registryConnect(t *testing.T) (context.Context, *PostgresStore) {
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
	return ctx, store
}

// isolateRegistry clears row 2's stamp and every test-owned instance row for
// the test, restoring row 2 afterwards. Test web bases use .invalid hosts.
func isolateRegistry(ctx context.Context, t *testing.T, store *PostgresStore) {
	t.Helper()
	var row2 *string
	if err := store.pool.QueryRow(ctx, `SELECT platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&row2); err != nil {
		t.Fatalf("platforms.platform_instance_url must exist after migrate: %v", err)
	}
	clear := func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND (platform_instance_url LIKE '%.invalid%' OR platform_name LIKE '_avsync%')`)
	}
	clear()
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`)
	t.Cleanup(func() {
		clear()
		cleanupExecRetry(context.Background(), store, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	})
}

func inst(base string, primary bool) GitLabInstanceRef {
	return GitLabInstanceRef{WebBase: base, Primary: primary}
}

func TestSyncGitLabInstancesStableIDs(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)

	main := "https://gitlab.main.invalid"
	a, b, c := "https://a.invalid", "https://code.b.invalid/gitlab", "https://c.invalid"

	first, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst(main, true), inst(a, false), inst(b, false)})
	if err != nil {
		t.Fatal(err)
	}
	if first[main] != model.PlatformGitLab {
		t.Errorf("main instance id = %d, want 2 (row 2 stamped from the main web base)", first[main])
	}
	if !first[a].IsGitLab() || first[a] < model.GitLabInstanceIDMin || !first[b].IsGitLab() || first[a] == first[b] {
		t.Fatalf("extra instances got ids %d and %d, want two distinct ids in [100, 199]", first[a], first[b])
	}

	// Reordered, one added, one removed: existing ids never move and are
	// never reused; the removed instance keeps its row.
	second, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst(main, true), inst(c, false), inst(b, false)})
	if err != nil {
		t.Fatal(err)
	}
	if second[b] != first[b] || second[main] != model.PlatformGitLab {
		t.Errorf("re-sync moved ids: b %d→%d, main %d", first[b], second[b], second[main])
	}
	if second[c] == first[a] || second[c] == first[b] || !second[c].IsGitLab() {
		t.Errorf("new instance c got id %d, which reuses or is outside the range (a=%d b=%d)", second[c], first[a], first[b])
	}
	reg, err := store.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reg[a] != first[a] {
		t.Errorf("an instance removed from config must keep its registry row (a: %d, want %d)", reg[a], first[a])
	}

	// Row 2's stamp is immutable: a different main web base becomes a new
	// instance id, row 2 keeps the historical instance.
	third, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://newmain.invalid", true)})
	if err != nil {
		t.Fatal(err)
	}
	if third["https://newmain.invalid"] == model.PlatformGitLab {
		t.Error("row 2 was re-stamped with a new main web base; it must keep the historical instance")
	}
	reg, _ = store.LoadGitLabInstanceRegistry(ctx)
	if reg[main] != model.PlatformGitLab {
		t.Errorf("row 2's web base changed: registry %v", reg)
	}
}

func TestSyncGitLabInstancesConcurrent(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	const n = 6
	var wg sync.WaitGroup
	results := make([]map[string]model.Platform, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = store.SyncGitLabInstances(ctx, []GitLabInstanceRef{
				inst("https://gitlab.main.invalid", true),
				inst(fmt.Sprintf("https://shard%d.invalid", i), false),
				inst("https://shared.invalid", false),
			})
		}(i)
	}
	wg.Wait()
	seen := map[model.Platform]string{}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("sync %d: %v", i, errs[i])
		}
		for base, id := range results[i] {
			if other, ok := seen[id]; ok && other != base {
				t.Errorf("id %d assigned to both %s and %s", id, other, base)
			}
			seen[id] = base
		}
	}
	if results[0]["https://shared.invalid"] != results[n-1]["https://shared.invalid"] {
		t.Error("concurrent syncs gave one web base two ids")
	}
}

func TestSyncGitLabInstancesRangeExhausted(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
		SELECT id, '_avsync fill ' || id FROM generate_series(100, 199) id
		ON CONFLICT DO NOTHING`)
	_, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true), inst("https://one-too-many.invalid", false)})
	if !errors.Is(err, ErrGitLabInstanceIDsExhausted) {
		t.Fatalf("sync with no free instance id = %v, want ErrGitLabInstanceIDsExhausted", err)
	}
}

// The database refuses an instance URL on a non-GitLab id.
func TestPlatformsInstanceURLCheck(t *testing.T) {
	ctx, store := registryConnect(t)
	_, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = 'https://x.invalid' WHERE platform_id = 1`)
	if err == nil {
		cleanupExecRetry(ctx, store, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 1`)
		t.Fatal("platforms must refuse an instance URL on platform 1 (CHECK)")
	}
}

// Review pass 2 of B1–B5 (finding 1): http:// and https:// of one web URL are
// one instance everywhere (config refuses both at once). An operator moving
// an instance to TLS keeps its platform_id, and the registry then stores the
// new spelling, so every exact-string lookup agrees again.
func TestSyncGitLabInstancesSchemeChangeKeepsTheID(t *testing.T) {
	ctx, store := registryConnect(t)
	isolateRegistry(ctx, t, store)
	first, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true), inst("http://tls-move.invalid", false)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true), inst("https://tls-move.invalid", false)})
	if err != nil {
		t.Fatal(err)
	}
	if second["https://tls-move.invalid"] != first["http://tls-move.invalid"] {
		t.Errorf("scheme change: id %d → %d, want the same instance id", first["http://tls-move.invalid"], second["https://tls-move.invalid"])
	}
	reg, err := store.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, stale := reg["http://tls-move.invalid"]; stale || reg["https://tls-move.invalid"] != first["http://tls-move.invalid"] {
		t.Errorf("registry after the scheme change = %v, want only the https spelling on the original id", reg)
	}
}

// The MAIN instance changing scheme keeps platform_id 2, its seeded name, and
// raises no "main instance is not the historical instance" warning (review
// pass 3, finding 6).
func TestSyncGitLabInstancesMainSchemeChangeKeepsPlatform2(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logBuf bytes.Buffer
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(&logBuf, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)
	isolateRegistry(ctx, t, store)

	if _, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("http://gitlab.main.invalid", true)}); err != nil {
		t.Fatal(err)
	}
	logBuf.Reset()
	ids, err := store.SyncGitLabInstances(ctx, []GitLabInstanceRef{inst("https://gitlab.main.invalid", true)})
	if err != nil {
		t.Fatal(err)
	}
	if ids["https://gitlab.main.invalid"] != model.PlatformGitLab {
		t.Errorf("main instance after its scheme change = platform_id %d, want 2", ids["https://gitlab.main.invalid"])
	}
	var name string
	var url *string
	if err := store.pool.QueryRow(ctx, `SELECT platform_name, platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&name, &url); err != nil {
		t.Fatal(err)
	}
	if name != "GitLab" || url == nil || *url != "https://gitlab.main.invalid" {
		t.Errorf("row 2 = (%q, %v), want (GitLab, https://gitlab.main.invalid)", name, url)
	}
	if strings.Contains(logBuf.String(), "not the historical instance") {
		t.Errorf("a scheme change of the main instance raised the historical-instance warning:\n%s", logBuf.String())
	}
}
