// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// tokenRecorder is a fake GitLab API that records the PRIVATE-TOKEN of every
// request and answers a minimal project.
type tokenRecorder struct {
	mu     sync.Mutex
	tokens []string
	srv    *httptest.Server
}

func newTokenRecorder(t *testing.T) *tokenRecorder {
	t.Helper()
	r := &tokenRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.tokens = append(r.tokens, req.Header.Get("PRIVATE-TOKEN"))
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(req.URL.Path, "/projects/") && !strings.Contains(strings.TrimPrefix(req.URL.EscapedPath(), "/api/v4/projects/"), "/") {
			_, _ = w.Write([]byte(`{"id": 7, "name": "project", "path_with_namespace": "group/project", "default_branch": "main"}`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *tokenRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.tokens...)
}

// End to end (SR-10), v0.30.0: config JSON → Load → a stored key tagged with
// --instance → buildForgeClients (registry sync, key partition, one client
// per instance) → UpsertRepo classification → routing → requests. Instance
// 2's API (B) is on a different host from its web URL (W), and the main
// instance's API (A) is a third host. Instance 2's requests carry only
// instance 2's keys (config and stored) and go only to B; A and W see
// nothing. With instance 2's keys removed, its repository is not collectable
// and still nothing reaches A.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestGitLabInstancesConfigToRouting(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	a, b, w := newTokenRecorder(t), newTokenRecorder(t), newTokenRecorder(t)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := db.RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	pool := store.Pool()
	var row2 *string
	if err := pool.QueryRow(ctx, `SELECT platform_instance_url FROM aveloxis_data.platforms WHERE platform_id = 2`).Scan(&row2); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token LIKE '_ave2e_%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_ave2e%')`)
		if _, err := pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_ave2e%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
		if _, err := pool.Exec(c, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND platform_instance_url LIKE 'http://127.0.0.1:%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
		_, _ = pool.Exec(c, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`); err != nil {
		t.Fatal(err)
	}

	writeConfig := func(instanceKeys string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		json := `{"gitlab": {"base_url": "` + a.srv.URL + `/api/v4", "api_keys": ["_ave2e_main"],
			"instances": [{"web_url": "` + w.srv.URL + `", "api_url": "` + b.srv.URL + `/api/v4", "api_keys": [` + instanceKeys + `]}]}}`
		if err := os.WriteFile(path, []byte(json), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}

	// Instance 2's second key is stored, tagged with its web URL.
	if _, _, err := db.SaveAPIKey(ctx, pool, "e2e", "_ave2e_instance_db", "gitlab", w.srv.URL); err != nil {
		t.Fatal(err)
	}

	cfg := writeConfig(`"_ave2e_instance_cfg"`)
	clients, err := buildForgeClients(ctx, cfg, store, false, true, logger)
	if err != nil {
		t.Fatal(err)
	}

	repoURL := w.srv.URL + "/_ave2e/project"
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: repoURL, Owner: "_ave2e", Name: "project"})
	if err != nil {
		t.Fatal(err)
	}
	repo, err := store.GetRepoByID(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := store.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if repo.Platform != ids[w.srv.URL] || !repo.Platform.IsGitLab() || repo.Platform == model.PlatformGitLab {
		t.Fatalf("repository under instance 2's web URL got platform_id %d, want instance 2's id %d", repo.Platform, ids[w.srv.URL])
	}

	client, err := clients.gl.ForRepo(repo.Platform, repo.GitURL)
	if err != nil {
		t.Fatalf("routing: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := client.FetchRepoInfo(ctx, repo.Owner, repo.Name); err != nil {
			t.Logf("FetchRepoInfo (fake API, partial answers are fine): %v", err)
		}
	}

	if got := a.seen(); len(got) != 0 {
		t.Errorf("the main instance's API received %d request(s) for instance 2's repository (tokens %q)", len(got), got)
	}
	if got := w.seen(); len(got) != 0 {
		t.Errorf("instance 2's WEB host received %d API request(s); its api_url is on another host", len(got))
	}
	bTokens := b.seen()
	if len(bTokens) == 0 {
		t.Fatal("instance 2's API received no requests")
	}
	used := map[string]bool{}
	for _, tok := range bTokens {
		if tok != "_ave2e_instance_cfg" && tok != "_ave2e_instance_db" {
			t.Errorf("instance 2's API received token %q, which is not one of instance 2's keys", tok)
		}
		used[tok] = true
	}
	if !used["_ave2e_instance_db"] || !used["_ave2e_instance_cfg"] {
		t.Errorf("instance 2's API saw tokens %v over %d requests, want both its config key and its stored key", used, len(bTokens))
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "web_url="+w.srv.URL) || !strings.Contains(logs, "api_url="+b.srv.URL+"/api/v4") || !strings.Contains(logs, "keys=2") {
		t.Errorf("the effective-instance log line must name instance 2's web URL, API URL and its 2 keys; log:\n%s", logs)
	}

	// Instance 2 without keys: its repository is not collectable, and no
	// request reaches the main instance either.
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token = '_ave2e_instance_db'`); err != nil {
		t.Fatal(err)
	}
	before := len(a.seen())
	keyless, err := buildForgeClients(ctx, writeConfig(``), store, false, true, logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keyless.gl.ForRepo(repo.Platform, repo.GitURL); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
		t.Errorf("keyless instance 2 routing = %v, want ErrInstanceNotConfigured", err)
	}
	if got := a.seen(); len(got) != before {
		t.Errorf("the main instance's API received %d request(s) after instance 2 lost its keys", len(got)-before)
	}
}
