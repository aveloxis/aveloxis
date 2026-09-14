// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/api"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/forgekeys"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// End to end (SR-10), v0.30.0 Phase C: the API keys admin page's add and
// remove, through the real api handler and the real store, reach a running
// serve's pools on the next reload — for the right instance only.
//
// Config: a main GitLab instance A with a config key, instance B with a
// config key, and instance C with NO keys, each API on its own fake host.
// buildForgeClients builds a pool for every registered instance; C's
// repository is not collectable. POST a key for C through the admin API and
// run one reload: C's repository routes, C's API receives exactly that key,
// and A and B never see it; the saved report and the admin list show it
// loaded for C with C's API URL. POST its removal and run one reload: C is
// not collectable again and the report drops the key.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestKeyAdminAddReloadRemove(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	a, b, c := newTokenRecorder(t), newTokenRecorder(t), newTokenRecorder(t)
	const cToken = "_avkadm_instance_c_key_01"

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
	reporter := forgekeys.ReporterID("serve")
	var adminID int
	cleanup := func() {
		bg := context.Background()
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.worker_oauth WHERE access_token LIKE '\_avkadm\_%'`)
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.forge_key_reports WHERE reporter = $1`, reporter)
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, adminID)
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '\_avkadm\_%'`)
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_avkadm%')`)
		if _, err := pool.Exec(bg, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE 'http://127.0.0.1:%/_avkadm%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
		if _, err := pool.Exec(bg, `DELETE FROM aveloxis_data.platforms WHERE platform_id BETWEEN 100 AND 199 AND platform_instance_url LIKE 'http://127.0.0.1:%'`); err != nil {
			t.Logf("cleanup: %v", err)
		}
		_, _ = pool.Exec(bg, `UPDATE aveloxis_data.platforms SET platform_instance_url = $1 WHERE platform_id = 2`, row2)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.platforms SET platform_instance_url = NULL WHERE platform_id = 2`); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "aveloxis.json")
	cfgJSON := `{"gitlab": {"base_url": "` + a.srv.URL + `/api/v4", "api_keys": ["_avkadm_main_cfg"],
		"instances": [
			{"web_url": "` + b.srv.URL + `", "api_url": "` + b.srv.URL + `/api/v4", "api_keys": ["_avkadm_b_cfg"]},
			{"web_url": "` + c.srv.URL + `", "api_url": "` + c.srv.URL + `/api/v4", "api_keys": []}
		]}}`
	if err := os.WriteFile(path, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := buildForgeClients(ctx, cfg, store, false, true, logger)
	if err != nil {
		t.Fatal(err)
	}
	maint := clients.keyMaintainer(cfg, store, logger)

	repoURL := c.srv.URL + "/_avkadm/project"
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: repoURL, Owner: "_avkadm", Name: "project"})
	if err != nil {
		t.Fatal(err)
	}
	repo, err := store.GetRepoByID(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clients.gl.ForRepo(repo.Platform, repo.GitURL); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
		t.Fatalf("keyless instance C before any key: %v, want ErrInstanceNotConfigured", err)
	}

	// The admin API process, with a real admin session.
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`, fmt.Sprintf("_avkadm_%d", time.Now().UnixNano())).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	session, err := store.CreateSessionToken(ctx, adminID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	apiSrv, err := api.NewWithOptions(store, slog.New(slog.NewTextHandler(io.Discard, nil)), api.Options{ExemptCIDRs: api.DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(apiSrv.Handler())
	t.Cleanup(ts.Close)
	call := func(method, route, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+route, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+session)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		if bytes.Contains(out, []byte(cToken)) {
			t.Fatalf("%s %s returned the token: %s", method, route, out)
		}
		return resp.StatusCode, out
	}

	// Add a key for C through the admin API.
	status, body := call("POST", "/api/v1/admin/api-keys", `{"platform":"gitlab","instance_url":"`+c.srv.URL+`","name":"e2e","token":"`+cToken+`"}`)
	if status != http.StatusCreated {
		t.Fatalf("add = %d %s", status, body)
	}
	var added struct {
		OAuthID int64  `json:"oauth_id"`
		KeyID   string `json:"key_id"`
	}
	if err := json.Unmarshal(body, &added); err != nil || added.KeyID != platform.KeyID(cToken) {
		t.Fatalf("add response %s (%v)", body, err)
	}

	// One reload + report, as serve's key-maintenance tick runs them.
	if err := maint.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := maint.Report(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := clients.gl.ForRepo(repo.Platform, repo.GitURL)
	if err != nil {
		t.Fatalf("instance C after its key was added and reloaded: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := client.FetchRepoInfo(ctx, repo.Owner, repo.Name); err != nil {
			t.Logf("FetchRepoInfo (fake API, partial answers are fine): %v", err)
		}
	}
	cSeen := c.seen()
	if len(cSeen) == 0 {
		t.Fatal("instance C's API received no requests")
	}
	for _, tok := range cSeen {
		if tok != cToken {
			t.Errorf("instance C's API received %q, want only the key added for C", tok)
		}
	}
	// A and B are collected too after the reload, so "they never see
	// another instance's key" is measured against real traffic (review of
	// Phase C, finding 8b): each receives requests, and none carries a key
	// of another instance. (The main instance A may also hold stored keys
	// already in the scratch database without an instance tag — those are
	// main-instance keys by definition — so A is checked for FOREIGN keys.)
	for _, other := range []struct {
		name    string
		rec     *tokenRecorder
		webURL  string
		foreign []string
	}{
		{"A (main)", a, a.srv.URL, []string{cToken, "_avkadm_b_cfg"}},
		{"B", b, b.srv.URL, []string{cToken, "_avkadm_main_cfg"}},
	} {
		id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: other.webURL + "/_avkadm/other", Owner: "_avkadm", Name: "other"})
		if err != nil {
			t.Fatal(err)
		}
		r, err := store.GetRepoByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		oc, err := clients.gl.ForRepo(r.Platform, r.GitURL)
		if err != nil {
			t.Fatalf("instance %s routing after the reload: %v", other.name, err)
		}
		if _, err := oc.FetchRepoInfo(ctx, r.Owner, r.Name); err != nil {
			t.Logf("FetchRepoInfo on %s (fake API, partial answers are fine): %v", other.name, err)
		}
		seen := other.rec.seen()
		if len(seen) == 0 {
			t.Fatalf("instance %s's API received no requests — the isolation check would be vacuous", other.name)
		}
		for _, tok := range seen {
			for _, f := range other.foreign {
				if tok == f {
					t.Errorf("instance %s's API received another instance's key %q", other.name, platform.MaskToken(tok))
				}
			}
		}
		if other.name == "B" {
			for _, tok := range seen {
				if tok != "_avkadm_b_cfg" {
					t.Errorf("instance B's API received %q, want only its own config key", platform.MaskToken(tok))
				}
			}
		}
	}

	// The admin list, from the saved report: loaded for C, with C's API URL.
	status, body = call("GET", "/api/v1/admin/api-keys", "")
	if status != http.StatusOK {
		t.Fatalf("list = %d %s", status, body)
	}
	cForge, cKey := findForgeKey(t, body, c.srv.URL, added.KeyID)
	if cForge == nil || cKey == nil || cKey["status"] != "loaded" || cKey["health"] != "ok" || cForge["api_url"] != c.srv.URL+"/api/v4" {
		t.Fatalf("admin list for C: forge=%v key=%v\n%s", cForge, cKey, body)
	}

	// Remove it through the admin API; one reload takes C back out.
	status, body = call("POST", fmt.Sprintf("/api/v1/admin/api-keys/%d/delete", added.OAuthID), "")
	if status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}
	if err := maint.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := maint.Report(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.gl.ForRepo(repo.Platform, repo.GitURL); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
		t.Fatalf("instance C after its key was removed and reloaded: %v, want ErrInstanceNotConfigured", err)
	}
	status, body = call("GET", "/api/v1/admin/api-keys", "")
	if status != http.StatusOK {
		t.Fatalf("list after removal = %d %s", status, body)
	}
	if _, key := findForgeKey(t, body, c.srv.URL, added.KeyID); key != nil {
		t.Fatalf("the removed key is still listed for C: %v", key)
	}
	if strings.Contains(logBuf.String(), cToken) {
		t.Error("a serve log line carries instance C's raw token")
	}
}

// findForgeKey returns the forge with webURL and its key with keyID from an
// admin list response.
func findForgeKey(t *testing.T, body []byte, webURL, keyID string) (map[string]any, map[string]any) {
	t.Helper()
	var resp struct {
		Forges []map[string]any `json:"forges"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	for _, f := range resp.Forges {
		if f["web_url"] != webURL {
			continue
		}
		keys, _ := f["keys"].([]any)
		for _, k := range keys {
			if km, ok := k.(map[string]any); ok && km["key_id"] == keyID {
				return f, km
			}
		}
		return f, nil
	}
	return nil, nil
}
