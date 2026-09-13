// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.11 — refreshGitLabGroup built its GitLab HTTP client on the GITHUB
// key pool (a TODO since the initial commit): every GitLab group refresh sent
// a GitHub token as PRIVATE-TOKEN to the GitLab host named by the group's
// website URL, and the 401s it earned recorded auth strikes against GitHub
// keys. The client now uses the GitLab pool, and only for the configured
// GitLab instance's host.

package scheduler

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

const (
	testGitHubSecret = "ghp_github_secret_never_to_gitlab"
	testGitLabToken  = "glpat_gitlab_token"
)

// gitlabRecorder is a fake GitLab API that records every request's
// PRIVATE-TOKEN and answers an empty project page.
type gitlabRecorder struct {
	mu     sync.Mutex
	tokens []string
	srv    *httptest.Server
}

func newGitLabRecorder(t *testing.T) *gitlabRecorder {
	t.Helper()
	g := &gitlabRecorder{}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.tokens = append(g.tokens, r.Header.Get("PRIVATE-TOKEN"))
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *gitlabRecorder) seen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.tokens...)
}

// The refusals need no database: every guard runs before the first store
// call, so a nil store proves no request is ever built.
func TestRefreshGitLabGroupRefusesWithoutGitLabKeysOrMatchingInstance(t *testing.T) {
	fake := newGitLabRecorder(t)
	ghPool := platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet())
	glPool := platform.NewKeyPool([]string{testGitLabToken}, rlQuiet())
	here := &config.PlatformConfig{BaseURL: fake.srv.URL + "/api/v4"}
	onFake := db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: fake.srv.URL + "/grp"}

	for _, tc := range []struct {
		name      string
		glKeys    *platform.KeyPool
		gitlabCfg *config.PlatformConfig
		group     db.OrgGroup
		wantLog   string
	}{
		{"no GitLab pool", nil, here, onFake, "no GitLab API keys"},
		{"empty GitLab pool", platform.NewKeyPool(nil, rlQuiet()), here, onFake, "no GitLab API keys"},
		{"no GitLab config", glPool, nil, onFake, "configured GitLab instance"},
		{"group on a foreign host", glPool, here,
			db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: "https://git.elsewhere.example/grp"},
			"configured GitLab instance"},
		// A website with no usable host must not be assumed to be the
		// configured instance: through v0.29.11's first draft a schemeless
		// or unparseable URL fell back to "gitlab.com", so a group from
		// another instance would have been listed against gitlab.com's group
		// of the same name (review of this change).
		{"schemeless website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "mesa", Type: "gitlab_group", Website: "gitlab.freedesktop.org/mesa"},
			"no usable host"},
		{"empty website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "mesa", Type: "gitlab_group", Website: ""},
			"no usable host"},
		{"unparseable website", glPool, &config.PlatformConfig{BaseURL: "https://gitlab.com/api/v4"},
			db.OrgGroup{Name: "grp", Type: "gitlab_group", Website: "https://gitlab.com%2Eevil.example/grp"},
			"no usable host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			s := NewWithKeys(nil, nil, nil, ghPool, tc.glKeys, logger,
				Config{Collection: &config.CollectionConfig{}, GitLab: tc.gitlabCfg})
			before := len(fake.seen())
			if n := s.refreshGitLabGroup(context.Background(), tc.group); n != 0 {
				t.Errorf("refreshGitLabGroup = %d, want 0", n)
			}
			if got := fake.seen()[before:]; len(got) != 0 {
				t.Fatalf("a refused refresh sent %d request(s) with tokens %q", len(got), got)
			}
			if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), tc.wantLog) {
				t.Errorf("a refused refresh must WARN naming the reason (%q); log:\n%s", tc.wantLog, buf.String())
			}
		})
	}
}

// End to end against a database (the refresh reads user_groups before
// listing): a group on the configured instance is listed with the GitLab
// token, and the GitHub token never leaves the process.
func TestRefreshGitLabGroupSendsOnlyTheGitLabToken(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, rlQuiet())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	fake := newGitLabRecorder(t)
	s := NewWithKeys(store, nil, nil,
		platform.NewKeyPool([]string{testGitHubSecret}, rlQuiet()),
		platform.NewKeyPool([]string{testGitLabToken}, rlQuiet()),
		rlQuiet(),
		Config{Collection: &config.CollectionConfig{}, GitLab: &config.PlatformConfig{BaseURL: fake.srv.URL + "/api/v4"}})
	s.refreshGitLabGroup(ctx, db.OrgGroup{Name: "_avgl-keys-grp", Type: "gitlab_group", Website: fake.srv.URL + "/_avgl-keys-grp"})

	got := fake.seen()
	if len(got) == 0 {
		t.Fatal("the refresh never listed the group")
	}
	for _, tok := range got {
		if tok == testGitHubSecret {
			t.Fatal("a GitHub token was sent to the GitLab host as PRIVATE-TOKEN")
		}
		if tok != testGitLabToken {
			t.Errorf("PRIVATE-TOKEN = %q, want the GitLab pool's token", tok)
		}
	}
}

// Wiring: serve hands the scheduler the GitLab pool and the gitlab config
// block (the refusals above make a forgotten wire loud, but this is the
// production path).
func TestServeWiresGitLabKeysIntoScheduler(t *testing.T) {
	body := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	call := "scheduler.NewWithKeys(store, ghClient, glClient, ghKeys, glKeys, logger, scheduler.Config{"
	if strings.Count(body, call) != 1 {
		t.Fatalf("serve must construct the scheduler with %q", call)
	}
	i := strings.Index(body, call)
	end := strings.Index(body[i:], "\n\t})")
	if end < 0 {
		t.Fatal("could not find the end of the scheduler.Config literal")
	}
	if !strings.Contains(body[i:i+end], "GitLab: &cfg.GitLab,") {
		t.Error("serve's scheduler.Config must carry GitLab: &cfg.GitLab")
	}
}

func rlQuiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
