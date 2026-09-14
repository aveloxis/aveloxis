// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.30.0 (multi-instance GitLab): a client belongs to one instance — its
// platform_id, its web base (where repositories live) and its API URL (where
// requests go, possibly another host). Every row it produces carries the
// instance's platform_id, so identities and messages from different
// instances never share a key.

const selfHostedID = model.GitLabInstanceIDMin + 1

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func instanceClient(t *testing.T, handler http.Handler, webBase string) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	logger := quietLogger()
	c, err := New(selfHostedID, webBase, srv.URL+"/api/v4", platform.NewKeyPool([]string{"instance-token"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientStampsInstancePlatformID(t *testing.T) {
	client := instanceClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/notes"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "body": "a note", "created_at": "2026-01-01T00:00:00Z", "author": map[string]any{"id": 5, "username": "alice"}}})
		case strings.Contains(r.URL.Path, "/users"):
			_ = json.NewEncoder(w).Encode([]glUser{{ID: 5, Username: "alice", Name: "Alice"}})
		default:
			http.NotFound(w, r)
		}
	}), "https://gitlab.example.invalid")

	if got := client.Platform(); got != selfHostedID {
		t.Errorf("Platform() = %d, want the instance id %d", got, selfHostedID)
	}
	n := 0
	for ref, err := range client.ListCommentsForIssue(context.Background(), "g", "r", 1) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if ref.Message.PlatformID != selfHostedID {
			t.Errorf("message PlatformID = %d, want %d", ref.Message.PlatformID, selfHostedID)
		}
	}
	if n != 1 {
		t.Fatalf("got %d notes, want 1", n)
	}
	c, err := client.EnrichContributor(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range c.Identities {
		if id.Platform != selfHostedID {
			t.Errorf("identity Platform = %d, want %d", id.Platform, selfHostedID)
		}
	}
}

// No row the client writes can carry the historical instance's id by
// accident: the constant does not appear in the package's non-test code.
func TestGitLabClientNeverHardCodesThePlatformID(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/platform/gitlab", 5)
	re := regexp.MustCompile(`\bPlatformGitLab\b`)
	for name, src := range files {
		if re.MatchString(srctest.StripGoComments(src)) {
			t.Errorf("%s uses model.PlatformGitLab — a GitLab client stamps its own instance id (c.platformID)", name)
		}
	}
}

func TestNewRefusesAnInvalidInstance(t *testing.T) {
	logger := quietLogger()
	keys := platform.NewKeyPool([]string{"k"}, logger)
	cases := []struct {
		name            string
		id              model.Platform
		webBase, apiURL string
		keys            *platform.KeyPool
	}{
		{"GitHub id", model.PlatformGitHub, "https://gitlab.example.invalid", "https://gitlab.example.invalid/api/v4", keys},
		{"generic git id", model.PlatformGenericGit, "https://gitlab.example.invalid", "https://gitlab.example.invalid/api/v4", keys},
		{"id past the range", model.GitLabInstanceIDMax + 1, "https://gitlab.example.invalid", "https://gitlab.example.invalid/api/v4", keys},
		{"nil pool", selfHostedID, "https://gitlab.example.invalid", "https://gitlab.example.invalid/api/v4", nil},
		{"empty pool", selfHostedID, "https://gitlab.example.invalid", "https://gitlab.example.invalid/api/v4", platform.NewKeyPool(nil, logger)},
		{"web base that is an API URL", selfHostedID, "https://gitlab.example.invalid/api/v4", "https://gitlab.example.invalid/api/v4", keys},
		{"no API URL", selfHostedID, "https://gitlab.example.invalid", "", keys},
	}
	for _, tc := range cases {
		if c, err := New(tc.id, tc.webBase, tc.apiURL, tc.keys, logger); err == nil || c != nil {
			t.Errorf("%s: New = (%v, %v), want a refusal", tc.name, c, err)
		}
	}
}

// Repository URLs are parsed against the web base, never the API host.
func TestClientParsesUnderWebBaseNotAPIHost(t *testing.T) {
	logger := quietLogger()
	c, err := New(selfHostedID, "https://code.example.invalid/gitlab", "https://gitlab-api.example.invalid/api/v4", platform.NewKeyPool([]string{"k"}, logger), logger)
	if err != nil {
		t.Fatal(err)
	}
	owner, repo, err := c.ParseRepoURL("https://code.example.invalid/gitlab/group/sub/project.git")
	if err != nil || owner != "group/sub" || repo != "project" {
		t.Errorf("ParseRepoURL(under web base) = (%q, %q, %v), want (group/sub, project)", owner, repo, err)
	}
	for _, u := range []string{
		"https://gitlab-api.example.invalid/group/project",
		"https://gitlab.com/group/project",
		"https://code.example.invalid/other/project",
	} {
		if _, _, err := c.ParseRepoURL(u); err == nil {
			t.Errorf("ParseRepoURL(%s) accepted a URL outside the instance's web base", u)
		}
	}
}
