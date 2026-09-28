// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGitHubRequestsPinTheAPIVersion pins worklist item 15: every GitHub
// REST request carries X-GitHub-Api-Version, and a GitLab request does not
// (the header is GitHub's).
func TestGitHubRequestsPinTheAPIVersion(t *testing.T) {
	var got, gotGitLab string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-GitHub-Api-Version")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer gh.Close()
	gl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotGitLab = r.Header.Get("X-GitHub-Api-Version")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer gl.Close()
	var out map[string]any
	if err := NewHTTPClient(gh.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub).GetJSON(context.Background(), "/repos/o/r", &out); err != nil {
		t.Fatal(err)
	}
	if got != GitHubAPIVersion {
		t.Errorf("GitHub request carried X-GitHub-Api-Version %q; want %q", got, GitHubAPIVersion)
	}
	if err := NewHTTPClient(gl.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitLab).GetJSON(context.Background(), "/projects/1", &out); err != nil {
		t.Fatal(err)
	}
	if gotGitLab != "" {
		t.Errorf("a GitLab request carried GitHub's version header %q", gotGitLab)
	}
}

// TestGitHubAPIVersionPredatesTheMergeCommitSHADrop is the tripwire on the
// pin: the 2026-03-10 REST version drops merge_commit_sha from the
// pull-request shape, which the `rest` escape hatch and the per-PR rescue
// read; moving the pin to or past it needs those paths to take mergeCommit
// from GraphQL first (worklist item 15), and the pin must stay within
// GitHub's support (2022-11-28 is supported until 2028-03-10).
func TestGitHubAPIVersionPredatesTheMergeCommitSHADrop(t *testing.T) {
	v, err := time.Parse("2006-01-02", GitHubAPIVersion)
	if err != nil {
		t.Fatalf("GitHubAPIVersion %q is not a date: %v", GitHubAPIVersion, err)
	}
	drop := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	if !v.Before(drop) {
		t.Errorf("GitHubAPIVersion %s is at or past the 2026-03-10 version that drops merge_commit_sha; the REST paths must read mergeCommit from GraphQL before the pin moves", GitHubAPIVersion)
	}
	if time.Now().After(time.Date(2028, 3, 10, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("GitHubAPIVersion %s passed GitHub's support end (2028-03-10); move the pin", GitHubAPIVersion)
	}
}

// TestEveryGitHubRESTSiteOutsideTheClientPinsTheVersion: the request sites
// that reach api.github.com by its literal host without HTTPClient — the
// web OAuth callback's /user and /user/emails, the tool-update check — set
// the header themselves (batch 7c review round 1: "every GitHub REST
// request" was true only for the client). Every non-test file under cmd/
// and internal/ containing the request literal `"https://api.github.com/`
// carries at least as many header SETs as such literals (round 2: one Set
// per file let a second site in the same file drop it), and no non-test
// file under those two roots but github_host.go may spell the host without
// the trailing slash (round 3: a concatenated request would slip under the
// count; scripts/loadorgs spells it once, into platform.NewHTTPClient,
// which sets the header — outside the walk).
// The scorecard rate-limit probe builds its URL from the configured base,
// so this sweep cannot see it: TestScorecardRateLimitProbePinsTheAPIVersion
// (internal/collector) pins it at runtime.
func TestEveryGitHubRESTSiteOutsideTheClientPinsTheVersion(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			src := srctest.StripGoComments(string(raw))
			rel, _ := filepath.Rel(root, p)
			if strings.Contains(src, `"https://api.github.com"`) && filepath.Base(p) != "github_host.go" {
				t.Errorf("%s spells the GitHub API host as a bare literal — a request built by concatenation escapes the per-literal count; use platform.PublicGitHubAPIBase or the client", rel)
			}
			if !strings.Contains(src, `"https://api.github.com/`) {
				return nil
			}
			examined++
			// The header SET, not a mention of the constant (a mutant
			// `_ = platform.GitHubAPIVersion` passed a name check), once per
			// request literal in the file.
			sites, sets := strings.Count(src, `"https://api.github.com/`), strings.Count(src, `Header.Set("X-GitHub-Api-Version", platform.GitHubAPIVersion)`)
			if sets < sites {
				t.Errorf("%s has %d api.github.com request literal(s) but %d `Header.Set(\"X-GitHub-Api-Version\", platform.GitHubAPIVersion)`", rel, sites, sets)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if examined < 2 {
		t.Fatalf("%d files request api.github.com directly; the sweep expected at least the web OAuth callback (internal/web/server.go) and the tool-update check (internal/collector/tools.go)", examined)
	}
}
