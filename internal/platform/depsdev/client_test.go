// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package depsdev

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.24.0 — deps.dev client tests.
//
// deps.dev is Google's open-source dependency graph database. The
// reverse-lookup endpoint we use:
//
//   GET /v3/projects/{project_id}:packageversions
//   where project_id = "github.com/{owner}/{repo}"
//
// Returns every published package version that mentions the repo as
// its source across 7 ecosystems (NPM, PYPI, MAVEN, CARGO, GO,
// RUBYGEMS, NUGET). Capped at 1500 versions; attestation-derived
// mappings served first. Public, unauthenticated, no documented
// rate limit (though 429s are observed in practice).

// canned response copied/abbreviated from deps.dev v3 docs.
// Two versions of one npm package + one version of one PyPI package.
const cannedResponse = `{
  "versions": [
    {
      "versionKey": {"system": "NPM", "name": "@example/widget", "version": "1.2.0"},
      "publishedAt": "2024-03-15T10:00:00Z",
      "isDefault": false
    },
    {
      "versionKey": {"system": "NPM", "name": "@example/widget", "version": "1.2.1"},
      "publishedAt": "2024-06-20T10:00:00Z",
      "isDefault": true
    },
    {
      "versionKey": {"system": "PYPI", "name": "example-widget", "version": "0.5.0"},
      "publishedAt": "2024-01-10T10:00:00Z",
      "isDefault": true
    }
  ]
}`

func TestGetPackageVersionsParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "packageversions") {
			t.Errorf("unexpected URL path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedResponse))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL, UserAgent: "aveloxis/test"})
	pkgs, err := c.GetPackageVersions(context.Background(), "example", "widget")
	if err != nil {
		t.Fatalf("GetPackageVersions: %v", err)
	}

	// Expect 2 distinct PackageDistribution rows: one for @example/widget
	// (npm) with VersionCount=2, one for example-widget (pypi) with
	// VersionCount=1. Aggregation collapses multiple versionKey entries
	// of the same (system, name) into a single row.
	if len(pkgs) != 2 {
		t.Fatalf("want 2 distinct packages, got %d", len(pkgs))
	}

	var npmRow, pypiRow bool
	for _, p := range pkgs {
		switch p.Ecosystem {
		case "npm":
			npmRow = true
			if p.PackageName != "@example/widget" {
				t.Errorf("npm PackageName = %q, want @example/widget", p.PackageName)
			}
			if p.VersionCount != 2 {
				t.Errorf("npm VersionCount = %d, want 2", p.VersionCount)
			}
			if p.Source != "deps.dev" {
				t.Errorf("Source = %q, want deps.dev", p.Source)
			}
			// First published = earliest publishedAt; latest = latest.
			if p.FirstPublishedAt.IsZero() || p.LatestPublishedAt.IsZero() {
				t.Errorf("npm timestamps must be populated: first=%v latest=%v", p.FirstPublishedAt, p.LatestPublishedAt)
			}
			if !p.FirstPublishedAt.Before(p.LatestPublishedAt) {
				t.Errorf("FirstPublishedAt %v should be < LatestPublishedAt %v", p.FirstPublishedAt, p.LatestPublishedAt)
			}
		case "pypi":
			pypiRow = true
			if p.PackageName != "example-widget" {
				t.Errorf("pypi PackageName = %q", p.PackageName)
			}
			if p.VersionCount != 1 {
				t.Errorf("pypi VersionCount = %d, want 1", p.VersionCount)
			}
		}
	}
	if !npmRow || !pypiRow {
		t.Errorf("missing rows: npm=%v pypi=%v", npmRow, pypiRow)
	}
}

func TestGetPackageVersionsHandles404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	pkgs, err := c.GetPackageVersions(context.Background(), "owner", "missing")
	// 404 = repo unknown to deps.dev = "no packages found", not an
	// error. Returning empty + nil is the contract callers expect so
	// the worker can move on to ecosyste.ms / GitHub fallbacks.
	if err != nil {
		t.Fatalf("404 must return nil error, got %v", err)
	}
	if len(pkgs) != 0 {
		t.Fatalf("404 must return empty slice, got %d packages", len(pkgs))
	}
}

func TestGetPackageVersionsClassifies5xxAsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gateway timeout", http.StatusBadGateway)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.GetPackageVersions(context.Background(), "owner", "repo")
	if err == nil {
		t.Fatal("5xx must surface as error")
	}
	if platform.ClassifyError(err) != platform.ClassTransient {
		t.Errorf("5xx error class = %v, want ClassTransient (so the worker retries)", platform.ClassifyError(err))
	}
}

func TestGetPackageVersionsClassifies429AsRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.GetPackageVersions(context.Background(), "owner", "repo")
	if err == nil {
		t.Fatal("429 must surface as error")
	}
	if platform.ClassifyError(err) != platform.ClassRateLimit {
		t.Errorf("429 error class = %v, want ClassRateLimit", platform.ClassifyError(err))
	}
}

func TestGetPackageVersionsSendsUserAgent(t *testing.T) {
	var observedUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`{"versions":[]}`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL, UserAgent: "aveloxis/0.24.0 (operator@example.org)"})
	_, err := c.GetPackageVersions(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("GetPackageVersions: %v", err)
	}
	if !strings.Contains(observedUA, "aveloxis") {
		t.Errorf("User-Agent = %q, must contain aveloxis identifier so deps.dev operators can route diagnostics", observedUA)
	}
}

func TestGetPackageVersionsDefaultUserAgentWhenUnset(t *testing.T) {
	var observedUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`{"versions":[]}`))
	}))
	defer srv.Close()

	// Empty UserAgent should pick up a sane default (aveloxis/<version>).
	c := New(Options{BaseURL: srv.URL})
	_, _ = c.GetPackageVersions(context.Background(), "owner", "repo")
	if !strings.Contains(observedUA, "aveloxis") {
		t.Errorf("Default User-Agent = %q, must contain aveloxis identifier", observedUA)
	}
}

func TestGetPackageVersionsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// hang briefly so ctx cancel takes effect
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before the request

	c := New(Options{BaseURL: srv.URL})
	_, err := c.GetPackageVersions(ctx, "owner", "repo")
	if err == nil {
		t.Fatal("canceled context must surface as error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error must wrap context.Canceled, got %v", err)
	}
}
