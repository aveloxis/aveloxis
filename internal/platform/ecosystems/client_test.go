// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package ecosystems

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.24.0 — ecosyste.ms client tests.
//
// ecosyste.ms is a federated indexer of package ecosystems. The
// reverse-lookup endpoint we use:
//
//   GET https://packages.ecosyste.ms/api/v1/packages/lookup?repository_url=<url>
//
// Returns every package whose registered repository URL matches the
// query. Covers Conda, Homebrew, CRAN, Packagist (PHP), Hex (Elixir),
// pub.dev (Dart), and ~50 other registries deps.dev doesn't index.
// No API keys; the "polite pool" From: header is the priority signal.

const cannedEcosystemsResponse = `[
  {
    "registry": {"ecosystem": "conda"},
    "name": "example-widget",
    "versions_count": 5,
    "first_release_published_at": "2023-01-01T00:00:00Z",
    "latest_release_published_at": "2024-06-01T00:00:00Z"
  },
  {
    "registry": {"ecosystem": "homebrew"},
    "name": "example-widget",
    "versions_count": 3,
    "first_release_published_at": "2023-03-01T00:00:00Z",
    "latest_release_published_at": "2024-05-01T00:00:00Z"
  }
]`

func TestLookupPackagesParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "packages/lookup") {
			t.Errorf("unexpected URL path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("repository_url"); got == "" {
			t.Error("repository_url query parameter must be set")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedEcosystemsResponse))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	pkgs, err := c.LookupPackages(context.Background(), "https://github.com/example/widget")
	if err != nil {
		t.Fatalf("LookupPackages: %v", err)
	}

	if len(pkgs) != 2 {
		t.Fatalf("want 2 packages (conda + homebrew), got %d", len(pkgs))
	}
	for _, p := range pkgs {
		if p.Source != "ecosyste.ms" {
			t.Errorf("Source = %q, want ecosyste.ms", p.Source)
		}
		if p.PackageName != "example-widget" {
			t.Errorf("PackageName = %q", p.PackageName)
		}
		if p.VersionCount == 0 {
			t.Errorf("VersionCount must be populated for %s", p.Ecosystem)
		}
		if p.FirstPublishedAt.IsZero() || p.LatestPublishedAt.IsZero() {
			t.Errorf("timestamps must be populated for %s", p.Ecosystem)
		}
	}
}

func TestLookupPackagesSetsPoliteEmailHeader(t *testing.T) {
	var observedFrom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedFrom = r.Header.Get("From")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL, PoliteEmail: "operator@example.org"})
	_, err := c.LookupPackages(context.Background(), "https://github.com/owner/repo")
	if err != nil {
		t.Fatalf("LookupPackages: %v", err)
	}
	if observedFrom != "operator@example.org" {
		t.Errorf("From header = %q, want operator@example.org (polite-pool opt-in)", observedFrom)
	}
}

func TestLookupPackagesOmitsFromHeaderWhenEmpty(t *testing.T) {
	var observedFrom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedFrom = r.Header.Get("From")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL}) // no PoliteEmail
	_, _ = c.LookupPackages(context.Background(), "https://github.com/owner/repo")
	if observedFrom != "" {
		t.Errorf("From header should be empty when PoliteEmail unset, got %q", observedFrom)
	}
}

func TestLookupPackagesHandles404AsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	pkgs, err := c.LookupPackages(context.Background(), "https://github.com/owner/missing")
	// Same contract as deps.dev: "no packages indexed for this repo" is
	// not a failure, it's a legitimate observation.
	if err != nil {
		t.Errorf("404 must return nil error, got %v", err)
	}
	if len(pkgs) != 0 {
		t.Errorf("404 must return empty slice, got %d", len(pkgs))
	}
}

func TestLookupPackagesClassifies5xxAsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.LookupPackages(context.Background(), "https://github.com/owner/repo")
	if err == nil {
		t.Fatal("5xx must surface as error")
	}
	if platform.ClassifyError(err) != platform.ClassTransient {
		t.Errorf("5xx error class = %v, want ClassTransient", platform.ClassifyError(err))
	}
}

func TestLookupPackagesClassifies429AsRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL})
	_, err := c.LookupPackages(context.Background(), "https://github.com/owner/repo")
	if err == nil {
		t.Fatal("429 must surface as error")
	}
	if platform.ClassifyError(err) != platform.ClassRateLimit {
		t.Errorf("429 error class = %v, want ClassRateLimit", platform.ClassifyError(err))
	}
}

func TestLookupPackagesSendsUserAgent(t *testing.T) {
	var observedUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := New(Options{BaseURL: srv.URL, UserAgent: "aveloxis/0.24.0"})
	_, _ = c.LookupPackages(context.Background(), "https://github.com/owner/repo")
	if !strings.Contains(observedUA, "aveloxis") {
		t.Errorf("User-Agent = %q, must contain aveloxis identifier", observedUA)
	}
}
