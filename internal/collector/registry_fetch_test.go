// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — fetchRegistryJSON is the single curl choke point for the
// package-registry lookups in analysis.go. The ~15 previously-bare
// `curl -sf` sites had NO wall-clock cap and were the #2
// subprocess-hang class in production goroutine dumps. The negative
// tripwire here makes the bug class unrepresentable: any new bare curl
// exec in analysis.go fails the build.

package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestRegistryFetchIsBoundedAndCurlFree pins the choke point in its
// v0.27.30 net/http form. The two invariants are transport-agnostic
// and both have production incidents behind them:
//   - ZERO curl exec sites in analysis.go: the transport moved to
//     net/http so every resolver's base URL is testable (7 registries
//     previously had NO coverage because curl-bound URLs were
//     unreachable by httptest). A curl reappearing means someone
//     bypassed the choke point.
//   - The shared client declares an explicit Timeout: bare unbounded
//     fetches were the #2 subprocess-hang class in production
//     goroutine dumps (v0.27.5).
func TestRegistryFetchIsBoundedAndCurlFree(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Strip // line comments so doc mentions of "curl" don't count.
	var sb strings.Builder
	for _, line := range strings.Split(code, "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	stripped := sb.String()

	if count := strings.Count(stripped, `"curl"`); count != 0 {
		t.Errorf("analysis.go contains %d curl exec references; want 0 — registry lookups route through fetchRegistryJSON's net/http client (v0.27.30)", count)
	}
	if !strings.Contains(code, "registryHTTPClient = &http.Client{Timeout:") {
		t.Error("registryHTTPClient must declare an explicit Timeout — unbounded fetches were the #2 production hang class")
	}
}

// TestFetchRegistryJSONBehavior drives the helper against a live
// httptest server through real curl: body round-trip, -f error on
// non-2xx, and extraArgs header pass-through (the Hackage Accept case).
func TestFetchRegistryJSONBehavior(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			fmt.Fprint(w, `{"hello":"world"}`)
		case "/accept":
			fmt.Fprint(w, r.Header.Get("Accept"))
		case "/missing":
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx := context.Background()

	body, err := fetchRegistryJSON(ctx, srv.URL+"/ok")
	if err != nil {
		t.Fatalf("fetchRegistryJSON(/ok): %v", err)
	}
	if string(body) != `{"hello":"world"}` {
		t.Errorf("body = %q", body)
	}

	// -f semantics preserved: non-2xx is an error, same as the
	// pre-v0.27.5 per-site pattern.
	if _, err := fetchRegistryJSON(ctx, srv.URL+"/missing"); err == nil {
		t.Error("fetchRegistryJSON must fail on 404 (curl -f semantics)")
	}

	// extraArgs flow through before the URL (Hackage's Accept header).
	body, err = fetchRegistryJSON(ctx, srv.URL+"/accept", "-H", "Accept: application/json")
	if err != nil {
		t.Fatalf("fetchRegistryJSON(/accept): %v", err)
	}
	if string(body) != "application/json" {
		t.Errorf("Accept header seen by server = %q, want application/json", body)
	}
}
