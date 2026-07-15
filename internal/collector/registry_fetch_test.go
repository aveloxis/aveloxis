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

// TestNoBareCurlWithoutMaxTime pins the choke point: analysis.go may
// contain exactly ONE `exec.CommandContext(ctx, "curl", ...)` call —
// inside fetchRegistryJSON — and that helper must apply --max-time.
func TestNoBareCurlWithoutMaxTime(t *testing.T) {
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

	count := strings.Count(stripped, `"curl"`)
	if count != 1 {
		t.Errorf(`analysis.go contains %d exec sites referencing "curl"; want exactly 1 `+
			`(inside fetchRegistryJSON). Every registry lookup must route through the `+
			`helper so the --max-time cap can never be forgotten — bare curls were the `+
			`#2 subprocess-hang class in production goroutine dumps.`, count)
	}

	// The helper itself must apply the wall-clock cap.
	idx := strings.Index(code, "func fetchRegistryJSON(")
	if idx < 0 {
		t.Fatal("analysis.go must declare fetchRegistryJSON — the single curl choke point")
	}
	body := code[idx:]
	if next := strings.Index(body[1:], "\nfunc "); next > 0 {
		body = body[:next+1]
	}
	if !strings.Contains(body, "--max-time") {
		t.Error("fetchRegistryJSON must pass --max-time to curl — the unbounded-hang fix IS the point of the helper")
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
