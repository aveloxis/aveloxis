package collector

import (
	"os"
	"strings"
	"testing"
)

// TestStagedCollectorIgnoresReleaseNotFound verifies that staged.go handles
// a not-found error from ListReleases WITHOUT adding it to result.Errors.
// A repo that has never cut a release (or was deleted/renamed) must not fail
// the overall collection pipeline — buildOutcome flips success=false on any
// result.Errors entry, so this is a whole-job failure if we don't filter it.
func TestStagedCollectorIgnoresReleaseNotFound(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The releases loop must treat 404 as non-fatal. Either it checks
	// errors.Is(err, platform.ErrNotFound) directly, or it delegates to the
	// isOptionalEndpointSkip helper (introduced in v0.16.8 to bundle 404+403
	// across every phase).
	idx := strings.Index(code, "ListReleases")
	if idx < 0 {
		t.Fatal("cannot find ListReleases call in staged.go")
	}
	// Scan a window around the ListReleases call.
	start := idx
	end := idx + 800
	if end > len(code) {
		end = len(code)
	}
	window := code[start:end]
	if !strings.Contains(window, "ErrNotFound") && !strings.Contains(window, "isOptionalEndpointSkip") {
		t.Error("staged.go: releases loop must treat 404 as non-fatal via either " +
			"errors.Is(err, platform.ErrNotFound) or isOptionalEndpointSkip(err) — " +
			"otherwise a 404 on /releases (repos with no releases, renamed repos) " +
			"fails the whole job")
	}
}

// TestCollectorIgnoresReleaseNotFound — same guarantee for the legacy
// (non-staged) Collector.collectReleases path in collector.go.
func TestCollectorIgnoresReleaseNotFound(t *testing.T) {
	src, err := os.ReadFile("collector.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (c *Collector) collectReleases(")
	if idx < 0 {
		t.Fatal("cannot find collectReleases function in collector.go")
	}
	// Look within the function body.
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}
	if !strings.Contains(fnBody, "ErrNotFound") {
		t.Error("collector.go: collectReleases must check errors.Is(err, platform.ErrNotFound) " +
			"and return nil so the collection doesn't fail on repos with no releases")
	}
}

// TestHTTPClientWrapsNotFoundSentinel verifies the platform HTTP client
// wraps 404 responses with the exported ErrNotFound sentinel rather than
// returning an opaque fmt.Errorf. This is what lets collector callers
// distinguish "endpoint missing" from every other error.
func TestHTTPClientWrapsNotFoundSentinel(t *testing.T) {
	src, err := os.ReadFile("../platform/httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ErrNotFound") {
		t.Error("httpclient.go must define and use an ErrNotFound sentinel so " +
			"callers can check errors.Is(err, platform.ErrNotFound)")
	}

	// The 404 branch must wrap ErrNotFound (not use a plain fmt.Errorf).
	notFoundIdx := strings.Index(code, "http.StatusNotFound")
	if notFoundIdx < 0 {
		t.Fatal("cannot find http.StatusNotFound branch in httpclient.go")
	}
	window := code[notFoundIdx:]
	if len(window) > 400 {
		window = window[:400]
	}
	if !strings.Contains(window, "ErrNotFound") {
		t.Error("the http.StatusNotFound branch in Get must wrap ErrNotFound " +
			"via fmt.Errorf with %%w so callers can errors.Is check it")
	}
}
