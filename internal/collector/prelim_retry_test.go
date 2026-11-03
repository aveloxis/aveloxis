// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestPrelimRetriesOnDNSErrors verifies that resolveRedirects retries on
// transient DNS errors instead of failing immediately. During system crashes
// or network blips, DNS resolution fails briefly and all prelim checks that
// happen to fire during that window permanently skip repos.
func TestPrelimRetriesOnDNSErrors(t *testing.T) {
	src, err := os.ReadFile("prelim.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func headWithRetry")
	if idx < 0 {
		t.Fatal("cannot find headWithRetry function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must have a retry loop for transient network errors.
	if !strings.Contains(fnBody, "retry") && !strings.Contains(fnBody, "attempt") {
		t.Error("headWithRetry must retry on transient DNS errors (no such host) " +
			"with exponential backoff — 3 retries at 1s, 3s, 9s before giving up")
	}
	// v0.29.57: the probe walks the redirect chain itself and must issue
	// every hop through headWithRetry — since 0.29.132 by handing it to the
	// one walker (walkRedirects), which issues each hop through the request
	// it was given.
	rr := strings.Index(code, "func resolveRedirects")
	if rr < 0 || !strings.Contains(code[rr:], "walkRedirects(ctx, repoURL, headWithRetry)") {
		t.Error("resolveRedirects must hand headWithRetry to walkRedirects")
	}
	wr := strings.Index(code, "func walkRedirects")
	if wr < 0 || !strings.Contains(code[wr:], "request(ctx, current)") {
		t.Error("walkRedirects must issue each hop through the request it was given")
	}
}
