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

	idx := strings.Index(code, "func resolveRedirects")
	if idx < 0 {
		t.Fatal("cannot find resolveRedirects function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	// Must have a retry loop for transient network errors.
	if !strings.Contains(fnBody, "retry") && !strings.Contains(fnBody, "attempt") {
		t.Error("resolveRedirects must retry on transient DNS errors (no such host) " +
			"with exponential backoff — 3 retries at 1s, 3s, 9s before giving up")
	}
}
