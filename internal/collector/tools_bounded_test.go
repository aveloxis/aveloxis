// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestToolFetchIsBounded pins batch 4a review rounds 11–12: the tool-update
// check's requests go through one bounded client and every subprocess it
// runs is exec.CommandContext on the caller's context, so a stalled proxy,
// PyPI or GitHub cannot hold serve's startup and a `stop serve` during the
// check ends the child instead of orphaning it. Source half: tools.go has no
// bare http.Get / http.DefaultClient and no exec.Command without a context.
// Behaviour half: the release lookup against a sleeping fixture returns the
// client's deadline error.
func TestToolFetchIsBounded(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/collector/tools.go"))
	if !strings.Contains(src, "toolFetchClient = &http.Client{Timeout:") {
		t.Error("tools.go must declare toolFetchClient with a Timeout")
	}
	for _, banned := range []string{"http.Get(", "http.DefaultClient", "exec.Command("} {
		if strings.Contains(src, banned) {
			t.Errorf("tools.go uses %s — every request goes through toolFetchClient and every subprocess through exec.CommandContext (an unbounded leg held serve's startup)", banned)
		}
	}
	if n := len(regexp.MustCompile(`exec\.CommandContext\(ctx,`).FindAllString(src, -1)); n < 7 {
		t.Errorf("tools.go has %d exec.CommandContext(ctx, …) sites; the install chain has at least seven subprocess legs", n)
	}

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	savedClient, savedURL := toolFetchClient, scorecardLatestReleaseURL
	toolFetchClient = &http.Client{Timeout: 100 * time.Millisecond}
	scorecardLatestReleaseURL = slow.URL + "/releases/latest"
	t.Cleanup(func() { toolFetchClient, scorecardLatestReleaseURL = savedClient, savedURL })
	start := time.Now()
	_, err := scorecardLatestVersion(context.Background())
	if err == nil || !errors.Is(err, os.ErrDeadlineExceeded) && !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "Timeout") {
		t.Fatalf("the release lookup against a stalled server = %v; want the client's timeout", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("the lookup took %s; want the 100 ms client bound", took)
	}
}
