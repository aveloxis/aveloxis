// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	// Every subprocess goes through runToolCommand (group kill + execErr)
	// except the output probes, which take groupKilled directly — so the
	// direct groupKilled calls are exactly the CommandContext sites that
	// are not runToolCommand calls, plus the one inside the helper (round
	// 14: a hand-written "+2" stayed green when a probe lost its group kill).
	runs, ctxs := strings.Count(src, "runToolCommand(ctx, "), len(regexp.MustCompile(`exec\.CommandContext\(ctx,`).FindAllString(src, -1))
	if runs < 1 || ctxs <= runs {
		t.Fatalf("tools.go has %d exec.CommandContext sites and %d runToolCommand calls; want at least one call and at least one direct probe", ctxs, runs)
	}
	if kills := strings.Count(src, "groupKilled("); kills != 1+ctxs-runs {
		t.Errorf("tools.go has %d groupKilled calls; want %d — one inside runToolCommand and one per CommandContext site that does not go through it (%d): a probe is missing the process-group kill", kills, 1+ctxs-runs, ctxs-runs)
	}
	if n := strings.Count(src, "cmd.Run()"); n != 1 || !strings.Contains(srctest.FuncBody(t, src, "func runToolCommand("), "cmd.Run()") {
		t.Errorf("tools.go calls cmd.Run() %d time(s); the one call belongs inside runToolCommand (group kill + execErr)", n)
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
	if !errors.Is(err, context.DeadlineExceeded) { // what http.Client.Timeout returns (round 13: the os.ErrDeadlineExceeded arm was never true)
		t.Fatalf("the release lookup against a stalled server = %v; want context.DeadlineExceeded from the client's timeout", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("the lookup took %s; want the 100 ms client bound", took)
	}
}
