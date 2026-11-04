// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// 0.29.137, review round 3 R3-4: a page-cache leader's detached run ends
// with the lifetime the api process binds (Server.BindLifetime), and that
// lifetime is the re-warm context — cancelled on the stop signal and after
// the server returns — bound BEFORE the re-warm starts. Dropping or moving
// the one line would leave `aveloxis stop api` waiting up to
// http_timeout_seconds on a computation nobody reads, with every unit test
// green (the runtime test is TestPageCacheLeaderRunEndsWithTheProcess in
// internal/api; this is its wiring pin).
func TestAPIBindsTheLeaderLifetimeToTheRewarmContext(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runAPI("))
	bind := strings.Index(body, "apiServer.BindLifetime(rewarmCtx)")
	run := strings.Index(body, "apiServer.RunRewarm(rewarmCtx)")
	if bind < 0 || run < 0 || bind > run {
		t.Fatalf("runAPI must bind the leader lifetime to rewarmCtx (apiServer.BindLifetime(rewarmCtx)) before apiServer.RunRewarm(rewarmCtx): bind at %d, run at %d", bind, run)
	}
}
