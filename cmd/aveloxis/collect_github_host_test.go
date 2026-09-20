// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.29.57 — `aveloxis collect` builds a collector with the deployment's
// GitHub keys, and commit resolution inside it builds clients of its own from
// them. The host is a REQUIRED constructor parameter now, so it cannot be
// forgotten (omitting it does not compile); what a pin still has to say is
// that the value passed is the CONFIGURED one, not a literal.
//
// The scheduler's half of this is TestSchedulerGitHubKeysNeverTravelWithoutTheHost.

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestCollectPassesTheConfiguredGitHubHost(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	body := srctest.StripGoComments(src)
	const call = "collector.NewWithOptions(client, store, logger, ghKeys, cfg.GitHub.BaseURL, cfg.Collection.RepoCloneDir)"
	if !strings.Contains(body, call) {
		t.Errorf("the collect command must build its collector as %q — the keys and the host they belong to travel together, or an Enterprise token reaches public GitHub", call)
	}
	// A literal here would compile and would be wrong: the whole point is
	// that this deployment's configuration decides.
	if strings.Contains(body, `NewWithOptions(client, store, logger, ghKeys, "`) {
		t.Error("the collector's GitHub host must come from cfg.GitHub.BaseURL, never a literal")
	}
}
