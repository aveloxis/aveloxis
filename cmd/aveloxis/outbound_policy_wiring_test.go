// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// loadConfig installs the outbound-address rule (0.29.132) for every
// command, on both of its arms (a file loaded; the defaults when there is
// none): the collector's zero value is the strict rule, so a command that
// skipped this could only be stricter, never looser — but the configured
// trust (a self-hosted forge) would be lost.
func TestLoadConfigWiresTheOutboundPolicy(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func loadConfig("))
	if n := strings.Count(body, "collector.SetOutboundPolicy(outboundPolicyFor(cfg, logger))"); n != 2 {
		t.Errorf("loadConfig installs the outbound-address rule on both arms (file loaded, defaults), found %d", n)
	}
	helper := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func outboundPolicyFor("))
	if !strings.Contains(helper, "cfg.OutboundPolicy()") || !strings.Contains(helper, "p.Allow()") {
		t.Error("outboundPolicyFor builds the rule from the configuration and logs the effective trust")
	}
}
