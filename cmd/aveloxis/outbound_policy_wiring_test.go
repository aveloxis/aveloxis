// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
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
	if !strings.Contains(helper, "cfg.OutboundPolicy()") || !strings.Contains(helper, "p.Allow()") || !strings.Contains(helper, "collector.ProxyHosts()") {
		t.Error("outboundPolicyFor builds the rule from the configuration and logs the effective trust and the configured proxies")
	}
}

// The three consumers of the probe answer the rule's refusal under the
// phrase the operator greps for (L10 round 2), as a skip — never gone.
func TestProbeConsumersNameTheOutboundRefusal(t *testing.T) {
	for _, c := range []struct{ dir, sig string }{
		{"cmd/aveloxis", "func runMarkGoneRepos("},
		{"cmd/aveloxis", "func runReconcileRepos("},
		{"internal/scheduler", "func (s *Scheduler) runGoneRecheck("},
	} {
		body := srctest.StripGoComments(srctest.PackageFunc(t, c.dir, c.sig))
		if !strings.Contains(body, "platform.ErrPrivateAddress") || !strings.Contains(body, "repository host is a private address") {
			t.Errorf("%s %s: no arm for platform.ErrPrivateAddress naming the refusal", c.dir, c.sig)
		}
	}
}

// import-from-augur's existence HEAD consults the rule before probing a
// row's host (L10 round 2): a poisoned Augur row is not probed from here.
func TestImportFromAugurConsultsTheOutboundRule(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runImportFromAugur("))
	if !strings.Contains(body, "collector.RefuseRepositoryHost(") || strings.Index(body, "collector.RefuseRepositoryHost(") > strings.Index(body, "verifyRepoExists(") {
		t.Error("runImportFromAugur must consult collector.RefuseRepositoryHost before verifyRepoExists")
	}
	// A resolver failure is a verification failure (counted, nonzero exit),
	// not a skip (round 6).
	at := strings.Index(body, "collector.RefuseRepositoryHost(")
	arm := body[at : strings.Index(body[at:], "verifyRepoExists(")+at]
	if !strings.Contains(arm, "errors.Is(err, platform.ErrPrivateAddress)") || !strings.Contains(arm, "failed++") {
		t.Error("runImportFromAugur must count a non-refusal error from the rule as a failure, not a skip")
	}
}

// Every test body that runs loadConfig restores the package's outbound
// policy (loadConfig installs the configuration's for the process).
func TestEveryLoadConfigTestRestoresTheOutboundPolicy(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "cmd", "aveloxis")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sites := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for fn, body := range srctest.FuncParts(t, srctest.StripGoComments(string(b))) {
			if !strings.Contains(body, "loadConfig(") || fn == "restoreOutboundPolicy" || fn == "TestEveryLoadConfigTestRestoresTheOutboundPolicy" || fn == "TestLoadConfigWiresTheOutboundPolicy" {
				continue // the helper, this pin and the source pin name the call without running it
			}
			sites++
			if !strings.Contains(body, "restoreOutboundPolicy(t)") {
				t.Errorf("%s: %s runs loadConfig without restoreOutboundPolicy(t)", e.Name(), fn)
			}
		}
	}
	srctest.MinCount(t, "tests running loadConfig", sites, 4)
}
