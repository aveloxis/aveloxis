// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/outboundtest"
	"github.com/aveloxis/aveloxis/internal/testdb"
	"github.com/aveloxis/aveloxis/internal/testdb/prepare"
)

// TestMain runs this package's DB-tier tests in a database of their own,
// created from AVELOXIS_TEST_DB, prepared as an established deployment
// (migrated, bootstrap admin signed up) before any test, checked with the
// data-verify battery after green tests, and dropped (internal/testdb), so
// rows an earlier or concurrent run left behind can never reach them.
// The tests of this package probe, clone or scan loopback servers: the
// outbound-address rule (0.29.132) trusts loopback here and resolves names
// deterministically (internal/outboundtest, the one owner) so no test
// resolves for real. A test of the rule itself sets its own policy and
// restores this one.
func TestMain(m *testing.M) {
	collector.SetOutboundPolicy(outboundtest.Policy())
	// The prose width is the default for every test (0.29.136, L10 round 1
	// F1): a developer's exported COLUMNS, or tmux's, must not change what
	// the printers render; the tests that drive COLUMNS set it themselves.
	os.Unsetenv("COLUMNS")
	os.Exit(testdb.Main(m, prepare.Deployment, prepare.Verify))
}

// restoreOutboundPolicy puts the package's outbound policy back after a
// test that ran loadConfig, which installs the configuration's (strict, the
// real resolver) for the process — every later boundary test would then
// resolve for real, order-dependently (L10 round 2 of 0.29.134; the class
// of feedback_process_wide_state_in_tests). Pinned: every test body that
// calls loadConfig registers it.
func restoreOutboundPolicy(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { collector.SetOutboundPolicy(outboundtest.Policy()) })
}
