// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/testdb"
	"github.com/aveloxis/aveloxis/internal/testdb/prepare"
)

// TestMain runs this package's DB-tier tests in a database of their own,
// created from AVELOXIS_TEST_DB, prepared as an established deployment
// (migrated, bootstrap admin signed up) before any test, checked with the
// data-verify battery after green tests, and dropped (internal/testdb), so
// rows an earlier or concurrent run left behind can never reach them.
// The tests of this package probe and clone loopback servers: the
// outbound-address rule (0.29.132) trusts loopback here through the same
// allowlist an operator would write, and resolves names deterministically
// (localhost to loopback, every other name to a TEST-NET address) so a
// fixture naming github.com or gitlab.com needs no DNS (Copilot's
// thirteenth review of PR #246: the rule's preflight made the fake-scorecard
// tests resolve for real). A test of the rule itself sets its own policy and
// restores this one.
func TestMain(m *testing.M) {
	loopback, err := platform.NewOutboundPolicy([]string{"127.0.0.0/8", "::1/128"})
	if err != nil {
		panic(err)
	}
	SetOutboundPolicy(loopback.WithResolver(testOutboundResolver))
	os.Exit(testdb.Main(m, prepare.Deployment, prepare.Verify))
}

// testOutboundResolver is the deterministic resolver of this package's
// outbound policies: localhost to loopback, a name under .internal to a
// private address (a redirect target the rule must refuse), every other
// name to a TEST-NET address. A live canary (AVELOXIS_TEST_NETWORK=1)
// installs its own policy and resolves for real.
func testOutboundResolver(_ context.Context, host string) ([]netip.Addr, error) {
	switch {
	case host == "localhost":
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	case strings.HasSuffix(host, ".internal"):
		return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
	}
	return []netip.Addr{netip.MustParseAddr("203.0.113.1")}, nil
}
