// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
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
// allowlist an operator would write. A test of the rule itself sets the
// strict policy and restores this one.
func TestMain(m *testing.M) {
	loopback, err := platform.NewOutboundPolicy([]string{"127.0.0.0/8", "::1/128"})
	if err != nil {
		panic(err)
	}
	SetOutboundPolicy(loopback)
	os.Exit(testdb.Main(m, prepare.Deployment, prepare.Verify))
}
