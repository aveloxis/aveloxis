// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package outboundtest is the outbound-address policy the test mains of the
// packages that probe, clone or scan install (0.29.134; the one owner of
// three copies, L18): loopback trusted through the same allowlist an
// operator would write, and names resolved deterministically so no test
// resolves for real — localhost to loopback, a name under .internal to a
// private address (a redirect target the rule must refuse), every other
// name to a TEST-NET address. A live canary (AVELOXIS_TEST_NETWORK=1)
// installs its own policy and resolves for real.
package outboundtest

import (
	"context"
	"net/netip"
	"strings"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// Policy is the loopback-trusting policy with the deterministic resolver.
func Policy() *platform.OutboundPolicy {
	loopback, err := platform.NewOutboundPolicy([]string{"127.0.0.0/8", "::1/128"})
	if err != nil {
		panic(err)
	}
	return loopback.WithResolver(Resolve)
}

// Resolve is the deterministic resolver: localhost → 127.0.0.1,
// *.internal → 10.0.0.5, anything else → 203.0.113.1.
func Resolve(_ context.Context, host string) ([]netip.Addr, error) {
	switch {
	case host == "localhost":
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	case strings.HasSuffix(host, ".internal"):
		return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
	}
	return []netip.Addr{netip.MustParseAddr("203.0.113.1")}, nil
}
