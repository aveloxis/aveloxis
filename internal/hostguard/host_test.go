// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostguard

import "testing"

// One spelling of a host, however it was written (review round 1 of the
// vulnerability program's first part): the distrust list, the console, the
// daily check and the fetch path must all mean the same host by it.
func TestNormalizeIsOneSpelling(t *testing.T) {
	for in, want := range map[string]string{
		"Nexus.Corp.Example":         "nexus.corp.example",
		"  nexus.corp.example  ":     "nexus.corp.example",
		"nexus.corp.example.":        "nexus.corp.example",
		"nexus.corp.example:8443":    "nexus.corp.example:8443", // a port is part of it
		"nexus.corp.example.:8443":   "nexus.corp.example:8443", // the dot is before the port
		"nexus.corp.example:443":     "nexus.corp.example",      // the port every request here uses unless told otherwise
		"NEXUS.corp.example.:443":    "nexus.corp.example",
		"repo.example:8443.":         "repo.example:8443",     // a dot after the port, as pasted
		"bücher.example":             "xn--bcher-kva.example", // what is dialed
		"BÜCHER.example:8443":        "xn--bcher-kva.example:8443",
		"xn--bcher-kva.example":      "xn--bcher-kva.example",
		"10.0.0.5:8081":              "10.0.0.5:8081",
		"[2001:db8::1]:8443":         "[2001:db8::1]:8443",
		"[2001:db8::1]:443":          "[2001:db8::1]",
		"[2001:DB8::1]":              "[2001:db8::1]",
		"":                           "",
		"   ":                        "",
		"under_score.example":        "under_score.example", // not a registrable name; kept as written, lower case
		"nexus.corp.example:notport": "nexus.corp.example:notport",
		// Round 2: spellings that DIAL the same service must be one
		// spelling, or a distrust is sidestepped by writing the other.
		"evil.example:0443":      "evil.example",      // Go dials port 443
		"evil.example:08443":     "evil.example:8443", // leading zeros
		"evil.example:000":       "evil.example:0",
		"[0:0:0:0:0:0:0:1]":      "[::1]",
		"[0:0:0:0:0:0:0:1]:8443": "[::1]:8443",
		"[2001:DB8:0:0::1]:0443": "[2001:db8::1]",
		// Round 3: an IPv4 address written in its IPv6-mapped form DIALS the
		// IPv4 address, so it is that host (this table said otherwise).
		"[::ffff:192.0.2.1]:8443": "192.0.2.1:8443",
		"[::ffff:192.0.2.1]":      "192.0.2.1",
		"[::FFFF:C000:201]:0443":  "192.0.2.1",
		// A zone is not part of the host's identity.
		"[fe80::1%eth0]:8443": "[fe80::1]:8443",
		"[fe80::1%25eth0]":    "[fe80::1]", // as a URL spells the zone
		"192.0.2.1:0443":      "192.0.2.1",
		"evil.example:65536":  "evil.example:65536", // not a port: kept as written
	} {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
		if got := Normalize(Normalize(in)); got != want {
			t.Errorf("Normalize is not stable on %q: %q", in, got)
		}
	}
}

func TestOfURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://Nexus.Corp.Example:8443/repository/public/": "nexus.corp.example:8443",
		"https://nexus.corp.example/repo":                    "nexus.corp.example",
		"https://nexus.corp.example:443/repo":                "nexus.corp.example",
		"https://user:secret@nexus.corp.example:8443/repo":   "nexus.corp.example:8443",
		"https://bücher.example/m2":                          "xn--bcher-kva.example",
		"not a url with spaces":                              "",
		"://":                                                "",
		"/a/local/path":                                      "",
	} {
		if got := OfURL(in); got != want {
			t.Errorf("OfURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// A host distrusted under one spelling is distrusted under every other.
func TestDistrustHoldsAcrossSpellings(t *testing.T) {
	list := NewSnapshot([]string{"Nexus.Corp.Example.:8443", "plain.example:443", "bücher.example"})
	for _, h := range []string{"nexus.corp.example:8443", "NEXUS.CORP.EXAMPLE:8443", "plain.example", "plain.example.", "xn--bcher-kva.example", OfURL("https://plain.example/m2")} {
		if MayUse(list, h) {
			t.Errorf("%q may be used; it is on the list under another spelling", h)
		}
	}
	// A different port is a different host.
	if !MayUse(list, "nexus.corp.example") || !MayUse(list, "nexus.corp.example:9000") {
		t.Error("distrusting one port distrusted another")
	}
}
