// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import "testing"

// TestIsLoopbackHost pins which Host headers may be trusted to build an
// emailed confirmation link. Everything non-loopback is attacker-reachable:
// the client sets Host, and a proxy forwarding it (nginx's usual
// `proxy_set_header Host $host`) passes it straight through. Trusting one
// let an attacker mail a victim a link to the attacker's server carrying
// the victim's confirmation token (Copilot, PR #207; CodeQL alert 197).
func TestIsLoopbackHost(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"localhost:8082", true},
		{"LOCALHOST:8082", true},
		{"127.0.0.1", true},
		{"127.0.0.1:8082", true},
		{"127.1.2.3", true},
		{"[::1]:8082", true},
		{"::1", true},
		// Everything below is attacker-controllable.
		{"evil.example.com", false},
		{"evil.example.com:443", false},
		{"chaoss.tv", false},
		{"localhost.evil.example.com", false},
		{"127.0.0.1.evil.example.com", false},
		{"0.0.0.0", false},
		{"10.0.0.5", false},
		{"", false},
	} {
		t.Run(tc.host, func(t *testing.T) {
			if got := isLoopbackHost(tc.host); got != tc.want {
				t.Errorf("isLoopbackHost(%q) = %v, want %v", tc.host, got, tc.want)
			}
		})
	}
}

// TestBracketBareIPv6 pins the URL-authority normalization for the bare
// IPv6 form isLoopbackHost accepts: "::1" concatenated into a URL would
// yield the invalid "http://::1/..." — IPv6 literals in authorities must
// be bracketed (RFC 3986 §3.2.2; Copilot review on PR #207).
func TestBracketBareIPv6(t *testing.T) {
	for _, tc := range []struct{ host, want string }{
		{"::1", "[::1]"},
		{"[::1]:8082", "[::1]:8082"}, // already bracketed — unchanged
		{"localhost", "localhost"},
		{"localhost:8082", "localhost:8082"},
		{"127.0.0.1", "127.0.0.1"},
		{"127.0.0.1:8082", "127.0.0.1:8082"},
		{"", ""},
	} {
		t.Run(tc.host, func(t *testing.T) {
			if got := bracketBareIPv6(tc.host); got != tc.want {
				t.Errorf("bracketBareIPv6(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}
