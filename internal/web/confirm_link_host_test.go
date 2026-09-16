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
