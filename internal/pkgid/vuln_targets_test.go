// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

func TestPurlWithVersion(t *testing.T) {
	cases := []struct{ purl, version, want string }{
		{"pkg:npm/express@4.18.0", "4.19.2", "pkg:npm/express@4.19.2"},
		{"pkg:npm/@scope/name@1.0.0", "2.0.0", "pkg:npm/@scope/name@2.0.0"},
		{"pkg:npm/@scope/name", "2.0.0", "pkg:npm/@scope/name@2.0.0"}, // scope @ is not a version separator
		{"pkg:pypi/flask@", "2.3.3", "pkg:pypi/flask@2.3.3"},
	}
	for _, c := range cases {
		if got := WithVersion(c.purl, c.version); got != c.want {
			t.Errorf("purlWithVersion(%q, %q) = %q, want %q", c.purl, c.version, got, c.want)
		}
	}
}
