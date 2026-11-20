// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// TestWireValidPurlRequiresNamespaceWhereOSVDoes pins the wire gate: a
// namespace-required type without one is not sendable; types where the
// spec makes it optional are unchanged.
func TestWireValidPurlRequiresNamespaceWhereOSVDoes(t *testing.T) {
	cases := []struct {
		purl string
		want bool
	}{
		// the three types OSV rejected in the 2026-09-22 log
		{"pkg:swift/alamofire@5.8.0", false},
		{"pkg:swift/github.com/Alamofire/Alamofire@5.8.0", true},
		{"pkg:maven/artifact@1.0", false},
		{"pkg:maven/org.apache.commons/commons-lang3@3.12.0", true},
		{"pkg:composer/php@8.2", false},
		{"pkg:composer/ext-mbstring@8.2", false},
		{"pkg:composer/laravel/framework@10.0.0", true},
		// an empty namespace segment is not a namespace
		{"pkg:maven//artifact@1.0", false},
		{"pkg:swift/@1.0", false},
		// optional-namespace types keep their single-segment names
		{"pkg:npm/express@4.18.0", true},
		{"pkg:npm/%40scope/name@1.0.0", true},
		{"pkg:pypi/flask@3.0.0", true},
		{"pkg:golang/golang.org/x/text@v0.14.0", true},
		{"pkg:cargo/serde@1.0.0", true},
		{"pkg:nuget/Newtonsoft.Json@13.0.3", true},
		// versionless (self-advisory) forms follow the same rule
		{"pkg:swift/alamofire", false},
		{"pkg:composer/vendor/pkg", true},
	}
	for _, tc := range cases {
		if got := WireValid(tc.purl); got != tc.want {
			t.Errorf("wireValidPurl(%q) = %v, want %v", tc.purl, got, tc.want)
		}
	}
}
