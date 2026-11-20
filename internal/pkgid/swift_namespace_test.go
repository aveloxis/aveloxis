// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

func TestPurlForPackageUsesTheNamespace(t *testing.T) {
	if got := PurlForPackage("swiftpm", "github.com/Alamofire", "Alamofire", "5.8.0"); got != "pkg:swift/github.com/Alamofire/Alamofire@5.8.0" {
		t.Errorf("swift with namespace = %q", got)
	}
	if got := PurlForPackage("swiftpm", "", "alamofire", "5.8.0"); got != "" {
		t.Errorf("swift without namespace must stay unminted, got %q", got)
	}
	if !WireValid(PurlForPackage("swiftpm", "github.com/Alamofire", "Alamofire", "5.8.0")) {
		t.Error("the namespaced swift purl must pass the wire gate")
	}
	// The namespace is generic: a maven group supplied separately works too,
	// and a type that never needs one ignores an empty namespace.
	if got := PurlForPackage("maven", "org.example", "artifact", "1.0"); got != "pkg:maven/org.example/artifact@1.0" {
		t.Errorf("maven with namespace = %q", got)
	}
	if got := PurlForPackage("npm", "", "express", "4.18.0"); got != "pkg:npm/express@4.18.0" {
		t.Errorf("npm = %q", got)
	}
}
