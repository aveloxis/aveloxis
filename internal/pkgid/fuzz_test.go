// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// FuzzPurlHelpers drives the purl construction/rewrite/validation trio
// on arbitrary input. Contract: never panic. NOTE: wire-validity of
// Purl output is deliberately NOT asserted — EscapeSegment
// handles the URL-reserved set, and wireValidPurl is the designed
// last-line gate for anything else (control bytes in garbage names);
// asserting the stronger property would flag designed behavior.
func FuzzPurlHelpers(f *testing.F) {
	f.Add("pypi", "flask", "2.5.0")
	f.Add("npm", "@babel/core", "7.0.0")
	f.Add("maven", "org.apache:commons", "1.0")
	f.Add("gem", "tzinfo-data", "platforms: %i[mingw]")
	f.Fuzz(func(t *testing.T, ecosystem, name, version string) {
		p := PurlForPackage(ecosystem, "", name, version)
		_ = WireValid(p)
		_ = WithVersion(p, version)
		_ = ReplaceVersion(p, version)
		_ = ReplaceVersion(name, version) // arbitrary non-purl input
		_ = SelfPurl(ecosystem, name)
	})
}
