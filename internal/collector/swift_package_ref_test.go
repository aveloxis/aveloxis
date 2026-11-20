// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// TestSwiftSitesAgreeOnOnePurl pins the invariant across the three
// consumers: the manifest extractor's name, the resolved pin's name and
// namespace, and the direct writer's purl all come from one parse.
func TestSwiftSitesAgreeOnOnePurl(t *testing.T) {
	line := `.package(url: "https://user:tok@GitHub.com/apple/swift-nio.git", from: "2.60.0"),`
	if got := manifest.ExtractSwiftPackageName(line); got != "swift-nio" {
		t.Errorf("extractSwiftPackageName = %q, want swift-nio", got)
	}
	ns, repo := swiftRepoNamespace("git@github.com:apple/swift-nio.git")
	if ns != "github.com/apple" || repo != "swift-nio" {
		t.Errorf("swiftRepoNamespace = %q/%q", ns, repo)
	}
	if got := pkgid.PurlForPackage("swiftpm", ns, repo, "2.60.0"); got != "pkg:swift/github.com/apple/swift-nio@2.60.0" {
		t.Errorf("transitive purl = %q", got)
	}
}
