// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// TestWireValidPurlRejectsEmptyNameAfterVersionSplit: the gate splits the
// version at the last '@' like OSV does, so a name region that is empty or
// ends in '/' after the split is refused before it can fail the batch.
func TestWireValidPurlRejectsEmptyNameAfterVersionSplit(t *testing.T) {
	for _, p := range []string{
		Purl("npm", "registry.npmjs.org/", "jest/transform/26.0.1"),
		Purl("npm", "registry.npmmirror.com/", "types/ms/0.7.31"),
		Purl("npm", "@pkgr/utils@https://registry.npmjs.org/", "pkgr/utils/-/utils-2.4.2.tgz"),
		Purl("npm", "@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/", "10.7.1"),
		"pkg:npm/registry.npmjs.org/@jest/transform/26.0.1",
		"pkg:npm/a//b@1.0.0",
	} {
		if WireValid(p) {
			t.Errorf("wireValidPurl(%q) = true, want false", p)
		}
	}
	for _, p := range []string{
		Purl("npm", "@babel/core", "7.24.0"),
		Purl("npm", "@babel/core", ""),
		"pkg:npm/@babel/core@7.24.0",
		Purl("golang", "github.com/spf13/cobra", "v1.8.0"),
		Purl("maven", "org.apache.commons/commons-lang3", "3.12.0"),
		"pkg:githubactions/actions/checkout@v4",
	} {
		if !WireValid(p) {
			t.Errorf("wireValidPurl(%q) = false, want true", p)
		}
	}
}
