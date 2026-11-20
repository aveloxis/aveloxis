// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import "testing"

func TestSplitNPMSpecUsesFirstAtAfterScope(t *testing.T) {
	cases := []struct {
		spec, name, ref string
		ok              bool
	}{
		{"lodash@4.17.21", "lodash", "4.17.21", true},
		{"@babel/core@^7.0.0", "@babel/core", "^7.0.0", true},
		{"@pkgr/utils@https://registry.npmjs.org/@pkgr/utils/-/utils-2.4.2.tgz", "@pkgr/utils", "https://registry.npmjs.org/@pkgr/utils/-/utils-2.4.2.tgz", true},
		{"@npmcli/config@patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/x.patch", "@npmcli/config", "patch:@npmcli/config@npm%3A10.7.1#~/.yarn/patches/x.patch", true},
		{"@mui/material@file:../node_modules/@mui/material", "@mui/material", "file:../node_modules/@mui/material", true},
		{"lodash", "", "", false},
		{"@scope/only", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		name, ref, ok := SplitNPMSpec(tc.spec)
		if name != tc.name || ref != tc.ref || ok != tc.ok {
			t.Errorf("splitNPMSpec(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.spec, name, ref, ok, tc.name, tc.ref, tc.ok)
		}
	}
}
