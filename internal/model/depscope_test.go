// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "testing"

func TestIsRuntimeScope(t *testing.T) {
	for _, s := range NonRuntimeScopes {
		if IsRuntimeScope(s) {
			t.Errorf("%q must be non-runtime", s)
		}
	}
	for _, s := range []string{"", ScopeRuntime, "future-unknown-value"} {
		if !IsRuntimeScope(s) {
			t.Errorf("%q must read as runtime — fail toward visibility, never toward hiding a dependency", s)
		}
	}
}
