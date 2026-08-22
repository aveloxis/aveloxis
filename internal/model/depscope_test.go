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

// v0.27.155 (round 34): the SBOM duplicate-scope fold.
func TestStrongerScope(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{ScopeDev, ScopeRuntime, ScopeRuntime},   // runtime beats dev regardless of order
		{ScopeRuntime, ScopeDev, ScopeRuntime},   //
		{ScopeDev, "", ""},                       // "" is runtime-class
		{ScopeDev, ScopeOptional, ScopeOptional}, // optional beats dev
		{ScopeOptional, ScopeRuntime, ScopeRuntime},
		{ScopeTest, ScopeBuild, ScopeTest},           // equal rank: first wins
		{ScopeRuntime, "", ScopeRuntime},             // equal rank: first wins
		{ScopeDev, "future-unknown", "future-unknown"}, // unknown reads as runtime (visibility)
	}
	for _, c := range cases {
		if got := StrongerScope(c.a, c.b); got != c.want {
			t.Errorf("StrongerScope(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}
