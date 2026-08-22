// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import "strings"

// NormalizeWS collapses every whitespace run to a single space — the
// idiom previously inlined in 12 test files. Use for needles that must
// survive gofmt column re-alignment.
func NormalizeWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// ContainsNormalized reports whether haystack contains needle after
// whitespace normalization of BOTH sides — the gofmt-tolerant needle
// match (the v0.22.0 phase-5 lesson: a struct-field pin broke when an
// unrelated field addition re-aligned gofmt's columns).
func ContainsNormalized(haystack, needle string) bool {
	return strings.Contains(NormalizeWS(haystack), NormalizeWS(needle))
}
