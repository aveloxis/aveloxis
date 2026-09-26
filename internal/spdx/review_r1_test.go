// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"sort"
	"strings"
	"testing"
	"time"
)

// v0.29.67 review round 1.

// TestDocumentRefNeverPanics — a bare DocumentRef-X reached go-spdx, which
// dereferences nil on it: every SBOM generation and every license-table
// request for a repository with such a registry string panicked. A
// DocumentRef names a license in ANOTHER document, which Aveloxis never
// references, so it is not a license here at all.
func TestDocumentRefNeverPanics(t *testing.T) {
	for _, in := range []string{"DocumentRef-X", "DocumentRef-a:LicenseRef-b", "MIT OR DocumentRef-X", "(DocumentRef-X)"} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%q panicked: %v", in, r)
				}
			}()
			if err := Validate(in); err == nil {
				t.Errorf("Validate(%q) = nil; a DocumentRef is not a license in this document", in)
			}
			if OSIApproved(in) && !strings.HasPrefix(in, "MIT") {
				t.Errorf("OSIApproved(%q) = true", in)
			}
			_ = NormalizeExpression(in, identity)
			_ = DisplayKey(in)
		}()
	}
}

// osiIDs returns n distinct OSI-approved, non-deprecated IDs.
func osiIDs(n int) []string {
	var ids []string
	for id, l := range licenses {
		if l.osi && !l.deprecated {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids[:n]
}

// TestOSIApprovedIsLinear — go-spdx's Satisfies expands an expression to
// disjunctive normal form before testing it: an AND of eighteen two-way
// ORs took three minutes, on a path evaluated per distinct license on every
// license-table request, from registry-controlled text. OSI status is now
// evaluated on the parsed tree (OR any, AND all), linear in the expression.
func TestOSIApprovedIsLinear(t *testing.T) {
	ids := osiIDs(80)
	var pairs []string
	for i := 0; i < len(ids); i += 2 {
		pairs = append(pairs, "("+ids[i]+" OR "+ids[i+1]+")")
	}
	expr := strings.Join(pairs, " AND ")
	done := make(chan bool, 1)
	go func() { done <- OSIApproved(expr) && Validate(expr) == nil }()
	select {
	case ok := <-done:
		if !ok {
			t.Errorf("an AND of 40 OSI-approved choices must be approved and valid")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OSIApproved/Validate on an AND of 40 two-way ORs did not finish in 5s: exponential evaluation")
	}
}

// TestLicenseRefCannotImpersonate — the old evaluator renamed IDs go-spdx
// lacks to LicenseRef-<id> and the family labels to LicenseRef-aveloxis-...,
// and approved those names: a registry's own LicenseRef with the same
// spelling read OSI-approved.
func TestLicenseRefCannotImpersonate(t *testing.T) {
	for _, in := range []string{"LicenseRef-BSD-ask-to-endorse", "LicenseRef-aveloxis-family-LGPL", "LicenseRef-MIT"} {
		if OSIApproved(in) {
			t.Errorf("OSIApproved(%q) = true; a LicenseRef is never OSI-approved", in)
		}
	}
	if !OSIApproved("BSD-ask-to-endorse") {
		t.Error("the real ID stays approved")
	}
}

// TestJoinExpressions — the ONE way to combine license expressions under
// one operator: every compound part is parenthesized, so a joined part's
// own OR or AND cannot regroup (scancode's per-file expressions were joined
// with a bare " AND ": "BSD-3-Clause AND MIT OR Apache-2.0" parses as
// (BSD-3-Clause AND MIT) OR Apache-2.0).
func TestJoinExpressions(t *testing.T) {
	for _, tc := range []struct {
		op    string
		parts []string
		want  string
	}{
		{"AND", []string{"BSD-3-Clause", "MIT OR Apache-2.0"}, "BSD-3-Clause AND (MIT OR Apache-2.0)"},
		{"AND", []string{"MIT"}, "MIT"},
		{"AND", []string{"", " MIT ", ""}, "MIT"},
		{"AND", nil, ""},
		{"OR", []string{"MIT AND BSD-3-Clause", "Apache-2.0"}, "(MIT AND BSD-3-Clause) OR Apache-2.0"},
		{"OR", []string{"GPL-2.0-only WITH Classpath-exception-2.0", "MIT"}, "GPL-2.0-only WITH Classpath-exception-2.0 OR MIT"},
		{"OR", []string{"mit or apache-2.0", "ISC"}, "(mit or apache-2.0) OR ISC"},
		// A slash is read as OR, the lowest precedence: it must be grouped too.
		{"AND", []string{"BSD-3-Clause", "MIT/Apache-2.0"}, "BSD-3-Clause AND (MIT/Apache-2.0)"},
	} {
		if got := JoinExpressions(tc.op, tc.parts); got != tc.want {
			t.Errorf("JoinExpressions(%s, %q) = %q, want %q", tc.op, tc.parts, got, tc.want)
		}
	}
	joined := JoinExpressions("AND", []string{"BSD-3-Clause", "MIT OR Apache-2.0"})
	if !OSIApproved(joined) || OSIApproved(JoinExpressions("AND", []string{"CC0-1.0", "MIT OR Apache-2.0"})) {
		t.Error("the joined AND must need every part approved")
	}
}
