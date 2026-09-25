// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"math/rand/v2"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"
)

// v0.29.67 review round 2.

// phraseSynonyms stands in for db's synonym map, with its operator-bearing
// keys passed as phrases.
func phraseSynonyms(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "zlib/libpng", "zlib/libpng license":
		return "Zlib"
	case "common development and distribution license":
		return "CDDL-1.0"
	case "gnu library or lesser general public license (lgpl)":
		return "LGPL"
	}
	return strings.TrimSpace(s)
}

var testPhrases = []string{"zlib/libpng", "zlib/libpng license", "common development and distribution license",
	"gnu library or lesser general public license (lgpl)"}

// TestOperatorBearingSynonymsStayWhole — the whole-string-first rule held
// only at the top level: inside a larger expression (a joined list, a
// stored " AND " row, a parenthesized part) the parser split the synonym on
// its own "and", "or" or slash.
func TestOperatorBearingSynonymsStayWhole(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"(zlib/libpng) OR MIT", "Zlib OR MIT"},
		{"MIT OR zlib/libpng", "MIT OR Zlib"},
		{"(zlib/libpng License) OR MIT", "Zlib OR MIT"},
		{"Common Development and Distribution License AND MIT", "CDDL-1.0 AND MIT"},
		{"(Common Development and Distribution License) OR MIT", "CDDL-1.0 OR MIT"},
		{"(GNU Library or Lesser General Public License (LGPL)) OR MIT", "LGPL OR MIT"},
		{"GNU Library or Lesser General Public License (LGPL) AND MIT", "LGPL AND MIT"},
		// A phrase only matches whole: "zlib/libpngX" is not the synonym.
		{"MIT OR zlib/libpngX", "MIT OR zlib/libpngX"},
		// The top-level rule is unchanged.
		{"zlib/libpng", "Zlib"},
		{"MIT/Apache-2.0", "MIT OR Apache-2.0"},
	} {
		if got := NormalizeExpression(tc.in, phraseSynonyms, testPhrases...); got != tc.want {
			t.Errorf("NormalizeExpression(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestJoinExpressionsUsesTheParsersOperators — JoinExpressions looked for
// " and " / " or " padded with spaces, while the tokenizer also splits on
// tabs and newlines: "MIT\nor Apache-2.0" joined unparenthesized and
// regrouped (it then read OSI-approved next to CC0-1.0).
func TestJoinExpressionsUsesTheParsersOperators(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		want  string
	}{
		{[]string{"MIT\nor Apache-2.0", "CC0-1.0"}, "(MIT\nor Apache-2.0) AND CC0-1.0"},
		{[]string{"MIT\tOR Apache-2.0", "CC0-1.0"}, "(MIT\tOR Apache-2.0) AND CC0-1.0"},
		{[]string{"LicenseRef-or-later", "MIT"}, "LicenseRef-or-later AND MIT"},
		{[]string{"GPL-2.0-only WITH Classpath-exception-2.0", "MIT"}, "GPL-2.0-only WITH Classpath-exception-2.0 AND MIT"},
	} {
		if got := JoinExpressions("AND", tc.parts); got != tc.want {
			t.Errorf("JoinExpressions(%q) = %q, want %q", tc.parts, got, tc.want)
		}
	}
	if OSIApproved(NormalizeExpression(JoinExpressions("AND", []string{"MIT\nor Apache-2.0", "CC0-1.0"}), identity)) {
		t.Error("(MIT OR Apache-2.0) AND CC0-1.0 must not be approved")
	}
}

// TestDisplayKeyIsNotQuadratic — sortTree re-rendered each subtree on every
// comparison; keys are now rendered once per node.
func TestDisplayKeyIsNotQuadratic(t *testing.T) {
	var b strings.Builder
	depth := 1500
	for i := 0; i < depth; i++ {
		if i%2 == 0 {
			b.WriteString("(MIT AND ")
		} else {
			b.WriteString("(ISC OR ")
		}
	}
	b.WriteString("Apache-2.0")
	b.WriteString(strings.Repeat(")", depth))
	expr := NormalizeExpression(b.String(), identity)
	counted := 0
	sortKeyRenders = func() { counted++ }
	defer func() { sortKeyRenders = func() {} }()
	_ = DisplayKey(expr)
	if counted > 4*depth {
		t.Errorf("DisplayKey rendered %d subtree keys for a %d-deep expression; want O(nodes)", counted, depth)
	}
}

// TestPlaceholderLookalikesAreNotPhrases — v0.29.67 review round 3: the
// phrase placeholders are private-use runes, and registry text that already
// contained one was read as the substituted phrase ("0 OR
// zlib/libpng" became "Zlib OR Zlib"). Input containing the placeholder rune
// is never substituted, so the look-alike is just an unknown operand.
func TestPlaceholderLookalikesAreNotPhrases(t *testing.T) {
	in := "0 OR zlib/libpng"
	if got := NormalizeExpression(in, phraseSynonyms, testPhrases...); Valid(got) {
		t.Errorf("NormalizeExpression(%q) = %q, a valid expression; the look-alike must not become a license", in, got)
	}
}

// TestOperandTexts — the name-list split (review rounds 4-8): "and"/"or"
// words only; parentheses and slashes stay inside; an and/or opening a
// version range ("or later", "or (at your option) any later version", "and
// above") is not a separator, but an and/or before a bare "any" is; "with"
// never is.
func TestOperandTexts(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"GPL-3.0-only AND Apache License 2.0 AND Proprietary", []string{"GPL-3.0-only", "Apache License 2.0", "Proprietary"}},
		{"ISC License (ISC) or MIT/X11", []string{"ISC License (ISC)", "MIT/X11"}},
		{"GPL-2.0 or later; see COPYING", []string{"GPL-2.0 or later; see COPYING"}},
		{"GPL v2 OR ANY later version", []string{"GPL v2 OR ANY later version"}},
		{"GPLv2 or (at your option) any later version", []string{"GPLv2 or (at your option) any later version"}},
		{"GPLv3 and later; see COPYING", []string{"GPLv3 and later; see COPYING"}},
		{"GPL-2.0 or (at your option) MIT", []string{"GPL-2.0", "(at your option) MIT"}},
		{"GPLv2 or (unclosed", []string{"GPLv2", "(unclosed"}},
		{"GPL-3.0-only and any code under MPL-2.0", []string{"GPL-3.0-only", "any code under MPL-2.0"}},
		{"Apache License 2.0 with LLVM Exceptions", []string{"Apache License 2.0 with LLVM Exceptions"}},
		{"MIT and", []string{"MIT"}},
		{"", nil},
	} {
		got := OperandTexts(tc.in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("OperandTexts(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestLicenseRefPrefixIsCanonicalized — mcp-gopls review (A5): a lowercase
// "licenseref-foo" parsed but kept its spelling, so "licenseref-foo OR MIT"
// failed Validate and the SBOM wrote NOASSERTION for a valid expression.
// The prefix is canonicalized; the idstring keeps its own spelling.
func TestLicenseRefPrefixIsCanonicalized(t *testing.T) {
	got := NormalizeExpression("licenseref-Foo OR MIT", identity)
	if got != "LicenseRef-Foo OR MIT" {
		t.Errorf("NormalizeExpression = %q, want LicenseRef-Foo OR MIT", got)
	}
	if !Valid(got) {
		t.Errorf("%q must validate", got)
	}
	if got := NormalizeExpression("LICENSEREF-x", identity); got != "LicenseRef-x" {
		t.Errorf("single ref = %q", got)
	}
	// Review round 15 C4: the prefix is matched on its own bytes, not by
	// lowercasing the whole leaf and slicing at the ASCII length. U+0130
	// lowercases from two bytes to one, which shifted the slice and minted
	// "LicenseRef--x". A non-ASCII prefix is not the SPDX prefix at all.
	if got := NormalizeExpression("L\u0130CENSEREF-x", identity); strings.HasPrefix(got, "LicenseRef-") {
		t.Errorf("a non-ASCII look-alike prefix = %q, want it left unresolved", got)
	}
}

// TestValidCostIsLinear — review round 22 S2: Validate handed the whole
// expression to go-spdx, whose validation is quadratic in the number of
// terms, and registry license strings reach it (the SBOM exporters, the
// fingerprint's SPDX tags). Four times the terms must cost well under
// sixteen times the time (linear about four, quadratic sixteen; the
// fastest of five runs).
func TestValidCostIsLinear(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	build := func(n int) string { return strings.Repeat("GPL-2.0-only OR ", n) + "MIT" }
	fastest := func(s string) time.Duration {
		best := time.Duration(1<<63 - 1)
		for range 5 {
			start := time.Now()
			if !Valid(s) {
				t.Fatalf("Valid rejects a valid OR chain of %d bytes", len(s))
			}
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	small, large := build(2000), build(8000)
	ts, tl := fastest(small), fastest(large)
	if tl > 8*ts {
		t.Errorf("Valid is superlinear: %v for %d bytes, %v for %d bytes", ts, len(small), tl, len(large))
	}
}

// TestValidStillAsksTheLibraryPerLeaf — review round 22 moved go-spdx from
// the whole expression to each leaf; expressions our grammar accepts but the
// library rejects must stay invalid: a LicenseRef with an exception (round 1
// s5) and an exception the library does not know (mcp-gopls review A2).
func TestValidStillAsksTheLibraryPerLeaf(t *testing.T) {
	for _, e := range []string{
		"LicenseRef-X WITH Classpath-exception-2.0",
		"MIT WITH Spelling-Provider-LGPL-exception",
		"Apache-2.0 OR (LicenseRef-X WITH Classpath-exception-2.0)",
	} {
		if Valid(e) {
			t.Errorf("Valid(%q) = true; the library rejects this leaf", e)
		}
	}
	if !Valid("GPL-2.0-only WITH Classpath-exception-2.0 OR (MIT AND Apache-2.0)") {
		t.Error("a valid compound expression is rejected")
	}
}

// TestValidMatchesTheWholeExpressionVerdict — review round 23 S2: go-spdx
// rejects "ID+ WITH exception" as a one-term expression but accepts it inside
// a compound one (SPDX Annex D allows it), so asking the library about each
// leaf alone turned valid compounds invalid. Validate must give the verdict
// the library gives the whole expression (its v0.29.67 pre-round-22
// behaviour), on generated expressions of every leaf shape.
func TestValidMatchesTheWholeExpressionVerdict(t *testing.T) {
	ids := []string{"MIT", "Apache-2.0", "GPL-2.0-only", "GPL-2.0", "GD", "UCAR", "ADSL", "LicenseRef-X", "EFL-1.0"}
	excs := []string{"", "Classpath-exception-2.0", "LLVM-exception", "GStreamer-exception-2008", "LLGPL"}
	var leaves []string
	for _, id := range ids {
		for _, plus := range []string{"", "+"} {
			if plus != "" && strings.HasPrefix(id, "LicenseRef-") {
				continue
			}
			for _, e := range excs {
				l := id + plus
				if e != "" {
					l += " WITH " + e
				}
				leaves = append(leaves, l)
			}
		}
	}
	checked := 0
	for i, a := range leaves {
		for j, b := range leaves {
			if (i*len(leaves)+j)%3 != 0 {
				continue // a third of the pairs keeps the test fast
			}
			for _, e := range []string{a + " AND " + b, "(" + a + " OR " + b + ") AND MIT"} {
				if _, err := parse(e, true, nil); err != nil {
					continue
				}
				checked++
				want := libraryValid(e) && !unofficialPlus(e)
				if got := Valid(e); got != want {
					t.Errorf("Valid(%q) = %v, want %v (the library's whole-expression verdict, and no unofficial \"+\")", e, got, want)
				}
			}
		}
	}
	if checked < len(leaves) {
		t.Fatalf("only %d expressions checked", checked)
	}
	// "ID+" where "ID+" is not itself a list ID is invalid everywhere: the
	// official SPDX tools reject it (round 24; TestValidFollowsTheOfficialToolsOnPlus).
}

// TestNestedExpressionCostIsLinear — review round 23 C1: parsing flattened a
// same-operator chain by copying at every nesting level, render built each
// level's string, and DisplayKey's keys contained their subtrees', so deep
// nesting was quadratic (0.48 s at 180 KB). Four times the depth must cost
// well under sixteen times the time.
//
// The garbage collector is paused while measuring: the parser recurses once
// per nesting level, and every collection scans the whole goroutine stack,
// which adds a cost growing with depth times collections (measured about
// 20 ms at 300 KB of nesting, against the 0.5 s quadratic this pins). The
// test pins the algorithm; that runtime cost is recorded in the ledger.
func TestNestedExpressionCostIsLinear(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	nest := func(n int, ops ...string) string {
		var b strings.Builder
		for i := range n {
			b.WriteString("(MIT " + ops[i%len(ops)] + " ")
		}
		b.WriteString("MIT")
		b.WriteString(strings.Repeat(")", n))
		return b.String()
	}
	for name, f := range map[string]func(string){
		"Valid":           func(s string) { Valid(s) },
		"ParseExpression": func(s string) { ParseExpression(s, strings.TrimSpace) },
		"DisplayKey":      func(s string) { DisplayKey(s) },
	} {
		for _, ops := range [][]string{{"OR"}, {"OR", "AND"}} {
			fastest := func(s string) time.Duration {
				best := time.Duration(1<<63 - 1)
				for range 5 {
					start := time.Now()
					f(s)
					if d := time.Since(start); d < best {
						best = d
					}
				}
				return best
			}
			small, large := nest(8000, ops...), nest(32000, ops...)
			ts, tl := fastest(small), fastest(large)
			if tl > 8*ts {
				t.Errorf("%s, nested %v: superlinear: %v for %d bytes, %v for %d bytes", name, ops, ts, len(small), tl, len(large))
			}
		}
	}
}

// unofficialPlus reports whether an expression has "ID+" that the official
// SPDX tools reject: they accept "ID+" exactly when "ID-or-later" is a list ID
// (round 25, checked against license-expression for all 740 list IDs).
func unofficialPlus(e string) bool {
	for _, w := range strings.Fields(strings.NewReplacer("(", " ", ")", " ").Replace(e)) {
		if strings.HasSuffix(w, "+") && !IsLicenseID(w) && !IsLicenseID(strings.TrimSuffix(w, "+")+"-or-later") {
			return true
		}
	}
	return false
}

// TestValidFollowsTheOfficialToolsOnPlus — review rounds 24-25: SPDX Annex D
// allows "ID+" on any ID, but the official SPDX tools (license-expression,
// behind pyspdxtools) accept "ID+" exactly when "ID-or-later" is a list ID
// ("GPL-2.0+", "AGPL-3.0+", "GFDL-1.3+" yes; "MIT+", "Apache-2.0+" no), alone
// or in a compound; checked for all 740 list IDs on 2026-09-24. The SBOMs are
// held to those tools, so Validate follows them; the SBOM then says
// NOASSERTION.
func TestValidFollowsTheOfficialToolsOnPlus(t *testing.T) {
	for e, want := range map[string]bool{
		"GPL-2.0+":                              true,
		"LGPL-2.1+":                             true,
		"GPL-3.0+":                              true,
		"GPL-2.0+ WITH Classpath-exception-2.0": true,
		// Round 25: the tools also accept "ID+" where "ID-or-later" is a list
		// ID; these were NOASSERTION for one round.
		"AGPL-3.0+":                              true,
		"AGPL-1.0+":                              true,
		"GFDL-1.3+":                              true,
		"GFDL-1.3+ AND MIT":                      true,
		"GPL-3.0+ AND AGPL-3.0+":                 true,
		"MIT+":                                   false,
		"Apache-2.0+":                            false,
		"Apache-2.0+ WITH LLVM-exception":        false,
		"(Apache-2.0+ WITH LLVM-exception)":      false,
		"MIT+ OR Apache-2.0":                     false,
		"Apache-2.0+ WITH LLVM-exception OR MIT": false,
	} {
		if got := Valid(e); got != want {
			t.Errorf("Valid(%q) = %v, want %v", e, got, want)
		}
	}
}

// TestOperandTextsCostIsLinear — review round 24 C1: every "and"/"or" before
// a parenthetical scanned forward for the word closing it, so a run of
// "or (" was quadratic (0.47 s at 184 KB, reached through nameListVerdict).
func TestOperandTextsCostIsLinear(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	build := func(n int) string { return "MIT " + strings.Repeat("or ( ", n) }
	fastest := func(s string) time.Duration {
		best := time.Duration(1<<63 - 1)
		for range 5 {
			start := time.Now()
			OperandTexts(s)
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	small, large := build(9000), build(36000)
	ts, tl := fastest(small), fastest(large)
	if tl > 8*ts {
		t.Errorf("OperandTexts is superlinear: %v for %d bytes, %v for %d bytes", ts, len(small), tl, len(large))
	}
}

// refSortedKey is the pre-round-25 DisplayKey sort, kept as the reference:
// each node's key built from its children's sorted keys, as strings.
func refSortedKey(n *node) string {
	if n.op == "" {
		return plainLeaf(n)
	}
	keys := make([]string, len(n.kids))
	for i, k := range n.kids {
		keys[i] = refSortedKey(k)
		if n.op == "AND" && k.op == "OR" {
			keys[i] = "(" + keys[i] + ")"
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, " "+n.op+" ")
}

// TestDisplayKeyMatchesTheStringSort — review round 25 C1: DisplayKey's
// round-23 bound fell back to an unsorted key on shallow real expressions (7
// leaves, 4 levels). It now sorts through ropes; its output must equal the
// string sort's on every expression, and merge operand orders at any depth.
func TestDisplayKeyMatchesTheStringSort(t *testing.T) {
	ids := []string{"MIT", "ISC", "Zlib", "0BSD", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "GPL-2.0-only WITH Classpath-exception-2.0"}
	rng := rand.New(rand.NewPCG(25, 25))
	var gen func(depth int) string
	gen = func(depth int) string {
		if depth == 0 || rng.IntN(3) == 0 {
			return ids[rng.IntN(len(ids))]
		}
		op := []string{" AND ", " OR "}[rng.IntN(2)]
		parts := make([]string, 2+rng.IntN(2))
		for i := range parts {
			parts[i] = "(" + gen(depth-1) + ")"
		}
		return strings.Join(parts, op)
	}
	for range 3000 {
		e := gen(6)
		n, err := parse(e, true, nil)
		if err != nil {
			t.Fatalf("generated %q does not parse: %v", e, err)
		}
		if got, want := DisplayKey(e), refSortedKey(n); got != want {
			t.Fatalf("DisplayKey(%q) = %q, want the string sort's %q", e, got, want)
		}
	}
	a := "(MIT AND ISC) OR ((Zlib AND 0BSD OR (BSD-2-Clause AND BSD-3-Clause)) AND Apache-2.0)"
	b := "((BSD-3-Clause AND BSD-2-Clause OR 0BSD AND Zlib) AND Apache-2.0) OR (ISC AND MIT)"
	if DisplayKey(a) != DisplayKey(b) {
		t.Errorf("two orders of one expression key apart: %q vs %q", DisplayKey(a), DisplayKey(b))
	}
}
