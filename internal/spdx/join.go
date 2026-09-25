// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"sort"
	"strings"
)

// JoinExpressions combines license expressions under one operator ("AND"
// or "OR") and is the ONE way to do it (SR-17): the registry-list writer
// (collector joinRegistryLicenseList, OR) and ScanCode's whole-tree
// conclusion (db, AND) both use it. Empty parts are skipped. When there is
// more than one part, every compound part (isCompound: an and, or, or slash
// in any case) is parenthesized, so its own operators cannot regroup
// with the join's (v0.29.67 review round 1: a bare " AND " join turned
// "BSD-3-Clause" + "MIT OR Apache-2.0" into (BSD-3-Clause AND MIT) OR
// Apache-2.0). A WITH clause binds tighter than both and is left alone.
//
// A part with unbalanced parentheses ("MIT) OR (ISC") is not repaired: it can
// still regroup with its neighbours. Declined in v0.29.67 review round 3;
// it needs a malformed registry list entry, and no registry publishes one.
func JoinExpressions(op string, parts []string) string {
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			kept = append(kept, p)
		}
	}
	if len(kept) > 1 {
		for i, p := range kept {
			if isCompound(p) {
				kept[i] = "(" + p + ")"
			}
		}
	}
	return strings.Join(kept, " "+op+" ")
}

// isCompound reports whether a part contains an AND or OR, by the lenient
// parser's own rule (any case, slash read as OR, any whitespace as a
// separator; v0.29.67 review round 2: a padded-space check missed
// "MIT\nor Apache-2.0").
func isCompound(part string) bool {
	p := &parser{}
	for _, tok := range tokenize(part, true) {
		if op := p.opOf(tok); op == "AND" || op == "OR" {
			return true
		}
	}
	return false
}

// OperandTexts splits raw at the list operator WORDS "and" and "or" (any case,
// whitespace-delimited; never "with") and returns the operand texts between
// them. It lets a caller tell a list of license NAMES ("GPL-3.0-only AND
// Apache License 2.0 AND Proprietary license of Example Corp") from a license
// TEXT or notice, whose prose runs far longer between its "and"s and "or"s
// (v0.29.67 review round 4).
//
// Unlike the parser's tokenizer it does NOT split at parentheses or slashes
// (review round 5): a URL, a "(c)" or a parenthesized short name belongs to
// the name or notice. "with" never splits: a WITH clause is one license and
// its exception (round 6). An "and"/"or" that opens a version range ("or
// later", "or (at your option) any later version", "and above") is part of
// ONE license's name (rounds 6-7), see versionRangeWords.
//
// Decided as a class (round 7): any OTHER one-license notice whose short prose
// tail follows "and"/"or" ("Apache License 2.0 and the additional terms in
// NOTICE") splits and reads as a name list, so the caller keeps it as text
// (NOASSERTION) instead of asserting its license. A heuristic can only lean
// one way here, and the other direction, collapsing a real list with a
// free-text operand into one license, asserts something false.
func OperandTexts(raw string) []string {
	var out, words []string
	flush := func() {
		if len(words) > 0 {
			out = append(out, strings.Join(words, " "))
			words = nil
		}
	}
	fields := strings.Fields(raw)
	// closeAt[i] is the first word at or after i that closes a parenthetical
	// (len(fields) when none does), found in one backward pass: scanning
	// forward from every "and"/"or" was quadratic on a run of "or (" (review
	// round 24: 0.47 s at 184 KB).
	closeAt := make([]int, len(fields)+1)
	closeAt[len(fields)] = len(fields)
	for i := len(fields) - 1; i >= 0; i-- {
		closeAt[i] = closeAt[i+1]
		if strings.HasSuffix(fields[i], ")") {
			closeAt[i] = i
		}
	}
	for i, w := range fields {
		switch strings.ToLower(w) {
		case "and", "or":
			if !opensVersionRange(fields[i+1:], closeAt[i+1]-(i+1)) {
				flush()
				continue
			}
		}
		words = append(words, w)
	}
	flush()
	return out
}

// versionRangeWords continue an "and"/"or" into a version range of the
// license before it ("or later", "or above"). "any" is only a lead-in to one
// of them ("or any later version"): on its own it starts a second license
// ("GPL-3.0-only and any code under the Mozilla Public License 2.0"), and
// treating it as a range merged that real list into one fingerprinted license
// (review round 8). A range word followed by another license ("OR any later
// version of the Mozilla Public License 2.0") still merges; such wordings
// are contrived and left as they are (round 8, noted).
var versionRangeWords = map[string]bool{
	"later": true, "above": true, "newer": true,
	"higher": true, "greater": true, "subsequent": true,
	// "or any future version" (review round 20 S4: the fingerprint read it
	// as no range at all).
	"future": true,
}

// VersionRangeWords returns the words that make a version range, sorted. It
// is the ONE list (SR-17): the full-text fingerprint in internal/db reads a
// notice's range with it, as OperandTexts does here (review round 17 found a
// second, narrower spelling there).
func VersionRangeWords() []string {
	out := make([]string, 0, len(versionRangeWords))
	for w := range versionRangeWords {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

// opensVersionRange reports whether the words after an "and"/"or" continue a
// version range: the next word, after skipping one leading parenthetical
// ("(at your option)") and trimming punctuation, is a versionRangeWords
// entry, or "any" followed by one.
// closeIdx is the index in rest of the first word ending in ")" (len(rest)
// when none does), precomputed by the caller.
func opensVersionRange(rest []string, closeIdx int) bool {
	i := 0
	if i < len(rest) && strings.HasPrefix(rest[i], "(") {
		i = closeIdx + 1 // past the word that closes the parenthetical
	}
	word := func(j int) string {
		if j >= len(rest) {
			return ""
		}
		return strings.Trim(strings.ToLower(rest[j]), ".,;:()")
	}
	if word(i) == "any" {
		return versionRangeWords[word(i+1)]
	}
	return versionRangeWords[word(i)]
}
