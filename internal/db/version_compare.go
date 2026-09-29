// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "strings"

// CompareVersionish compares two dotted versions segment-wise (-1/0/1).
// It is the ONE "semantic-ish" version order: the GitHub Actions
// advisory-range check (collector.compareVersionish delegates here) and
// the per-version order of the exposed-repositories list
// (GetPackageExposedRepos) both use it.
//
// A leading "v" is ignored and a missing segment reads as "0" ("4.1" ==
// "4.1.0"). Each segment is split into its leading digits and the rest: a
// segment with leading digits sorts before one without; two with digits
// compare by that number, then by the rest as text ("1b1" < "2" < "10");
// two without compare as text — except that with equal numbers a
// pre-release suffix ("-rc1", "b1"; isPreReleaseSuffix) sorts before the
// bare number (worklist item 59). Every step is a comparison of a fixed key,
// so the order is transitive and safe as a sort key (a fresh-context
// review of the 2026-09-23 table change found the earlier rule, text
// comparison whenever either segment was not a plain number, made a cycle
// of 1.0.2 < 1.0.10 < 1.0.1b1 < 1.0.2). On plain numeric segments the two
// rules agree.
func CompareVersionish(a, b string) int {
	as := strings.Split(strings.TrimPrefix(a, "v"), ".")
	bs := strings.Split(strings.TrimPrefix(b, "v"), ".")
	n := max(len(as), len(bs))
	for i := 0; i < n; i++ {
		av, bv := "0", "0"
		if i < len(as) {
			av = as[i]
		}
		if i < len(bs) {
			bv = bs[i]
		}
		if c := compareVersionSegment(av, bv); c != 0 {
			return c
		}
	}
	return 0
}

// compareVersionSegment orders one segment by the key (has leading
// digits, their numeric value, the rest as text). The numeric value is
// compared as a digit string (leading zeros dropped, then length, then
// text), so no segment can overflow an integer.
func compareVersionSegment(a, b string) int {
	ad, ar := splitLeadingDigits(a)
	bd, br := splitLeadingDigits(b)
	switch {
	case ad == "" && bd == "":
		// Two digitless segments: a pre-release tag precedes any other
		// text ("dev" < "build"), then text (batch 7c review round 1: the
		// plain text compare here broke transitivity against the arms
		// below — 1.2.dev < 1.2.3 < 1.2.build, but dev > build as text).
		if ra, rb := suffixRank(a), suffixRank(b); ra != rb {
			return ra - rb
		}
		return strings.Compare(a, b)
	case ad == "":
		// A segment that is only a pre-release tag ("dev3" in 1.0.1.dev3,
		// "rc" in 2.0.0.rc.1) precedes any numbered segment, the filled-in
		// zero of a shorter version included (worklist item 59); any other
		// digitless segment ("x") follows.
		if isPreReleaseSuffix(a) {
			return -1
		}
		return 1
	case bd == "":
		if isPreReleaseSuffix(b) {
			return 1
		}
		return -1
	}
	an, bn := strings.TrimLeft(ad, "0"), strings.TrimLeft(bd, "0")
	if len(an) != len(bn) {
		if len(an) < len(bn) {
			return -1
		}
		return 1
	}
	if c := strings.Compare(an, bn); c != 0 {
		return c
	}
	// Equal numbers: the suffix's CLASS first — pre-release, then none,
	// then anything else — and text within a class (worklist item 59):
	// SemVer's "-rc1" and PEP 440's "b1"/"rc1"/"a1"/"dev1" denote a version
	// that precedes the release; the old text compare put "1.0.0-rc1" after
	// "1.0.0" and "1.0.1b1" after "1.0.1", so a pin at v1.0.0-rc1 read as
	// not affected by an advisory fixed in 1.0.0. A build or post suffix
	// ("+build") follows the bare number. Comparing the class at every
	// branch is what keeps this a key comparison, hence a total order
	// (batch 7c review round 1: ranking only pre-release against empty
	// left "+build" sorting by text BEFORE "-rc1", and a sort of a set
	// holding both came out in input order).
	if ra, rb := suffixRank(ar), suffixRank(br); ra != rb {
		return ra - rb
	}
	return strings.Compare(ar, br)
}

// suffixRank orders the text after a segment's digits by class: 0 for a
// pre-release tag, 1 for none, 2 for anything else (a build or post marker,
// an arbitrary word). compareVersionSegment compares this before the text,
// so every branch orders by the same key.
func suffixRank(rest string) int {
	switch {
	case rest == "":
		return 1
	case isPreReleaseSuffix(rest):
		return 0
	}
	return 2
}

// isPreReleaseSuffix reports whether the text after a segment's digits
// denotes a pre-release: SemVer's "-" (anything after a hyphen precedes the
// release) or a PEP 440 pre-release/dev tag — a, b, c, rc, alpha, beta,
// pre, preview, dev — with or without a separator, followed by digits or
// nothing. "post" and "+build" are not pre-releases.
func isPreReleaseSuffix(rest string) bool {
	if rest == "" {
		return false
	}
	if rest[0] == '-' {
		return true
	}
	tag := strings.ToLower(strings.TrimLeft(rest, "._"))
	for _, p := range []string{"alpha", "beta", "preview", "pre", "rc", "dev", "a", "b", "c"} {
		if strings.HasPrefix(tag, p) {
			after := tag[len(p):]
			return after == "" || strings.TrimLeft(after, "0123456789") == ""
		}
	}
	return false
}

func splitLeadingDigits(s string) (digits, rest string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}
