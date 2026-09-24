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
// two without compare as text. Every step is a comparison of a fixed key,
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
		return strings.Compare(a, b)
	case ad == "":
		return 1
	case bd == "":
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
	return strings.Compare(ar, br)
}

func splitLeadingDigits(s string) (digits, rest string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}
