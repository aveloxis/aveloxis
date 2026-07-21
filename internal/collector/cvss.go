// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — cvss.go implements the CVSS base-score formulas so
// stored scores are spec-conformant instead of approximated.
//
// v0.27.23 replaces the old parseCVSSScore, which substring-matched vector
// components and returned one of six hardcoded buckets (9.8 / 7.5 / 6.5 /
// 5.3 / 5.0 / 4.0). Those buckets were not even monotonic — an AV:N vector
// WITHOUT AC:L scored 6.5, higher than AV:N + AC:L at 5.3, despite lower
// attack complexity being strictly worse. Every stored cvss_score before
// v0.27.23 is an approximation; rows heal on each repo's next scan and via
// `aveloxis heal-vulnerabilities --rescore-only`.
//
// Implemented: CVSS v3.0 / v3.1 (identical base-score formula; the spec's
// §7.5 "Roundup" applies to both) and CVSS v2 (old CVE-only records).
// NOT implemented: CVSS v4 — its scoring is a MacroVector equivalence-set
// lookup, not a formula; a v4-only vector yields NO score rather than a
// guess. The severity LABEL still arrives via OSV database_specific.
package collector

import (
	"math"
	"strings"
)

// cvssBaseScore computes the base score for a CVSS vector string.
// Returns (score, true) for a parseable v3.0/v3.1/v2 vector and
// (0, false) for anything else — v4, malformed, or empty. Callers
// treat ok=false as "no score" (stored as 0, the column's no-score
// sentinel), never as a guessed value.
func cvssBaseScore(vector string) (float64, bool) {
	switch {
	case strings.HasPrefix(vector, "CVSS:3.0/"), strings.HasPrefix(vector, "CVSS:3.1/"):
		return cvssV3BaseScore(vector)
	case strings.HasPrefix(vector, "CVSS:4."):
		return 0, false // MacroVector lookup, deliberately not implemented
	case strings.HasPrefix(vector, "CVSS:"):
		return 0, false // unknown future major version
	case strings.Contains(vector, "Au:"):
		// v2 vectors have no CVSS: prefix and are recognizable by the
		// Authentication metric (Au:), which v3 renamed to PR:.
		return cvssV2BaseScore(vector)
	default:
		return 0, false
	}
}

// parseVectorMetrics splits "AV:N/AC:L/..." into a metric→value map.
func parseVectorMetrics(vector string) map[string]string {
	m := make(map[string]string, 11)
	for part := range strings.SplitSeq(vector, "/") {
		if kv := strings.SplitN(part, ":", 2); len(kv) == 2 {
			m[kv[0]] = kv[1]
		}
	}
	return m
}

// cvssV3BaseScore implements the CVSS v3.1 specification §7.1 base-score
// equations (v3.0 uses the same equations; the §7.5 Roundup definition
// below is v3.1's, which exists precisely to pin down v3.0's floating-
// point ambiguity). All eight base metrics must be present and valid or
// the vector is rejected.
func cvssV3BaseScore(vector string) (float64, bool) {
	m := parseVectorMetrics(vector)

	av, ok1 := map[string]float64{"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}[m["AV"]]
	ac, ok2 := map[string]float64{"L": 0.77, "H": 0.44}[m["AC"]]
	ui, ok3 := map[string]float64{"N": 0.85, "R": 0.62}[m["UI"]]

	var scopeChanged bool
	switch m["S"] {
	case "U":
		scopeChanged = false
	case "C":
		scopeChanged = true
	default:
		return 0, false
	}

	// PR weights differ when scope is changed (spec Table 8.2).
	var pr float64
	var ok4 bool
	if scopeChanged {
		pr, ok4 = map[string]float64{"N": 0.85, "L": 0.68, "H": 0.5}[m["PR"]]
	} else {
		pr, ok4 = map[string]float64{"N": 0.85, "L": 0.62, "H": 0.27}[m["PR"]]
	}

	cia := map[string]float64{"H": 0.56, "L": 0.22, "N": 0}
	c, ok5 := cia[m["C"]]
	i, ok6 := cia[m["I"]]
	a, ok7 := cia[m["A"]]

	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) {
		return 0, false
	}

	iss := 1 - (1-c)*(1-i)*(1-a)
	var impact float64
	if scopeChanged {
		impact = 7.52*(iss-0.029) - 3.25*math.Pow(iss-0.02, 15)
	} else {
		impact = 6.42 * iss
	}
	exploitability := 8.22 * av * ac * pr * ui

	if impact <= 0 {
		return 0, true // spec: base score is 0.0 when there is no impact
	}
	if scopeChanged {
		return cvssV31Roundup(math.Min(1.08*(impact+exploitability), 10)), true
	}
	return cvssV31Roundup(math.Min(impact+exploitability, 10)), true
}

// cvssV31Roundup implements the CVSS v3.1 specification Appendix A
// Roundup function: the smallest number, specified to one decimal
// place, that is equal to or higher than its input. The integer-space
// detour avoids the floating-point artifacts that made naive
// math.Ceil(x*10)/10 disagree with the spec on some inputs.
func cvssV31Roundup(x float64) float64 {
	i := int(math.Round(x * 100000))
	if i%10000 == 0 {
		return float64(i) / 100000
	}
	return (math.Floor(float64(i)/10000) + 1) / 10
}

// cvssV2BaseScore implements the CVSS v2 guide §3.2.1 base equation.
// v2 vectors look like "AV:N/AC:L/Au:N/C:P/I:P/A:P", optionally
// parenthesized, with no CVSS: prefix.
func cvssV2BaseScore(vector string) (float64, bool) {
	m := parseVectorMetrics(strings.Trim(vector, "()"))

	av, ok1 := map[string]float64{"L": 0.395, "A": 0.646, "N": 1.0}[m["AV"]]
	ac, ok2 := map[string]float64{"H": 0.35, "M": 0.61, "L": 0.71}[m["AC"]]
	au, ok3 := map[string]float64{"M": 0.45, "S": 0.56, "N": 0.704}[m["Au"]]

	cia := map[string]float64{"N": 0, "P": 0.275, "C": 0.660}
	c, ok4 := cia[m["C"]]
	i, ok5 := cia[m["I"]]
	a, ok6 := cia[m["A"]]

	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
		return 0, false
	}

	impact := 10.41 * (1 - (1-c)*(1-i)*(1-a))
	exploitability := 20 * av * ac * au
	fImpact := 1.176
	if impact == 0 {
		fImpact = 0
	}
	score := (0.6*impact + 0.4*exploitability - 1.5) * fImpact
	// v2 rounds to one decimal (standard rounding, not v3's Roundup).
	return math.Round(score*10) / 10, true
}
