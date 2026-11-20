// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

func row(kind, resolution, ecosystem string, resolved bool) *model.VulnerabilityRow {
	v := &model.VulnerabilityRow{DependencyKind: kind, VersionResolution: resolution, Ecosystem: ecosystem}
	if resolved {
		t := time.Unix(1_700_000_000, 0)
		v.ResolvedAt = &t
	}
	return v
}

func TestOf(t *testing.T) {
	cases := []struct {
		name string
		v    *model.VulnerabilityRow
		want Class
	}{
		{"a locked direct dependency", row("direct", "locked", "PyPI", false), Exposure},
		{"a range floor is a conservative lower bound: exposure", row("direct", "range-floor", "npm", false), Exposure},
		{"a bounded range likewise", row("direct", "bounded-range", "npm", false), Exposure},
		{"an exact version", row("direct", "exact", "Go", false), Exposure},
		{"a transitive dependency", row("transitive", "locked", "npm", false), Exposure},
		{"a row from before kinds were recorded", row("", "", "PyPI", false), Exposure},
		{"no declared version", row("direct", "unpinned", "PyPI", false), VersionNotDetermined},
		{"no declared version, kind not recorded", row("", "unpinned", "npm", false), VersionNotDetermined},
		{"a floating GitHub Actions ref was matched against the ref", row("direct", "unpinned", "githubactions", false), Exposure},
		{"the repository's own package", row("self", "", "PyPI", false), Self},
		{"own package wins over unpinned", row("self", "unpinned", "PyPI", false), Self},
		{"closed wins over everything", row("direct", "locked", "PyPI", true), Closed},
		{"closed, was version not determined", row("direct", "unpinned", "PyPI", true), Closed},
		{"closed, own package", row("self", "", "PyPI", true), Closed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Of(c.v); got != c.want {
				t.Errorf("Of = %q, want %q", got, c.want)
			}
		})
	}
}

// The ecosystem comparison is exact, as the SQL twin's is: OSV's "GitHub
// Actions" findings are stored with ecosystem 'githubactions', in that case.
func TestTheGitHubActionsExceptionIsExact(t *testing.T) {
	if got := Of(row("direct", "unpinned", "GitHubActions", false)); got != VersionNotDetermined {
		t.Errorf("Of = %q; only the stored spelling 'githubactions' is the exception", got)
	}
}

func TestIsVersionNotDeterminedIgnoresWhetherTheFindingIsClosed(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		if !IsVersionNotDetermined(row("direct", "unpinned", "PyPI", resolved)) {
			t.Errorf("resolved=%v: an unpinned dependency finding must answer true", resolved)
		}
		if IsVersionNotDetermined(row("self", "unpinned", "PyPI", resolved)) {
			t.Errorf("resolved=%v: the repository's own package must answer false", resolved)
		}
		if IsVersionNotDetermined(row("direct", "unpinned", "githubactions", resolved)) {
			t.Errorf("resolved=%v: a GitHub Actions ref must answer false", resolved)
		}
		if IsVersionNotDetermined(row("direct", "locked", "PyPI", resolved)) {
			t.Errorf("resolved=%v: a locked version must answer false", resolved)
		}
	}
}

// For a live finding the predicate and the class agree, whatever the columns.
func TestIsVersionNotDeterminedAgreesWithOfForLiveFindings(t *testing.T) {
	n := 0
	for _, kind := range []string{"", "direct", "transitive", "self"} {
		for _, res := range []string{"", "locked", "exact", "bounded-range", "range-floor", "unpinned"} {
			for _, eco := range []string{"", "PyPI", "npm", "githubactions"} {
				v := row(kind, res, eco, false)
				n++
				if got, want := IsVersionNotDetermined(v), Of(v) == VersionNotDetermined; got != want {
					t.Errorf("(%q, %q, %q): IsVersionNotDetermined = %v, Of = %q", kind, res, eco, got, Of(v))
				}
			}
		}
	}
	if n != 96 {
		t.Errorf("examined %d combinations, want 96", n)
	}
}

func TestIsCritical(t *testing.T) {
	cases := []struct {
		severity string
		score    float64
		want     bool
	}{
		{"CRITICAL", 0, true},
		{"CRITICAL", 9.8, true},
		{"HIGH", 9.0, true}, // the boundary is included
		{"HIGH", 8.9, false},
		{"", 9.5, true},
		{"", 0, false},
		{"critical", 0, false}, // the stored label is upper case; the rule is exact, as the SQL twin's is
	}
	for _, c := range cases {
		v := &model.VulnerabilityRow{Severity: c.severity, CVSSScore: c.score}
		if got := IsCritical(v); got != c.want {
			t.Errorf("IsCritical(%q, %v) = %v, want %v", c.severity, c.score, got, c.want)
		}
	}
}
