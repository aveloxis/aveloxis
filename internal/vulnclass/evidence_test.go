// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The evidence grade (summary/70, 8.2): how sure the version behind an
// exposure finding is. One rule, sent by the API, so no client works it out
// in its own words.
func TestEvidence(t *testing.T) {
	now := time.Now()
	row := func(resolution, ecosystem, kind string) *model.VulnerabilityRow {
		return &model.VulnerabilityRow{VersionResolution: resolution, Ecosystem: ecosystem, DependencyKind: kind}
	}
	for _, c := range []struct {
		what string
		v    *model.VulnerabilityRow
		want string
	}{
		{"a locked version", row("locked", "npm", "direct"), EvidenceInUse},
		{"an exact declaration", row("exact", "pypi", "direct"), EvidenceInUse},
		{"a managed version", row("managed", "maven", "direct"), EvidenceInUse},
		{"a range's floor", row("range-floor", "npm", "direct"), EvidenceLowestAllowed},
		{"a bounded range", row("bounded-range", "npm", "transitive"), EvidenceLowestAllowed},
		{"a floating Actions ref", row("unpinned", "githubactions", "direct"), EvidenceFloatingRef},
		{"a row from before the resolution was stored", row("", "npm", "direct"), EvidenceNotClassified},
		{"a value this release does not know", row("something-new", "npm", "direct"), EvidenceNotClassified},
		// Only an exposure finding has a grade.
		{"a version that is not determined", row("unpinned", "npm", "direct"), ""},
		{"the repository's own package", row("locked", "npm", "self"), ""},
		{"a closed finding", func() *model.VulnerabilityRow { v := row("locked", "npm", "direct"); v.ResolvedAt = &now; return v }(), ""},
	} {
		if got := Evidence(c.v); got != c.want {
			t.Errorf("%s: evidence %q, want %q", c.what, got, c.want)
		}
	}
}

// The reasons a finding closes that are not a fix, each with the count key
// the API sends it under and its one wording. Every reason the model
// defines is here: a reason added without a wording fails.
func TestClosedReasonsAreComplete(t *testing.T) {
	want := []string{model.ClosedSuperseded, model.ClosedVersionDetermined, model.ClosedVersionLost,
		model.ClosedVersionChanged, model.ClosedNeverApplied, model.ClosedUnsendable}
	if len(ClosedReasons) != len(want) {
		t.Fatalf("ClosedReasons = %v; want the model's %d reasons", ClosedReasons, len(want))
	}
	seen := map[string]bool{}
	for i, r := range ClosedReasons {
		if r != want[i] {
			t.Errorf("ClosedReasons[%d] = %q, want %q", i, r, want[i])
		}
		w := ClosedReasonWording(r)
		if w == "" || seen[w] {
			t.Errorf("reason %q has no wording of its own (%q)", r, w)
		}
		seen[w] = true
		if ResolvedCountKey(r) != "resolved_"+r {
			t.Errorf("count key for %q = %q", r, ResolvedCountKey(r))
		}
	}
	// No reason is "resolved": none of these closures is a fix.
	for w := range seen {
		if w == "resolved" || w == "Resolved" || w == "fixed" {
			t.Errorf("a reasoned closure is worded %q", w)
		}
	}
	if ResolvedCountKey("") != ResolvedUnlabelledKey || ResolvedCountKey("a-reason-from-a-later-release") != ResolvedOtherReasonKey || ResolvedOtherReasonKey != "resolved_other_reason" {
		t.Errorf("keys: %q, %q", ResolvedCountKey(""), ResolvedCountKey("a-reason-from-a-later-release"))
	}
	if ClosedReasonWording("a-reason-from-a-later-release") == "" {
		t.Error("an unknown reason has no wording: it must never read as resolved")
	}
}
