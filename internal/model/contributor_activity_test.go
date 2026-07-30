// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "testing"

// contributor_activity_test.go — TDD suite for the v0.27.57 activity
// classification (GitHub contributionsCollection).
//
// The four classes and their boundaries:
//   public-active          — any public contributions in the trailing year
//   private-active         — zero public, but restrictedContributionsCount > 0
//                            (the user DISCLOSED private activity)
//   dormant                — zero observable activity this year, but
//                            contributionYears proves past activity
//   no-observable-activity — nothing, ever. Deliberately NOT labeled
//                            "inactive": GitHub makes truly-inactive
//                            indistinguishable from active-only-in-private
//                            with disclosure off (privacy by design).

func TestClassifyContributorActivity(t *testing.T) {
	cases := []struct {
		name                       string
		public, restricted, lastYr int
		want                       string
	}{
		{"public activity wins", 1742, 1895, 2026, ActivityPublicActive},
		{"public only", 3303, 0, 2026, ActivityPublicActive},
		{"private only (disclosed)", 0, 812, 2026, ActivityPrivateActive},
		{"dormant with history", 0, 0, 2021, ActivityDormant},
		{"nothing ever", 0, 0, 0, ActivityNoObservable},
		// A single public contribution beats any restricted count for
		// classification — the page shows both numbers anyway.
		{"boundary: one public", 1, 500, 2026, ActivityPublicActive},
		{"boundary: one restricted", 0, 1, 2026, ActivityPrivateActive},
	}
	for _, c := range cases {
		if got := ClassifyContributorActivity(c.public, c.restricted, c.lastYr); got != c.want {
			t.Errorf("%s: ClassifyContributorActivity(%d, %d, %d) = %q, want %q",
				c.name, c.public, c.restricted, c.lastYr, got, c.want)
		}
	}
}

// PublicContributions is derived as calendar-total minus restricted
// (the calendar INCLUDES disclosed private contributions), clamped at
// zero so a GitHub-side accounting quirk can never store a negative.
func TestContributionActivityPublicDerivation(t *testing.T) {
	a := ContributionActivity{CalendarTotal: 3637, Restricted: 1895}
	if got := a.PublicContributions(); got != 1742 {
		t.Errorf("PublicContributions() = %d, want 1742 (calendar 3637 - restricted 1895)", got)
	}
	clamped := ContributionActivity{CalendarTotal: 10, Restricted: 25}
	if got := clamped.PublicContributions(); got != 0 {
		t.Errorf("PublicContributions() must clamp at 0, got %d", got)
	}
}

func TestLastContributionYear(t *testing.T) {
	a := ContributionActivity{ContributionYears: []int{2019, 2021, 2016}}
	if got := a.LastContributionYear(); got != 2021 {
		t.Errorf("LastContributionYear() = %d, want 2021 (max of the years list, order-independent)", got)
	}
	var empty ContributionActivity
	if got := empty.LastContributionYear(); got != 0 {
		t.Errorf("LastContributionYear() on no history = %d, want 0", got)
	}
}
