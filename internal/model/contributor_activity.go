// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

// contributor_activity.go — GitHub contribution-activity classification
// (v0.27.57). Sourced from GraphQL contributionsCollection, which is the
// only API surface that can distinguish "publicly active" from
// "privately active but disclosed" from "quiet". GITHUB-ONLY: GitLab has
// no equivalent of restrictedContributionsCount — private-profile users
// are simply invisible — so gl-side contributors keep an empty class
// (documented parity gap, per the house both-platforms-where-possible
// rule).

// Activity classes stored in contributors.gh_activity_class. The empty
// string means "never checked" (or checked but the user was absent from
// the API response — deleted/renamed accounts).
const (
	// ActivityPublicActive: public contributions > 0 in the trailing
	// year. Public presence wins classification even when a larger
	// restricted count exists — both numbers are stored, the class is
	// the headline.
	ActivityPublicActive = "public-active"
	// ActivityPrivateActive: zero public contributions but a non-zero
	// restrictedContributionsCount — the user works in private repos
	// AND enabled "include private contributions" on their profile.
	ActivityPrivateActive = "private-active"
	// ActivityDormant: nothing observable in the trailing year, but
	// contributionYears proves past activity.
	ActivityDormant = "dormant"
	// ActivityNoObservable: no observable activity, ever. Deliberately
	// NOT named "inactive": GitHub makes truly-inactive users
	// indistinguishable from users active only in private repos with
	// disclosure OFF — privacy by design. Front ends must label this
	// class honestly ("no observable activity").
	ActivityNoObservable = "no-observable-activity"
)

// ContributionActivity is one user's trailing-year contribution summary
// from GitHub's contributionsCollection.
type ContributionActivity struct {
	Login string
	// CalendarTotal is contributionCalendar.totalContributions — public
	// contributions PLUS disclosed private ones.
	CalendarTotal int
	// Restricted is restrictedContributionsCount: contributions in
	// repositories the viewer cannot access. Non-zero only when the
	// user enabled private-contribution disclosure; zero is therefore
	// ambiguous (no private activity OR disclosure off).
	Restricted int
	// ContributionYears lists every year the user has any recorded
	// contributions (e.g. [2026, 2025, 2019]).
	ContributionYears []int
}

// PublicContributions derives the public share: the calendar total
// includes disclosed private contributions, so public = total −
// restricted, clamped at zero so an upstream accounting quirk can never
// produce a negative stored value.
func (a ContributionActivity) PublicContributions() int {
	if p := a.CalendarTotal - a.Restricted; p > 0 {
		return p
	}
	return 0
}

// LastContributionYear returns the most recent year with any recorded
// contributions, or 0 when the user has none. Order-independent — the
// API returns years descending but the contract must not depend on it.
func (a ContributionActivity) LastContributionYear() int {
	last := 0
	for _, y := range a.ContributionYears {
		if y > last {
			last = y
		}
	}
	return last
}

// ContributorDayActivity is one (day, repository) cell of a
// contributor's public activity history (v0.27.58) — binned from the
// dated nodes of the four contributionsCollection by-repository
// connections. Day is "2006-01-02". RepoFullName is GitHub's
// nameWithOwner — usually a repository Aveloxis does NOT track (that's
// the point: the ecosystem outside the fleet), so it is deliberately a
// name, not a repos FK.
type ContributorDayActivity struct {
	Day          string
	RepoFullName string
	Commits      int
	Issues       int
	PRs          int
	Reviews      int
}

// ContributorDayTotal is one day of the contribution calendar: the
// TOTAL contribution count across all repositories, INCLUDING disclosed
// private contributions — the only daily signal that carries private
// activity (untyped and repo-less by GitHub's design).
type ContributorDayTotal struct {
	Day   string
	Total int
}

// ClassifyContributorActivity maps the trailing-year numbers onto the
// four activity classes. public and restricted are the trailing-year
// counts; lastContributionYear is LastContributionYear() (0 = none
// ever).
func ClassifyContributorActivity(public, restricted, lastContributionYear int) string {
	switch {
	case public > 0:
		return ActivityPublicActive
	case restricted > 0:
		return ActivityPrivateActive
	case lastContributionYear > 0:
		return ActivityDormant
	default:
		return ActivityNoObservable
	}
}
