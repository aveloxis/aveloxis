// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// TrafficKind names what a traffic window or weekly row counts: page
// views or git clones. The values are the stored spelling of the
// traffic_kind column, which a CHECK constraint limits to these two.
type TrafficKind string

const (
	// TrafficKindViews marks a row counting page views and visitors.
	TrafficKindViews TrafficKind = "views"
	// TrafficKindClones marks a row counting clones and cloners.
	TrafficKindClones TrafficKind = "clones"
)

// RepoTraffic bundles one traffic read of a repository, apart from the
// daily clones that FetchCloneStats returns. The platform client fills
// the raw fields; the collector adds SnapshotDate, derives Windows and
// Weeks, and stages the whole value as one row. Every bucket GitHub
// returns is kept, zeros included: a stored zero is a day without
// traffic, a missing row a day that was not read.
type RepoTraffic struct {
	// Origin says where the read came from; its DataSource is stored on
	// the windows, weeks, paths and referrers rows (the daily views
	// carry their own).
	Origin DataOrigin

	// SnapshotDate is the UTC collection date (midnight UTC) that keys
	// the path and referrer snapshot. Set by the collector.
	SnapshotDate time.Time

	// Views holds the daily views buckets in the order GitHub returns
	// them (14 consecutive UTC days, zero-filled).
	Views []RepoView

	// ViewUniques is the number of distinct visitors over the 14-day
	// window, the top-level uniques of the daily views response. It is
	// not the sum of the daily uniques.
	ViewUniques int
	// CloneUniques is the number of distinct cloners over the same
	// 14-day window as the daily clones FetchCloneStats returns, whose
	// dates the collector uses for the clones window. FetchTraffic
	// leaves it unset: its clones read (per=week) is a later request,
	// and a window's dates and uniques must describe one period.
	CloneUniques int

	// WeeklyViews and WeeklyClones hold every weekly bucket GitHub
	// returns, partial first and last weeks included; each entry's
	// Kind is set. The collector keeps only complete weeks in Weeks.
	WeeklyViews  []RepoTrafficWeek
	WeeklyClones []RepoTrafficWeek

	// Paths and Referrers are the top paths and referrers GitHub
	// reports for its 14-day period, in the order it returns them.
	Paths     []RepoTrafficPath
	Referrers []RepoTrafficReferrer

	// Windows holds one 14-day window per kind and Weeks the complete
	// Monday-to-Sunday weeks inside them. Both are derived by the
	// collector, not filled by the platform client.
	Windows []RepoTrafficWindow
	Weeks   []RepoTrafficWeek
}

// RepoView holds the views of one repository on one UTC day, shaped
// like RepoClone.
type RepoView struct {
	ID          int64
	RepoID      int64
	Timestamp   time.Time
	TotalViews  int
	UniqueViews int
	Origin      DataOrigin
}

// RepoTrafficPath is one entry of the top paths list: a page of the
// repository with its title and its views and distinct visitors over
// GitHub's 14-day period.
type RepoTrafficPath struct {
	Path        string
	Title       string
	TotalViews  int
	UniqueViews int
}

// RepoTrafficReferrer is one entry of the top referrers list: a
// referring site with the views and distinct visitors it sent over
// GitHub's 14-day period.
type RepoTrafficReferrer struct {
	Referrer    string
	TotalViews  int
	UniqueViews int
}

// RepoTrafficWindow records one observed 14-day window of one kind:
// its first and last daily buckets and the distinct visitors or
// cloners GitHub reports for it. Its presence marks a successful read
// even when Uniques is 0.
type RepoTrafficWindow struct {
	Kind    TrafficKind
	Start   time.Time
	End     time.Time
	Uniques int
}

// RepoTrafficWeek holds the distinct visitors or cloners of one weekly
// bucket, keyed by the bucket's start. No total is kept: a week's views
// or clones are the sum of its daily rows.
type RepoTrafficWeek struct {
	Kind      TrafficKind
	WeekStart time.Time
	Uniques   int
}
