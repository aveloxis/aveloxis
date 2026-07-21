// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.16 — contributor_retention: catalog registration + threshold
// parameter + the multi-series compare plumbing (an 8Knot port,
// operator ask 2026-07-15). Kept in its own file so the metric's
// registration and query routing stay additive to analytics.go.
//
// The heavy lifting (live base-table SQL, exclusions, first-
// contribution bucketing) lives in internal/db/retention_store.go —
// see the architecture comment there. This file deliberately never
// references the fleet-scale contributor matview.

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

const retentionMetricID = "contributor_retention"

// defaultRetentionThreshold mirrors 8Knot's UI default for its
// drive-by/repeat contributors visualization: the "Contributions
// Required" input is dbc.Input(type="number", min=1, max=15, step=1,
// value=4) in 8Knot/pages/contributors/visualizations/
// contrib_drive_repeat.py (oss-aspen/8Knot, dev branch — verified
// 2026-07-15). 8Knot classifies repeat contributors as >= the
// threshold; ContributorRetentionSeries uses the same comparison.
// Operator-adjustable per request via ?retention_threshold=N.
const defaultRetentionThreshold = 4

// Registered via a package-level var (v0.27.41, summary/18 Phase 4):
// the codebase's ONLY init() made catalog order file-name-dependent if
// a second registration-style init() ever appeared. Var initialization
// is dependency-ordered and explicit.
var _ = func() bool {
	metricCatalog = append(metricCatalog, metricDef(retentionMetricID,
		"Contributor Retention (Drive-by vs Repeat)",
		"Each contributor in the entity's repo set is classified by their TOTAL contribution count over all collected history (distinct commits, issues opened, change requests opened, reviews, and conversation comments): below the threshold = drive-by, at/above = repeat (?retention_threshold, default 4, mirroring 8Knot's Contributions Required input). Contributors bucket by the month of their FIRST contribution; the series counts drive-by vs repeat per bucket. Bots and soft-deleted merge-loser identities are excluded.",
		"contributors", "temporal",
		"https://chaoss.community/kb/metric-new-contributors/",
		"Splits every new-contributor cohort into drive-by vs repeat by eventual total engagement (an 8Knot port), computed live from base tables over resolved platform identities — so one chart answers both 'how many arrived' and 'how many stayed'."))
	return true
}()

// parseRetentionThreshold reads ?retention_threshold (default 4).
// Rejects non-integer and sub-1 values with an error the handler
// turns into a 400.
func parseRetentionThreshold(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("retention_threshold")
	if raw == "" {
		return defaultRetentionThreshold, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("retention_threshold must be a positive integer, got %q", raw)
	}
	return n, nil
}

// metricSeriesAndParts is handleCompare's per-entity series entry
// point (v0.27.16). Single-series metrics return (points, nil);
// multi-series metrics return the headline total in points plus the
// named component series in parts (contributor_retention:
// "drive_by" + "repeat"). Every returned series is densified.
func (s *Server) metricSeriesAndParts(r *http.Request, ids []int64, metric, bucket string, since, until time.Time, retentionThreshold int) ([]db.WeeklyPoint, map[string][]db.WeeklyPoint, error) {
	if metric != retentionMetricID {
		points, err := s.metricSeries(r, ids, metric, bucket, since, until)
		if err != nil {
			return nil, nil, err
		}
		return fillBuckets(points, since, until, bucket), nil, nil
	}
	driveBy, repeat, err := s.store.ContributorRetentionSeries(r.Context(), ids, bucket, since, until, retentionThreshold)
	if err != nil {
		return nil, nil, err
	}
	driveBy = fillBuckets(driveBy, since, until, bucket)
	repeat = fillBuckets(repeat, since, until, bucket)
	total := make([]db.WeeklyPoint, len(driveBy))
	for i := range driveBy {
		total[i] = db.WeeklyPoint{Bucket: driveBy[i].Bucket, Value: driveBy[i].Value + repeat[i].Value}
	}
	return total, map[string][]db.WeeklyPoint{"drive_by": driveBy, "repeat": repeat}, nil
}
