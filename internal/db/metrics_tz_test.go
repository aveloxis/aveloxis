// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.29 — the Augur-compat metric queries bucketed TIMESTAMPTZ
// columns with session-timezone ::DATE casts while the analytics
// queries used AT TIME ZONE 'UTC': two timezone treatments,
// dual-maintained — the exact class behind the 2026-07-10 flat-line
// bug, found latent by the wrong-answer-tests audit. CI now runs the
// integration tier under TZ/PGTZ=America/Chicago so any regression
// that reaches an executed query fails loudly; this pin covers the
// metric endpoints whose smoke tests check shape, not values.

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestMetricsTemporalCastsAreUTCAnchored(t *testing.T) {
	for file, minGuards := range map[string]int{
		"metrics.go":    8,
		"timeseries.go": 4,
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		s := string(src)
		if got := strings.Count(s, "AT TIME ZONE 'UTC'"); got < minGuards {
			t.Errorf("%s has %d AT TIME ZONE 'UTC' guards, want >= %d — a timestamptz ::DATE or date_trunc without it buckets in the SESSION timezone (the flat-line class)", file, got, minGuards)
		}
		// No timestamptz column may be ::DATE-cast bare. cmt_author_date
		// is TEXT (safe) and first_seen is already a DATE alias — both
		// excluded. The pattern catches the known timestamptz columns.
		bare := regexp.MustCompile(`(created_at|closed_at|merged_at|msg_timestamp)::DATE`)
		for _, m := range bare.FindAllString(s, -1) {
			t.Errorf("%s: %q casts a timestamptz to DATE in session timezone — wrap with (col AT TIME ZONE 'UTC')::DATE", file, m)
		}
		trunc := regexp.MustCompile(`date_trunc\('week', (cmt_author_timestamp|created_at|merged_at)\)`)
		for _, m := range trunc.FindAllString(s, -1) {
			t.Errorf("%s: %q truncates in session timezone — add AT TIME ZONE 'UTC'", file, m)
		}
	}
}
