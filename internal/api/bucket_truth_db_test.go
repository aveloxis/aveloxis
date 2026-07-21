// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.31 (audit Phase 3, C2) — Postgres is the ground truth for
// bucket alignment. The v0.27.2 flat-line incident happened because
// Go's bucket generator and Postgres's date_trunc disagreed in a
// non-UTC session and every joined value silently became 0. The old
// regression test verified Go against Go; this one runs the REAL
// date_trunc(... AT TIME ZONE 'UTC') under a session FORCED to
// America/Chicago and asserts truncBucket lands on the same UTC date —
// plus a negative control proving the bare (pre-fix) form actually
// diverges here, i.e. the test can see the bug class it guards.

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestTruncBucketMatchesPostgresDateTruncInNonUTCSession(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	// Force the trap regardless of the host's TZ: the whole session
	// runs in America/Chicago (UTC-5/-6, DST-observing).
	if _, err := conn.Exec(ctx, "SET TIME ZONE 'America/Chicago'"); err != nil {
		t.Fatalf("set time zone: %v", err)
	}

	// Edge timestamps chosen to straddle the UTC/local date boundary
	// and both DST transitions.
	edges := []time.Time{
		time.Date(2025, 3, 3, 3, 0, 0, 0, time.UTC),   // Mon 03:00Z = Sun 21:00 CST
		time.Date(2025, 3, 2, 23, 30, 0, 0, time.UTC), // Sun late UTC
		time.Date(2025, 3, 9, 7, 30, 0, 0, time.UTC),  // inside the spring-forward hour
		time.Date(2025, 11, 2, 6, 30, 0, 0, time.UTC), // inside the fall-back hour
		time.Date(2025, 3, 1, 2, 0, 0, 0, time.UTC),   // month boundary: Mar UTC, Feb local
		time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC),
	}

	sameUTCDate := func(a, b time.Time) bool {
		a, b = a.UTC(), b.UTC()
		return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
	}

	bareDiverged := false
	for _, ts := range edges {
		for _, bucket := range []string{"week", "month"} {
			var anchored, bare time.Time
			err := conn.QueryRow(ctx,
				"SELECT date_trunc($1, $2::timestamptz AT TIME ZONE 'UTC'), date_trunc($1, $2::timestamptz)",
				bucket, ts).Scan(&anchored, &bare)
			if err != nil {
				t.Fatalf("date_trunc query (%s, %s): %v", bucket, ts, err)
			}
			want := truncBucket(ts, bucket)
			if !sameUTCDate(anchored, want) {
				t.Errorf("%s bucket of %s: Postgres (UTC-anchored) says %s, Go truncBucket says %s — the two sides of the series join disagree",
					bucket, ts.Format(time.RFC3339), anchored.UTC().Format("2006-01-02"), want.Format("2006-01-02"))
			}
			if !sameUTCDate(bare, want) {
				bareDiverged = true
			}
		}
	}
	// Negative control: at least one edge must make the BARE form
	// (pre-v0.27.2's date_trunc without the UTC anchor) land on a
	// different date. If it never diverges, this test lost the power
	// to detect the flat-line class and needs new edge timestamps.
	if !bareDiverged {
		t.Error("bare date_trunc never diverged from truncBucket under a Chicago session — the negative control is dead; the test can no longer detect the flat-line bug class")
	}
}
