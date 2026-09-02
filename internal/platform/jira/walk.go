// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"time"
)

// TimeLayout parses Jira Server timestamps
// ("2024-01-05T10:00:00.000+0000").
const TimeLayout = "2006-01-02T15:04:05.000-0700"

// maxStalePages: consecutive pages contributing zero NEW (key, updated)
// pairs before the walk fails. A boundary-minute re-list legally
// repeats up to one full page; two would need a pathological cohort;
// three consecutive means the server is re-serving content regardless
// of the cursor/offset.
const maxStalePages = 3

// WalkProjectByUpdated is THE drift-safe project walk (SR-17: one
// spelling — Copilot round 3 on PR #193 found the identity backfill
// re-spelling it as bare offsets over `ORDER BY updated ASC`, the
// exact permanent-skip class the round-2 C2 fix removed from the
// worker). Both the incremental worker and the full-history backfill
// route through this function.
//
// NEVER walk bare offsets over ORDER BY updated ASC — the window's
// membership MUTATES during the scan (an issue touched mid-walk moves
// later, shifting every offset left), so offset paging can skip an
// unseen issue while the caller's checkpoint advances past it: a
// PERMANENT skip. The drift-safe walk instead:
//
//   - advances the WINDOW (a jql `updated >=` cursor at Jira's minute
//     precision) with TRUE startAt=0 after every page whose max
//     updated moved the cursor forward — the boundary minute re-lists
//     whole, which idempotent consumers make a no-op (a skip-count
//     "optimization" here is an offset assumption drift defeats: the
//     round-2 L10 pass reproduced the permanent skip);
//   - holds a fixed CEILING (`updated <=` the walk-start minute) so a
//     busy project cannot chase its own tail — later updates belong to
//     the next cycle via the caller's checkpoint;
//   - drains a same-minute cohort wider than a page (the cursor
//     cannot advance at minute precision) by KEY KEYSET over the
//     frozen minute window (`updated >= M AND updated < M+1m AND
//     issuekey > K ORDER BY issuekey ASC`) — Copilot round 9 on
//     PR #193 retired the offset fallback, whose page gaps drift
//     could still permanently skip: issue keys NEVER change, so the
//     key order is stable under any drift; cohort membership only
//     SHRINKS (a touched issue leaves the minute), and departures
//     land beyond the cursor where the outer walk lists them. The
//     tie drain restarts the minute from the beginning (idempotent
//     consumers no-op the re-listed items) and exits to
//     cursor = M+1min on an empty page;
//   - fails after maxStalePages consecutive NON-tie pages whose
//     every (key, updated) pair was already seen this walk, and
//     bounds the tie drain by its own first page's Total plus a
//     key-progress guard (the last key of each tie page must
//     advance) — the termination bounds a misbehaving server cannot
//     satisfy.
//
// since zero = full history. visit receives each page's issues with
// their PARSED updated times (aligned slices) — boundary re-lists ARE
// passed through (consumers dedup by natural key); a visit error
// aborts the walk and is returned as-is, so callers can classify it
// (context.Canceled, ClassSkip, ...) at one site.
func (c *Client) WalkProjectByUpdated(ctx context.Context, projectKey string, fields []string, pageSize int, pageSleep time.Duration, since time.Time, visit func(issues []Issue, updated []time.Time) error) error {
	const jqlMinute = "2006-01-02 15:04"
	ceiling := time.Now().UTC().Truncate(time.Minute)
	var cursor time.Time
	if !since.IsZero() {
		cursor = since.UTC().Truncate(time.Minute)
	}
	// Tie-drain state: non-zero tieMinute = draining that minute's
	// cohort by key keyset (tieKey = last key served; "" = restart).
	var tieMinute time.Time
	tieKey := ""
	tiePages, tieBudget := 0, 0
	buildJQL := func() string {
		if !tieMinute.IsZero() {
			keyClause := ""
			if tieKey != "" {
				keyClause = fmt.Sprintf(" AND issuekey > '%s'", tieKey)
			}
			return fmt.Sprintf("project = %s AND updated >= '%s' AND updated < '%s'%s ORDER BY issuekey ASC",
				projectKey, tieMinute.Format(jqlMinute), tieMinute.Add(time.Minute).Format(jqlMinute), keyClause)
		}
		if cursor.IsZero() {
			return fmt.Sprintf("project = %s AND updated <= '%s' ORDER BY updated ASC",
				projectKey, ceiling.Format(jqlMinute))
		}
		return fmt.Sprintf("project = %s AND updated >= '%s' AND updated <= '%s' ORDER BY updated ASC",
			projectKey, cursor.Format(jqlMinute), ceiling.Format(jqlMinute))
	}
	seenThisWalk := map[string]struct{}{}
	stalePages := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		page, err := c.SearchPage(ctx, buildJQL(), fields, 0, pageSize)
		if err != nil {
			return err
		}
		if len(page.Issues) == 0 {
			if !tieMinute.IsZero() {
				// The frozen minute is DRAINED: resume the window just
				// past it (nothing can re-enter a past minute — updated
				// only moves forward).
				cursor = tieMinute.Add(time.Minute)
				tieMinute, tieKey, tiePages, tieBudget = time.Time{}, "", 0, 0
				continue
			}
			return nil // window exhausted — the walk is done
		}
		if !tieMinute.IsZero() {
			tiePages++
			if tieBudget == 0 {
				// First tie page: the response's Total is the cohort's
				// remaining size — the page budget a well-behaved server
				// cannot exceed (drift only shrinks the cohort).
				tieBudget = page.Total/pageSize + maxStalePages + 2
			}
			last := page.Issues[len(page.Issues)-1].Key
			if last == tieKey || tiePages > tieBudget {
				return fmt.Errorf("project %s: tie-minute drain of %s made no key progress (last=%q pages=%d budget=%d) — misbehaving server?",
					projectKey, tieMinute.Format(jqlMinute), last, tiePages, tieBudget)
			}
			tieKey = last
		}
		var pageMax time.Time
		pageNew := 0
		updated := make([]time.Time, len(page.Issues))
		for i, is := range page.Issues {
			// A skipped issue would let later issues in the ASC page
			// push the caller's checkpoint past it (SR-3) — Jira's
			// timestamp format is fixed, so a parse failure is a defect
			// signal and fails the walk.
			t, perr := time.Parse(TimeLayout, is.Fields.Updated)
			if perr != nil {
				return fmt.Errorf("unparseable updated timestamp on %s (%q): %w", is.Key, is.Fields.Updated, perr)
			}
			updated[i] = t
			nk := is.Key + "@" + is.Fields.Updated
			if _, dup := seenThisWalk[nk]; !dup {
				seenThisWalk[nk] = struct{}{}
				pageNew++
			}
			if t.After(pageMax) {
				pageMax = t
			}
		}
		if tieMinute.IsZero() {
			if pageNew == 0 {
				stalePages++
				if stalePages >= maxStalePages {
					return fmt.Errorf("project %s: %d consecutive pages contributed nothing new (misbehaving server?) cursor=%v",
						projectKey, stalePages, cursor)
				}
			} else {
				stalePages = 0
			}
		}
		if err := visit(page.Issues, updated); err != nil {
			return err
		}
		// Advance the window. A page whose max cannot move the cursor
		// (an unmovable minute) enters the KEY-KEYSET tie drain — with
		// tieKey restarted to "" so the minute re-lists WHOLE in stable
		// key order (the page just processed listed an arbitrary
		// updated-order subset of the cohort; keysetting from its max
		// key would skip unlisted members with smaller keys).
		if tieMinute.IsZero() {
			pageMaxMinute := pageMax.UTC().Truncate(time.Minute)
			if pageMaxMinute.After(cursor) {
				cursor = pageMaxMinute
			} else {
				tieMinute = cursor
				tieKey = ""
			}
		}
		if pageSleep > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pageSleep):
			}
		}
	}
}
