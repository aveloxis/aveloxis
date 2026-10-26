// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// gitlabStatsSource is the data source the GitLab client sets on its
// daily clone rows.
const gitlabStatsSource = "GitLab API"

// gitlabCloneRows returns the 30 rows the GitLab client builds for a
// collection on last (UTC midnight): oldest first, one per day, the
// count of a day from counts (keyed YYYY-MM-DD) or 0, no unique count.
func gitlabCloneRows(last time.Time, counts map[string]int) []model.RepoClone {
	rows := make([]model.RepoClone, 30)
	for i := range rows {
		day := last.AddDate(0, 0, i-29)
		rows[i] = model.RepoClone{
			Timestamp:   day,
			TotalClones: counts[day.Format(time.DateOnly)],
			Origin:      model.DataOrigin{DataSource: gitlabStatsSource},
		}
	}
	return rows
}

// The errors below have the shapes the GitLab client returns: the HTTP
// client wraps a 403 that is not a rate limit as ErrForbidden, a 404 as
// ErrNotFound and a 5xx that outlived its retries as ErrTransient, and
// FetchCloneStats puts "GET /projects/:id/statistics: " in front.
var (
	errGitLabStatsForbidden = fmt.Errorf("GET /projects/:id/statistics: %w: https://gitlab.example/api/v4/projects/o%%2Fr/statistics (not a rate limit — may be a private repo or insufficient scope)", platform.ErrForbidden)
	errGitLabStatsNotFound  = fmt.Errorf("GET /projects/:id/statistics: %w: https://gitlab.example/api/v4/projects/o%%2Fr/statistics", platform.ErrNotFound)
	errGitLabStatsServer    = fmt.Errorf("GET /projects/:id/statistics: exhausted 10 retries for https://gitlab.example/api/v4/projects/o%%2Fr/statistics: %w", platform.ErrTransient)
)

// A key with the Reporter role: the 30 days are staged as clone rows
// and nothing else. GitLab has no views, paths, referrers or uniques, so
// no traffic row (and with it no window or week) is staged, and no
// failure or skip is logged. The collection day is 2026-11-30.
func TestCollectTrafficGitLabStagesThirtyCloneRows(t *testing.T) {
	last := time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)
	rows := gitlabCloneRows(last, map[string]int{"2026-11-02": 7, "2026-11-30": 3})
	client := &trafficFakeClient{plat: model.PlatformGitLab, clones: rows}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, time.Date(2026, time.November, 30, 15, 45, 0, 0, time.UTC))

	if len(result.Errors) != 0 {
		t.Errorf("result.Errors = %v, want none", result.Errors)
	}
	if len(sw.staged) != 30 || sw.count(EntityCloneStats) != 30 {
		t.Fatalf("staged %d rows, %d of them clone stats; want exactly 30 clone stats rows and nothing else", len(sw.staged), sw.count(EntityCloneStats))
	}
	for i, s := range sw.staged {
		var got model.RepoClone
		if err := json.Unmarshal(s.payload, &got); err != nil {
			t.Fatalf("staged row %d does not decode as model.RepoClone: %v", i, err)
		}
		want := rows[i]
		if !got.Timestamp.Equal(want.Timestamp) || got.TotalClones != want.TotalClones || got.Origin.DataSource != gitlabStatsSource {
			t.Errorf("staged row %d = %v %d %q, want %v %d %q", i, got.Timestamp, got.TotalClones, got.Origin.DataSource,
				want.Timestamp, want.TotalClones, gitlabStatsSource)
		}
		if got.UniqueClones != nil {
			t.Errorf("staged row %d has uniques %d, want none: GitLab counts no distinct cloners", i, *got.UniqueClones)
		}
		if !strings.Contains(string(s.payload), `"UniqueClones":null`) {
			t.Errorf("staged row %d is %s, want the absent unique count staged as null", i, s.payload)
		}
	}
	if logs != "" {
		t.Errorf("a successful GitLab read logged:\n%s", logs)
	}
}

// A 403 (a key below the Reporter role) or 404 (a project the key cannot
// see) on the statistics read is a routine skip: one Info line naming
// the repository and the reason, nothing staged, nothing recorded and
// no further traffic request.
func TestCollectTrafficGitLabSkipsCleanly(t *testing.T) {
	last := time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		err  error
		word string
	}{
		{"403", errGitLabStatsForbidden, "forbidden"},
		{"404", errGitLabStatsNotFound, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &trafficFakeClient{plat: model.PlatformGitLab, cloneErr: tc.err,
				clones: gitlabCloneRows(last, map[string]int{"2026-11-30": 3})}

			result, sw, logs := runCollectTraffic(context.Background(), t, client, last)

			if len(result.Errors) != 0 {
				t.Errorf("result.Errors = %v, want none: a skip is not an error", result.Errors)
			}
			if len(sw.staged) != 0 {
				t.Errorf("staged %d rows after a skip, want none", len(sw.staged))
			}
			if client.trafficCalls != 0 {
				t.Errorf("FetchTraffic called %d times after a skip, want 0", client.trafficCalls)
			}
			lines := skipLines(logs)
			if len(lines) != 1 {
				t.Fatalf("skip lines = %d, want exactly 1:\n%s", len(lines), logs)
			}
			for _, want := range []string{"level=INFO", "owner=o", "repo=r", "reason=", "/projects/:id/statistics", tc.word} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("skip line %q lacks %q", lines[0], want)
				}
			}
			if f := failureLines(logs); len(f) != 0 {
				t.Errorf("a skip logged failure lines:\n%s", strings.Join(f, "\n"))
			}
		})
	}
}

// Any other error on the statistics read is recorded as the clones
// error, as for GitHub, and the collection goes on: nothing the failed
// read returned is staged, the step returns, and FetchTraffic (which
// returns nothing for GitLab) stages nothing.
func TestCollectTrafficGitLabServerErrorIsRecorded(t *testing.T) {
	last := time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)
	client := &trafficFakeClient{plat: model.PlatformGitLab, cloneErr: errGitLabStatsServer,
		clones: gitlabCloneRows(last, map[string]int{"2026-11-30": 3})}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, last)

	if len(result.Errors) != 1 {
		t.Fatalf("result.Errors = %v, want exactly the clone stats error", result.Errors)
	}
	err := result.Errors[0]
	if !errors.Is(err, platform.ErrTransient) || !strings.HasPrefix(err.Error(), "clone stats: GET /projects/:id/statistics: ") {
		t.Errorf("recorded %q, want it prefixed \"clone stats: GET /projects/:id/statistics: \" and wrapping the cause", err)
	}
	if len(sw.staged) != 0 {
		t.Errorf("staged %d rows after a failed read, want none", len(sw.staged))
	}
	if lines := skipLines(logs); len(lines) != 0 {
		t.Errorf("a 5xx logged a skip line:\n%s", strings.Join(lines, "\n"))
	}
}
