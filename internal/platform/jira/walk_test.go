// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// walk_test.go — the shared drift-safe walk's own contract (Copilot
// round 3 on PR #193, #2: the identity backfill had re-spelled the
// walk as bare offsets over a mutable ORDER BY — the permanent-skip
// class — so the walk became ONE function both consumers route
// through, tested here at its own level; the worker's behavioral
// suite drives it end-to-end too).
package jira

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func walkPage(keys []string, updated []time.Time, startAt, maxResults int) string {
	total := len(keys)
	if startAt > len(keys) {
		startAt = len(keys)
	}
	keys, updated = keys[startAt:], updated[startAt:]
	if len(keys) > maxResults {
		keys, updated = keys[:maxResults], updated[:maxResults]
	}
	var issues []string
	for i, k := range keys {
		up := updated[i].Format(TimeLayout)
		issues = append(issues, fmt.Sprintf(
			`{"id":"%d","key":"%s","fields":{"summary":"s","updated":"%s","created":"%s"}}`,
			3000+startAt+i, k, up, up))
	}
	return fmt.Sprintf(`{"startAt":%d,"maxResults":%d,"total":%d,"issues":[%s]}`,
		startAt, len(keys), total, strings.Join(issues, ","))
}

// TestWalkProjectByUpdatedSurvivesMidWalkDrift — the permanent-skip
// red-proof at the walker's own level: B is touched between pages so
// the drifted window re-orders; the walk must still visit C.
func TestWalkProjectByUpdatedSurvivesMidWalkDrift(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	requests := 0
	bUpdated := base.Add(30 * time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		jql := r.URL.Query().Get("jql")
		cursor := time.Time{}
		if i := strings.Index(jql, "updated >= '"); i >= 0 {
			rest := jql[i+len("updated >= '"):]
			cursor, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
		}
		mu.Lock()
		requests++
		if requests == 2 {
			bUpdated = base.Add(45 * time.Minute) // the mid-walk touch
		}
		type row struct {
			key string
			up  time.Time
		}
		all := []row{{"AVWK-A", base}, {"AVWK-B", bUpdated}, {"AVWK-C", base.Add(30 * time.Minute)}}
		mu.Unlock()
		sort.Slice(all, func(i, j int) bool { return all[i].up.Before(all[j].up) })
		var keys []string
		var when []time.Time
		for _, rw := range all {
			if !rw.up.Before(cursor) {
				keys = append(keys, rw.key)
				when = append(when, rw.up)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, walkPage(keys, when, start, 2))
	}))
	defer srv.Close()

	visited := map[string]bool{}
	err := New(srv.URL, "").WalkProjectByUpdated(context.Background(), "AVWK", nil, 2, 0, time.Time{},
		func(issues []Issue, updated []time.Time) error {
			if len(issues) != len(updated) {
				t.Fatalf("issues/updated misaligned: %d vs %d", len(issues), len(updated))
			}
			for _, is := range issues {
				visited[is.Key] = true
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !visited["AVWK-C"] {
		t.Fatalf("visited = %v — the boundary-minute sibling C was skipped (the permanent-skip class)", visited)
	}
}

// TestWalkProjectByUpdatedFailsOnReServingServer — the termination
// bound: a server re-serving the same non-empty page regardless of
// cursor/offset must fail the walk in a handful of requests, never
// loop.
func TestWalkProjectByUpdatedFailsOnReServingServer(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, walkPage([]string{"AVWK-1", "AVWK-2"}, []time.Time{base, base.Add(time.Minute)}, 0, 2))
	}))
	defer srv.Close()

	done := make(chan error, 1)
	go func() {
		done <- New(srv.URL, "").WalkProjectByUpdated(context.Background(), "AVWK", nil, 2, 0, time.Time{},
			func([]Issue, []time.Time) error { return nil })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("walk must FAIL against a re-serving server, not complete")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("walk did not terminate against a re-serving server")
	}
	mu.Lock()
	defer mu.Unlock()
	if requests > 10 {
		t.Fatalf("requests = %d — the bound must trip within a handful of stale pages", requests)
	}
}

// TestWalkProjectByUpdatedVisitErrorAborts — a visit error aborts and
// surfaces AS-IS so callers classify at one site.
func TestWalkProjectByUpdatedVisitErrorAborts(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, walkPage([]string{"AVWK-1"}, []time.Time{base}, 0, 2))
	}))
	defer srv.Close()

	boom := fmt.Errorf("store exploded")
	err := New(srv.URL, "").WalkProjectByUpdated(context.Background(), "AVWK", nil, 2, 0, time.Time{},
		func([]Issue, []time.Time) error { return boom })
	if err == nil || !strings.Contains(err.Error(), "store exploded") {
		t.Fatalf("visit error must surface as-is, got: %v", err)
	}
}
