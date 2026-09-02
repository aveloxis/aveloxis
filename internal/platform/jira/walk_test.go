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
		mu.Lock()
		requests++
		if requests == 2 {
			bUpdated = base.Add(45 * time.Minute) // the mid-walk touch
		}
		keys := []string{"AVWK-1", "AVWK-2", "AVWK-3"}
		when := []time.Time{base, bUpdated, base.Add(30 * time.Minute)}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jqlServe(r.URL.Query().Get("jql"), keys, when, 2))
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
	if !visited["AVWK-3"] {
		t.Fatalf("visited = %v — the boundary-minute sibling AVWK-3 was skipped (the permanent-skip class)", visited)
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

// jqlServe emulates real Jira Server jql semantics for walk fixtures:
// `updated >= 'X'`, `updated < 'Y'`, `issuekey > 'K'` (numeric by
// issue number, as Jira orders same-project keys), and
// `ORDER BY issuekey ASC` vs `ORDER BY updated ASC`. The walk
// paginates purely by cursor/keyset (startAt is always 0), so a
// fixture that ignored these clauses would re-serve the same page
// forever and trip the walker's own termination bounds.
func jqlServe(jql string, keys []string, ups []time.Time, maxResults int) string {
	type row struct {
		key string
		up  time.Time
	}
	keyNum := func(k string) int {
		i := strings.LastIndex(k, "-")
		n, _ := strconv.Atoi(k[i+1:])
		return n
	}
	var ge, lt time.Time
	if i := strings.Index(jql, "updated >= '"); i >= 0 {
		rest := jql[i+len("updated >= '"):]
		ge, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
	}
	if i := strings.Index(jql, "updated < '"); i >= 0 {
		rest := jql[i+len("updated < '"):]
		lt, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
	}
	keyGT := ""
	if i := strings.Index(jql, "issuekey > '"); i >= 0 {
		rest := jql[i+len("issuekey > '"):]
		keyGT = rest[:strings.Index(rest, "'")]
	}
	var rows []row
	for i := range keys {
		if !ge.IsZero() && ups[i].Before(ge) {
			continue
		}
		if !lt.IsZero() && !ups[i].Before(lt) {
			continue
		}
		if keyGT != "" && keyNum(keys[i]) <= keyNum(keyGT) {
			continue
		}
		rows = append(rows, row{keys[i], ups[i]})
	}
	if strings.Contains(jql, "ORDER BY issuekey") {
		sort.Slice(rows, func(i, j int) bool { return keyNum(rows[i].key) < keyNum(rows[j].key) })
	} else {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].up.Before(rows[j].up) })
	}
	total := len(rows)
	if len(rows) > maxResults {
		rows = rows[:maxResults]
	}
	var issues []string
	for _, r := range rows {
		up := r.up.Format(TimeLayout)
		issues = append(issues, fmt.Sprintf(
			`{"id":"%d","key":"%s","fields":{"summary":"s","updated":"%s","created":"%s"}}`,
			4000+keyNum(r.key), r.key, up, up))
	}
	return fmt.Sprintf(`{"startAt":0,"maxResults":%d,"total":%d,"issues":[%s]}`,
		len(rows), total, strings.Join(issues, ","))
}

// TestWalkProjectByUpdatedDrainsWideTieMinute (Copilot round 9 on
// PR #193): a same-minute cohort WIDER than a page drains by key
// keyset — drift between the tie pages (an issue touched mid-drain
// leaves the minute) must not skip an unseen sibling, and the drain
// must terminate and resume the outer window past the minute.
func TestWalkProjectByUpdatedDrainsWideTieMinute(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	requests := 0
	// Five issues share minute 10:00 (pageSize 2 → the cohort spans 3
	// pages); one late issue sits at 11:00. AVTM-2 is touched after
	// the SECOND request — the old offset fallback would have shifted
	// the remaining cohort left and skipped a sibling.
	// Seconds deliberately SHUFFLE updated-order away from key-order
	// (5 earliest, then 1..4): the boundary page lists an arbitrary
	// updated-order subset, so a tie drain that keysets from that
	// subset's max key instead of RESTARTING the minute would skip
	// every lower-key member it never listed.
	ups := map[string]time.Time{
		"AVTM-5": base, "AVTM-1": base.Add(10 * time.Second), "AVTM-2": base.Add(20 * time.Second),
		"AVTM-3": base.Add(30 * time.Second), "AVTM-4": base.Add(40 * time.Second),
		"AVTM-6": base.Add(time.Hour),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		if requests == 2 {
			ups["AVTM-2"] = base.Add(45 * time.Minute) // the mid-drain touch
		}
		keys := []string{"AVTM-1", "AVTM-2", "AVTM-3", "AVTM-4", "AVTM-5", "AVTM-6"}
		var when []time.Time
		for _, k := range keys {
			when = append(when, ups[k])
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jqlServe(r.URL.Query().Get("jql"), keys, when, 2))
	}))
	defer srv.Close()

	visited := map[string]bool{}
	err := New(srv.URL, "").WalkProjectByUpdated(context.Background(), "AVTM", nil, 2, 0, time.Time{},
		func(issues []Issue, _ []time.Time) error {
			for _, is := range issues {
				visited[is.Key] = true
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"AVTM-1", "AVTM-2", "AVTM-3", "AVTM-4", "AVTM-5", "AVTM-6"} {
		if !visited[k] {
			t.Fatalf("visited = %v — %s was skipped (the tie-minute drift class the key keyset retires)", visited, k)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if requests > 12 {
		t.Fatalf("requests = %d — the tie drain must be bounded", requests)
	}
}
