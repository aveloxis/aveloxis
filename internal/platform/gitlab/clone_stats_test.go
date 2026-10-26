// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// The response bodies below are built to GitLab's ProjectDailyStatistics
// entity, not copied from its documentation: {"fetches": {"total": N,
// "days": [{"count": N, "date": "YYYY-MM-DD"}]}}, newest day first, only
// days that had a fetch. The entity files are project_daily_statistics.rb
// and project_daily_fetches.rb in
// https://github.com/gitlabhq/gitlabhq/blob/d88541597fd81cd491ef17d6eeb27c125337c488/lib/api/entities/

type statsDay struct {
	date  string
	count int
}

// statsBody renders days (given newest first, as GitLab lists them) in the
// entity's shape; total is the sum of the listed counts.
func statsBody(t *testing.T, days ...statsDay) string {
	t.Helper()
	type fetchDay struct {
		Count int    `json:"count"`
		Date  string `json:"date"`
	}
	listed := make([]fetchDay, 0, len(days))
	total := 0
	for _, d := range days {
		listed = append(listed, fetchDay{Count: d.count, Date: d.date})
		total += d.count
	}
	body, err := json.Marshal(map[string]any{"fetches": map[string]any{"total": total, "days": listed}})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// statsServer answers every request with status and body, and records the
// method and escaped path of each one.
type statsServer struct {
	status int
	body   string
	// date is the response's Date header, the server's own clock (the
	// window GitLab serves is computed on it, not on this host's).
	date string
	// omitDate drops the Date header, as a proxy that strips it would.
	omitDate bool

	mu       sync.Mutex
	requests []string
}

func (s *statsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, r.Method+" "+r.URL.EscapedPath())
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case s.omitDate:
		w.Header()["Date"] = nil
	case s.date != "":
		w.Header().Set("Date", s.date)
	}
	w.WriteHeader(s.status)
	_, _ = w.Write([]byte(s.body))
}

func (s *statsServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// statsClient serves status and body from a test GitLab and reads the
// clock at now.
func statsClient(t *testing.T, now time.Time, status int, body string) (*Client, *statsServer) {
	t.Helper()
	srv := &statsServer{status: status, body: body}
	if !now.IsZero() {
		// The server answers on the clock the caller injected, so the
		// window it serves and the window this host would compute agree
		// unless a test says otherwise.
		srv.date = now.UTC().Format(http.TimeFormat)
	}
	client := testClient(t, srv)
	client.now = func() time.Time { return now }
	return client, srv
}

func utc(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

const statsRequest = "GET /projects/group%2Fproject/statistics"

// wantPeriod fails the test unless got holds one row per day from first to
// last, oldest first, each at UTC midnight, with the counts in want (a day
// missing from want must be 0), no unique count and the GitLab data source.
func wantPeriod(t *testing.T, got []model.RepoClone, first, last time.Time, want map[time.Time]int) {
	t.Helper()
	days := int(last.Sub(first)/(24*time.Hour)) + 1
	if len(got) != days {
		t.Fatalf("%d rows, want %d (%s to %s)", len(got), days, first.Format("2006-01-02"), last.Format("2006-01-02"))
	}
	for i, row := range got {
		day := first.AddDate(0, 0, i)
		if !row.Timestamp.Equal(day) || row.Timestamp.Location() != time.UTC {
			t.Errorf("row %d Timestamp = %v, want %v in UTC", i, row.Timestamp, day)
		}
		if row.TotalClones != want[day] {
			t.Errorf("%s TotalClones = %d, want %d", day.Format("2006-01-02"), row.TotalClones, want[day])
		}
		if row.UniqueClones != nil {
			t.Errorf("%s UniqueClones = %d, want nil (GitLab counts no distinct cloners; 0 would claim nobody cloned)",
				day.Format("2006-01-02"), *row.UniqueClones)
		}
		if row.Origin.DataSource != "GitLab API" {
			t.Errorf("%s DataSource = %q, want %q", day.Format("2006-01-02"), row.Origin.DataSource, "GitLab API")
		}
	}
}

// A project with fetches on two days of the 30: every other day of the
// period comes back as a stored zero, because GitLab lists only days that
// had a fetch.
func TestFetchCloneStatsFillsAbsentDaysWithZero(t *testing.T) {
	body := statsBody(t, statsDay{"2026-11-30", 7}, statsDay{"2026-11-02", 3})
	client, srv := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC), http.StatusOK, body)

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 11, 1), utc(2026, 11, 30), map[time.Time]int{
		utc(2026, 11, 2):  3,
		utc(2026, 11, 30): 7,
	})
	if reqs := srv.seen(); len(reqs) != 1 || reqs[0] != statsRequest {
		t.Errorf("requests = %v, want exactly [%s]", reqs, statsRequest)
	}
}

// A project without any fetch in 30 days: GitLab answers an empty list,
// and the period is 30 stored zeros, not an empty result.
func TestFetchCloneStatsEmptyDaysIsThirtyZeros(t *testing.T) {
	client, _ := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC), http.StatusOK, statsBody(t))

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 11, 1), utc(2026, 11, 30), nil)
}

// The collection day is part of the period (GitLab counts it while it is
// still going on), and the 30 days cross a month and a year boundary here.
func TestFetchCloneStatsIncludesTheCurrentDay(t *testing.T) {
	body := statsBody(t, statsDay{"2027-01-05", 5}, statsDay{"2026-12-07", 2}, statsDay{"2026-12-06", 9})
	client, _ := statsClient(t, time.Date(2027, 1, 5, 0, 0, 1, 0, time.UTC), http.StatusOK, body)

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 12, 7), utc(2027, 1, 5), map[time.Time]int{
		utc(2026, 12, 7): 2,
		utc(2027, 1, 5):  5,
	})
}

// The collection day is the UTC day, whatever zone the clock reads in:
// 22:30 at UTC-5 on 11-30 is already 12-01 in UTC.
func TestFetchCloneStatsCollectionDayIsTheUTCDay(t *testing.T) {
	body := statsBody(t, statsDay{"2026-12-01", 4}, statsDay{"2026-11-02", 8}, statsDay{"2026-11-01", 6})
	local := time.Date(2026, 11, 30, 22, 30, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	client, _ := statsClient(t, local, http.StatusOK, body)

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 11, 2), utc(2026, 12, 1), map[time.Time]int{
		utc(2026, 11, 2): 8,
		utc(2026, 12, 1): 4,
	})
}

// Mid-day, GitLab and this clock can disagree about which day it is for a
// moment. A listed day outside the 30 UTC days ending today (one before the
// oldest, one after today) is dropped and does not touch any other day.
func TestFetchCloneStatsDropsListedDaysOutsideThePeriod(t *testing.T) {
	body := statsBody(t,
		statsDay{"2026-12-01", 40},
		statsDay{"2026-11-30", 7},
		statsDay{"2026-11-01", 3},
		statsDay{"2026-10-31", 20},
	)
	client, _ := statsClient(t, time.Date(2026, 11, 30, 15, 45, 0, 0, time.UTC), http.StatusOK, body)

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 11, 1), utc(2026, 11, 30), map[time.Time]int{
		utc(2026, 11, 1):  3,
		utc(2026, 11, 30): 7,
	})
}

// A key below the Reporter role gets 403 and an unreadable project 404;
// both classify as a routine skip, keep their sentinel and return no rows,
// and the client asks once.
func TestFetchCloneStatsRefusalIsASkip(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{
		{http.StatusForbidden, platform.ErrForbidden},
		{http.StatusNotFound, platform.ErrNotFound},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			client, srv := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC),
				tc.status, `{"message":"refused"}`)

			got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
			if err == nil {
				t.Fatalf("FetchCloneStats on %d returned no error", tc.status)
			}
			if got != nil {
				t.Errorf("FetchCloneStats on %d returned rows %+v, want nil", tc.status, got)
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("errors.Is(err, %v) = false for %v", tc.want, err)
			}
			if class := platform.ClassifyError(err); class != platform.ClassSkip {
				t.Errorf("ClassifyError = %v, want ClassSkip: %v", class, err)
			}
			if reqs := srv.seen(); len(reqs) != 1 {
				t.Errorf("requests = %v, want one", reqs)
			}
		})
	}
}

// A server error is a real failure: after the client's retries the error
// names the endpoint, keeps its transient cause, and no rows come back.
func TestFetchCloneStats500NamesTheEndpoint(t *testing.T) {
	// The client retries a 500 with backoff; skip the waits.
	t.Cleanup(platform.SetServerErrorSleepForTest(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }))
	client, _ := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC),
		http.StatusInternalServerError, `{"message":"500 Internal Server Error"}`)

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err == nil {
		t.Fatal("FetchCloneStats on a 500 returned no error")
	}
	if got != nil {
		t.Errorf("FetchCloneStats on a 500 returned rows %+v, want nil", got)
	}
	if want := "/projects/:id/statistics"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name the endpoint %q", err, want)
	}
	if !errors.Is(err, platform.ErrTransient) || platform.ClassifyError(err) != platform.ClassTransient {
		t.Errorf("error %v lost its cause: want ErrTransient, ClassTransient", err)
	}
}

// An answer that is not the entity's shape is an error, never a period of
// zeros: storing zeros for a day nobody counted claims that nobody cloned.
func TestFetchCloneStatsUnreadableAnswerIsAnError(t *testing.T) {
	for name, body := range map[string]string{
		"no fetches member":   `{"message":"ok"}`,
		"not JSON":            `<html>sign in</html>`,
		"unreadable date":     `{"fetches":{"total":1,"days":[{"count":1,"date":"yesterday"}]}}`,
		"no days array":       `{"fetches":{"total":7}}`,
		"null days":           `{"fetches":{"total":7,"days":null}}`,
		"a day without count": `{"fetches":{"total":7,"days":[{"date":"2026-11-29"}]}}`,
		"null day count":      `{"fetches":{"total":7,"days":[{"count":null,"date":"2026-11-29"}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC), http.StatusOK, body)

			got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
			if err == nil {
				t.Fatalf("FetchCloneStats returned %d rows and no error for %s", len(got), name)
			}
			if got != nil {
				t.Errorf("FetchCloneStats returned rows %+v with error %v, want nil", got, err)
			}
			if want := "/projects/:id/statistics"; !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name the endpoint %q", err, want)
			}
			if class := platform.ClassifyError(err); class == platform.ClassSkip {
				t.Errorf("ClassifyError = ClassSkip for %v: an unreadable answer is not a routine skip", err)
			}
		})
	}
}

// A client built by New has no injected clock and reads the real one: the
// last row is the UTC day of the call.
func TestFetchCloneStatsDefaultClockIsTheUTCDay(t *testing.T) {
	client, _ := statsClient(t, time.Time{}, http.StatusOK, statsBody(t))
	client.now = nil

	before := time.Now().UTC().Truncate(24 * time.Hour)
	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	after := time.Now().UTC().Truncate(24 * time.Hour)
	if len(got) != 30 {
		t.Fatalf("%d rows, want 30", len(got))
	}
	if last := got[len(got)-1].Timestamp; !last.Equal(before) && !last.Equal(after) {
		t.Errorf("last row %v, want the UTC day of the call (%v)", last, before)
	}
}

// The window GitLab serves is computed on GitLab's clock, and the
// response says which day that is in its Date header. Zero-filling from
// this host's date instead erases a saved count whenever GitLab has
// crossed midnight and this host has not: GitLab has then expired the
// oldest day this loop still includes and emits nothing for it, so the
// loop writes a 0 over a count no later read can recover.
func TestFetchCloneStatsWindowFollowsTheServerDate(t *testing.T) {
	// The host reads 2026-11-30 23:58 UTC; GitLab answered two minutes
	// later, on 12-01, so its 30 days are 11-02..12-01 and 11-01 is no
	// longer listed. The host's own window (11-01..11-30) would write a
	// 0 over 11-01.
	hostNow := time.Date(2026, 11, 30, 23, 58, 0, 0, time.UTC)
	body := statsBody(t, statsDay{"2026-12-01", 5}, statsDay{"2026-11-02", 8})
	client, srv := statsClient(t, hostNow, http.StatusOK, body)
	srv.date = "Tue, 01 Dec 2026 00:00:00 GMT"

	got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	wantPeriod(t, got, utc(2026, 11, 2), utc(2026, 12, 1), map[time.Time]int{
		utc(2026, 11, 2): 8,
		utc(2026, 12, 1): 5,
	})
}

// A Date header that is absent or unreadable cannot move the window: the
// host's own UTC day is the fallback, as before.
func TestFetchCloneStatsWithoutAServerDateUsesTheHostDay(t *testing.T) {
	for name, date := range map[string]string{"absent": "", "unreadable": "not-a-date"} {
		t.Run(name, func(t *testing.T) {
			body := statsBody(t, statsDay{"2026-11-30", 7})
			client, srv := statsClient(t, time.Date(2026, 11, 30, 8, 0, 0, 0, time.UTC), http.StatusOK, body)
			if date == "" {
				srv.omitDate = true
			} else {
				srv.date = date
			}

			got, _, err := client.FetchCloneStats(context.Background(), "group", "project")
			if err != nil {
				t.Fatalf("FetchCloneStats: %v", err)
			}
			wantPeriod(t, got, utc(2026, 11, 1), utc(2026, 11, 30), map[time.Time]int{
				utc(2026, 11, 30): 7,
			})
		})
	}
}
