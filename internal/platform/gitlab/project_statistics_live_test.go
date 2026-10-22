// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestLiveGitLabProjectStatisticsContract is the network canary of
// FetchCloneStats. Its unit tests serve bodies built to GitLab's entity,
// which cannot show GitLab changing the live answer. This reads
// GET /projects/:id/statistics and checks the shape the collection relies
// on, never the values, which change every day (see
// statsContractProblems), then runs FetchCloneStats on the same project.
//
// Gated on AVELOXIS_TEST_NETWORK=1, AVELOXIS_TEST_GITLAB_TOKEN and
// AVELOXIS_TEST_GITLAB_STATS_PROJECT (group/project on gitlab.com). The
// endpoint needs at least the Reporter role and the read_api scope, so a
// token without them (403) or a project it cannot see (404) skips and
// says so.
func TestLiveGitLabProjectStatisticsContract(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITLAB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITLAB_TOKEN to run the live project statistics canary")
	}
	full := os.Getenv("AVELOXIS_TEST_GITLAB_STATS_PROJECT")
	if full == "" {
		t.Skip("set AVELOXIS_TEST_GITLAB_STATS_PROJECT (group/project, a project the token has the Reporter role on) to run the live project statistics canary")
	}
	// A project in a subgroup has a path of three or more parts; the
	// client takes everything before the last slash as the owner.
	cut := strings.LastIndex(full, "/")
	if cut <= 0 || cut == len(full)-1 {
		t.Fatalf("AVELOXIS_TEST_GITLAB_STATS_PROJECT is %q, want group/project", full)
	}
	owner, repo := full[:cut], full[cut+1:]

	lg := slog.Default()
	keys := platform.NewKeyPool([]string{tok}, lg)
	client := New("https://gitlab.com/api/v4", keys, lg)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	read := func() json.RawMessage {
		t.Helper()
		var body json.RawMessage
		err := client.http.GetJSON(ctx, "/projects/"+projectPath(owner, repo)+"/statistics", &body)
		if reason, skip := statsSkipReason(err); skip {
			t.Skipf("AVELOXIS_TEST_GITLAB_TOKEN cannot read the statistics of %s: %s (%v)", full, reason, err)
		}
		if err != nil {
			t.Fatalf("%s statistics: %v", full, err)
		}
		return body
	}
	problems := statsContractProblems(read(), time.Now())
	if len(problems) > 0 {
		// GitLab sums the total and lists the days in two queries, and
		// today's counter keeps moving, so one read can disagree with
		// itself. A problem that a second read repeats is real.
		problems = statsContractProblems(read(), time.Now())
	}
	for _, p := range problems {
		t.Errorf("%s statistics: %s", full, p)
	}

	rows, err := client.FetchCloneStats(ctx, owner, repo)
	if err != nil {
		t.Fatalf("FetchCloneStats on %s: %v", full, err)
	}
	if len(rows) != cloneStatsDays {
		t.Errorf("FetchCloneStats on %s returned %d rows, want %d", full, len(rows), cloneStatsDays)
	}
}

// statsSkipReason says why a failed read means the token cannot see the
// statistics rather than that GitLab changed: 403 (below the Reporter
// role, or a token without read_api) and 404 (a project the token cannot
// see, which GitLab answers the same as one that does not exist).
func statsSkipReason(err error) (string, bool) {
	switch {
	case errors.Is(err, platform.ErrForbidden):
		return "403: GitLab serves the statistics only to a key with at least the Reporter role on the project and the read_api scope", true
	case errors.Is(err, platform.ErrNotFound):
		return "404: the project is not visible to the token, or does not exist", true
	}
	return "", false
}

// statsDayWindow is how many UTC days the endpoint lists, the current one
// included.
const statsDayWindow = 30

// statsContractProblems checks the answer of GET /projects/:id/statistics
// as of now and returns one message per difference, each naming the field.
// It checks shape, never values:
//
//   - fetches.total is an integer and fetches.days an array;
//   - every day has an integer count and a date that parses as
//     YYYY-MM-DD;
//   - the days are listed newest first, one date at most once, all within
//     the last 30 UTC days (the current day included, and a day of clock
//     tolerance after it);
//   - total is the sum of the listed counts, which GitLab computes over
//     the same rows;
//   - no unique count appears in the answer, so the collection can store
//     none.
func statsContractProblems(raw json.RawMessage, now time.Time) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	noUniques := func(where string, obj map[string]json.RawMessage) {
		for field := range obj {
			if strings.Contains(strings.ToLower(field), "unique") {
				add("%s has the field %q, want no unique count", where, field)
			}
		}
	}

	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil || top == nil {
		return []string{"not a JSON object"}
	}
	noUniques("the answer", top)
	var fetches map[string]json.RawMessage
	if _, ok := top["fetches"]; !ok {
		return append(problems, `"fetches" is missing`)
	}
	if json.Unmarshal(top["fetches"], &fetches) != nil || fetches == nil {
		return append(problems, `"fetches" is not an object`)
	}
	noUniques("fetches", fetches)

	total, totalOK := statsJSONInt(fetches["total"])
	if !totalOK {
		add("fetches.total is missing or not an integer")
	}
	var days []map[string]json.RawMessage
	if json.Unmarshal(fetches["days"], &days) != nil || days == nil {
		add("fetches.days is missing or not an array")
		return problems
	}

	y, m, d := now.UTC().Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	oldest := today.AddDate(0, 0, 1-statsDayWindow)
	latest := today.AddDate(0, 0, 1)

	sum, countsOK := 0, true
	var prev time.Time
	for i, day := range days {
		where := fmt.Sprintf("fetches.days[%d]", i)
		noUniques(where, day)
		if n, ok := statsJSONInt(day["count"]); ok {
			sum += n
		} else {
			countsOK = false
			add("%s.count is missing or not an integer", where)
		}
		date, ok := statsJSONDate(day["date"])
		if !ok {
			add("%s.date is missing or not a YYYY-MM-DD date", where)
			continue
		}
		stamp := date.Format(time.DateOnly)
		if date.Before(oldest) {
			add("%s.date %s is older than %s, the oldest of the last %d UTC days", where, stamp, oldest.Format(time.DateOnly), statsDayWindow)
		}
		if date.After(latest) {
			add("%s.date %s is after %s, more than a day ahead of today", where, stamp, latest.Format(time.DateOnly))
		}
		if i > 0 && !prev.IsZero() && !date.Before(prev) {
			add("%s.date %s does not come after the one before it, %s (want newest first, each date once)", where, stamp, prev.Format(time.DateOnly))
		}
		prev = date
	}
	if totalOK && countsOK && total != sum {
		add("fetches.total is %d, want %d, the sum of the listed counts", total, sum)
	}
	return problems
}

// statsJSONInt decodes a JSON integer; a missing field (nil), null, a
// string or a fraction is not one.
func statsJSONInt(raw json.RawMessage) (int, bool) {
	var n *int
	if json.Unmarshal(raw, &n) != nil || n == nil {
		return 0, false
	}
	return *n, true
}

// statsJSONDate decodes a JSON string holding a YYYY-MM-DD date, the form
// FetchCloneStats parses.
func statsJSONDate(raw json.RawMessage) (time.Time, bool) {
	var s *string
	if json.Unmarshal(raw, &s) != nil || s == nil {
		return time.Time{}, false
	}
	date, err := time.Parse(time.DateOnly, *s)
	return date, err == nil
}

// statsContractNow is the clock of the hermetic checks below.
var statsContractNow = time.Date(2026, 10, 20, 6, 0, 0, 0, time.UTC)

// statsContractBody is an answer built to the shape of GitLab's
// ProjectDailyStatistics entity: three days, newest first, the oldest of
// them the oldest day of the window at statsContractNow.
func statsContractBody(t *testing.T) json.RawMessage {
	t.Helper()
	return json.RawMessage(`{"fetches":{"total":17,"days":[` +
		`{"count":4,"date":"2026-10-20"},` +
		`{"count":12,"date":"2026-10-18"},` +
		`{"count":1,"date":"2026-09-21"}]}}`)
}

// editStatsBody decodes an answer, lets edit change it and encodes the
// result.
func editStatsBody(t *testing.T, raw json.RawMessage, edit func(fetches map[string]any)) json.RawMessage {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decoding a statistics answer: %v", err)
	}
	edit(v["fetches"].(map[string]any))
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encoding a statistics answer: %v", err)
	}
	return out
}

// statsDays returns the day list of an answer decoded by editStatsBody.
func statsDays(fetches map[string]any) []any {
	return fetches["days"].([]any)
}

// The answers the canary accepts: the entity's shape, a project without
// fetches (GitLab lists no day then), the oldest day of the window, and a
// date one day ahead, which a clock a little behind GitLab's produces.
func TestProjectStatisticsContractCheckAcceptsEntityShapes(t *testing.T) {
	cases := []struct {
		name string
		body json.RawMessage
	}{
		{"three days", statsContractBody(t)},
		{"no fetches", json.RawMessage(`{"fetches":{"total":0,"days":[]}}`)},
		{"a date one day ahead", json.RawMessage(`{"fetches":{"total":2,"days":[{"count":2,"date":"2026-10-21"}]}}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if p := statsContractProblems(tc.body, statsContractNow); len(p) != 0 {
				t.Errorf("problems %q, want none", p)
			}
		})
	}
}

// When GitLab changes a field the collection reads, the canary must fail
// and name that field (its failure is the only signal: the unit tests keep
// serving the old shape).
func TestProjectStatisticsContractCheckNamesTheFieldThatChanged(t *testing.T) {
	cases := []struct {
		name string
		edit func(fetches map[string]any)
		want string // a substring of one of the problems
	}{
		{
			name: "total missing",
			edit: func(f map[string]any) { delete(f, "total") },
			want: "fetches.total is missing or not an integer",
		},
		{
			name: "total a string",
			edit: func(f map[string]any) { f["total"] = "17" },
			want: "fetches.total is missing or not an integer",
		},
		{
			name: "days renamed",
			edit: func(f map[string]any) { f["items"] = f["days"]; delete(f, "days") },
			want: "fetches.days is missing or not an array",
		},
		{
			name: "days an object",
			edit: func(f map[string]any) { f["days"] = map[string]any{"2026-10-20": 4} },
			want: "fetches.days is missing or not an array",
		},
		{
			name: "day without count",
			edit: func(f map[string]any) { delete(statsDays(f)[1].(map[string]any), "count") },
			want: "fetches.days[1].count is missing or not an integer",
		},
		{
			name: "count a string",
			edit: func(f map[string]any) { statsDays(f)[0].(map[string]any)["count"] = "4" },
			want: "fetches.days[0].count is missing or not an integer",
		},
		{
			name: "day without date",
			edit: func(f map[string]any) { delete(statsDays(f)[2].(map[string]any), "date") },
			want: "fetches.days[2].date is missing or not a YYYY-MM-DD date",
		},
		{
			name: "date with a time",
			edit: func(f map[string]any) { statsDays(f)[0].(map[string]any)["date"] = "2026-10-20T00:00:00Z" },
			want: "fetches.days[0].date is missing or not a YYYY-MM-DD date",
		},
		{
			name: "oldest first",
			edit: func(f map[string]any) {
				d := statsDays(f)
				d[0], d[2] = d[2], d[0]
			},
			want: "fetches.days[1].date 2026-10-18 does not come after the one before it, 2026-09-21 (want newest first, each date once)",
		},
		{
			name: "a date listed twice",
			edit: func(f map[string]any) { statsDays(f)[1].(map[string]any)["date"] = "2026-10-20" },
			want: "fetches.days[1].date 2026-10-20 does not come after the one before it, 2026-10-20",
		},
		{
			name: "a day older than 30 days",
			edit: func(f map[string]any) { statsDays(f)[2].(map[string]any)["date"] = "2026-09-20" },
			want: "fetches.days[2].date 2026-09-20 is older than 2026-09-21, the oldest of the last 30 UTC days",
		},
		{
			name: "a day two days ahead",
			edit: func(f map[string]any) {
				f["days"] = append([]any{map[string]any{"count": 0, "date": "2026-10-22"}}, statsDays(f)...)
			},
			want: "fetches.days[0].date 2026-10-22 is after 2026-10-21, more than a day ahead of today",
		},
		{
			name: "total not the sum",
			edit: func(f map[string]any) { f["total"] = 18 },
			want: "fetches.total is 18, want 17, the sum of the listed counts",
		},
		{
			name: "unique count in a day",
			edit: func(f map[string]any) { statsDays(f)[1].(map[string]any)["uniques"] = 3 },
			want: `fetches.days[1] has the field "uniques", want no unique count`,
		},
		{
			name: "unique count beside the total",
			edit: func(f map[string]any) { f["unique_total"] = 3 },
			want: `fetches has the field "unique_total", want no unique count`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			problems := statsContractProblems(editStatsBody(t, statsContractBody(t), tc.edit), statsContractNow)
			for _, p := range problems {
				if strings.Contains(p, tc.want) {
					return
				}
			}
			t.Errorf("problems %q, want one containing %q", problems, tc.want)
		})
	}

	t.Run("fetches missing", func(t *testing.T) {
		problems := statsContractProblems(json.RawMessage(`{"statistics":{}}`), statsContractNow)
		if len(problems) != 1 || problems[0] != `"fetches" is missing` {
			t.Errorf("problems %q, want exactly %q", problems, `"fetches" is missing`)
		}
	})
	t.Run("fetches not an object", func(t *testing.T) {
		problems := statsContractProblems(json.RawMessage(`{"fetches":[]}`), statsContractNow)
		if len(problems) != 1 || problems[0] != `"fetches" is not an object` {
			t.Errorf("problems %q, want exactly %q", problems, `"fetches" is not an object`)
		}
	})
	t.Run("answer not an object", func(t *testing.T) {
		problems := statsContractProblems(json.RawMessage(`[]`), statsContractNow)
		if len(problems) != 1 || problems[0] != "not a JSON object" {
			t.Errorf("problems %q, want exactly %q", problems, "not a JSON object")
		}
	})
}

// A 403 and a 404 mean the token cannot see the statistics, and the canary
// skips with the reason; any other failure is for the canary to report.
func TestProjectStatisticsSkipReasons(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string // "" when the read must not skip
	}{
		{"403", fmt.Errorf("GET /projects/x/statistics: %w", platform.ErrForbidden), "Reporter role"},
		{"404", fmt.Errorf("GET /projects/x/statistics: %w", platform.ErrNotFound), "not visible to the token"},
		{"a server error", platform.ErrTransient, ""},
		{"a transport error", errors.New("connection refused"), ""},
		{"no error", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, skip := statsSkipReason(tc.err)
			if skip != (tc.want != "") || !strings.Contains(reason, tc.want) {
				t.Errorf("statsSkipReason(%v) = %q, %v; want a skip containing %q", tc.err, reason, skip, tc.want)
			}
		})
	}
}

// TestProjectStatisticsCanaryIsScheduled: an unscheduled canary provides
// no protection (network-canary.yml). The weekly canary job must run the
// statistics canary once, in a step that sets AVELOXIS_TEST_NETWORK=1 and
// passes the token and the project, with no if or continue-on-error that
// could switch it off. The test name comes from the function, so a rename
// breaks this build instead of leaving "-run <old name>" running no test.
func TestProjectStatisticsCanaryIsScheduled(t *testing.T) {
	var wf struct {
		Jobs map[string]struct {
			If              any `yaml:"if"`
			ContinueOnError any `yaml:"continue-on-error"`
			Steps           []struct {
				If              any               `yaml:"if"`
				ContinueOnError any               `yaml:"continue-on-error"`
				Env             map[string]string `yaml:"env"`
				Run             string            `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".github/workflows/network-canary.yml")), &wf); err != nil {
		t.Fatalf("network-canary.yml does not parse: %v", err)
	}
	fn := runtime.FuncForPC(reflect.ValueOf(TestLiveGitLabProjectStatisticsContract).Pointer()).Name()
	want := "go test ./internal/platform/gitlab/ -run " + fn[strings.LastIndex(fn, ".")+1:] + " -count=1 -v"

	job, ok := wf.Jobs["canary"]
	if !ok {
		t.Fatal("network-canary.yml has no canary job")
	}
	if job.If != nil || job.ContinueOnError != nil {
		t.Errorf("the canary job must have no if or continue-on-error (if=%v continue-on-error=%v)", job.If, job.ContinueOnError)
	}
	found := 0
	for _, st := range job.Steps {
		for _, line := range strings.Split(st.Run, "\n") {
			if strings.TrimSpace(line) != want {
				continue
			}
			found++
			if st.Env["AVELOXIS_TEST_NETWORK"] != "1" || st.Env["AVELOXIS_TEST_GITLAB_TOKEN"] == "" || st.Env["AVELOXIS_TEST_GITLAB_STATS_PROJECT"] == "" {
				t.Errorf("the step running %q must set AVELOXIS_TEST_NETWORK=1, AVELOXIS_TEST_GITLAB_TOKEN and AVELOXIS_TEST_GITLAB_STATS_PROJECT (env %v)", want, st.Env)
			}
			if st.If != nil || st.ContinueOnError != nil {
				t.Errorf("the step running %q must have no if or continue-on-error", want)
			}
		}
	}
	if found != 1 {
		t.Errorf("the canary job runs %q %d times, want once", want, found)
	}
}
