// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

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

// trafficLiveDefaultRepo is the repository TestLiveGitHubTrafficContract
// reads unless AVELOXIS_TEST_TRAFFIC_REPO names another. GitHub serves
// traffic only to a token with push access to the repository, and
// Aveloxis' own repository is the one its maintainers can give the
// canary token that access on.
const trafficLiveDefaultRepo = "aveloxis/aveloxis"

// trafficLiveReads are the traffic answers the canary checks, by their
// endpoint below /repos/{owner}/{repo}/traffic/: the five reads of
// FetchTraffic and the daily clones of FetchCloneStats (clones?per=day,
// GitHub's default, which FetchCloneStats leaves implicit).
var trafficLiveReads = []string{
	"views?per=day", "views?per=week",
	"clones?per=day", "clones?per=week",
	"popular/paths", "popular/referrers",
}

// TestLiveGitHubTrafficContract is the network canary of the traffic
// reads (FetchCloneStats, FetchTraffic). Their unit tests serve GitHub's
// OpenAPI examples and a zero-traffic recording, which cannot show GitHub
// changing its live answers. This reads them and checks the shape the
// collection relies on, never the values, which change every day (see
// trafficContractProblems).
//
// Gated on AVELOXIS_TEST_NETWORK=1 and AVELOXIS_TEST_GITHUB_TOKEN. A token
// without push access to the repository gets 403, and the test then skips
// saying so. Set AVELOXIS_TEST_TRAFFIC_REPO=owner/repo to read another
// repository than aveloxis/aveloxis.
func TestLiveGitHubTrafficContract(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the live traffic canary")
	}
	full := os.Getenv("AVELOXIS_TEST_TRAFFIC_REPO")
	if full == "" {
		full = trafficLiveDefaultRepo
	}
	owner, repo, ok := strings.Cut(full, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		t.Fatalf("AVELOXIS_TEST_TRAFFIC_REPO is %q, want owner/repo", full)
	}

	lg := slog.Default()
	keys := platform.NewKeyPool([]string{tok}, lg)
	httpClient := platform.NewHTTPClient("https://api.github.com", keys, lg, platform.AuthGitHub)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	answers := map[string]json.RawMessage{}
	for _, read := range trafficLiveReads {
		var body json.RawMessage
		err := httpClient.GetJSON(ctx, fmt.Sprintf("/repos/%s/%s/traffic/%s", owner, repo, read), &body)
		if errors.Is(err, platform.ErrForbidden) {
			t.Skipf("AVELOXIS_TEST_GITHUB_TOKEN gets 403 on %s traffic/%s: GitHub serves traffic only with push access to the repository (%v)", full, read, err)
		}
		if err != nil {
			t.Fatalf("%s traffic/%s: %v", full, read, err)
		}
		answers[read] = body
	}
	for _, p := range trafficContractProblems(answers) {
		t.Errorf("%s traffic/%s", full, p)
	}
}

// trafficLiveDays is how many daily buckets GitHub returns for views and
// clones: its 14-day window, every day present, zero-filled. The OpenAPI
// examples have 15, so only the live API shows this.
const trafficLiveDays = 14

// trafficContractProblems checks the traffic answers, keyed like
// trafficLiveReads, and returns one message per difference, each starting
// with the read and naming the field. It checks shape, never values:
//
//   - views and clones, per=day and per=week: a top-level count and
//     uniques, and a bucket list named after the kind whose buckets have
//     an RFC 3339 timestamp, a count and uniques;
//   - per=day: exactly 14 buckets, each a UTC midnight, on consecutive
//     days, so the days without traffic are zero-filled rather than left
//     out;
//   - per kind, the same top-level uniques for per=day and per=week:
//     FetchTraffic takes the clones' 14-day uniques from clones?per=week;
//   - popular/paths and popular/referrers: arrays whose entries have the
//     fields the client maps.
func trafficContractProblems(answers map[string]json.RawMessage) []string {
	var problems []string
	add := func(read, format string, args ...any) {
		problems = append(problems, read+": "+fmt.Sprintf(format, args...))
	}
	for _, read := range trafficLiveReads {
		if _, ok := answers[read]; !ok {
			add(read, "no answer")
		}
	}

	for _, kind := range []string{"views", "clones"} {
		uniques := map[string]int{}
		for _, per := range []string{"day", "week"} {
			read := kind + "?per=" + per
			raw, ok := answers[read]
			if !ok {
				continue
			}
			var top map[string]json.RawMessage
			if json.Unmarshal(raw, &top) != nil || top == nil {
				add(read, "not a JSON object")
				continue
			}
			for _, field := range []string{"count", "uniques"} {
				n, ok := trafficJSONInt(top[field])
				if !ok {
					add(read, "top-level %q is missing or not an integer", field)
					continue
				}
				if field == "uniques" {
					uniques[per] = n
				}
			}
			var buckets []map[string]json.RawMessage
			if json.Unmarshal(top[kind], &buckets) != nil || buckets == nil {
				add(read, "%q is missing or not an array", kind)
				continue
			}
			times := make([]time.Time, 0, len(buckets))
			for i, b := range buckets {
				ts, ok := trafficJSONTime(b["timestamp"])
				if !ok {
					add(read, "%q[%d].timestamp is missing or not an RFC 3339 time", kind, i)
				} else {
					times = append(times, ts)
				}
				for _, field := range []string{"count", "uniques"} {
					if _, ok := trafficJSONInt(b[field]); !ok {
						add(read, "%q[%d].%s is missing or not an integer", kind, i, field)
					}
				}
			}
			if per != "day" || len(times) != len(buckets) {
				continue
			}
			if len(buckets) != trafficLiveDays {
				add(read, "%q has %d daily buckets, want %d", kind, len(buckets), trafficLiveDays)
			}
			for i, ts := range times {
				y, m, d := ts.UTC().Date()
				if !ts.Equal(time.Date(y, m, d, 0, 0, 0, 0, time.UTC)) {
					add(read, "%q[%d].timestamp %s is not a UTC midnight", kind, i, ts.Format(time.RFC3339))
					continue
				}
				if want := times[0].AddDate(0, 0, i); i > 0 && !ts.Equal(want) {
					add(read, "%q[%d].timestamp is %s, want %s (consecutive UTC days, zero-filled)",
						kind, i, ts.UTC().Format(time.RFC3339), want.UTC().Format(time.RFC3339))
				}
			}
		}
		day, dayOK := uniques["day"]
		week, weekOK := uniques["week"]
		if dayOK && weekOK && day != week {
			add(kind, "top-level %q is %d with per=day and %d with per=week, want equal", "uniques", day, week)
		}
	}

	popular := []struct {
		read    string
		strings []string
	}{
		{"popular/paths", []string{"path", "title"}},
		{"popular/referrers", []string{"referrer"}},
	}
	for _, pop := range popular {
		raw, ok := answers[pop.read]
		if !ok {
			continue
		}
		var entries []map[string]json.RawMessage
		if json.Unmarshal(raw, &entries) != nil || entries == nil {
			add(pop.read, "not a JSON array")
			continue
		}
		for i, e := range entries {
			for _, field := range pop.strings {
				var s *string
				if json.Unmarshal(e[field], &s) != nil || s == nil {
					add(pop.read, "[%d].%s is missing or not a string", i, field)
				}
			}
			for _, field := range []string{"count", "uniques"} {
				if _, ok := trafficJSONInt(e[field]); !ok {
					add(pop.read, "[%d].%s is missing or not an integer", i, field)
				}
			}
		}
	}
	return problems
}

// trafficJSONInt decodes a JSON integer; a missing field (nil), null, a
// string or a fraction is not one.
func trafficJSONInt(raw json.RawMessage) (int, bool) {
	var n *int
	if json.Unmarshal(raw, &n) != nil || n == nil {
		return 0, false
	}
	return *n, true
}

// trafficJSONTime decodes a JSON string holding an RFC 3339 time, the
// form the client's time.Time fields decode.
func trafficJSONTime(raw json.RawMessage) (time.Time, bool) {
	var s *string
	if json.Unmarshal(raw, &s) != nil || s == nil {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339, *s)
	return ts, err == nil
}

// trafficContractAnswers returns the zero-traffic recording for every
// read of trafficLiveReads: a valid set of answers each test case edits.
func trafficContractAnswers(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	return map[string]json.RawMessage{
		"views?per=day":     readTrafficBody(t, "traffic_zero_views_day.json"),
		"views?per=week":    readTrafficBody(t, "traffic_zero_views_week.json"),
		"clones?per=day":    readTrafficBody(t, "traffic_zero_clones_day.json"),
		"clones?per=week":   readTrafficBody(t, "traffic_zero_clones_week.json"),
		"popular/paths":     readTrafficBody(t, "traffic_zero_popular_paths.json"),
		"popular/referrers": readTrafficBody(t, "traffic_zero_popular_referrers.json"),
	}
}

// editTrafficAnswer decodes one answer, lets edit change it and encodes
// the result.
func editTrafficAnswer(t *testing.T, raw json.RawMessage, edit func(v any) any) json.RawMessage {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decoding a traffic answer: %v", err)
	}
	out, err := json.Marshal(edit(v))
	if err != nil {
		t.Fatalf("encoding a traffic answer: %v", err)
	}
	return out
}

// trafficBuckets returns the bucket list of a views or clones answer
// decoded by editTrafficAnswer.
func trafficBuckets(v any, list string) []any {
	return v.(map[string]any)[list].([]any)
}

// The answers the canary accepts: the zero-traffic recording, which is a
// real answer of the live API, and the same with GitHub's OpenAPI
// examples of the popular paths and referrers, whose entries carry every
// field the client maps.
func TestTrafficContractCheckAcceptsRealShapes(t *testing.T) {
	t.Run("zero-traffic recording", func(t *testing.T) {
		if p := trafficContractProblems(trafficContractAnswers(t)); len(p) != 0 {
			t.Errorf("problems %q, want none", p)
		}
	})
	t.Run("popular entries of the OpenAPI examples", func(t *testing.T) {
		answers := trafficContractAnswers(t)
		answers["popular/paths"] = readTrafficBody(t, "traffic_openapi_popular_paths.json")
		answers["popular/referrers"] = readTrafficBody(t, "traffic_openapi_popular_referrers.json")
		if p := trafficContractProblems(answers); len(p) != 0 {
			t.Errorf("problems %q, want none", p)
		}
	})
}

// When GitHub changes a field the collection reads, the canary must fail
// and name that field (the canary's failure is the only signal: the unit
// tests keep serving the old shape).
func TestTrafficContractCheckNamesTheFieldThatChanged(t *testing.T) {
	cases := []struct {
		name string
		read string
		edit func(v any) any
		want string // a substring of one of the problems
	}{
		{
			name: "daily top-level count missing",
			read: "views?per=day",
			edit: func(v any) any { delete(v.(map[string]any), "count"); return v },
			want: `views?per=day: top-level "count" is missing or not an integer`,
		},
		{
			name: "weekly top-level uniques missing",
			read: "clones?per=week",
			edit: func(v any) any { delete(v.(map[string]any), "uniques"); return v },
			want: `clones?per=week: top-level "uniques" is missing or not an integer`,
		},
		{
			name: "bucket list renamed",
			read: "views?per=day",
			edit: func(v any) any {
				m := v.(map[string]any)
				m["items"] = m["views"]
				delete(m, "views")
				return m
			},
			want: `views?per=day: "views" is missing or not an array`,
		},
		{
			name: "bucket count no longer an integer",
			read: "clones?per=day",
			edit: func(v any) any {
				trafficBuckets(v, "clones")[0].(map[string]any)["count"] = "0"
				return v
			},
			want: `clones?per=day: "clones"[0].count is missing or not an integer`,
		},
		{
			name: "weekly bucket timestamp missing",
			read: "views?per=week",
			edit: func(v any) any {
				delete(trafficBuckets(v, "views")[1].(map[string]any), "timestamp")
				return v
			},
			want: `views?per=week: "views"[1].timestamp is missing or not an RFC 3339 time`,
		},
		{
			name: "a day without traffic no longer returned",
			read: "clones?per=day",
			edit: func(v any) any {
				m := v.(map[string]any)
				b := trafficBuckets(v, "clones")
				m["clones"] = append(append([]any{}, b[:5]...), b[6:]...)
				return m
			},
			want: `clones?per=day: "clones"[5].timestamp is 2026-10-12T00:00:00Z, want 2026-10-11T00:00:00Z (consecutive UTC days, zero-filled)`,
		},
		{
			name: "fifteen days, as in the OpenAPI example",
			read: "views?per=day",
			edit: func(v any) any {
				m := v.(map[string]any)
				m["views"] = append(trafficBuckets(v, "views"),
					map[string]any{"timestamp": "2026-10-20T00:00:00Z", "count": 0, "uniques": 0})
				return m
			},
			want: `views?per=day: "views" has 15 daily buckets, want 14`,
		},
		{
			name: "daily bucket not at a UTC midnight",
			read: "views?per=day",
			edit: func(v any) any {
				for i, b := range trafficBuckets(v, "views") {
					b.(map[string]any)["timestamp"] = fmt.Sprintf("2026-10-%02dT00:00:00+02:00", 6+i)
				}
				return v
			},
			want: `views?per=day: "views"[0].timestamp 2026-10-06T00:00:00+02:00 is not a UTC midnight`,
		},
		{
			name: "per=week uniques differ from per=day",
			read: "clones?per=week",
			edit: func(v any) any { v.(map[string]any)["uniques"] = 3; return v },
			want: `clones: top-level "uniques" is 0 with per=day and 3 with per=week, want equal`,
		},
		{
			name: "path entry without title",
			read: "popular/paths",
			edit: func(any) any {
				return []any{map[string]any{"path": "/o/r", "count": 1, "uniques": 1}}
			},
			want: `popular/paths: [0].title is missing or not a string`,
		},
		{
			name: "referrers no longer an array",
			read: "popular/referrers",
			edit: func(any) any { return map[string]any{"referrers": []any{}} },
			want: `popular/referrers: not a JSON array`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answers := trafficContractAnswers(t)
			answers[tc.read] = editTrafficAnswer(t, answers[tc.read], tc.edit)
			problems := trafficContractProblems(answers)
			for _, p := range problems {
				if strings.Contains(p, tc.want) {
					return
				}
			}
			t.Errorf("problems %q, want one containing %q", problems, tc.want)
		})
	}

	t.Run("read missing", func(t *testing.T) {
		answers := trafficContractAnswers(t)
		delete(answers, "popular/referrers")
		problems := trafficContractProblems(answers)
		if len(problems) != 1 || problems[0] != "popular/referrers: no answer" {
			t.Errorf("problems %q, want exactly %q", problems, "popular/referrers: no answer")
		}
	})
}

// TestTrafficCanaryIsScheduled: an unscheduled canary provides no
// protection (network-canary.yml). The weekly canary job must run the
// traffic canary in a step that sets AVELOXIS_TEST_NETWORK=1 and passes
// AVELOXIS_TEST_GITHUB_TOKEN, with no if or continue-on-error that could
// switch it off. The test name comes from the function, so a rename
// breaks this build instead of leaving "-run <old name>" running no test.
func TestTrafficCanaryIsScheduled(t *testing.T) {
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
	fn := runtime.FuncForPC(reflect.ValueOf(TestLiveGitHubTrafficContract).Pointer()).Name()
	want := "go test ./internal/platform/github/ -run " + fn[strings.LastIndex(fn, ".")+1:] + " -count=1 -v"

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
			if st.Env["AVELOXIS_TEST_NETWORK"] != "1" || st.Env["AVELOXIS_TEST_GITHUB_TOKEN"] == "" {
				t.Errorf("the step running %q must set AVELOXIS_TEST_NETWORK=1 and AVELOXIS_TEST_GITHUB_TOKEN (env %v)", want, st.Env)
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
