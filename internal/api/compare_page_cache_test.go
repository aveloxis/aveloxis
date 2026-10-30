// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The page answer is used only when it is a complete single-entity series
// answer; anything else falls back to computing the entity directly (an
// optional path is never terminal).
func TestSeriesFromPageAnswer(t *testing.T) {
	good := `{"bucket":"week","since":"2023-10-16","until":"2026-10-19","series":[{"entity":{"kind":"repo","repo_id":7,"label":"o/r"},"points":[{"bucket":"2026-01-05T00:00:00Z","value":3}],"data_start":"2025-01-01"}]}`
	want := compareWindowOf("week", day(t, "2023-10-16"), day(t, "2026-10-19"))
	cs, ok := seriesFromPageAnswer(200, []byte(good), want)
	if !ok || len(cs.Points) != 1 || cs.Points[0].Value != 3 || cs.DataStart != "2025-01-01" {
		t.Fatalf("a complete answer must be used: ok=%v %+v", ok, cs)
	}
	for name, c := range map[string]struct {
		status int
		body   string
	}{
		"not 200":        {500, good},
		"no series":      {200, `{"series":[]}`},
		"two series":     {200, `{"series":[{"points":[]},{"points":[]}]}`},
		"not JSON":       {200, `series failed`},
		"half an answer": {200, good[:40]},
	} {
		if _, ok := seriesFromPageAnswer(c.status, []byte(c.body), want); ok {
			t.Errorf("%s: must not be used", name)
		}
	}
}

// Whole-branch review of 0.29.119–0.29.123, A9: /compare reads the clock
// for its window, and the page route reads it again for each repository
// entity; a comparison straddling a week's (or a month's) start got its
// organisation series on one window and its repository series on the next,
// one bucket apart. A page answer is used only for /compare's own window —
// otherwise the entity is computed directly on it.
func TestSeriesFromPageAnswerKeepsTheComparisonsWindow(t *testing.T) {
	body := func(bucket, since, until string) []byte {
		return []byte(`{"bucket":"` + bucket + `","since":"` + since + `","until":"` + until + `","series":[{"points":[]}]}`)
	}
	want := compareWindowOf("week", day(t, "2023-10-16"), day(t, "2026-10-19"))
	if _, ok := seriesFromPageAnswer(200, body("week", "2023-10-16", "2026-10-19"), want); !ok {
		t.Fatal("an answer on the comparison's window must be used")
	}
	for name, b := range map[string][]byte{
		"the next week":   body("week", "2023-10-23", "2026-10-26"),
		"another until":   body("week", "2023-10-16", "2026-10-26"),
		"another since":   body("week", "2023-10-23", "2026-10-19"),
		"another bucket":  body("month", "2023-10-16", "2026-10-19"),
		"no window given": []byte(`{"series":[{"points":[]}]}`),
	} {
		if _, ok := seriesFromPageAnswer(200, b, want); ok {
			t.Errorf("%s: an answer on another window must not be used", name)
		}
	}
}

func day(t *testing.T, s string) time.Time {
	t.Helper()
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// /compare tries the page cache for a repository entity, and computes the
// entity itself when that fails; organization entities keep the series cache.
func TestCompareRepoEntitiesGoThroughThePageCache(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) handleCompare("))
	page := strings.Index(body, "s.repoSeriesFromPage(r, e, metric, compareWindowOf(bucket, since, until))")
	direct := strings.Index(body, "s.entitySeriesFor(r, e, ids, metric, bucket, since, until, retentionThreshold, s.cachedFirstActivity, s.cachedMetricSeries)")
	if page < 0 || direct < 0 {
		t.Fatal("handleCompare must try s.repoSeriesFromPage(r, e, metric, compareWindowOf(bucket, since, until)) — its own window — and keep the direct computation")
	}
	if page > direct {
		t.Error("the page cache is tried before the direct computation")
	}
	fn := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) repoSeriesFromPage("))
	if !strings.Contains(fn, `e.Kind != "repo"`) {
		t.Error("only repository entities go through the page cache")
	}
	if !strings.Contains(fn, "s.internalGET(") {
		t.Error("repoSeriesFromPage must serve the request through internalGET, the re-warm's internal caller")
	}
}
