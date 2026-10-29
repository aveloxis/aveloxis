// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The page answer is used only when it is a complete single-entity series
// answer; anything else falls back to computing the entity directly (an
// optional path is never terminal).
func TestSeriesFromPageAnswer(t *testing.T) {
	good := `{"bucket":"week","series":[{"entity":{"kind":"repo","repo_id":7,"label":"o/r"},"points":[{"bucket":"2026-01-05T00:00:00Z","value":3}],"data_start":"2025-01-01"}]}`
	cs, ok := seriesFromPageAnswer(200, []byte(good))
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
		if _, ok := seriesFromPageAnswer(c.status, []byte(c.body)); ok {
			t.Errorf("%s: must not be used", name)
		}
	}
}

// /compare tries the page cache for a repository entity, and computes the
// entity itself when that fails; organization entities keep the series cache.
func TestCompareRepoEntitiesGoThroughThePageCache(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/api", "func (s *Server) handleCompare("))
	page := strings.Index(body, "s.repoSeriesFromPage(r, e, metric)")
	direct := strings.Index(body, "s.entitySeriesFor(r, e, ids, metric, bucket, since, until, retentionThreshold, s.cachedFirstActivity, s.cachedMetricSeries)")
	if page < 0 || direct < 0 {
		t.Fatal("handleCompare must try s.repoSeriesFromPage(r, e, metric) and keep the direct computation")
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
