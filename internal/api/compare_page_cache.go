// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// errPageSeries is the logged failure of a repository entity's series
// through the repository-page cache (the comparison then computes it).
var errPageSeries = errors.New("repository series through the page cache failed")

// repoSeriesFromPage is a /compare repository entity's series from the
// repository-page cache (D5, 0.29.121, summary/63 §7): the answer of the
// repository's own metric-series route for the same metric, window, bucket
// and threshold — the entry the repository page warms, durable until the
// repository is collected again — taken through the routes as the internal
// caller (resolveEntityRepos already admitted the entity for this caller).
// ok is false for an organization entity, a server without the page cache,
// and any failure: the comparison then computes the entity directly, and a
// failure is logged (an optional path is never terminal).
func (s *Server) repoSeriesFromPage(r *http.Request, e entity, metric string) (compareSeries, bool) {
	if e.Kind != "repo" || s.pageCache == nil || s.repoStates == nil {
		return compareSeries{}, false
	}
	w, status, reason := s.internalGET(r.Context(), "compare repository series", repoSeriesURI(r, e.RepoID, metric))
	if status == http.StatusNotFound {
		// No such repository: the definitive answer, not a failure (an
		// unscoped caller's entity is admitted unread). The comparison
		// computes its empty series as before 0.29.121, unlogged (round 1
		// finding 5).
		return compareSeries{}, false
	}
	var cs compareSeries
	ok := false
	if w != nil {
		cs, ok = seriesFromPageAnswer(status, w.buf.Bytes())
	}
	if !ok {
		if r.Context().Err() == nil {
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, fmt.Errorf("%w: status %d (%s)", errPageSeries, status, reason),
				"compare: a repository entity's series through the page cache failed — computing it directly",
				"entity", logSafe(e.Label), "metric", logSafe(metric))
		}
		return compareSeries{}, false
	}
	cs.Entity = e // the comparison's own entity and label
	return cs, true
}

// repoSeriesURI is the repository metric-series request for a comparison's
// query: its own values of the window, bucket and threshold, as given, so
// the route reads them with the same rules (compareWindow) and keys them as
// the repository page does.
func repoSeriesURI(r *http.Request, repoID int64, metric string) string {
	q := r.URL.Query()
	keep := url.Values{}
	keep.Set("metric", metric)
	for _, name := range []string{"since", "until", "bucket", "retention_threshold"} {
		if v := q.Get(name); v != "" {
			keep.Set(name, v)
		}
	}
	return "/api/v1/repos/" + strconv.FormatInt(repoID, 10) + "/metric-series?" + keep.Encode()
}

// seriesFromPageAnswer is the one series of a metric-series answer, when the
// answer is a complete 200 with exactly one.
func seriesFromPageAnswer(status int, body []byte) (compareSeries, bool) {
	if status != http.StatusOK {
		return compareSeries{}, false
	}
	var ans struct {
		Series []compareSeries `json:"series"`
	}
	if err := json.Unmarshal(body, &ans); err != nil || len(ans.Series) != 1 {
		return compareSeries{}, false
	}
	return ans.Series[0], true
}
