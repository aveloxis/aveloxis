// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// The query parsing the cached repository-page handlers share with the
// cache key (pageParamValue). The handler and the key call the same
// function, so the key holds exactly the value the handler acts on: every
// spelling the handler folds into one value shares one entry (PR #226
// review 5403959037), and two values the handler tells apart never do.

// errWindowOrder is the 400 of a window whose start is not before its end.
// (Declared before the first function: the query-reader scan attributes a
// later var block to the function above it.)
var errWindowOrder = errors.New("since must be before until")

// errNotTemporalMetric is the 400 of a series request whose ?metric is not a
// temporal catalog id (/compare and the repository's metric series).
var errNotTemporalMetric = errors.New("metric must be one of the temporal catalog ids (see /api/v1/metrics)")

// defaultCompareBucket is the bucket a series request without ?bucket gets
// (compareWindow); the cache key folds the spelled-out default to it.
const defaultCompareBucket = "week"

// queryDay reads one day-valued query parameter (stage 1b, the one date
// reader; eval-4 item 2): v is the parameter's value and name is its name
// for the refusal. given is false when the parameter is absent; a value
// that is not a YYYY-MM-DD date is an error the handler answers with 400
// (D7: never a silent default — a silent fallback hides a client bug). The
// cache key uses the same reader (dayKey), so an invalid value is never
// keyed.
func queryDay(name, v string) (t time.Time, given bool, err error) {
	if v == "" {
		return time.Time{}, false, nil
	}
	t, err = time.Parse("2006-01-02", v)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("invalid %s %q: want YYYY-MM-DD", name, v)
	}
	return t, true, nil
}

// windowAnchor is the instant a default window that moves with the clock is
// measured from: the UTC Monday midnight that starts now's ISO week (the
// bucket grid of /compare and the weekly series, truncBucket). A default
// window moves when a week completes, so a cached answer of an unchanged
// repository lasts the week (D6, summary/63).
func windowAnchor(now time.Time) time.Time { return truncBucket(now, "week") }

// inclusiveEnd is the half-open upper bound of a window whose last day is
// day: the midnight after it (D4: a person typing an end date means that
// day included; the store compares `< hi`).
func inclusiveEnd(day time.Time) time.Time { return day.AddDate(0, 0, 1) }

// parseWindow reads ?since= and ?until= (the native routes: /timeseries,
// contributions/*, contributors/top) as a (since, until) pair:
//
//   - default since: two years before the week anchor (windowAnchor, the
//     UTC Monday that starts the current week; D6, 0.29.120 — it was the
//     UTC midnight two years back, which moved the window, and so the
//     cached answer, every day).
//   - default until: zero, which the store reads as "no upper bound"
//     (resolveWindow).
//   - until is INCLUSIVE: it is handed on as the next midnight, so the
//     store's `< until` covers the requested final day.
//
// A date that does not parse, or since not before until, is an error the
// handler answers with 400.
func parseWindow(r *http.Request) (since, until time.Time, err error) {
	q := r.URL.Query()
	since = windowAnchor(time.Now()).AddDate(-2, 0, 0)
	if t, given, err := queryDay("since", q.Get("since")); err != nil {
		return since, until, err
	} else if given {
		since = t
	}
	if t, given, err := queryDay("until", q.Get("until")); err != nil {
		return since, until, err
	} else if given {
		until = inclusiveEnd(t)
	}
	if !until.IsZero() && !since.Before(until) {
		return since, until, errWindowOrder
	}
	return since, until, nil
}

// parseDateRange reads Augur's ?begin_date= and ?end_date= as the
// half-open window [lo, hi): the default is the year before today through
// today, both UTC days, and a given end_date is inclusive (D4) — so hi is
// always a UTC midnight, and the store's `< hi` is exact on DATE columns
// too. A date that does not parse is an error the handler answers with
// 400 (D7). Augur answers an empty series for begin after end; so does
// this (no refusal).
func parseDateRange(r *http.Request) (lo, hi time.Time, err error) {
	q := r.URL.Query()
	today := db.UTCDay(time.Now())
	lo, hi = today.AddDate(-1, 0, 0), inclusiveEnd(today)
	if t, given, err := queryDay("begin_date", q.Get("begin_date")); err != nil {
		return lo, hi, err
	} else if given {
		lo = t
	}
	if t, given, err := queryDay("end_date", q.Get("end_date")); err != nil {
		return lo, hi, err
	} else if given {
		hi = inclusiveEnd(t)
	}
	return lo, hi, nil
}

// defaultTopContributors and maxTopContributors bound ?limit= on
// contributors/top.
const (
	defaultTopContributors = 20
	maxTopContributors     = 100
)

// topContributorsLimit is the number of contributors ?limit= asks for: a
// positive integer, capped at maxTopContributors; anything else is the
// default.
func topContributorsLimit(v string) int {
	limit := defaultTopContributors
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		limit = n
	}
	if limit > maxTopContributors {
		limit = maxTopContributors
	}
	return limit
}

// botsHidden reports whether ?bots= asks to leave bot accounts out; only
// "hide" does.
func botsHidden(v string) bool { return v == "hide" }

// vulnsRequested reports whether ?vulns= asks for the SBOM's vulnerability
// section; only "1" does.
func vulnsRequested(v string) bool { return v == "1" }

// topContributorsArgs is everything handleTopContributors acts on from its
// query; parseTopContributorsArgs is its only reader of the query.
type topContributorsArgs struct {
	since, until time.Time
	limit        int
	excludeBots  bool
}

// parseTopContributorsArgs reads contributors/top's query; the error is
// the window's (a bad date, or since not before until), a 400.
func parseTopContributorsArgs(r *http.Request) (topContributorsArgs, error) {
	since, until, err := parseWindow(r)
	q := r.URL.Query()
	// v0.27.69 — the "hide bots" checkbox: ?bots=hide filters bot
	// identities (App accounts, [bot] logins, logins ending in "bot" —
	// the k8s-ci-robot and pytorchmergebot class; db.displayBotLoginSQL).
	return topContributorsArgs{
		since:       since,
		until:       until,
		limit:       topContributorsLimit(q.Get("limit")),
		excludeBots: botsHidden(q.Get("bots")),
	}, err
}

// sbomArgs is everything handleSBOMDownload acts on from its query;
// parseSBOMArgs is its only reader of the query.
type sbomArgs struct {
	format    string // "cyclonedx" (the default) or "spdx"
	scope     string // "", "all" or "runtime"
	withVulns bool
}

// sbomDefaultFormat is what the SBOM download serves when no format is
// asked for. The cache key folds the same default through it, so ?format=
// absent and ?format=cyclonedx share one entry (final whole-PR review A2).
const sbomDefaultFormat = "cyclonedx"

// parseSBOMArgs reads the SBOM download's query. A format or scope it does
// not serve is passed through: the handler's switches refuse it with 400.
func parseSBOMArgs(r *http.Request) sbomArgs {
	q := r.URL.Query()
	a := sbomArgs{format: q.Get("format"), scope: q.Get("scope"), withVulns: vulnsRequested(q.Get("vulns"))}
	if a.format == "" {
		a.format = sbomDefaultFormat
	}
	return a
}

// metricSeriesArgs is everything handleRepoMetricSeries acts on from its
// query; parseMetricSeriesArgs is its only reader of the query.
type metricSeriesArgs struct {
	def                *MetricDef
	since, until       time.Time
	bucket             string
	retentionThreshold int
}

// parseMetricSeriesArgs reads the repository metric series' query through
// /compare's own readers (the window, the bucket and the threshold mean
// what they mean there); the error is a 400.
func parseMetricSeriesArgs(r *http.Request) (metricSeriesArgs, error) {
	var a metricSeriesArgs
	if a.def = catalogEntry(r.URL.Query().Get("metric")); a.def == nil || a.def.Kind != "temporal" {
		return metricSeriesArgs{}, errNotTemporalMetric
	}
	var err error
	if a.since, a.until, a.bucket, err = compareWindow(r); err != nil {
		return metricSeriesArgs{}, err
	}
	if a.retentionThreshold, err = parseRetentionThreshold(r); err != nil {
		return metricSeriesArgs{}, err
	}
	return a, nil
}

// pageParamValue maps each parameter a cached route reads to the key form
// of its effective value; "" means the handler's default and leaves the
// parameter out of the key. Every name in pageParams has a rule here
// (TestEveryPageParamHasAnEffectiveValueRule). A rule answering false
// names a value the handler refuses with 400 and the key cannot express
// (an invalid date, stage 1b): the request is answered live, never keyed
// and never 304 — keyed on the default it would be served a stored 200
// (eval-4 item 2). Values a handler refuses but the key can spell (a bad
// format or scope) are kept as spelled: a refusal is never stored.
var pageParamValue = map[string]func(string) (string, bool){
	"since": dayKey,
	"until": dayKey,
	"limit": func(v string) (string, bool) {
		if n := topContributorsLimit(v); n != defaultTopContributors {
			return strconv.Itoa(n), true
		}
		return "", true
	},
	"bots": func(v string) (string, bool) {
		if botsHidden(v) {
			return "hide", true
		}
		return "", true
	},
	"vulns": func(v string) (string, bool) {
		if vulnsRequested(v) {
			return "1", true
		}
		return "", true
	},
	// Each value is its own answer, or a 400 never stored. /deps and
	// /libyear tell "" (the legacy full list) from "all" (the filtered
	// list), so scope is not folded.
	"scope": identityParam,
	// Only /sbom reads format; its default spelled out is the default.
	"format": func(v string) (string, bool) {
		if v == sbomDefaultFormat {
			return "", true
		}
		return v, true
	},
	// License names are data: each is its own filter.
	"license": identityParam,
	// A metric id is its own answer, or a 400 never stored.
	"metric": identityParam,
	"bucket": func(v string) (string, bool) {
		if v == defaultCompareBucket {
			return "", true
		}
		return v, true
	},
	"retention_threshold": func(v string) (string, bool) {
		n, err := retentionThresholdValue(v)
		switch {
		case err != nil:
			return v, true // refused with 400, never stored
		case n == DefaultRetentionThreshold:
			return "", true
		}
		return strconv.Itoa(n), true
	},
}

// dayKey is the key form of a day parameter: absent is the default ("");
// a date is itself; a value that is not a date is not keyable (false).
func dayKey(v string) (string, bool) {
	t, given, err := queryDay("", v)
	if err != nil {
		return "", false
	}
	if !given {
		return "", true
	}
	return t.Format("2006-01-02"), true
}

func identityParam(v string) (string, bool) { return v, true }
