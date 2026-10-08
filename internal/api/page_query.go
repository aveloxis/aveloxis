// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"strconv"
	"time"
)

// The query parsing the cached repository-page handlers share with the
// cache key (pageParamValue). The handler and the key call the same
// function, so the key holds exactly the value the handler acts on: every
// spelling the handler folds into one value shares one entry (PR #226
// review 5403959037), and two values the handler tells apart never do.

// parseDayParam reads a ?since= / ?until= day. Anything that is not a
// YYYY-MM-DD date is not a date: the caller uses its default.
func parseDayParam(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
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

// parseTopContributorsArgs reads contributors/top's query; ok is false when
// the window is empty (since not before until).
func parseTopContributorsArgs(r *http.Request) (topContributorsArgs, bool) {
	since, until, ok := parseWindow(r)
	q := r.URL.Query()
	// v0.27.69 — the "hide bots" checkbox: ?bots=hide filters bot
	// identities (App accounts, [bot] logins, logins ending in "bot" —
	// the k8s-ci-robot and pytorchmergebot class; db.displayBotLoginSQL).
	return topContributorsArgs{
		since:       since,
		until:       until,
		limit:       topContributorsLimit(q.Get("limit")),
		excludeBots: botsHidden(q.Get("bots")),
	}, ok
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

// pageParamValue maps each parameter a cached route reads to the key form
// of its effective value; "" means the handler's default and leaves the
// parameter out of the key. Every name in pageParams has a rule here
// (TestEveryPageParamHasAnEffectiveValueRule). Values a handler refuses
// with 400 (a bad format or scope) are kept as spelled: a refusal is never
// stored.
var pageParamValue = map[string]func(string) string{
	"since": dayKey,
	"until": dayKey,
	"limit": func(v string) string {
		if n := topContributorsLimit(v); n != defaultTopContributors {
			return strconv.Itoa(n)
		}
		return ""
	},
	"bots": func(v string) string {
		if botsHidden(v) {
			return "hide"
		}
		return ""
	},
	"vulns": func(v string) string {
		if vulnsRequested(v) {
			return "1"
		}
		return ""
	},
	// Each value is its own answer, or a 400 never stored. /deps and
	// /libyear tell "" (the legacy full list) from "all" (the filtered
	// list), so scope is not folded.
	"scope": identityParam,
	// Only /sbom reads format; its default spelled out is the default.
	"format": func(v string) string {
		if v == sbomDefaultFormat {
			return ""
		}
		return v
	},
	// License names are data: each is its own filter.
	"license": identityParam,
}

func dayKey(v string) string {
	if t, ok := parseDayParam(v); ok {
		return t.Format("2006-01-02")
	}
	return ""
}

func identityParam(v string) string { return v }
