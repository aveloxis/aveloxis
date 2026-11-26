// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The query phase of a vulnerability scan (summary/70, 4.2): each sendable
// purl is either ANSWERED (a list of advisory ids, possibly empty) or
// REJECTED (OSV refused the query). The two are different facts. An empty
// answer says the package has no advisory at that version, and lets findings
// close; a rejection says nothing about the package, and closes nothing.
//
// Before 0.29.168 one rejected query failed its whole batch, and with it the
// repository's scan, on every scan until the manifest changed.

// osvQueryAnswers is what the query phase learned.
type osvQueryAnswers struct {
	// IDs holds the advisory ids of every ANSWERED purl, never nil for one.
	// A purl with no entry was not answered.
	IDs map[string][]string
	// Rejected holds the purls OSV refused, this scan or within the answer
	// cache's lifetime.
	Rejected map[string]bool
	// Truncated (0.29.169) holds the answered purls whose answer was not
	// read to its end: the page cap was reached, or OSV handed back a page
	// token it had already given. Their ids are real and are written;
	// nothing may close on such an answer.
	Truncated map[string]bool
	// AskedAt (0.29.171) is when each answered purl's answer was asked of
	// OSV: a database time, kept with the cache entry. Zero when the caller
	// tracks none.
	AskedAt map[string]time.Time
}

// incomplete reports whether what is held for a purl is not a complete
// answer: OSV refused it, the answer was not read to its end, or there is
// no answer at all. The last should not happen (every target is asked);
// if it ever does, nothing closes on it.
func (a osvQueryAnswers) incomplete(purl string) bool {
	_, answered := a.IDs[purl]
	return a.Rejected[purl] || a.Truncated[purl] || !answered
}

// replace puts fresh answers for purls in place of the ones held.
func (a *osvQueryAnswers) replace(purls []string, fresh osvQueryAnswers) {
	for _, p := range purls {
		delete(a.IDs, p)
		delete(a.Rejected, p)
		delete(a.Truncated, p)
		delete(a.AskedAt, p)
		if ids, ok := fresh.IDs[p]; ok {
			a.IDs[p] = ids
		}
		if fresh.Rejected[p] {
			a.Rejected[p] = true
		}
		if fresh.Truncated[p] {
			a.Truncated[p] = true
		}
		if at, ok := fresh.AskedAt[p]; ok {
			a.AskedAt[p] = at
		}
	}
}

// osvFetchOptions are a fetch's times and cache use.
type osvFetchOptions struct {
	// AskedAt is the database time read before this fetch's network phase;
	// every answer it asks for is stamped with it.
	AskedAt time.Time
	// BypassCache asks OSV even where the cache has an answer, and replaces
	// that entry: the re-ask of an answer a newer one had overtaken.
	BypassCache bool
}

// osvRejectedQueryCap is how many rejected queries one batch may isolate
// (collection.osv_rejected_queries_per_batch). Each isolation sends the batch
// again without the refused query, so the cap bounds the requests one batch
// can cost at cap+1. Zero, the value before loadConfig sets it, isolates
// nothing: the first rejection fails the scan, as before 0.29.168.
var osvRejectedQueryCap atomic.Int64

// SetOSVRejectedQueryCap installs the cap. loadConfig calls it with the
// configuration's effective value.
func SetOSVRejectedQueryCap(n int) { osvRejectedQueryCap.Store(int64(n)) }

// osvStatusError is a non-200 answer from OSV's batch endpoint.
type osvStatusError struct {
	Status int
	Body   string // its start only
}

func (e *osvStatusError) Error() string {
	return fmt.Sprintf("OSV API returned status %d: %.200s", e.Status, e.Body)
}

// rejectedQueryIndex reports which query of a batch of n OSV refused, and
// whether err is such a refusal at all: a 400 whose message names a query
// index inside the batch. Anything else (a server error, a 400 about the
// batch as a whole, an index outside it) is not, and the scan fails on it as
// before: removing a guessed query would be a wrong answer for that purl.
func rejectedQueryIndex(err error, n int) (int, bool) {
	var se *osvStatusError
	if !errors.As(err, &se) || se.Status != http.StatusBadRequest {
		return 0, false
	}
	m := osvQueryIndexRe.FindStringSubmatch(se.Body)
	if m == nil {
		return 0, false
	}
	i, convErr := strconv.Atoi(m[1])
	if convErr != nil || i < 0 || i >= n {
		return 0, false
	}
	return i, true
}

// osvPagesPerQuery is how many pages of one query's answer a scan reads
// (collection.osv_pages_per_query). OSV cuts an answer at about 1,000
// advisories for one query, or 3,000 for a batch, and hands back a token for
// the rest (https://google.github.io/osv.dev/post-v1-querybatch/). Zero is
// no cap: an answer is read to its end (operator, 2026-11-19). It is also the
// value before loadConfig sets it. A read with no cap still ends: a token
// OSV has already given for the same query ends that query (see
// fetchOSVQueryAnswers), and the scan's context ends all of them.
var osvPagesPerQuery atomic.Int64

// SetOSVPagesPerQuery installs the page cap. loadConfig calls it with the
// configuration's effective value.
func SetOSVPagesPerQuery(n int) { osvPagesPerQuery.Store(int64(n)) }

// osvQueryKey is what OSV is actually asked for a target, and so what an
// answer is cached under: the purl, or for a name-and-ecosystem query (a
// GitHub Action, whose purl carries a ref the query does not) the name and
// ecosystem. Every ref of one action shares one question and one answer.
func osvQueryKey(purl string, targets map[string]model.VulnScanTarget) string {
	if t, ok := targets[purl]; ok && t.OSVQueryEcosystem != "" {
		key := "name:" + t.OSVQueryEcosystem + "|" + t.OSVQueryName
		if t.OSVQueryVersion != "" {
			key += "@" + t.OSVQueryVersion // a versioned question is its own
		}
		return key
	}
	return purl
}

// osvPendingQuery is one query still owed an answer page.
type osvPendingQuery struct {
	key   string
	purl  string // a purl that asks it: the request is built from its target
	token string // the page to ask for; "" for the first
}

// fetchOSVQueryAnswers asks OSV about every purl the cache cannot answer, in
// batches. A query OSV refuses is isolated and the rest sent again; an answer
// OSV cut short is followed by its token, page after page, the re-queries
// batched, until it ends or the page cap is reached. It fails on any other
// error, on an answer it cannot match to its queries, and when one batch has
// more refused queries than the cap.
func fetchOSVQueryAnswers(ctx context.Context, cache *OSVCache, purls []string, targets map[string]model.VulnScanTarget, result *VulnerabilityResult, opts osvFetchOptions) (osvQueryAnswers, error) {
	out := osvQueryAnswers{IDs: make(map[string][]string, len(purls)), Rejected: map[string]bool{}, Truncated: map[string]bool{},
		AskedAt: map[string]time.Time{}}
	purlsOf := map[string][]string{}
	var pending []osvPendingQuery
	// The cache is read once per QUESTION, and every purl that asks it gets
	// that one reading (0.29.208). Read once per purl, an entry that
	// expired or was evicted between two purls of one question answered
	// the first and left the second neither answered nor asked.
	type cached struct {
		ids      []string
		rejected bool
		at       time.Time
		ok       bool
	}
	read := map[string]cached{}
	for _, p := range purls {
		key := osvQueryKey(p, targets)
		first := len(purlsOf[key]) == 0
		purlsOf[key] = append(purlsOf[key], p)
		if first {
			ids, rejected, at, ok := cache.lookupQuery(key)
			read[key] = cached{ids: ids, rejected: rejected, at: at, ok: ok}
		}
		ids, rejected, cachedAt, ok := read[key].ids, read[key].rejected, read[key].at, read[key].ok
		switch {
		case !ok || opts.BypassCache:
			result.OSVQueryMisses++
			if first {
				pending = append(pending, osvPendingQuery{key: key, purl: p})
			}
		case rejected:
			out.Rejected[p] = true
			result.OSVQueriesRejected++
			result.OSVQueryHits++
		default:
			out.IDs[p] = ids
			out.AskedAt[p] = cachedAt
			result.OSVQueryHits++
		}
	}
	// A key that was cached is settled for all its purls above; a key that
	// was not is pending once and fanned out below.
	acc := map[string][]string{}
	have := map[string]map[string]bool{}
	settle := func(key string, truncated bool) {
		ids := acc[key]
		if ids == nil {
			ids = []string{}
		}
		if !truncated {
			cache.putAnswer(key, ids, opts.AskedAt)
		}
		for _, p := range purlsOf[key] {
			out.IDs[p] = ids
			out.AskedAt[p] = opts.AskedAt
			if truncated {
				out.Truncated[p] = true
				result.OSVQueriesTruncated++
			}
		}
	}
	maxPages := int(osvPagesPerQuery.Load()) // 0: no cap
	// The tokens each query has been given. A token that comes back for the
	// same query is OSV going round in a circle: that query ends there, not
	// whole. A page with nothing new in it is NOT that sign (OSV documents
	// pages that hold only a token), so pages are never counted as progress.
	given := map[string]map[string]bool{}
	pagesRead := map[string]int{}
	for page := 0; len(pending) > 0; page++ {
		if maxPages > 0 && page >= maxPages {
			// Past the cap: what was read is kept, and the answer is not
			// whole. Not cached, so the next scan asks again.
			for _, q := range pending {
				settle(q.key, true)
			}
			break
		}
		var next []osvPendingQuery
		for start := 0; start < len(pending); start += osvBatchMaxQueries {
			chunk := pending[start:min(start+osvBatchMaxQueries, len(pending))]
			kept, results, refused, err := sendOSVBatchIsolating(ctx, chunk, targets)
			if err != nil {
				return out, err
			}
			for _, q := range refused {
				cache.putRejectedQuery(q.key)
				delete(acc, q.key)
				for _, p := range purlsOf[q.key] {
					out.Rejected[p] = true
					result.OSVQueriesRejected++
				}
			}
			for i, q := range kept {
				if pagesRead[q.key]++; pagesRead[q.key] > result.OSVLongestAnswerPages {
					result.OSVLongestAnswerPages = pagesRead[q.key]
				}
				if have[q.key] == nil {
					have[q.key] = map[string]bool{}
				}
				for _, v := range results[i].Vulns {
					if !have[q.key][v.ID] {
						have[q.key][v.ID] = true
						acc[q.key] = append(acc[q.key], v.ID)
					}
				}
				if tok := results[i].NextPageToken; tok != "" {
					if given[q.key] == nil {
						given[q.key] = map[string]bool{}
					}
					if given[q.key][tok] {
						settle(q.key, true) // a circle: what was read is kept
						continue
					}
					given[q.key][tok] = true
					next = append(next, osvPendingQuery{key: q.key, purl: q.purl, token: tok})
					continue
				}
				settle(q.key, false)
			}
		}
		pending = next
	}
	return out, nil
}

// sendOSVBatchIsolating sends one batch and returns the queries that were
// answered, with their results in the same order, and the queries OSV
// refused. A refused query is removed and the rest sent again, up to the
// cap; a short or long result list is an error (it cannot be matched).
func sendOSVBatchIsolating(ctx context.Context, chunk []osvPendingQuery, targets map[string]model.VulnScanTarget) (kept []osvPendingQuery, results []osvResult, refused []osvPendingQuery, err error) {
	limit := int(osvRejectedQueryCap.Load())
	// A copy: chunk aliases the caller's list.
	kept = append([]osvPendingQuery(nil), chunk...)
	for len(kept) > 0 {
		purls := make([]string, len(kept))
		for i, q := range kept {
			purls[i] = q.purl
		}
		req := buildOSVBatchRequest(purls, targets)
		for i, q := range kept {
			req.Queries[i].PageToken = q.token
		}
		resp, qerr := queryOSVBatch(ctx, req)
		if qerr == nil {
			if len(resp.Results) != len(kept) {
				return nil, nil, nil, fmt.Errorf("OSV batch query failed: %d results for %d queries; they cannot be matched to their queries", len(resp.Results), len(kept))
			}
			return kept, resp.Results, refused, nil
		}
		i, isRejection := rejectedQueryIndex(qerr, len(kept))
		if !isRejection {
			return nil, nil, nil, fmt.Errorf("OSV batch query failed: %w", annotateOSVBatchError(qerr, purls))
		}
		if len(refused) >= limit {
			// The refusals isolated so far are NOT cached (declined, review
			// round 1: caching them would let the next scans work through
			// the rest ten at a time and "converge"). Past the cap the
			// refusals are no longer evidence about single packages: OSV
			// may be refusing everything, and a day of cached refusals
			// would turn that into a day of partial scans of good
			// dependencies. The scan fails, and says which setting.
			return nil, nil, nil, fmt.Errorf("OSV batch query failed: OSV refused more than %d queries of one batch (collection.osv_rejected_queries_per_batch): %w",
				limit, annotateOSVBatchError(qerr, purls))
		}
		refused = append(refused, kept[i])
		kept = append(kept[:i:i], kept[i+1:]...)
	}
	return kept, nil, refused, nil
}
