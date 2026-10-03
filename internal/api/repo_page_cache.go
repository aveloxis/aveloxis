// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// The repository-page response cache (v0.29.73). A repository's GET answers
// do not depend on who asks (authorizeRepo decides WHETHER they may ask,
// before any lookup), so one copy serves every caller. An answer is valid
// for exactly as long as the repository's db.RepoCacheState is unchanged:
// a collection, a scancode run or a vulnerability scan replaces it, and an
// unchanged repository is never recomputed. Answers that carry contributor
// identities are also bounded by the enrichment interval, because the
// enrichment sweep fills names and logins outside any collection (the
// v0.29.71 collectionCache contract).
//
// Each cacheable answer carries an ETag. A client (or proxy) that already
// holds it gets a 304 after the authorization check and one primary-key read
// of the state, without running the handler. Bodies are kept in a
// byte-bounded LRU; concurrent misses for one answer run the handler once.

// pagePolicy is how one route's answers age.
type pagePolicy struct {
	// enriched: the body carries contributor names, logins or affiliations,
	// which the enrichment sweep changes outside collection — the entry is
	// recomputed after the enrichment interval.
	enriched bool
	// nowRelative: the handler's default window is anchored at the current
	// time, so the UTC day is part of the key (a quantized clock; an
	// unquantized time.Now() in a key never hits).
	nowRelative bool
}

var (
	// pageExact: valid until the repository's state changes.
	pageExact = pagePolicy{}
	// pageDated: the default window starts relative to today.
	pageDated = pagePolicy{nowRelative: true}
	// pageEnriched: contributor identities, over a default window ending now.
	pageEnriched = pagePolicy{enriched: true, nowRelative: true}
)

// pageParams lists, for each cached route, the query parameters its handler
// reads. Only these are part of the key, so a parameter the handler ignores
// (?x=1) shares the real answer instead of storing, and later re-warming, a
// copy of it (review round 1, finding 5). A cached route missing here is
// served uncached; TestPageParamsMatchTheHandlers keeps the lists in step
// with the handlers.
var pageParams = map[string][]string{
	"GET /api/v1/repos/{repoID}/sbom":                       {"format", "scope", "vulns"},
	"GET /api/v1/repos/{repoID}/timeseries":                 {"since", "until"},
	"GET /api/v1/repos/{repoID}/licenses":                   {"scope"},
	"GET /api/v1/repos/{repoID}/scancode-licenses":          {},
	"GET /api/v1/repos/{repoID}/scancode-files":             {},
	"GET /api/v1/repos/{repoID}/contributions/identities":   {"since", "until"},
	"GET /api/v1/repos/{repoID}/contributions/affiliations": {"since", "until"},
	"GET /api/v1/repos/{repoID}/contributions/coverage":     {"since", "until"},
	"GET /api/v1/repos/{repoID}/vulnerabilities":            {},
	"GET /api/v1/repos/{repoID}/scorecard":                  {},
	"GET /api/v1/repos/{repoID}/contributors/top":           {"since", "until", "limit", "bots"},
	"GET /api/v1/repos/{repoID}/deps":                       {"license", "scope"},
	"GET /api/v1/repos/{repoID}/libyear":                    {"license", "scope"},
}

// canonicalQuery is the request's query reduced to params, one value each
// (the first, which is what the handlers' Query().Get reads), in sorted
// order.
func canonicalQuery(r *http.Request, params []string) string {
	q := r.URL.Query()
	keep := url.Values{}
	for _, p := range params {
		if vs, ok := q[p]; ok && len(vs) > 0 {
			keep.Set(p, vs[0])
		}
	}
	return keep.Encode()
}

// pageCacheCtlKey carries the per-request "this answer is partial" flag.
type pageCacheCtlKey struct{}

type pageCacheCtl struct {
	mu      sync.Mutex
	partial bool
}

// partialAnswer logs a failure a handler degrades past instead of failing
// the request, and marks the answer partial so it is never stored or
// tagged: a cached degraded answer would outlive the failure until the next
// collection. Handlers behind cachedRepoGET use this for every degrade-and-
// continue failure, never a bare log call (pinned by test).
func (s *Server) partialAnswer(r *http.Request, err error, msg string, args ...any) {
	if c, ok := r.Context().Value(pageCacheCtlKey{}).(*pageCacheCtl); ok {
		c.mu.Lock()
		c.partial = true
		c.mu.Unlock()
	}
	httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, msg, args...)
}

// pageEntry is one cached answer.
type pageEntry struct {
	key         string // the lookup key (route, repository, query, state)
	etag        string // quoted ETag sent with the body
	repoID      int64
	uri         string // request URI, replayed by the re-warm
	fingerprint string // db.RepoCacheState.Fingerprint() when computed
	body        []byte
	contentType string
	disposition string
	computed    time.Time
	cost        time.Duration // handler run time, orders the re-warm
	enriched    bool
	hits        int // served from the cache since computed; the re-warm replays only answers asked for again
}

func (e *pageEntry) size() int64 {
	// The body dominates; the strings are counted so a flood of tiny
	// answers still meets the bound.
	return int64(len(e.body) + len(e.key) + len(e.etag) + len(e.uri) + len(e.fingerprint) + len(e.contentType) + len(e.disposition))
}

// pageResult is what one handler run produced.
type pageResult struct {
	status      int
	header      http.Header // everything the handler set; the cache keeps two
	body        []byte
	contentType string
	disposition string
	cacheable   bool
}

type pageFlight struct {
	done chan struct{}
	res  pageResult
}

// repoPageCache is the byte-bounded LRU. A zero maxBytes stores nothing
// (ETags and 304s still work).
type repoPageCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	ll       *list.List // front = most recently used; values *pageEntry
	m        map[string]*list.Element
	byRepo   map[int64]map[*list.Element]struct{}
	inflight map[string]*pageFlight
	ttl      time.Duration // enriched entries' lifetime
	now      func() time.Time
}

func newRepoPageCache(maxBytes int64, ttl time.Duration) *repoPageCache {
	if ttl <= 0 {
		ttl = compareCacheTTL
	}
	return &repoPageCache{maxBytes: maxBytes, ll: list.New(), m: map[string]*list.Element{},
		byRepo: map[int64]map[*list.Element]struct{}{}, inflight: map[string]*pageFlight{}, ttl: ttl, now: time.Now}
}

// get returns a fresh entry for key; an enriched entry past the TTL is a
// miss (and is dropped).
func (c *repoPageCache) get(key string) (*pageEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.m[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*pageEntry)
	if e.enriched && c.now().Sub(e.computed) >= c.ttl {
		c.removeLocked(el)
		return nil, false
	}
	c.ll.MoveToFront(el)
	e.hits++
	return e, true
}

func (c *repoPageCache) put(e *pageEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.m[e.key]; ok {
		c.removeLocked(old)
	}
	if e.size() > c.maxBytes {
		return // larger than the whole budget: never stored
	}
	el := c.ll.PushFront(e)
	c.m[e.key] = el
	set := c.byRepo[e.repoID]
	if set == nil {
		set = map[*list.Element]struct{}{}
		c.byRepo[e.repoID] = set
	}
	set[el] = struct{}{}
	c.bytes += e.size()
	for c.bytes > c.maxBytes {
		c.removeLocked(c.ll.Back())
	}
}

func (c *repoPageCache) removeLocked(el *list.Element) {
	e := el.Value.(*pageEntry)
	c.ll.Remove(el)
	delete(c.m, e.key)
	if set := c.byRepo[e.repoID]; set != nil {
		delete(set, el)
		if len(set) == 0 {
			delete(c.byRepo, e.repoID)
		}
	}
	c.bytes -= e.size()
}

// repoIDs lists the repositories with at least one cached answer.
func (c *repoPageCache) repoIDs() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]int64, 0, len(c.byRepo))
	for id := range c.byRepo {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// takeOutdated removes every entry of repoID computed under a state other
// than fingerprint and returns the request URIs worth recomputing: those of
// entries asked for again after they were computed (a returning visitor, a
// front end revalidating), costliest first, each once. An answer requested
// once — a one-off, or a crafted URL — is dropped without being replayed, so
// the re-warm's work after a collection is bounded by what visitors come
// back to. Entries already under the current state stay.
func (c *repoPageCache) takeOutdated(repoID int64, fingerprint string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	type item struct {
		uri  string
		cost time.Duration
	}
	best := map[string]time.Duration{}
	current := map[string]bool{}
	var drop []*list.Element
	for el := range c.byRepo[repoID] {
		e := el.Value.(*pageEntry)
		if e.fingerprint == fingerprint {
			current[e.uri] = true
			continue
		}
		drop = append(drop, el)
		if e.hits == 0 {
			continue
		}
		if prev, seen := best[e.uri]; !seen || e.cost > prev {
			best[e.uri] = e.cost
		}
	}
	for _, el := range drop {
		c.removeLocked(el)
	}
	items := make([]item, 0, len(best))
	for uri, cost := range best {
		if !current[uri] {
			items = append(items, item{uri, cost})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].cost != items[j].cost {
			return items[i].cost > items[j].cost
		}
		return items[i].uri < items[j].uri
	})
	uris := make([]string, len(items))
	for i, it := range items {
		uris[i] = it.uri
	}
	return uris
}

// dropRepo removes every entry of a repository that no longer exists.
func (c *repoPageCache) dropRepo(repoID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := range c.byRepo[repoID] {
		c.removeLocked(el)
	}
}

// fingerprints returns the distinct states repoID's entries were computed
// under.
func (c *repoPageCache) fingerprints(repoID int64) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]bool{}
	for el := range c.byRepo[repoID] {
		out[el.Value.(*pageEntry).fingerprint] = true
	}
	return out
}

// pageKey is the lookup key of one answer: route pattern, repository,
// canonical query (canonicalQuery: the route's own parameters, sorted), the
// state, the binary's version (a deploy can change any body's shape) and,
// for a now-relative route, the UTC day.
func pageKey(pattern string, repoID int64, query string, st db.RepoCacheState, pol pagePolicy, now time.Time) string {
	var b strings.Builder
	b.WriteString(db.ToolVersion)
	b.WriteByte('\n')
	b.WriteString(pattern)
	b.WriteByte('\n')
	b.WriteString(strconv.FormatInt(repoID, 10))
	b.WriteByte('\n')
	b.WriteString(query)
	b.WriteByte('\n')
	b.WriteString(st.Fingerprint())
	if pol.nowRelative {
		b.WriteByte('\n')
		b.WriteString(now.UTC().Format("2006-01-02"))
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}

// etagMatches reports whether an If-None-Match header names etag.
func etagMatches(header, etag string) bool {
	if header == "" || etag == "" {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		p := strings.TrimSpace(part)
		p = strings.TrimPrefix(p, "W/")
		if p == etag || p == "*" {
			return true
		}
	}
	return false
}

// Response headers. A shareable answer: the browser revalidates every use
// (no-cache), a front-end cache may keep it for one second before it asks
// again (X-Accel-Expires; never forwarded to the browser). A per-caller
// answer: nobody stores it.
func setShareableHeaders(h http.Header, etag string) {
	h.Set("ETag", etag)
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Expires", "1")
	h.Add("Access-Control-Expose-Headers", "ETag")
}

func setNoStoreHeaders(h http.Header) {
	h.Del("ETag")
	h.Set("Cache-Control", "private, no-store")
	h.Set("X-Accel-Expires", "0")
}

// captureWriter records a handler's answer for the cache.
type captureWriter struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func newCaptureWriter() *captureWriter { return &captureWriter{header: http.Header{}} }

func (c *captureWriter) Header() http.Header { return c.header }
func (c *captureWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}
}
func (c *captureWriter) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	return c.buf.Write(p)
}

// cachedRepoGET wraps a repository-scoped GET handler with the cache. The
// handler keeps its own authorizeRepo call (it is also reachable without
// the wrapper in tests); this one runs first, so no lookup precedes the
// scope check.
func (s *Server) cachedRepoGET(pol pagePolicy, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.servePageCached(pol, h, w, r) }
}

// servePageCached is one request through the cache (cachedRepoGET).
func (s *Server) servePageCached(pol pagePolicy, h http.HandlerFunc, w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("repoID")
	repoID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || s.pageCache == nil || s.repoStates == nil {
		h(w, r) // the handler answers the 400 (or runs uncached)
		return
	}
	if strconv.FormatInt(repoID, 10) != raw {
		// +7 or 07: the same repository to the handler, but another key here
		// and another path to a front end's location match — answered live,
		// never stored (review round 2).
		setNoStoreHeaders(w.Header())
		h(w, r)
		return
	}
	// Before authorizeRepo, which writes its 403 itself: headers set after
	// it never reach the wire. Every arm below that answers a shareable body
	// replaces these.
	setNoStoreHeaders(w.Header())
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	// The Shared-with-Me auto-add answers this caller with a one-time
	// header: that response must never be stored for anyone else.
	perCaller := w.Header().Get(sharedWithMeHeader) != ""

	states, err := s.repoStates(r.Context(), []int64{repoID})
	if err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err,
			"repository cache state unreadable — answering without the cache", "repo_id", repoID, "error", err)
	}
	st, known := states[repoID]
	if err != nil || !known {
		// No state, no validator: answer live, untagged (SR-5). An
		// unknown repository is the handler's own 404/empty answer.
		setNoStoreHeaders(w.Header())
		h(w, r)
		return
	}
	params, listed := pageParams[r.Pattern]
	if !listed {
		h(w, r) // a route without a parameter list is never keyed (untagged, no-store)
		return
	}
	query := canonicalQuery(r, params)
	c := s.pageCache
	key := pageKey(r.Pattern, repoID, query, st, pol, c.now())

	if e, ok := c.get(key); ok {
		if perCaller {
			setNoStoreHeaders(w.Header())
		} else {
			setShareableHeaders(w.Header(), e.etag)
			if etagMatches(r.Header.Get("If-None-Match"), e.etag) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		writeEntry(w, e.contentType, e.disposition, e.body, "hit")
		return
	}
	// An exact answer's ETag is a pure function of the key, so a client
	// holding it needs no body even when this process has none (a
	// restart, an eviction). An enriched answer's ETag also names the
	// body, which is known only after computing.
	if !pol.enriched && !perCaller && etagMatches(r.Header.Get("If-None-Match"), exactETag(key)) {
		setShareableHeaders(w.Header(), exactETag(key))
		w.WriteHeader(http.StatusNotModified)
		return
	}

	res, cost := s.runOnce(c, key, r, h)
	if !res.cacheable {
		// Partial, an error, or a refusal: passed through untagged,
		// with every header the handler set.
		for k, v := range res.header {
			w.Header()[k] = v
		}
		setNoStoreHeaders(w.Header())
		if res.status != 0 && res.status != http.StatusOK {
			w.WriteHeader(res.status)
		}
		_, _ = w.Write(res.body)
		return
	}
	uri := r.URL.EscapedPath()
	if query != "" {
		uri += "?" + query
	}
	e := &pageEntry{key: key, repoID: repoID, uri: uri, fingerprint: st.Fingerprint(),
		body: res.body, contentType: res.contentType, disposition: res.disposition,
		computed: c.now(), cost: cost, enriched: pol.enriched}
	if pol.enriched {
		sum := sha256.Sum256(res.body)
		e.etag = `"` + key + "." + hex.EncodeToString(sum[:6]) + `"`
	} else {
		e.etag = exactETag(key)
	}
	c.put(e)
	if perCaller {
		setNoStoreHeaders(w.Header())
	} else {
		setShareableHeaders(w.Header(), e.etag)
	}
	writeEntry(w, e.contentType, e.disposition, e.body, "")
}

func exactETag(key string) string { return `"` + key + `"` }

func writeEntry(w http.ResponseWriter, contentType, disposition string, body []byte, xcache string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if disposition != "" {
		w.Header().Set("Content-Disposition", disposition)
	}
	if xcache != "" {
		w.Header().Set("X-Cache", xcache)
	}
	_, _ = w.Write(body)
}

// runOnce runs h for key, once across concurrent callers: the first caller
// runs it, the others wait for that run and share an answer it could cache.
// A follower whose leader produced nothing cacheable (the leader's client
// left, a partial answer, an error) runs h itself, so one caller's failure
// never becomes another's.
func (s *Server) runOnce(c *repoPageCache, key string, r *http.Request, h http.HandlerFunc) (pageResult, time.Duration) {
	c.mu.Lock()
	if f, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			if f.res.cacheable {
				return f.res, 0
			}
		case <-r.Context().Done():
			return pageResult{status: http.StatusServiceUnavailable}, 0
		}
		return s.runHandler(r, h)
	}
	f := &pageFlight{done: make(chan struct{})}
	c.inflight[key] = f
	c.mu.Unlock()

	// Released however the handler ends — a panic included (it leaves f.res
	// uncacheable, so the waiters run the handler themselves) — or every
	// later request for this answer would wait on a flight that never lands.
	defer func() {
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(f.done)
	}()
	res, cost := s.runHandler(r, h)
	f.res = res // written before close(f.done), read only after it
	return res, cost
}

func (s *Server) runHandler(r *http.Request, h http.HandlerFunc) (pageResult, time.Duration) {
	ctl := &pageCacheCtl{}
	cw := newCaptureWriter()
	start := time.Now()
	h(cw, r.WithContext(context.WithValue(r.Context(), pageCacheCtlKey{}, ctl)))
	cost := time.Since(start)
	ctl.mu.Lock()
	partial := ctl.partial
	ctl.mu.Unlock()
	status := cw.status
	if status == 0 {
		status = http.StatusOK
	}
	return pageResult{
		status:      status,
		header:      cw.header,
		body:        cw.buf.Bytes(),
		contentType: cw.header.Get("Content-Type"),
		disposition: cw.header.Get("Content-Disposition"),
		// A run whose request ended (client gone, http_timeout_seconds)
		// may have written a half answer; only a complete 200 is kept.
		cacheable: status == http.StatusOK && !partial && r.Context().Err() == nil,
	}, cost
}

// handleRepoAuthz answers whether the caller may read one repository's
// pages: 204 when authorizeRepo admits the caller (the Shared-with-Me
// auto-add included, with its one-time notice header), the structured 403
// when it refuses, and the middleware's 401 when require_auth is on and the
// caller presents no valid token. It reads no repository data; a deployment
// front end calls it before serving a stored copy of a cacheable answer.
func (s *Server) handleRepoAuthz(w http.ResponseWriter, r *http.Request) {
	setNoStoreHeaders(w.Header())
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// frontEndAuthorizedHeader carries api.front_end_secret on a request a
// front end forwards after GET /api/v1/authz/repos/{id} admitted the
// visitor for it. The authorization request was counted against the
// visitor's rate limit, so the forwarded one is not counted again
// (frontEndAuthorized).
const frontEndAuthorizedHeader = "X-Aveloxis-Authorized"

// frontEndAuthorized reports a request the limiter does not count: its
// frontEndAuthorizedHeader equals api.front_end_secret (constant-time), it
// arrives from api.trusted_proxy, and it is on a route the repository-page
// cache serves (pageParams: the one list of those routes). The value is a
// secret, not a flag (review round 2): a fixed value is forwarded by any
// proxy path that does not strip it, and was a bypass of the limit with no
// authorization request at all. No secret configured, or any condition
// unmet: the request costs what it always did.
func (s *Server) frontEndAuthorized(r *http.Request) bool {
	if s.frontEndSecret == "" || s.limiter == nil || s.limiter.opts.TrustedProxy == "" {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(frontEndAuthorizedHeader)), []byte(s.frontEndSecret)) != 1 {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host != s.limiter.opts.TrustedProxy {
		return false
	}
	_, pattern := s.mux.Handler(r)
	_, cached := pageParams[pattern]
	return cached
}
