// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// uncachedRepoGETs is the reviewed list of repository-scoped GET routes that
// stay outside the repository-page cache (v0.29.73), each with its reason. A
// new repository-scoped GET must either go through cachedRepoGET or be added
// here with a reason.
var uncachedRepoGETs = map[string]string{
	"handleRepoStats":             "cheap (the queue row's cached counts) and shows the live collecting state",
	"handleRepoStarState":         "per caller (the caller's own star)",
	"handleContributorsElsewhere": "reads other repositories' activity, which this repository's state does not cover; keeps its 60 s cache",
	"handleRepoByID":              "one row; Augur-compatible lookup",
	// The Augur-compatible metric routes (metrics.go) are not called by the
	// GUI; their clients and windows are outside this cache's contract.
	"handleIssuesNew": "Augur metric", "handleIssuesClosed": "Augur metric", "handleIssuesActive": "Augur metric",
	"handleIssueBacklog": "Augur metric", "handleIssueThroughput": "Augur metric", "handleIssueDuration": "Augur metric",
	"handleAvgIssueResolution": "Augur metric", "handleAbandonedIssues": "Augur metric", "handleOpenIssuesCount": "Augur metric",
	"handleClosedIssuesCount": "Augur metric", "handlePRsNew": "Augur metric", "handleReviews": "Augur metric",
	"handleReviewsAccepted": "Augur metric", "handleReviewsDeclined": "Augur metric", "handleReviewDuration": "Augur metric",
	"handleCommitters": "Augur metric", "handleCodeChanges": "Augur metric", "handleCodeChangesLines": "Augur metric",
	"handleContributors": "Augur metric", "handleContributorsNew": "Augur metric", "handleStars": "Augur metric",
	"handleStarsCount": "Augur metric", "handleForks": "Augur metric", "handleForkCount": "Augur metric",
	"handleWatchers": "Augur metric", "handleWatchersCount": "Augur metric", "handleLanguages": "Augur metric",
	"handleRepoMessages": "Augur metric", "handleReleases": "Augur metric", "handleProjectLanguages": "Augur metric",
}

var repoGETRoute = regexp.MustCompile(`s\.mux\.HandleFunc\("GET (/api/v1/repos/\{repoID\}[^"]*)", (s\.cachedRepoGET\((page\w+), )?s\.(handle\w+)\)?\)`)

type repoGETRegistration struct {
	path, handler, policy string
	cached                bool
}

func repoGETRegistrations(t *testing.T) []repoGETRegistration {
	t.Helper()
	var out []repoGETRegistration
	for _, f := range []string{"internal/api/server.go", "internal/api/metrics.go"} {
		for _, m := range repoGETRoute.FindAllStringSubmatch(srctest.StripGoComments(srctest.Read(t, f)), -1) {
			out = append(out, repoGETRegistration{path: m[1], handler: m[4], policy: m[3], cached: m[2] != ""})
		}
	}
	return out
}

// Every repository-scoped GET is either cached or reviewed — counted over
// the registrations examined, so a pattern that stopped matching fails
// instead of passing on nothing.
func TestEveryRepoScopedGETIsCachedOrReviewed(t *testing.T) {
	regs := repoGETRegistrations(t)
	srctest.MinCount(t, "repository-scoped GET registrations", len(regs), 40)
	cached := 0
	for _, reg := range regs {
		reason, reviewed := uncachedRepoGETs[reg.handler]
		switch {
		case reg.cached && reviewed:
			t.Errorf("%s (%s) is cached AND on the reviewed uncached list (%q) — drop one", reg.path, reg.handler, reason)
		case !reg.cached && !reviewed:
			t.Errorf("%s (%s) is a repository-scoped GET outside the cache with no reviewed reason: wrap it in s.cachedRepoGET or add it to uncachedRepoGETs", reg.path, reg.handler)
		case reg.cached:
			cached++
		}
	}
	srctest.MinCount(t, "cached repository-page routes", cached, 13)
}

// The wrapper decides scope before any lookup: a cached body must never
// reach a caller authorizeRepo would refuse (runtime twin:
// TestPageCacheRefusalsAndAutoAddsAreNeverShared).
func TestCachedRepoGETAuthorizesBeforeAnyLookup(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/api/repo_page_cache.go"), "func (s *Server) servePageCached("))
	authz := strings.Index(body, "s.authorizeRepo(")
	if authz < 0 {
		t.Fatal("servePageCached must call s.authorizeRepo")
	}
	for _, lookup := range []string{"s.repoStates(", "c.get(", "etagMatches(", "s.runOnce("} {
		i := strings.Index(body, lookup)
		if i < 0 {
			t.Errorf("cachedRepoGET no longer calls %s — update this pin", lookup)
			continue
		}
		if i < authz {
			t.Errorf("cachedRepoGET calls %s before authorizeRepo — a cached answer must never bypass repo scope", lookup)
		}
	}
}

// A handler behind the cache degrades past a failure only through
// partialAnswer, which marks the answer so it is never stored; a bare log
// call (or a discarded store error) would cache the degraded answer until
// the next collection.
func TestCachedHandlersDegradeOnlyThroughPartialAnswer(t *testing.T) {
	files := map[string]string{}
	for _, f := range []string{"server.go", "metrics.go", "vulnerabilities.go", "contributions.go", "top_contributors.go"} {
		files[f] = srctest.StripGoComments(srctest.Read(t, "internal/api/"+f))
	}
	discarded := regexp.MustCompile(`(, _ :?=|_ = )s\.store\.`)
	examined := 0
	seen := map[string]bool{}
	for _, reg := range repoGETRegistrations(t) {
		if !reg.cached || seen[reg.handler] {
			continue
		}
		seen[reg.handler] = true
		var body string
		for _, src := range files {
			if strings.Contains(src, "func (s *Server) "+reg.handler+"(") {
				body = srctest.FuncBody(t, src, "func (s *Server) "+reg.handler+"(")
			}
		}
		if body == "" {
			t.Errorf("cannot find %s", reg.handler)
			continue
		}
		examined++
		if strings.Contains(body, "LogFailure(") {
			t.Errorf("%s logs a failure and continues without partialAnswer: its degraded answer would be cached", reg.handler)
		}
		if discarded.MatchString(body) {
			t.Errorf("%s discards a store error: its degraded answer would be cached", reg.handler)
		}
	}
	srctest.MinCount(t, "cached handlers examined", examined, 12)
}

// TestCachedRepoRoutesServeFromTheCache drives every cached route on the real
// server and database: the second request is a hit with the same ETag, a
// conditional request gets a 304, and the repository's next collection
// replaces the answer.
func TestCachedRepoRoutesServeFromTheCache(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avpagecache/r', 'r', '_avpagecache', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, '2026-09-01T00:00:00Z')`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	s, err := NewWithOptions(store, slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 8 << 20, ResponseCacheMaxAge: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	call := func(path, inm string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.RemoteAddr = "127.0.0.1:1234" // exempt from the limiter
		if inm != "" {
			r.Header.Set("If-None-Match", inm)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	routes := 0
	for _, reg := range repoGETRegistrations(t) {
		if !reg.cached {
			continue
		}
		routes++
		path := strings.Replace(reg.path, "{repoID}", strconv.FormatInt(repoID, 10), 1)
		first := call(path, "")
		if first.Code != http.StatusOK {
			t.Errorf("%s: %d %s", reg.path, first.Code, first.Body.String())
			continue
		}
		etag := first.Header().Get("ETag")
		if etag == "" || first.Header().Get("Cache-Control") != "no-cache" {
			t.Errorf("%s: a cached route's answer needs an ETag and Cache-Control no-cache, got %v", reg.path, first.Header())
			continue
		}
		second := call(path, "")
		if second.Header().Get("X-Cache") != "hit" || second.Header().Get("ETag") != etag || second.Body.String() != first.Body.String() {
			t.Errorf("%s: the second request must be a hit with the same ETag and body (X-Cache %q)", reg.path, second.Header().Get("X-Cache"))
		}
		if w := call(path, etag); w.Code != http.StatusNotModified {
			t.Errorf("%s: a conditional request with the current ETag must be a 304, got %d", reg.path, w.Code)
		}
	}
	srctest.MinCount(t, "cached routes driven", routes, 13)
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = last_collected + interval '1 day', updated_at = NOW() WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	for _, reg := range repoGETRegistrations(t) {
		if !reg.cached {
			continue
		}
		path := strings.Replace(reg.path, "{repoID}", strconv.FormatInt(repoID, 10), 1)
		if w := call(path, ""); w.Header().Get("X-Cache") == "hit" {
			t.Errorf("%s: after the next collection the answer must be recomputed", reg.path)
		}
	}
}

// helperFiles lists the parse helpers a cached handler may read its query
// through, and their files. TestPageParamsMatchTheHandlers derives what each
// reads from its body; TestEveryQueryReaderIsScannedOrReviewed requires
// every other query reader to be reviewed as serving uncached routes.
var helperFiles = map[string]string{
	"parseWindow":              "contributions.go",
	"parseDateRange":           "metrics.go",
	"parseTopContributorsArgs": "page_query.go",
	"parseSBOMArgs":            "page_query.go",
}

// TestPageParamsMatchTheHandlers — the cache keys a route on the query
// parameters pageParams lists for it (review round 1, finding 5). A
// parameter the handler reads but the list lacks would share one answer
// across different requests; one the list has but the handler never reads
// would split identical answers. Each cached handler's body is read for
// what it reads: Query().Get literals and the window helpers.
func TestPageParamsMatchTheHandlers(t *testing.T) {
	files := map[string]string{}
	for _, f := range []string{"server.go", "metrics.go", "vulnerabilities.go", "contributions.go", "top_contributors.go"} {
		files[f] = srctest.StripGoComments(srctest.Read(t, "internal/api/"+f))
	}
	files["page_query.go"] = srctest.StripGoComments(srctest.Read(t, "internal/api/page_query.go"))
	// The parse helpers a handler may read its query through. What each
	// reads is taken from its own body (and the helpers it calls), never
	// from a list here: a parameter added inside a helper is a parameter the
	// route reads (review round on 5403959037: a fixed list let one escape).
	helperGet := regexp.MustCompile(`(?:\bq|Query\(\))\.Get\("([a-z_]+)"\)`)
	helperAnyGet := regexp.MustCompile(`(?:\bq|Query\(\))\.Get\(`)
	// A binding of the whole query (nothing chained after Query()), or a
	// url.Values parameter.
	helperBind := regexp.MustCompile(`\b(\w+)\s*:?=\s*\w+\.URL\.Query\(\)[ \t]*(?:\n|;|$)|\b(\w+)\s+url\.Values\b`)
	qWord := regexp.MustCompile(`\bq\b`)
	qLitGet := regexp.MustCompile(`\bq\.Get\("[a-z_]+"\)`)
	helperOther := regexp.MustCompile(`\.FormValue\(|RawQuery|Query\(\)\.(Has|Encode)\(|Query\(\)\[|\bq\.(Has|Encode)\(|\bq\[`)
	var readsOf func(name string, seen map[string]bool) []string
	readsOf = func(name string, seen map[string]bool) []string {
		if seen[name] {
			return nil
		}
		seen[name] = true
		body := srctest.FuncBody(t, files[helperFiles[name]], "func "+name+"(")
		if m := helperOther.FindString(body); m != "" {
			t.Errorf("%s reads the query another way (%q): pageParams cannot be checked against it", name, m)
		}
		// Only reads it can account for: the query is bound or passed as q,
		// and every Get names a lowercase literal (a camelCase name or a
		// constant would otherwise be silently skipped).
		for _, m := range helperBind.FindAllStringSubmatch(body, -1) {
			if m[1] != "q" && m[2] != "q" {
				t.Errorf("%s holds the query as %q: name it q, so its reads are seen", name, m[0])
			}
		}
		var out []string
		lits := helperGet.FindAllStringSubmatch(body, -1)
		if n := len(helperAnyGet.FindAllString(body, -1)); n != len(lits) {
			t.Errorf("%s has %d query Get calls but %d with a lowercase literal name: every read must name its parameter literally", name, n, len(lits))
		}
		for _, m := range lits {
			out = append(out, m[1])
		}
		// Guard the denominator: every use of q is accounted for — its
		// binding or parameter, a literal Get, or q handed to another
		// scanned helper (whose reads are followed below). Anything else
		// (range q, len(q), an alias) reads parameters this scan cannot name.
		accounted := len(qLitGet.FindAllString(body, -1))
		for _, m := range helperBind.FindAllStringSubmatch(body, -1) {
			if m[1] == "q" || m[2] == "q" {
				accounted++
			}
		}
		for other := range helperFiles {
			accounted += len(regexp.MustCompile(`\b`+other+`\(q\)`).FindAllString(body, -1))
		}
		if n := len(qWord.FindAllString(body, -1)); n != accounted {
			t.Errorf("%s uses q %d times but only %d are its binding, a literal q.Get or a scanned helper call: read the query only through q.Get(\"name\")", name, n, accounted)
		}
		for other := range helperFiles {
			// Any call, whatever it passes (r, or q url.Values).
			if other != name && regexp.MustCompile(`\b`+other+`\(`).MatchString(body) {
				out = append(out, readsOf(other, seen)...)
			}
		}
		return out
	}
	helpers := map[string][]string{}
	for name := range helperFiles {
		reads := readsOf(name, map[string]bool{})
		if len(reads) == 0 {
			t.Errorf("found no query reads in %s: the helper scan is broken", name)
		}
		helpers[name] = reads
	}
	get := regexp.MustCompile(`Query\(\)\.Get\("([a-z_]+)"\)`)
	other := regexp.MustCompile(`URL\.Query\(\)[^.]|\.FormValue\(|URL\.RawQuery|Query\(\)\.(Has|Encode)\(`)
	examined := 0
	for _, reg := range repoGETRegistrations(t) {
		if !reg.cached {
			continue
		}
		pattern := "GET " + reg.path
		listed, ok := pageParams[pattern]
		if !ok {
			t.Errorf("%s is cached but pageParams has no list for it (it would be served uncached)", pattern)
			continue
		}
		var body string
		for _, src := range files {
			if strings.Contains(src, "func (s *Server) "+reg.handler+"(") {
				body = srctest.FuncBody(t, src, "func (s *Server) "+reg.handler+"(")
			}
		}
		if body == "" {
			t.Errorf("cannot find %s", reg.handler)
			continue
		}
		examined++
		reads := map[string]bool{}
		lits := get.FindAllStringSubmatch(body, -1)
		if n := strings.Count(body, "Query().Get("); n != len(lits) {
			t.Errorf("%s has %d query Get calls but %d with a lowercase literal name: every read must name its parameter literally", reg.handler, n, len(lits))
		}
		for _, m := range lits {
			reads[m[1]] = true
		}
		for call, ps := range helpers {
			if regexp.MustCompile(`\b` + call + `\(`).MatchString(body) {
				for _, p := range ps {
					reads[p] = true
				}
			}
		}
		if m := other.FindString(body); m != "" {
			t.Errorf("%s reads the query another way (%q): pageParams cannot be checked against it", reg.handler, m)
		}
		want := map[string]bool{}
		for _, p := range listed {
			want[p] = true
		}
		for p := range reads {
			if !want[p] {
				t.Errorf("%s reads ?%s but pageParams[%q] lacks it: different requests would share one answer", reg.handler, p, pattern)
			}
		}
		for p := range want {
			if !reads[p] {
				t.Errorf("pageParams[%q] lists ?%s, which %s never reads", pattern, p, reg.handler)
			}
		}
	}
	srctest.MinCount(t, "cached routes checked against their parameters", examined, 13)
}

// Every function in the package that reads the query, other than a route
// handler, is either a parse helper TestPageParamsMatchTheHandlers scans
// (its reads count as the calling route's) or reviewed here as serving
// uncached routes only. And nothing a cached handler can reach, through any
// chain of calls (a route handler it delegates to, a helper named handleX,
// an intermediate that calls a reviewed reader), reads the query unless it
// is a scanned helper. A new parser a cached handler calls
// (vulnSeverityFilter(r) reading ?severity) would otherwise be read by no
// test, and its parameter would never reach the key: ?severity=critical and
// ?severity=low would share one answer (review rounds on 5403959037).
var uncachedQueryReaders = map[string]string{
	"compareWindow":           "the /compare routes, which are not repository-page cached",
	"parsePeriod":             "the metrics routes (period=), not repository-page cached",
	"parsePortalPage":         "the portal list pages, per caller",
	"parseRetentionThreshold": "the retention admin route",
	"resolveSupplyChainScope": "the supply-chain routes, not repository-page cached",
	"canonicalQuery":          "the cache key itself (reads only the listed parameters)",
}

func TestEveryQueryReaderIsScannedOrReviewed(t *testing.T) {
	// url.Values: a function handed the parsed query (q) reads it too.
	reads := regexp.MustCompile(`Query\(\)|\.FormValue\(|RawQuery|url\.Values`)
	// The name of a function or method, generic ones included (name[T ...]).
	name := regexp.MustCompile(`^(?:\([^)]*\)\s*)?(\w+)[\[(]`)
	bodies := map[string]string{}
	found := map[string]bool{}
	examined := 0
	for file, src := range srctest.PackageFiles(t, "internal/api", 20) {
		src = srctest.StripGoComments(src)
		for _, part := range strings.Split(src, "\nfunc ")[1:] {
			m := name.FindStringSubmatch(part)
			if m == nil {
				t.Errorf("%s: cannot read the name of the function starting %q", file, strings.SplitN(part, "\n", 2)[0])
				continue
			}
			examined++
			fn := m[1]
			// Same-named functions (methods on several types) are merged:
			// deterministic, and errs toward reporting.
			bodies[fn] += part
			if !reads.MatchString(part) || strings.HasPrefix(fn, "handle") {
				continue
			}
			found[fn] = true
			if _, scanned := helperFiles[fn]; !scanned && uncachedQueryReaders[fn] == "" {
				t.Errorf("%s: %s reads the query but is neither a parse helper TestPageParamsMatchTheHandlers scans (add it to helperFiles there) nor reviewed in uncachedQueryReaders", file, fn)
			}
		}
	}
	srctest.MinCount(t, "functions examined", examined, 100)
	for fn := range uncachedQueryReaders {
		if !found[fn] {
			t.Errorf("uncachedQueryReaders lists %s, which no longer reads the query: remove it", fn)
		}
	}
	// Everything a cached handler can reach, through any chain of calls
	// (another route handler it delegates to, a helper named handleX, an
	// intermediate that calls a reviewed reader): a reached function that
	// reads the query and is not a scanned helper reads a parameter the key
	// cannot see.
	callRE := map[string]*regexp.Regexp{}
	for fn := range bodies {
		callRE[fn] = regexp.MustCompile(`\b` + fn + `[\[(]`)
	}
	cachedHandlers := 0
	for _, reg := range repoGETRegistrations(t) {
		if !reg.cached {
			continue
		}
		cachedHandlers++
		if bodies[reg.handler] == "" {
			t.Errorf("cannot find cached handler %s", reg.handler)
			continue
		}
		seen := map[string]bool{reg.handler: true}
		queue := []string{reg.handler}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for fn, re := range callRE {
				if seen[fn] || !re.MatchString(bodies[cur]) {
					continue
				}
				seen[fn] = true
				queue = append(queue, fn)
				if _, scanned := helperFiles[fn]; !scanned && reads.MatchString(bodies[fn]) {
					t.Errorf("cached handler %s reaches %s (through %s), which reads the query: its reads are not in the key — make it a scanned helper (helperFiles) and list what it reads in pageParams, or stop calling it from a cached route", reg.handler, fn, cur)
				}
			}
		}
	}
	srctest.MinCount(t, "cached handlers checked", cachedHandlers, 13)
}
