// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

// graphqlPath is the suffix appended to baseURL for GraphQL POST requests.
// GitHub's REST baseURL is "https://api.github.com" and its GraphQL endpoint
// is "https://api.github.com/graphql" — the same host, a distinct path.
// Tests point at httptest servers whose URL is just the server address, so
// GraphQL calls land at "<server.URL>/graphql" and the test handler can
// intercept them.
const graphqlPath = "/graphql"

// graphqlEndpointForBase returns the GraphQL endpoint URL for a given
// REST baseURL. GitHub's REST API sits at "https://api.github.com" and
// its GraphQL endpoint at "https://api.github.com/graphql" — the path
// suffix is the same whether we're pointing at production or a test
// server. The one complication is GitHub Enterprise, where the REST
// baseURL ends with "/api/v3" and the GraphQL endpoint is at "/api/graphql"
// — not a direct suffix. We don't support GraphQL on Enterprise in this
// phase; the REST path still works there and the GitHub impl falls back.
func (c *HTTPClient) graphqlEndpoint() string {
	return c.baseURL + graphqlPath
}

// graphqlRequestBody is the top-level shape GitHub's GraphQL endpoint
// expects: a JSON object with "query" (always) and "variables" (optional).
type graphqlRequestBody struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// graphqlResponseEnvelope wraps every GraphQL response. "data" is null on
// total failure; "errors" is populated on any failure (including partial).
//
// We keep data as json.RawMessage so we can decode it into the caller's
// destination type AFTER inspecting errors. If we naively decoded both in
// one pass with a typed struct, a NOT_FOUND on a single nested field
// would either be silently dropped (loses signal) or block access to the
// partial data (loses completeness).
type graphqlResponseEnvelope struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphqlError  `json:"errors,omitempty"`
}

// graphqlError matches the GitHub-documented error object shape. "path"
// is present when the error is scoped to a specific field in the response
// (e.g. NOT_FOUND on one aliased PR in a batch query); its absence
// usually indicates a whole-query failure (e.g. RATE_LIMITED, bad syntax).
type graphqlError struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message"`
	Path    []any  `json:"path,omitempty"`
}

// classifiedGraphQLError implements platform.ClassifiedError so that
// platform.ClassifyError(err) returns the right class without the caller
// having to inspect err.Error() string tokens. Wraps the existing
// sentinels (ErrNotFound, ErrForbidden) so errors.Is works transparently
// for callers that already branch on those.
type classifiedGraphQLError struct {
	class   ErrorClass
	message string
	wrapped error // sentinel for errors.Is, nil for rate limit / generic
}

func (e *classifiedGraphQLError) Error() string     { return e.message }
func (e *classifiedGraphQLError) Class() ErrorClass { return e.class }
func (e *classifiedGraphQLError) Unwrap() error     { return e.wrapped }

// graphqlRetrySleep is the ctx-aware backoff sleep used by the GraphQL
// retry loop. Package-level seam (v0.27.87) so backoff-SHAPE tests can
// count sleeps instead of paying real wall-clock — the fast-fail
// poison-account test spent 18s in genuine jittered sleeps before the
// seam existed. Production code never replaces it.
var graphqlRetrySleep = func(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// SetGraphQLSleepForTest replaces the GraphQL retry backoff sleep and
// returns a restore func. TEST-ONLY seam — see graphqlRetrySleep.
func SetGraphQLSleepForTest(f func(ctx context.Context, d time.Duration) error) (restore func()) {
	old := graphqlRetrySleep
	graphqlRetrySleep = f
	return func() { graphqlRetrySleep = old }
}

// retrySleep waits before the NEXT retry attempt. On the FINAL allowed
// attempt it returns immediately — sleeping after the last attempt
// only delays the caller's failure (v0.27.87, Copilot round on
// PR #173). This matters most under WithGraphQLFastFail, where the
// wasted tail backoff partially defeated the fast-fail budget and
// batch subdivision MULTIPLIES exhausted chains.
func retrySleep(ctx context.Context, wait time.Duration, attempt, budget int) error {
	if attempt+1 >= budget {
		return nil
	}
	return graphqlRetrySleep(ctx, wait)
}

// GraphQL executes a GraphQL query against <baseURL>/graphql.
//
// The query and variables are JSON-encoded into the POST body. The response
// envelope's "data" field is decoded into dest; if the "errors" field is
// populated and the errors are whole-query failures (no "path"), a
// platform.ClassifiedError is returned. Per-path errors (one aliased field
// out of many) are logged at WARN level and the data is still returned —
// this matches GitHub's partial-success semantic.
//
// Reuses HTTPClient's retry, rate-limit, and ctx-aware-sleep infrastructure
// from Get, so GraphQL calls get the same firewall resilience the REST
// path has.
func (c *HTTPClient) GraphQL(ctx context.Context, query string, variables map[string]any, dest any) error {
	return c.GraphQLAt(ctx, c.graphqlEndpoint(), query, variables, dest)
}

// BaseURL returns the REST base URL this client was built with (e.g.
// "https://gitlab.com/api/v4"); forge clients derive sibling endpoints
// from it (v0.28.18: GitLab's GraphQL lives at /api/graphql, not under
// /api/v4).
func (c *HTTPClient) BaseURL() string { return c.baseURL }

// GraphQLAt is GraphQL against an explicit endpoint (v0.28.18): same
// retry loop, key rotation and bearer auth; GitLab accepts a personal
// access token as a bearer token on its GraphQL API.
func (c *HTTPClient) GraphQLAt(ctx context.Context, endpoint, query string, variables map[string]any, dest any) error {
	body, err := json.Marshal(graphqlRequestBody{Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("marshal graphql body: %w", err)
	}

	url := endpoint
	// v0.29.12: an explicit endpoint gets the same host rule as every keyed
	// REST request (onClientHost) — refused before the loop leases a key.
	if herr := onClientHostString(c.baseURL, url); herr != nil {
		c.logger.Error("off-host GraphQL request refused — the endpoint leaves this client's API host or scheme, so no API key is sent",
			"endpoint", url, "error", herr)
		return herr
	}

	// Body-read retries (Fix C) have a tighter sub-budget than the outer
	// retry loop. If three fresh streams in a row all abort mid-body, the
	// query shape itself is probably the problem and further retries
	// won't help — better to fail fast and let the scheduler flag the
	// repo for a force-full-recollect (Fix D) on the next cycle than to
	// burn a 10-minute backoff chain. The outer loop's maxRetries=10
	// budget still applies to status-code-driven retries (5xx, 403, etc.).
	const maxReadRetries = 3
	readRetries := 0

	// v0.27.81: callers with their own recovery machinery (batch
	// subdivision — FetchContributorActivity) opt into a tight retry
	// budget via WithGraphQLFastFail. Same rationale as maxReadRetries:
	// when the QUERY CONTENT is the problem (an account too dense for
	// GitHub's resolver draws deterministic 500s — the 2026-08-04 pilot
	// measured one such query burning ~7 minutes of the 10-retry
	// backoff chain), the caller's subdivision IS the retry strategy;
	// the inner budget only needs to smooth transport blips.
	budget := maxRetries
	if graphqlFastFailEnabled(ctx) {
		budget = graphqlFastFailRetries
	}

	// lastCause remembers the most recent named condition so the
	// budget-exhausted error names the real one (the bare "exhausted N
	// retries" text hid the cause for the 2026-05-13 stuck-repo cohort).
	// lastCauseAttempt records WHICH attempt it was: the wrap applies only
	// when the FINAL attempt carried it — a mixed exhaustion (one rate
	// limit then nine 5xxs) must classify Transient, or subdividing and
	// deferring callers dispatch on a stale cause (review F3).
	//
	// ONE pair, not one per condition (v0.29.57): a rate limit rotates to a
	// fresh key and undoes the budget spend, so the replacement request
	// REUSES that attempt index. Separate per-condition markers could both
	// equal budget-1, and whichever arm was written first won — reporting a
	// rate limit for an attempt that actually timed out, which makes a
	// subdivision caller defer where it should halve. Whichever condition
	// is recorded last is the one that exhausted the budget.
	var lastCause error
	lastCauseAttempt := -1
	noteCause := func(err error, attempt int) { lastCause, lastCauseAttempt = err, attempt }

	// Copilot round 24 (PR #193): an in-body rate-limit rotation swaps to a
	// FRESH key — it is not a transport retry and must not spend the fixed
	// transport `budget`, or a pool with more keys than `budget` would give
	// up before trying them all (contradicting "the error escapes only after
	// every key is spent"). Bound rotations by the key count so every key is
	// considered; under fast-fail the caller owns recovery, so keep the tight
	// budget (rotations spend it as before).
	maxRotations := 0
	if !graphqlFastFailEnabled(ctx) {
		maxRotations = c.keys.Len()
	}
	rotations := 0

	for attempt := 0; attempt < budget; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// GitHub checkout gates on the key's GRAPHQL budget — the two
		// buckets are independent per user (2026-09-01 fix). GitLab has
		// ONE unified rate limit (no X-RateLimit-Resource header; its
		// RateLimit-* headers route to the CORE bucket), so its GraphQL
		// checkout gates on core — the graphql bucket is never updated
		// for GitLab and would hand out unified-exhausted keys (review
		// F2).
		res := ResourceGraphQL
		if c.authStyle == AuthGitLab {
			res = ResourceCore
		}
		// 2026-09-12: Acquire is a LEASE — it counts this request against
		// the key's and the pool's in-flight ceilings and picks the
		// least-loaded key. The lease covers the wire request: released
		// at once when Do fails, and otherwise once the response's pool
		// state is applied — for a 200 that is after the body is read and
		// parsed (see the block after Do) — but always BEFORE any
		// Retry-After or backoff sleep below, so a throttled caller
		// sleeping out a 5-minute Retry-After never pins an in-flight
		// slot (which is what the ceilings exist to keep free).
		key, release, err := c.keys.Acquire(ctx, res)
		if err != nil {
			return fmt.Errorf("getting API key: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			release()
			return err
		}
		// GraphQL requires "bearer" token format. Classic PATs without GraphQL
		// scope will get a 401 here; the retry loop invalidates the key
		// (same as REST 401 handling) and rotates.
		req.Header.Set("Authorization", "bearer "+key.Token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := c.inner.Do(req)
		if err != nil {
			// A failed Do has no response state to apply, so release
			// at once.
			release()
			// v0.27.28: cancellation bails quietly before the
			// "retrying" WARN — same contract as httpclient.Get.
			if ctx.Err() != nil {
				c.logger.Debug("graphql request aborted by context cancellation", "url", RedactURLUserinfo(url))
				return ctx.Err()
			}
			c.logger.Warn("graphql request failed, retrying",
				"url", RedactURLUserinfo(url), "query", query, "attempt", attempt+1, "error", err)
			if err := retrySleep(ctx, time.Duration(attempt+1)*2*time.Second, attempt, budget); err != nil {
				return err
			}
			continue
		}

		// Copilot review on PR #203: EVERY response-derived mark a
		// waiter's key selection reads is applied BEFORE the lease is
		// released — release() Broadcasts, so any mark that lands after
		// it is a gap in which every parked waiter may select this key.
		// UpdateFromResponse carries the header state (primary budget,
		// secondary-limit rest, 401 strike). The two GraphQL-only marks
		// follow it here (round 2; round 1 had left both after release
		// with a site note that undercounted the gap as "one wasted
		// request"):
		//
		//   - the in-body RATE_LIMITED mark, which needs the 200 body.
		//     Reading the body under the lease is not "holding a slot
		//     across a wait": the response is still on the wire (GitHub
		//     counts the request against its concurrency limit until it
		//     finishes sending), and the read is bounded by the client's
		//     60-second whole-request Timeout. The decode into dest also
		//     runs under the lease, though the mark needs only the
		//     envelope's classification: parseGraphQLResponse classifies
		//     and then decodes in one call, and it is kept whole for
		//     simplicity. The extra pass is CPU on a body already in
		//     memory (≤ ~1 MB batches), small against the request's wire
		//     time, but unmeasured — split classification from the
		//     decode if lease hold time ever shows up here. Waits — Retry-After,
		//     backoff, read-retry pacing — all happen after release.
		//   - the 403 + Remaining: 0 belt for the older resource-header-
		//     less shape, whose zero UpdateFromResponse routed into CORE.
		//
		// Released here — before any retry sleep or key rotation below —
		// so the slot is never held across a wait
		// (lease_every_exit_test.go drives every exit;
		// copilot_pr203_round2_test.go observes the pool AT the release).
		c.keys.UpdateFromResponse(key, resp)
		var (
			respBody []byte
			readErr  error
			parsed   error
		)
		switch {
		case resp.StatusCode == http.StatusOK:
			respBody, readErr = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			// v0.23.9: GitHub's GraphQL gateway has been observed
			// returning HTTP 200 with a zero-byte body when the
			// upstream resolver times out AFTER headers have been
			// committed (production: apache/felix, 2026-05-21). The
			// TCP stream closes cleanly so io.ReadAll returns
			// (nil, nil) — no transport error to drive Fix C's
			// retry. Synthesize io.ErrUnexpectedEOF here so the
			// existing retry-on-fresh-stream path handles it the
			// same way it handles mid-body RST_STREAM aborts. An
			// empty body on a JSON endpoint is never a valid
			// success: even GraphQL's null-data response is
			// `{"data":null}` (15 bytes), not zero.
			if readErr == nil && len(respBody) == 0 {
				readErr = io.ErrUnexpectedEOF
			}
			if readErr == nil {
				parsed = parseGraphQLResponse(respBody, dest, c.logger)
				if parsed != nil && ClassifyError(parsed) == ClassRateLimit {
					c.markBudgetExhausted(key, resp)
				}
			}
		case isPrimaryRefusal(resp):
			c.markBudgetExhausted(key, resp)
		}
		release()

		if remaining := resp.Header.Get("X-RateLimit-Remaining"); remaining != "" {
			resource := resp.Header.Get("X-RateLimit-Resource")
			if resource == "" {
				resource = "graphql"
			}
			c.logger.Debug("graphql rate limit status",
				"resource", resource,
				"remaining", remaining,
				"limit", resp.Header.Get("X-RateLimit-Limit"),
				"reset", resp.Header.Get("X-RateLimit-Reset"))
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			// The body was read (and an empty body turned into
			// io.ErrUnexpectedEOF) before release — see above.
			if readErr != nil {
				// Fix C (v0.18.23): an HTTP/2 RST_STREAM or a connection
				// abort during body read used to be terminal here. In
				// production against large repos (apache/spark,
				// grpc/grpc) GitHub's edge frequently ended streams
				// mid-response when the query was expensive to compute
				// — a retry on a fresh stream usually succeeds. We now
				// classify these shapes as retryable under a tight
				// sub-budget (maxReadRetries) so a genuinely-broken
				// query fails fast instead of grinding through the full
				// 10-retry budget with exponential backoff. Genuine
				// decode/wire-format errors still return immediately.
				if isRetryableReadError(readErr) && readRetries < maxReadRetries {
					readRetries++
					// Use a short linear wait (1s, 2s, 3s) for body-read
					// retries, not the exponential jitteredBackoff —
					// stream CANCELs are not a "server is overloaded"
					// signal and we don't want to compound latency on
					// the happy-path-after-abort recovery.
					wait := time.Duration(readRetries) * time.Second
					c.logger.Warn("graphql body read error, retrying",
						"url", RedactURLUserinfo(url), "error", readErr, "query", query,
						"read_retry", readRetries, "wait", wait)
					// The read-retry sub-budget's sleep always precedes a
					// real retry (the guard above), so it routes through
					// the seam directly — no final-attempt skip applies.
					if err := graphqlRetrySleep(ctx, wait); err != nil {
						return err
					}
					continue
				}
				return fmt.Errorf("read graphql response: %w", readErr)
			}
			if parsed != nil && ClassifyError(parsed) == ClassRateLimit {
				// GitHub reports graphql exhaustion as HTTP 200 with an
				// errors array — no status-code arm ever sees it. Before
				// 2026-09-01 this returned straight to the caller with no
				// rotation or wait, which is what killed pytorch's 86h43m
				// run (one hit in shard 41's child pagination). The key's
				// budget was marked dead before release (markBudgetExhausted,
				// above — belt: the headers on this response normally said
				// Remaining: 0 already); retry: the next attempt's Acquire
				// returns a fresh key, waits for the earliest window reset,
				// or fast-fails for callers with their own recovery
				// machinery.
				c.logger.Info("graphql in-body rate limit — rotating to a fresh key",
					"url", RedactURLUserinfo(url), "attempt", attempt+1, "error", parsed,
					"token_prefix", tokenPrefix(key.Token))
				noteCause(parsed, attempt)
				if rotations < maxRotations {
					// A key rotation, not a transport retry — undo this
					// iteration's budget spend (the loop post-increment
					// re-adds it) so all keys get a shot.
					rotations++
					attempt--
				}
				continue
			}
			if errors.Is(parsed, ErrGraphQLExecutionTimeout) {
				// v0.29.56: GitHub could not finish the query. It is
				// intermittent, so retry it like a 5xx (same backoff, same
				// budget); an exhausted budget returns the timeout itself,
				// which classifies ClassTransient for subdivision callers.
				noteCause(parsed, attempt)
				wait := jitteredBackoff(attempt)
				c.logger.Warn("graphql execution timeout, retrying with backoff",
					"url", RedactURLUserinfo(url), "query", query, "wait", wait, "attempt", attempt+1, "error", parsed)
				if err := retrySleep(ctx, wait, attempt, budget); err != nil {
					return err
				}
				continue
			}
			return parsed

		case resp.StatusCode == http.StatusUnauthorized:
			// Same transient-tolerant policy as REST Get: a single 401 is
			// treated as a transient auth-backend hiccup, not a dead token.
			// the pool quarantines only after consecutive failures and
			// auto-recovers; we rotate to the next key on the next iteration.
			_ = resp.Body.Close()
			// The strike was recorded by UpdateFromResponse under the
			// lease (Copilot review round 2 on PR #203).
			c.logger.Warn("graphql 401 — auth failure recorded (quarantined only after repeated 401s)", "url", RedactURLUserinfo(url))
			continue

		case resp.StatusCode == http.StatusForbidden:
			_ = resp.Body.Close()
			// Retry-After is a secondary limit (rest the key, pace this
			// attempt); a primary refusal (isPrimaryRefusal) rotates to another
			// key; anything else is a permission error.
			if resp.Header.Get("Retry-After") != "" {
				wait := parseRetryAfter(resp)
				c.logger.Info("graphql secondary rate limit", "url", RedactURLUserinfo(url), "query", query, "wait", wait,
					"token_prefix", tokenPrefix(key.Token))
				// 2026-09-12 (Bug C of the chaoss.tv analysis): rest THIS
				// key in the pool for the Retry-After. Pre-fix only this
				// goroutine slept and every other caller kept being handed
				// the throttled key — 179 rejections in one second. Now the
				// pool routes the next checkout to a healthy key while this
				// one sits out; the sleep below is only this attempt's own
				// pacing. The rest itself is applied by UpdateFromResponse
				// under the lease (PR #203 review), not here.
				// Copilot round 6 on PR #193 (suppressed #3): HTTP
				// throttling must feed the same final-attempt state as
				// in-body RATE_LIMITED, or a persistently-throttled
				// exhaustion wears ErrTransient and downstream
				// deferral/subdivision never sees ClassRateLimit.
				noteCause(&classifiedGraphQLError{class: ClassRateLimit,
					message: "graphql secondary rate limit (403 + Retry-After) persisted through the retry budget"}, attempt)
				if err := retrySleep(ctx, wait, attempt, budget); err != nil {
					return err
				}
				continue
			}
			// isPrimaryRefusal, the pool's predicate (SR-17), so a GitLab
			// RateLimit-Remaining: 0 rotates here too instead of returning
			// ErrForbidden for a key the pool just benched (Copilot review
			// on PR #209).
			if isPrimaryRefusal(resp) {
				c.logger.Info("graphql rate limit exhausted", "url", RedactURLUserinfo(url),
					"token_prefix", tokenPrefix(key.Token))
				// Copilot round 7 on PR #193: a 403 carrying
				// Remaining: 0 WITHOUT X-RateLimit-Resource (the older
				// GitHub response shape the pool explicitly supports)
				// routes the zero into the CORE bucket via
				// UpdateFromResponse — but GitHub's GraphQL checkout
				// reads GraphQLRemaining, so the exhausted key stayed
				// eligible and the retry budget burned on immediate
				// reuse. The budget THIS client's checkout reads was
				// marked before release (markBudgetExhausted, above).
				noteCause(&classifiedGraphQLError{class: ClassRateLimit,
					message: "graphql rate limit exhausted (403 primary refusal, remaining 0) persisted through the retry budget"}, attempt)
				// 2026-09-17: a refusal is a key ROTATION like the in-body
				// arm, not a transport retry. The key is benched pool-wide,
				// and spending the attempt let a pool with more refused keys
				// than the budget give up with healthy keys unused.
				if rotations < maxRotations {
					rotations++
					attempt--
				}
				continue
			}
			return fmt.Errorf("%w: %s (graphql 403, not a rate limit)", ErrForbidden, url)

		case resp.StatusCode == http.StatusTooManyRequests && isPrimaryRefusal(resp):
			// GitHub also spells primary exhaustion as a 429 (Remaining: 0,
			// no Retry-After): the same rotation as the 403 arm above. The
			// key's checkout budget was marked before release.
			_ = resp.Body.Close()
			c.logger.Info("graphql rate limit exhausted", "url", RedactURLUserinfo(url), "status", resp.StatusCode,
				"token_prefix", tokenPrefix(key.Token))
			noteCause(&classifiedGraphQLError{class: ClassRateLimit,
				message: "graphql rate limit exhausted (429 primary refusal, remaining 0) persisted through the retry budget"}, attempt)
			if rotations < maxRotations {
				rotations++
				attempt--
			}
			continue

		case resp.StatusCode == http.StatusTooManyRequests:
			_ = resp.Body.Close()
			wait := parseRetryAfter(resp)
			c.logger.Info("graphql 429 rate limited", "url", RedactURLUserinfo(url), "wait", wait,
				"token_prefix", tokenPrefix(key.Token))
			// 429 is the same per-key throttle as 403 + Retry-After
			// (GitHub documents both shapes for secondary limits): the
			// key is already resting (UpdateFromResponse, under the
			// lease — PR #203 review).
			// Round 6 suppressed #3: see the 403 branch — HTTP 429 is
			// explicit throttling and must win the exhaustion class
			// when it lands on the final attempt.
			noteCause(&classifiedGraphQLError{class: ClassRateLimit,
				message: "graphql 429 rate limited persisted through the retry budget"}, attempt)
			if err := retrySleep(ctx, wait, attempt, budget); err != nil {
				return err
			}
			continue

		case resp.StatusCode >= 500 && resp.StatusCode < 600:
			_ = resp.Body.Close()
			// v0.27.34: feed the fleet-level API-outage breaker (the
			// 2026-07-21 storm was 1,044/1,045 GraphQL 502s).
			c.keys.NoteServerError()
			wait := jitteredBackoff(attempt)
			c.logger.Warn("graphql server error, retrying with backoff",
				"url", RedactURLUserinfo(url), "query", query, "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
			if err := retrySleep(ctx, wait, attempt, budget); err != nil {
				return err
			}
			continue

		default:
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			c.logger.Warn("graphql unexpected status",
				"url", RedactURLUserinfo(url), "status", resp.StatusCode,
				"body_snippet", truncateBody(string(respBody), 200),
				"attempt", attempt+1)
			if err := retrySleep(ctx, time.Duration(attempt+1)*2*time.Second, attempt, budget); err != nil {
				return err
			}
		}
	}

	// v0.20.19 (Fix J): wrap with ErrTransient so
	// platform.ClassifyError returns ClassTransient. The
	// v0.20.8 fetchPRBatchWithSubdivide path keys off this
	// classification to halve the batch on transient errors;
	// without the wrap, it falls through to ClassFatal and
	// subdivision never fires. Production diagnostic on
	// 2026-05-13 traced 6 of 7 stuck repos to exactly this
	// missing classification.
	if lastCause != nil && lastCauseAttempt == budget-1 {
		// The FINAL attempt carried a named condition: surface it. A
		// rate limit makes subdivision defer and the ticker callers wait
		// for their next claim; an execution timeout classifies
		// ClassTransient so they halve instead. Reporting the wrong one
		// sends the caller down the wrong path.
		return fmt.Errorf("graphql: exhausted %d retries for %s: %w", budget, url, lastCause)
	}
	return fmt.Errorf("graphql: exhausted %d retries for %s: %w", budget, url, ErrTransient)
}

// markBudgetExhausted benches the bucket THIS client's GraphQL checkout
// reads (KeyPool.MarkBudgetExhausted: a refusal until the response's reset,
// else the probe window; the balance is zeroed only when no future window
// is tracked), after a response
// that says the key is rate-limited but whose headers may not have benched
// that bucket (an in-body RATE_LIMITED, or a primary refusal without
// X-RateLimit-Resource). Budget routing
// (Copilot round 2 on PR #193, suppressed #2): GitLab's GraphQL shares
// the UNIFIED core bucket (no X-RateLimit-Resource header; checkout
// gates on ResourceCore), so a graphql-bucket mark would be decorative
// there — the next Acquire would re-serve the same exhausted token.
// Called only while the lease is held.
func (c *HTTPClient) markBudgetExhausted(key *APIKey, resp *http.Response) {
	res := ResourceGraphQL
	if c.authStyle == AuthGitLab {
		res = ResourceCore
	}
	c.keys.MarkBudgetExhausted(key, res, resp)
}

// graphqlFastFailRetries is the retry budget under WithGraphQLFastFail
// — deliberately equal in spirit to maxReadRetries: three consecutive
// failures on fresh attempts mean the query content is the problem,
// and the caller's subdivision machinery recovers faster than the
// exponential backoff chain ever could.
const graphqlFastFailRetries = 3

// ctxKeyGraphQLFastFail is the context flag type for WithGraphQLFastFail
// (the WithoutETag pattern).
type ctxKeyGraphQLFastFail struct{}

// WithGraphQLFastFail returns a context that caps the GraphQL retry
// loop at graphqlFastFailRetries attempts. ONLY for callers that have
// their own recovery strategy for transient failures (batch
// subdivision); everything else should keep the full maxRetries
// budget. Note the cap applies to ALL retryable conditions in the
// loop (5xx, 401 rotation, rate-limit waits), so a caller may see
// occasional spurious exhaustion under pool contention — acceptable
// because subdivision retries the same content immediately in halves.
func WithGraphQLFastFail(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyGraphQLFastFail{}, true)
}

func graphqlFastFailEnabled(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyGraphQLFastFail{}).(bool)
	return v
}

// ctxKeyGraphQLBackground is the context flag type for
// WithGraphQLBackgroundBudget (the WithoutETag pattern).
type ctxKeyGraphQLBackground struct{}

// WithGraphQLBackgroundBudget marks a context as BACKGROUND GraphQL work
// (the contributor activity-history and classification sweeps): Acquire
// admits it only while the POOL's remaining graphql budget is above the
// foreground reservation (KeyPool.SetAdmission's reservePct), leaving
// that share for foreground collection. The 2026-09-01 pytorch
// diagnostic measured the history sweep at ~20% of the fleet's graphql
// budget running back-to-back — enough sustained pressure to keep
// individual keys graphql-dry under multi-day collection jobs; the
// 2026-09-12 chaoss.tv analysis measured it at 113% and replaced the
// original per-key 500-point cliff with the pool-level line (a per-key
// cliff shrank the eligible set as keys depleted and concentrated the
// whole sweep onto the survivors).
func WithGraphQLBackgroundBudget(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyGraphQLBackground{}, true)
}

func graphqlBackgroundBudgetEnabled(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyGraphQLBackground{}).(bool)
	return v
}

// parseGraphQLResponse decodes a GraphQL response body, populates dest
// from the "data" field, and returns a classified error if the "errors"
// field indicates a whole-query failure.
//
// Partial-path errors (each error has a non-empty "path") are logged at
// WARN and treated as informational — the corresponding field in the
// data will be null, and the caller's decoding logic is responsible for
// skipping nulls. This matches GitHub's semantics for batched queries
// where one item is inaccessible but the others succeed.
func parseGraphQLResponse(body []byte, dest any, logger interface {
	Warn(msg string, args ...any)
}) error {
	var env graphqlResponseEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("decode graphql envelope: %w (body: %s)",
			err, truncateBody(string(body), 200))
	}

	// Classify errors: partial-path vs global.
	//
	// v0.27.79: RESOURCE_LIMITS_EXCEEDED is ALWAYS global even though
	// GitHub reports it with per-path entries — the message literally
	// says "Resource limits for THIS QUERY exceeded" and every node in
	// the query arrives null. Treating it as per-path turned an
	// oversized aliased batch into an empty-but-successful result: the
	// 2026-07-30/31 production incident stamped 216,000 contributors
	// "activity checked, no data" because every contributionsCollection
	// alias errored this way and the tolerance path swallowed it.
	var globalErrs []graphqlError
	var partialErrs []graphqlError
	for _, e := range env.Errors {
		// Rate limits hoist like RESOURCE_LIMITS_EXCEEDED (review F1,
		// 2026-09-01): the v0.27.79 incident proved GitHub reports
		// GLOBAL budget conditions as per-path entries, and a per-path
		// rate limit left in the partial arm returns an
		// empty-but-successful result that bypasses the mark/rotate
		// machinery entirely.
		// v0.29.56: the execution timeout hoists for the same reason. The
		// 2026-09-17 samples were all pathless, but its two siblings above
		// both arrive per-path, and left in the partial arm it would be
		// logged, swallowed, and its null nodes read as ABSENT — which for
		// the activity sweep means mark-stamping those contributors for a
		// whole cooldown on a query that never ran.
		if len(e.Path) == 0 || e.Type == "RESOURCE_LIMITS_EXCEEDED" || isGraphQLRateLimitError(e) ||
			isGraphQLExecutionTimeout(e) {
			globalErrs = append(globalErrs, e)
		} else {
			partialErrs = append(partialErrs, e)
		}
	}

	// Log partial errors but don't fail on them. These come from
	// aliased-batch queries where one item was deleted/hidden AND from a
	// too-expensive contributionsCollection where GitHub INTERNAL-errors
	// on per-repo nodes. chaoss.tv log (2026-09-05): one such history
	// window emitted ~187 per-path INTERNAL errors, and ~470 expensive
	// queries produced 87,728 WARNs in 90 minutes (the v0.27.91 flood
	// class). AGGREGATE to one line per response — the type histogram +
	// first sample keeps the diagnostic without the flood.
	if len(partialErrs) > 0 {
		typeCounts := map[string]int{}
		for _, e := range partialErrs {
			typeCounts[e.Type]++
		}
		logger.Warn("graphql per-path errors",
			"count", len(partialErrs),
			"types", fmt.Sprint(typeCounts),
			"first_path", fmt.Sprint(partialErrs[0].Path),
			"first_message", partialErrs[0].Message)
	}

	// Global errors fail the whole query.
	if len(globalErrs) > 0 {
		return classifyGraphQLErrors(globalErrs)
	}

	// Decode the data field if a destination was provided.
	if dest != nil && len(env.Data) > 0 && !bytes.Equal(env.Data, []byte("null")) {
		if err := json.Unmarshal(env.Data, dest); err != nil {
			return fmt.Errorf("decode graphql data: %w", err)
		}
	}

	return nil
}

// isGraphQLRateLimitError is THE rate-limit recognizer (SR-17): one
// spelling shared by classifyGraphQLErrors AND parseGraphQLResponse's
// per-path hoist, so the two can never drift. GitHub's documented type
// is RATE_LIMITED, production received RATE_LIMIT (2026-09-01, the
// pytorch shard-41 failure), and a typeless message-only variant
// exists; the defensive direction is cheap (a mis-classified rate
// limit costs bounded retries), the miss cost is a multi-day job.
func isGraphQLRateLimitError(e graphqlError) bool {
	return e.Type == "RATE_LIMITED" || e.Type == "RATE_LIMIT" ||
		strings.Contains(strings.ToLower(e.Message), "rate limit")
}

// isGraphQLExecutionTimeout recognizes GitHub's answer for a query its
// resolver could not finish (ErrGraphQLExecutionTimeout). The error has no
// type, so the message prefix is the only signal; a typed error is never
// read as one.
func isGraphQLExecutionTimeout(e graphqlError) bool {
	return e.Type == "" && strings.HasPrefix(e.Message, "Something went wrong while executing your query")
}

// rateLimitTypeOrDefault names the rate-limit error type in the message,
// falling back to the documented spelling for typeless variants.
func rateLimitTypeOrDefault(t string) string {
	if t == "" {
		return "RATE_LIMITED"
	}
	return t
}

// classifyGraphQLErrors turns GitHub's errors array into a single
// classified error the caller can dispatch on. Rate limits (any spelling
// isGraphQLRateLimitError accepts) → ClassRateLimit; NOT_FOUND →
// ClassSkip (wraps ErrNotFound); FORBIDDEN → ClassSkip (wraps
// ErrForbidden). Anything else becomes a generic ClassFatal.
func classifyGraphQLErrors(errs []graphqlError) error {
	// If any single error is rate-limited, the whole query is — the
	// remaining data is unreliable. Prefer RATE_LIMITED as the dominant
	// class even if other error types are also in the array.
	for _, e := range errs {
		if isGraphQLRateLimitError(e) {
			return &classifiedGraphQLError{
				class:   ClassRateLimit,
				message: "graphql " + rateLimitTypeOrDefault(e.Type) + ": " + e.Message,
			}
		}
	}
	// RESOURCE_LIMITS_EXCEEDED dominates like RATE_LIMITED: the query
	// as shaped is too expensive and every node is null. ClassTransient
	// so batch callers with subdivision machinery (fetchPRBatchWith-
	// Subdivide, v0.20.8) automatically retry in halves; callers
	// without it fail loudly instead of persisting an empty result.
	// Wraps ErrResourceLimits (v0.27.81) so subdivision callers can
	// gate on THIS condition specifically via errors.Is — halving
	// provably helps RLE and nothing else that arrives in-body.
	for _, e := range errs {
		if e.Type == "RESOURCE_LIMITS_EXCEEDED" {
			return &classifiedGraphQLError{
				class:   ClassTransient,
				message: "graphql RESOURCE_LIMITS_EXCEEDED (query too expensive — subdivide the batch): " + e.Message,
				wrapped: ErrResourceLimits,
			}
		}
	}
	for _, e := range errs {
		if isGraphQLExecutionTimeout(e) {
			return &classifiedGraphQLError{
				class:   ClassTransient,
				message: "graphql execution timeout (GitHub could not finish the query; retry, then subdivide): " + e.Message,
				wrapped: ErrGraphQLExecutionTimeout,
			}
		}
	}
	first := errs[0]
	switch first.Type {
	case "NOT_FOUND":
		return &classifiedGraphQLError{
			class:   ClassSkip,
			message: "graphql NOT_FOUND: " + first.Message,
			wrapped: ErrNotFound,
		}
	case "FORBIDDEN":
		return &classifiedGraphQLError{
			class:   ClassSkip,
			message: "graphql FORBIDDEN: " + first.Message,
			wrapped: ErrForbidden,
		}
	default:
		var msgs []string
		for _, e := range errs {
			if e.Type != "" {
				msgs = append(msgs, e.Type+": "+e.Message)
			} else {
				msgs = append(msgs, e.Message)
			}
		}
		return &classifiedGraphQLError{
			class:   ClassFatal,
			message: "graphql errors: " + joinErrs(msgs),
		}
	}
}

// joinErrs concatenates error messages without pulling in strings.Join
// just for this; keeps the import surface small.
func joinErrs(msgs []string) string {
	if len(msgs) == 0 {
		return ""
	}
	total := 0
	for _, m := range msgs {
		total += len(m) + 2
	}
	out := make([]byte, 0, total)
	for i, m := range msgs {
		if i > 0 {
			out = append(out, "; "...)
		}
		out = append(out, m...)
	}
	return string(out)
}

// ErrNotGraphQLClassified is an unused placeholder kept for symmetry; in a
// later phase we may add a marker so callers can distinguish "this came
// from GraphQL" vs REST. For now the classes are enough.
var ErrNotGraphQLClassified = errors.New("graphql: not classified")

// ErrResourceLimits (v0.27.81) marks GitHub's RESOURCE_LIMITS_EXCEEDED
// — "Resource limits for this query exceeded", the per-query cost cap
// that is independent of rate-limit points. It is the ONLY in-body
// GraphQL condition where subdividing an aliased batch provably helps
// (the cap is on the whole query's resolution cost), so subdivision
// callers gate on errors.Is(err, ErrResourceLimits) rather than the
// broad ClassTransient. Observed edges move: 100/50/40 aliases failed
// in the 2026-07-30 incident, 35 passed a 2026-08-02 probe, and 25
// failed in production on 2026-08-04 — cost depends on WHICH accounts
// are in the batch, so no fixed batch size is safe without
// subdivision.
var ErrResourceLimits = errors.New("graphql resource limits exceeded")

// ErrGraphQLExecutionTimeout (v0.29.56) marks GitHub's answer for a query
// its resolver could not finish: HTTP 200 with a single TYPELESS
// top-level error, "Something went wrong while executing your query …
// Please include `ID` when reporting this issue." GitHub's own text for
// it says it may be the result of a timeout. It is intermittent (the
// 2026-09-17 probe re-ran the batch that had failed 7 production ticks
// running and all 100 queries succeeded), so the GraphQL loop retries it
// like a 5xx and an exhausted budget classifies ClassTransient, which is
// what subdivision callers act on. It is deliberately NOT
// ErrResourceLimits: that is the one condition where halving provably
// helps.
var ErrGraphQLExecutionTimeout = errors.New("graphql execution timeout")

// isRetryableReadError classifies an error surfaced while READING or
// DECODING a 200-OK response body — GraphQL (io.ReadAll) and REST
// pagination (json.Decoder) alike. v0.27.37 (summary/18 Phase 1g)
// promoted it from isRetryableGraphQLReadError: the identical failure
// (GitHub's edge RST_STREAM/CANCEL mid-body) was killing whole
// collection jobs on the repo-wide REST walks, which is why
// pytorch-class repos could never complete a force-full cycle.
//
// We recognize these shapes:
//
//   - http2.StreamError — the HTTP/2 transport surfaces RST_STREAM frames
//     as this concrete type. CANCEL and INTERNAL_ERROR are the codes
//     GitHub uses when it gives up; both retryable.
//   - io.ErrUnexpectedEOF — the transport closed before the declared
//     Content-Length was delivered. Common when a load balancer times
//     out mid-response.
//   - Substring match on "stream error" / "CANCEL" / "connection reset"
//     / "unexpected EOF" in the error message. Belt and braces for
//     wrapped/translated errors that don't preserve As-compatible types.
//
// Not retryable: decode failures on intact bodies, context cancellation
// (ctx path handles that separately), nil. Keeping the substring list
// tight avoids the classic "retry everything" failure mode.
func isRetryableReadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var streamErr http2.StreamError
	if errors.As(err, &streamErr) {
		return true
	}
	msg := err.Error()
	for _, needle := range retryableReadErrorSubstrings {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// retryableReadErrorSubstrings is the list of error-message fragments we
// treat as transient transport failures. Kept small on purpose — each
// entry is a concrete production-observed shape, not a speculative "this
// might be flaky" pattern.
var retryableReadErrorSubstrings = []string{
	"stream error", // http2.StreamError wrapped by outer errors
	"CANCEL",       // HTTP/2 RST_STREAM code (observed in production log)
	"connection reset",
	"unexpected EOF",
	"broken pipe",
}
