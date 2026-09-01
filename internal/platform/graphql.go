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

	// lastRateLimit remembers the most recent in-body RATE_LIMITED so the
	// budget-exhausted error names the real condition (the bare "exhausted
	// N retries" text hid the cause for the 2026-05-13 stuck-repo cohort).
	var lastRateLimit error

	for attempt := range budget {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// GraphQL checkout gates on the key's GRAPHQL budget, not core —
		// the two buckets are independent per user (2026-09-01 fix).
		key, err := c.keys.GetGraphQLKey(ctx)
		if err != nil {
			return fmt.Errorf("getting API key: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
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
			// v0.27.28: cancellation bails quietly before the
			// "retrying" WARN — same contract as httpclient.Get.
			if ctx.Err() != nil {
				c.logger.Debug("graphql request aborted by context cancellation", "url", url)
				return ctx.Err()
			}
			c.logger.Warn("graphql request failed, retrying",
				"url", url, "query", query, "attempt", attempt+1, "error", err)
			if err := retrySleep(ctx, time.Duration(attempt+1)*2*time.Second, attempt, budget); err != nil {
				return err
			}
			continue
		}

		c.keys.UpdateFromResponse(key, resp)

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
			respBody, readErr := io.ReadAll(resp.Body)
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
						"url", url, "error", readErr, "query", query,
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
			parsed := parseGraphQLResponse(respBody, dest, c.logger)
			if parsed != nil && ClassifyError(parsed) == ClassRateLimit {
				// GitHub reports graphql exhaustion as HTTP 200 with an
				// errors array — no status-code arm ever sees it. Before
				// 2026-09-01 this returned straight to the caller with no
				// rotation or wait, which is what killed pytorch's 86h43m
				// run (one hit in shard 41's child pagination). Mark the
				// key's graphql budget dead (belt — the headers on this
				// response normally said Remaining: 0 already) and retry:
				// the next attempt's GetGraphQLKey returns a fresh key,
				// waits for the earliest window reset, or fast-fails for
				// callers with their own recovery machinery.
				c.keys.MarkGraphQLExhausted(key)
				c.logger.Info("graphql in-body rate limit — rotating to a fresh key",
					"url", url, "attempt", attempt+1, "error", parsed)
				lastRateLimit = parsed
				continue
			}
			return parsed

		case resp.StatusCode == http.StatusUnauthorized:
			// Same transient-tolerant policy as REST Get: a single 401 is
			// treated as a transient auth-backend hiccup, not a dead token.
			// RecordAuthFailure quarantines only after consecutive failures and
			// auto-recovers; we rotate to the next key on the next iteration.
			_ = resp.Body.Close()
			c.logger.Warn("graphql 401 — recording auth failure (quarantined only after repeated 401s)", "url", url)
			c.keys.RecordAuthFailure(key)
			continue

		case resp.StatusCode == http.StatusForbidden:
			_ = resp.Body.Close()
			// Same policy as REST Get: Retry-After or X-RateLimit-Remaining=0
			// means wait; otherwise it's a permission error.
			if resp.Header.Get("Retry-After") != "" {
				wait := parseRetryAfter(resp)
				c.logger.Info("graphql secondary rate limit", "url", url, "query", query, "wait", wait)
				if err := retrySleep(ctx, wait, attempt, budget); err != nil {
					return err
				}
				continue
			}
			if resp.Header.Get("X-RateLimit-Remaining") == "0" {
				c.logger.Info("graphql rate limit exhausted", "url", url)
				continue
			}
			return fmt.Errorf("%w: %s (graphql 403, not a rate limit)", ErrForbidden, url)

		case resp.StatusCode == http.StatusTooManyRequests:
			_ = resp.Body.Close()
			wait := parseRetryAfter(resp)
			c.logger.Info("graphql 429 rate limited", "url", url, "wait", wait)
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
				"url", url, "query", query, "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
			if err := retrySleep(ctx, wait, attempt, budget); err != nil {
				return err
			}
			continue

		default:
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			c.logger.Warn("graphql unexpected status",
				"url", url, "status", resp.StatusCode,
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
	if lastRateLimit != nil {
		// Every attempt burned on rate-limited keys: surface the rate-limit
		// class (subdivision defers/halves on it; the ticker callers defer
		// to their next claim) rather than the generic transient wrap.
		return fmt.Errorf("graphql: exhausted %d retries for %s: %w", budget, url, lastRateLimit)
	}
	return fmt.Errorf("graphql: exhausted %d retries for %s: %w", budget, url, ErrTransient)
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
// (the contributor activity-history and classification sweeps): key
// checkout refuses keys whose graphql budget is below
// GraphQLBackgroundReserve, leaving that headroom for foreground
// collection. The 2026-09-01 pytorch diagnostic measured the history
// sweep at ~20% of the fleet's graphql budget running back-to-back —
// enough sustained pressure to keep individual keys graphql-dry under
// multi-day collection jobs.
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
		if len(e.Path) == 0 || e.Type == "RESOURCE_LIMITS_EXCEEDED" {
			globalErrs = append(globalErrs, e)
		} else {
			partialErrs = append(partialErrs, e)
		}
	}

	// Log partial errors but don't fail on them. These typically come
	// from aliased-batch queries where one item was deleted/hidden.
	for _, e := range partialErrs {
		logger.Warn("graphql per-path error",
			"type", e.Type,
			"path", fmt.Sprint(e.Path),
			"message", e.Message)
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

// classifyGraphQLErrors turns GitHub's errors array into a single
// classified error the caller can dispatch on. RATE_LIMITED → ClassRateLimit;
// NOT_FOUND → ClassSkip (wraps ErrNotFound); FORBIDDEN → ClassSkip (wraps
// ErrForbidden). Anything else becomes a generic ClassFatal.
// rateLimitTypeOrDefault names the rate-limit error type in the message,
// falling back to the documented spelling for typeless variants.
func rateLimitTypeOrDefault(t string) string {
	if t == "" {
		return "RATE_LIMITED"
	}
	return t
}

func classifyGraphQLErrors(errs []graphqlError) error {
	// If any single error is rate-limited, the whole query is — the
	// remaining data is unreliable. Prefer RATE_LIMITED as the dominant
	// class even if other error types are also in the array.
	for _, e := range errs {
		// GitHub's documented type is RATE_LIMITED, but production
		// (2026-09-01, the pytorch shard-41 failure) received "RATE_LIMIT"
		// — and a typeless variant exists too. An unrecognized spelling
		// fell to the generic ClassFatal arm, so subdivision and the
		// size-1 REST fallback never engaged and an 86h job died on one
		// hit. Classify by type OR by the message shape; the defensive
		// direction is cheap (a mis-classified rate limit costs bounded
		// retries), the miss cost is a multi-day job.
		if e.Type == "RATE_LIMITED" || e.Type == "RATE_LIMIT" ||
			strings.Contains(strings.ToLower(e.Message), "rate limit") {
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
