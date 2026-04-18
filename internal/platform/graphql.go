package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
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

func (e *classifiedGraphQLError) Error() string   { return e.message }
func (e *classifiedGraphQLError) Class() ErrorClass { return e.class }
func (e *classifiedGraphQLError) Unwrap() error    { return e.wrapped }

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
	body, err := json.Marshal(graphqlRequestBody{Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("marshal graphql body: %w", err)
	}

	url := c.graphqlEndpoint()

	for attempt := range maxRetries {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		key, err := c.keys.GetKey(ctx)
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
			c.logger.Warn("graphql request failed, retrying",
				"url", url, "attempt", attempt+1, "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
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
			if readErr != nil {
				return fmt.Errorf("read graphql response: %w", readErr)
			}
			return parseGraphQLResponse(respBody, dest, c.logger)

		case resp.StatusCode == http.StatusUnauthorized:
			_ = resp.Body.Close()
			c.logger.Warn("graphql 401, invalidating key", "url", url)
			c.keys.InvalidateKey(key)
			continue

		case resp.StatusCode == http.StatusForbidden:
			_ = resp.Body.Close()
			// Same policy as REST Get: Retry-After or X-RateLimit-Remaining=0
			// means wait; otherwise it's a permission error.
			if resp.Header.Get("Retry-After") != "" {
				wait := parseRetryAfter(resp)
				c.logger.Info("graphql secondary rate limit", "url", url, "wait", wait)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue

		case resp.StatusCode >= 500 && resp.StatusCode < 600:
			_ = resp.Body.Close()
			wait := jitteredBackoff(attempt)
			c.logger.Warn("graphql server error, retrying with backoff",
				"url", url, "status", resp.StatusCode, "wait", wait, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue

		default:
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			c.logger.Warn("graphql unexpected status",
				"url", url, "status", resp.StatusCode,
				"body_snippet", truncateBody(string(respBody), 200),
				"attempt", attempt+1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 2 * time.Second):
			}
		}
	}

	return fmt.Errorf("graphql: exhausted %d retries for %s", maxRetries, url)
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
	var globalErrs []graphqlError
	var partialErrs []graphqlError
	for _, e := range env.Errors {
		if len(e.Path) == 0 {
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
func classifyGraphQLErrors(errs []graphqlError) error {
	// If any single error is rate-limited, the whole query is — the
	// remaining data is unreliable. Prefer RATE_LIMITED as the dominant
	// class even if other error types are also in the array.
	for _, e := range errs {
		if e.Type == "RATE_LIMITED" {
			return &classifiedGraphQLError{
				class:   ClassRateLimit,
				message: "graphql RATE_LIMITED: " + e.Message,
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
