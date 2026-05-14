// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// v0.20.19 (Fix J): the v0.20.8 sub-batch retry path in
// github.Client.fetchPRBatchWithSubdivide is gated on
// platform.ClassifyError(err) ∈ {ClassTransient, ClassRateLimit}.
// Pre-v0.20.19, the retry-exhaustion errors emitted from
// graphql.go:263 ("graphql: exhausted N retries for ...") and
// httpclient.go:468 ("exhausted N retries for ...") were plain
// fmt.Errorf strings with no sentinel — they classified as
// ClassFatal, bypassing subdivision entirely. Production
// diagnostic on 2026-05-13 showed 6/7 stuck repos had this
// exact error and were never recovering.
//
// The fix: wrap both retry-exhaustion sites with %w against a
// new ErrTransient sentinel. ClassifyError adds ErrTransient to
// its ClassTransient arm. Now subdivision actually fires for
// the dominant failure mode.

func TestErrTransientExists(t *testing.T) {
	if ErrTransient == nil {
		t.Fatal("ErrTransient sentinel must exist for retry-exhaustion errors to classify as ClassTransient")
	}
	if ErrTransient.Error() == "" {
		t.Error("ErrTransient must carry a non-empty message")
	}
}

func TestRetryExhaustionWrapsErrTransient(t *testing.T) {
	// Build a fmt.Errorf shaped like the production string.
	// The wrapping must use %w against ErrTransient so
	// errors.Is and errors.As work through the chain.
	sample := fmt.Errorf("graphql: exhausted %d retries for %s: %w",
		10, "https://api.github.com/graphql", ErrTransient)
	if !errors.Is(sample, ErrTransient) {
		t.Error("retry-exhaustion error must wrap ErrTransient via %w so errors.Is detects it through the wrapping chain that callers add (graphql PR batch: ..., gap fill PR batch: ..., pull requests graphql batch shard N: ...)")
	}
}

func TestClassifyErrorClassifiesErrTransientAsTransient(t *testing.T) {
	sample := fmt.Errorf("graphql: exhausted 10 retries: %w", ErrTransient)
	got := ClassifyError(sample)
	if got != ClassTransient {
		t.Errorf("ClassifyError(retry-exhaustion wrapped in ErrTransient) = %v, want ClassTransient — this is what makes Fix C's fetchPRBatchWithSubdivide actually subdivide on the production failure mode", got)
	}
}

// TestClassifyErrorClassifiesWrappedRetryExhaustion mirrors the
// production wrapping chain: graphql.go's bare exhaustion error
// is wrapped by FetchPRBatch, then by fillPRGaps, then by
// collectPRsGraphQL/shard runner. The classifier must see
// ClassTransient through all those layers.
func TestClassifyErrorClassifiesWrappedRetryExhaustion(t *testing.T) {
	inner := fmt.Errorf("graphql: exhausted 10 retries for https://api.github.com/graphql: %w", ErrTransient)
	pr := fmt.Errorf("graphql PR batch: %w", inner)
	shard := fmt.Errorf("pull requests graphql batch shard 7: %w", pr)
	if got := ClassifyError(shard); got != ClassTransient {
		t.Errorf("ClassifyError through the full wrapping chain = %v, want ClassTransient. Without this, the shard worker bubbles a Fatal error and Fix C's subdivision never runs.", got)
	}
}

// TestGraphqlExhaustionWrapsTransient pins the graphql.go return
// site. Source-contract test rather than runtime: invoking the
// retry loop end-to-end requires a 10-attempt failing httptest
// server, which is slow.
func TestGraphqlExhaustionWrapsTransient(t *testing.T) {
	// Defer to a separate package-level helper test that reads
	// graphql.go and pins the `%w` against ErrTransient on the
	// exhausted-retries return. Keeping the test here so it
	// sits with its semantic siblings.
	body, err := readPackageFile("graphql.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, `"graphql: exhausted %d retries for %s: %w"`) {
		t.Error(`graphql.go must wrap the exhaustion error with ErrTransient via %w. Without this, ClassifyError returns ClassFatal for the most common production failure (502/504 bursts that exceed the retry budget) and Fix C's sub-batch retry never fires.`)
	}
	if !strings.Contains(body, "ErrTransient") {
		t.Error("graphql.go must reference ErrTransient on the retry-exhaustion return so the wrap actually triggers classification as ClassTransient")
	}
}

func TestHTTPClientExhaustionWrapsTransient(t *testing.T) {
	body, err := readPackageFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, `"exhausted %d retries for %s: %w"`) {
		t.Error(`httpclient.go must wrap the REST retry-exhaustion error with ErrTransient via %w so REST callers (contributors, releases, events) that hit transient 5xx storms also subdivide-or-skip cleanly`)
	}
	if !strings.Contains(body, "ErrTransient") {
		t.Error("httpclient.go must reference ErrTransient on the retry-exhaustion return")
	}
}

// TestReleasesPaginationCapClassifiedAsSkip pins Fix K. GitHub
// limits /releases pagination to the first 1000 results — past
// that, it returns HTTP 422 with body "Only the first 1000 are
// available". Pre-v0.20.19 this was treated as
// "unprocessable entity (not retrying)" → ClassFatal, killing
// the entire collection job. Production diagnostic on
// 2026-05-13: Azure/azure-sdk-for-java was stuck on
// `releases?per_page=100&page=101` for this reason.
//
// Fix: when 422 body mentions the 1000-result cap, return
// ErrPaginationLimitExceeded (a ClassSkip sentinel) so the
// paginator stops cleanly without failing the job.
func TestPaginationLimitSentinelExists(t *testing.T) {
	if ErrPaginationLimitExceeded == nil {
		t.Fatal("ErrPaginationLimitExceeded sentinel must exist so GitHub's hard 1000-result pagination cap on /releases (and similar endpoints) stops being a job-killer")
	}
}

func TestPaginationLimitClassifiesAsSkip(t *testing.T) {
	wrapped := fmt.Errorf("releases page 101: %w", ErrPaginationLimitExceeded)
	got := ClassifyError(wrapped)
	if got != ClassSkip {
		t.Errorf("ClassifyError(ErrPaginationLimitExceeded chain) = %v, want ClassSkip — exhausting GitHub's pagination cap is end-of-data, not a fatal error", got)
	}
}

// readPackageFile reads a sibling file relative to the test's
// package dir so source-contract checks can inspect retry-loop
// implementations.
func readPackageFile(name string) (string, error) {
	data, err := os.ReadFile(name)
	return string(data), err
}
