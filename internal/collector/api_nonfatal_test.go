package collector

import (
	"os"
	"strings"
	"testing"
)

// TestStagedCollectorTreatsNotFoundAndForbiddenAsNonFatal — source-level
// contract for the staged collector's per-phase loops. A single 404 or 403
// on an optional endpoint (contributors on an archived repo, /pulls on a
// repo with PRs disabled, GitLab merge_requests on a private project, etc.)
// must not bubble into result.Errors, which would flip
// buildOutcome.success=false and fail the whole job.
//
// The fix pattern (established for releases in v0.16.4): check
// errors.Is(err, platform.ErrNotFound) / ErrForbidden, log a skip line, and
// break out of the phase without appending to result.Errors.
func TestStagedCollectorTreatsNotFoundAndForbiddenAsNonFatal(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Every collectX phase that lists from a single upstream endpoint must
	// guard that list's error with the sentinel check. Pin each by name.
	phases := []string{
		"ListContributors",
		"ListIssues",
		"ListPullRequests",
		"ListIssueEvents",
		"ListPREvents",
		"ListIssueComments",
		"ListReviewComments",
	}

	for _, phase := range phases {
		idx := strings.Index(code, phase)
		if idx < 0 {
			t.Errorf("cannot find %s call in staged.go", phase)
			continue
		}
		// Examine a generous window starting from the phase call. The
		// non-fatal check should appear before the next append to
		// result.Errors or before the next phase.
		end := idx + 800
		if end > len(code) {
			end = len(code)
		}
		window := code[idx:end]
		// Accept either explicit sentinel checks or the helper that bundles
		// them (isOptionalEndpointSkip). The helper is the preferred form
		// because it keeps the 404+403 policy in one place.
		hasSentinels := strings.Contains(window, "ErrNotFound") && strings.Contains(window, "ErrForbidden")
		hasHelper := strings.Contains(window, "isOptionalEndpointSkip")
		if !hasSentinels && !hasHelper {
			t.Errorf("staged.go: the loop over %s must treat 404 AND 403 as "+
				"non-fatal — either call isOptionalEndpointSkip(err) (preferred) "+
				"or inline errors.Is checks for both platform.ErrNotFound and "+
				"platform.ErrForbidden — so a single bad endpoint doesn't fail "+
				"the whole job", phase)
		}
	}

	// And the helper itself must exist. Since v0.18.0 the helper delegates
	// to platform.ClassifyError — the sentinel coverage (ErrNotFound,
	// ErrForbidden, ErrGone, ErrWrongEntityKind) is pinned by tests in the
	// platform package (errors_test.go). We assert the delegation here.
	helperIdx := strings.Index(code, "func isOptionalEndpointSkip(")
	if helperIdx < 0 {
		t.Error("staged.go should define isOptionalEndpointSkip(err) to centralize " +
			"the routine-skip check — otherwise every phase duplicates the " +
			"policy and drift is guaranteed")
	} else {
		helperBody := code[helperIdx:]
		end := strings.Index(helperBody, "\n}\n")
		if end > 0 {
			helperBody = helperBody[:end]
		}
		if !strings.Contains(helperBody, "platform.ClassifyError") ||
			!strings.Contains(helperBody, "platform.ClassSkip") {
			t.Error("isOptionalEndpointSkip must delegate to platform.ClassifyError(err) == platform.ClassSkip " +
				"so new error shapes (ErrWrongEntityKind, GraphQL errors) classify through a single source")
		}
	}
}

// TestHTTPClient403WrapsErrForbidden — pins the 403 branch in httpclient.go
// to wrap ErrForbidden so callers can distinguish "private repo / bad scope"
// from rate-limit and auth-failure cases.
func TestHTTPClient403WrapsErrForbidden(t *testing.T) {
	src, err := os.ReadFile("../platform/httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the "not a rate limit" branch (the one we care about).
	idx := strings.Index(code, "not a rate limit")
	if idx < 0 {
		t.Fatal("cannot find 'not a rate limit' branch in httpclient.go")
	}
	// Look backward ~400 chars for the ErrForbidden wrap.
	start := idx - 400
	if start < 0 {
		start = 0
	}
	window := code[start : idx+100]
	if !strings.Contains(window, "ErrForbidden") {
		t.Error("the 403 'not a rate limit' branch must wrap ErrForbidden " +
			"via fmt.Errorf with %%w so the collector can errors.Is check it")
	}
}
