package collector

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// TestIsOptionalEndpointSkipRecognizesErrGone — runtime check. A per-resource
// 410 or unfollowable 3xx from the HTTP client surfaces as platform.ErrGone.
// The staged collector's per-phase loops must treat this like ErrNotFound /
// ErrForbidden: skip the phase cleanly, do not fail the whole job. Otherwise
// a single deleted issue returning 410 would kill the entire collection pass.
func TestIsOptionalEndpointSkipRecognizesErrGone(t *testing.T) {
	// Wrap like the real HTTPClient does in its switch.
	wrapped := fmt.Errorf("%w: https://api.github.com/repos/o/r/issues/115", platform.ErrGone)
	if !isOptionalEndpointSkip(wrapped) {
		t.Error("isOptionalEndpointSkip must return true for an error wrapping platform.ErrGone — " +
			"otherwise a single 410 Gone (deleted issue) or an unfollowable 301 kills the whole job")
	}
	// Sanity: a random error must not be skippable.
	if isOptionalEndpointSkip(errors.New("DB connection refused")) {
		t.Error("isOptionalEndpointSkip must NOT return true for unrelated errors — that would silently swallow real failures")
	}
}

// TestIsOptionalEndpointSkipDelegatesToClassify — source contract. Since
// v0.18.0 isOptionalEndpointSkip is a thin delegate to platform.ClassifyError;
// the ErrGone (and ErrNotFound/ErrForbidden/ErrWrongEntityKind) handling
// lives in the platform package. Pin the delegation so a future refactor
// can't quietly drop it and re-introduce the sentinel-ladder drift.
func TestIsOptionalEndpointSkipDelegatesToClassify(t *testing.T) {
	data, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	idx := strings.Index(src, "func isOptionalEndpointSkip(")
	if idx < 0 {
		t.Fatal("isOptionalEndpointSkip no longer in staged.go — update this test if it moved")
	}
	fn := src[idx:]
	if end := strings.Index(fn, "\nfunc "); end > 0 {
		fn = fn[:end]
	}
	if !strings.Contains(fn, "platform.ClassifyError") ||
		!strings.Contains(fn, "platform.ClassSkip") {
		t.Error("isOptionalEndpointSkip must delegate to platform.ClassifyError(err) == platform.ClassSkip " +
			"so the sentinel set (ErrNotFound, ErrForbidden, ErrGone, ErrWrongEntityKind) is covered by " +
			"a single classifier instead of duplicated at every call site")
	}
}

// TestIsOptionalEndpointSkipRecognizesErrWrongEntityKind — runtime check
// for the new v0.18.0 sentinel. Regression prevention: before the sentinel,
// "issue N is a pull request" escaped as an untyped error, the gap filler
// treated it as fatal, and heudiconv got stuck in `collecting` status
// because fillIssueGaps bailed at the first PR-number.
func TestIsOptionalEndpointSkipRecognizesErrWrongEntityKind(t *testing.T) {
	wrapped := fmt.Errorf("issue 4: %w", platform.ErrWrongEntityKind)
	if !isOptionalEndpointSkip(wrapped) {
		t.Error("isOptionalEndpointSkip must return true for platform.ErrWrongEntityKind — " +
			"GitHub's issue and PR number spaces overlap; /issues/N returning a PR is routine, not fatal")
	}
}
