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

// TestIsOptionalEndpointSkipSourceContainsErrGone — source contract. Belt
// and braces for the runtime test above: future refactors that move the
// helper must keep the ErrGone branch.
func TestIsOptionalEndpointSkipSourceContainsErrGone(t *testing.T) {
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
	if !strings.Contains(fn, "ErrGone") {
		t.Error("isOptionalEndpointSkip must check platform.ErrGone alongside ErrNotFound/ErrForbidden — " +
			"required so 410 Gone on per-resource endpoints (deleted issues) and unfollowable 3xx " +
			"redirects are treated as 'skip this item' instead of failing the job")
	}
}
