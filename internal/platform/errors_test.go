package platform

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestErrorClassConstantsAreDistinct pins the class enum as a closed set of
// mutually-exclusive values. Without distinct integer values, a bug could
// silently conflate two classes and skip handling that should have bubbled.
func TestErrorClassConstantsAreDistinct(t *testing.T) {
	seen := map[ErrorClass]string{}
	for name, c := range map[string]ErrorClass{
		"ClassOK":          ClassOK,
		"ClassNotModified": ClassNotModified,
		"ClassSkip":        ClassSkip,
		"ClassTransient":   ClassTransient,
		"ClassRateLimit":   ClassRateLimit,
		"ClassAuth":        ClassAuth,
		"ClassFatal":       ClassFatal,
	} {
		if existing, ok := seen[c]; ok {
			t.Errorf("%s has same value as %s (= %d) — classes must be distinct", name, existing, c)
		}
		seen[c] = name
	}
	if len(seen) != 7 {
		t.Errorf("expected exactly 7 classes, found %d — if you're adding a new class, update this test AND ClassifyError", len(seen))
	}
}

// TestClassifyError_Nil — a nil error means success. Any other mapping would
// force callers to special-case nil everywhere.
func TestClassifyError_Nil(t *testing.T) {
	if got := ClassifyError(nil); got != ClassOK {
		t.Errorf("ClassifyError(nil) = %v, want ClassOK", got)
	}
}

// TestClassifyError_Sentinels verifies that all existing package sentinels
// map to the right class. If someone adds a new sentinel without updating
// ClassifyError, it would default to ClassFatal (safe), but we pin the
// expected values here so the intended mapping can't drift silently.
func TestClassifyError_Sentinels(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"ErrNotModified bare", ErrNotModified, ClassNotModified},
		{"ErrNotModified wrapped", fmt.Errorf("fetching: %w", ErrNotModified), ClassNotModified},
		{"ErrNotFound bare", ErrNotFound, ClassSkip},
		{"ErrNotFound wrapped", fmt.Errorf("issue 42: %w", ErrNotFound), ClassSkip},
		{"ErrForbidden bare", ErrForbidden, ClassSkip},
		{"ErrForbidden wrapped", fmt.Errorf("pr labels: %w", ErrForbidden), ClassSkip},
		{"ErrGone bare", ErrGone, ClassSkip},
		{"ErrGone wrapped", fmt.Errorf("issue 7: %w", ErrGone), ClassSkip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyError(tc.err); got != tc.want {
				t.Errorf("ClassifyError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestClassifyError_WrongEntityKind pins the regression-proof fix: the raw
// `"issue N is a pull request"` string that the GitHub client used to return
// is now a typed error wrapping ErrWrongEntityKind, which maps to ClassSkip.
// Before this, that error escaped as ClassFatal (default) and aborted gap
// fills mid-stream. Regression history: v0.17.2.
func TestClassifyError_WrongEntityKind(t *testing.T) {
	err := fmt.Errorf("issue 4: %w", ErrWrongEntityKind)
	if got := ClassifyError(err); got != ClassSkip {
		t.Errorf("wrong-entity-kind error should classify as ClassSkip (GitHub returned /issues/N where N is a PR — routine), got %v", got)
	}

	// Bare sentinel too.
	if got := ClassifyError(ErrWrongEntityKind); got != ClassSkip {
		t.Errorf("bare ErrWrongEntityKind should classify as ClassSkip, got %v", got)
	}
}

// TestClassifyError_UnknownIsFatal verifies the safe default: any error the
// classifier doesn't recognize becomes ClassFatal. Callers then bubble up
// instead of silently swallowing. This is the behavior that made the
// v0.17.2 regression VISIBLE (as a WARN log) rather than a silent gap.
func TestClassifyError_UnknownIsFatal(t *testing.T) {
	err := errors.New("database connection refused")
	if got := ClassifyError(err); got != ClassFatal {
		t.Errorf("unrecognized error must classify as ClassFatal so it bubbles, got %v", got)
	}
}

// TestClassifyError_ContextCanceledIsTransient — when the scheduler cancels
// a job context (shutdown, stale lock recovery), in-flight calls return
// context.Canceled or context.DeadlineExceeded. These shouldn't be ClassFatal
// — they mean "abort cleanly", which the runJob layer already handles.
// Mapping to ClassTransient lets retry loops give up immediately without
// logging a spurious "fatal error" at ERROR level.
func TestClassifyError_ContextCanceledIsTransient(t *testing.T) {
	if got := ClassifyError(context.Canceled); got != ClassTransient {
		t.Errorf("context.Canceled should classify as ClassTransient, got %v", got)
	}
	if got := ClassifyError(context.DeadlineExceeded); got != ClassTransient {
		t.Errorf("context.DeadlineExceeded should classify as ClassTransient, got %v", got)
	}
}

// TestClassifiedErrorInterface verifies the escape hatch: errors that
// implement the ClassifiedError interface override the fallthrough mapping.
// This is how new error shapes (e.g. GraphQL-specific classes we'll add in
// phase 1) integrate without touching ClassifyError's body.
func TestClassifiedErrorInterface(t *testing.T) {
	var e error = customClassifiedError{class: ClassAuth}
	if got := ClassifyError(e); got != ClassAuth {
		t.Errorf("ClassifiedError.Class() should dominate, got %v want ClassAuth", got)
	}

	// Wrapped inside a fmt.Errorf: the interface check must see through the wrap.
	wrapped := fmt.Errorf("http POST failed: %w", e)
	if got := ClassifyError(wrapped); got != ClassAuth {
		t.Errorf("wrapped ClassifiedError should still surface its class, got %v", got)
	}
}

type customClassifiedError struct {
	class ErrorClass
}

func (customClassifiedError) Error() string       { return "custom" }
func (c customClassifiedError) Class() ErrorClass { return c.class }

// TestGet_401ReturnsClassAuth is the HTTP integration point: a 401 from
// GitHub must surface as a ClassAuth-classifiable error. Today httpclient
// invalidates the key and loops, so 401 typically never escapes. But when
// ALL keys are invalidated it eventually bubbles up via "all API keys have
// been invalidated" — that error should classify correctly.
func TestGet_401ReturnsClassAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"bad-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := client.Get(ctx, "/repos/o/r")
	if err == nil {
		t.Fatal("expected error — all keys invalidated by 401")
	}
	if got := ClassifyError(err); got != ClassAuth {
		t.Errorf("error from all-keys-invalidated should classify as ClassAuth, got %v (err=%v)", got, err)
	}
}

// TestGet_404ReturnsClassSkip rebuilds the existing TestGet_404ReturnsErrNotFound
// guarantee using the new classifier. Keeps old behavior pinned via new API.
func TestGet_404ReturnsClassSkip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	_, err := client.Get(context.Background(), "/any")
	if got := ClassifyError(err); got != ClassSkip {
		t.Errorf("404 should classify as ClassSkip, got %v (err=%v)", got, err)
	}
}

// TestIsOptionalEndpointSkipAliasesClassifyError pins the backward-compat
// shim: the legacy isOptionalEndpointSkip helper in the collector package
// must delegate to ClassifyError so downstream packages don't have to be
// changed simultaneously. (The collector_test accesses this via source
// inspection since isOptionalEndpointSkip lives in a different package.)
func TestIsOptionalEndpointSkipAliasesClassifyError(t *testing.T) {
	// Live contract: sentinels that used to match isOptionalEndpointSkip
	// must still return ClassSkip under the new classifier. Pin each one.
	for _, e := range []error{ErrNotFound, ErrForbidden, ErrGone, ErrWrongEntityKind} {
		if ClassifyError(e) != ClassSkip {
			t.Errorf("%v no longer classifies as ClassSkip — isOptionalEndpointSkip callers across collector/staged, refresh_open, gap_fill rely on this", e)
		}
	}
}
