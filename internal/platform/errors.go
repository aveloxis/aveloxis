package platform

import (
	"context"
	"errors"
)

// ErrorClass is a coarse categorization of what a caller should do with an
// error. It exists so every call site has the same vocabulary: instead of
// each loop writing its own `errors.Is(err, ErrNotFound) || errors.Is(err,
// ErrForbidden) || errors.Is(err, ErrGone)` chain, it writes
// `ClassifyError(err) == ClassSkip`.
//
// The set is closed on purpose — unknown errors fall to ClassFatal so they
// bubble up visibly rather than being silently swallowed. The regression at
// v0.17.2 happened because a new error shape ("issue N is a pull request")
// slipped through the sentinel-only check and was treated as fatal by the
// gap filler. Under this taxonomy, adding a new error type requires an
// explicit decision about its class, and untyped errors stay safe-by-default.
type ErrorClass int

const (
	// ClassOK is the class of a nil error. Makes it safe to call
	// ClassifyError unconditionally without a nil guard.
	ClassOK ErrorClass = iota

	// ClassNotModified corresponds to a 304 response (GitHub respected our
	// If-None-Match header). Callers with an ETag cache should consult it;
	// callers without a cache should treat this like an empty-page response.
	ClassNotModified

	// ClassSkip means "this item can't be collected, continue the loop."
	// Covers 404 (doesn't exist or invisible), 403-private (token lacks
	// scope), 410 (deliberately removed), and routine classification errors
	// (issue-number-that-is-actually-a-PR). None of these indicate a bug on
	// our side; all represent normal states of the remote API.
	ClassSkip

	// ClassTransient is a temporary failure: 5xx, connection reset, DNS
	// blip, context cancellation. httpclient.Get's internal retry loop
	// absorbs most of these — callers only see them when the retry budget
	// was exhausted, or when the context itself was cancelled (scheduler
	// shutdown, stale-lock recovery). Mapping to Transient rather than
	// Fatal keeps shutdown logs free of spurious ERROR lines.
	ClassTransient

	// ClassRateLimit is a rate-limit signal. Like ClassTransient, these are
	// handled internally by httpclient; callers only see them if the
	// context was cancelled during the wait. Kept distinct from Transient
	// so logs/metrics can break out throttling vs genuine transient errors.
	ClassRateLimit

	// ClassAuth means the token was rejected. httpclient invalidates the
	// key and rotates; callers see this class only when every key has
	// been invalidated. The scheduler treats this as "abort all in-flight
	// jobs and stop" since no amount of retry will help without operator
	// intervention (new/refreshed tokens).
	ClassAuth

	// ClassFatal is the safe default for any error the classifier doesn't
	// recognize: JSON parse failures, database errors, unknown 4xx status
	// codes, corrupted state. Callers bubble these up to runJob which
	// marks the job failed with the error message preserved.
	ClassFatal
)

// String returns the class name for logging. Keep in sync with the const block.
func (c ErrorClass) String() string {
	switch c {
	case ClassOK:
		return "ok"
	case ClassNotModified:
		return "not-modified"
	case ClassSkip:
		return "skip"
	case ClassTransient:
		return "transient"
	case ClassRateLimit:
		return "rate-limit"
	case ClassAuth:
		return "auth"
	case ClassFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// ClassifiedError is the escape hatch for error types that don't match one
// of the package sentinels but still know their own class. Typical
// implementors: GraphQL-specific errors (phase 1) whose class depends on
// the GraphQL "type" field in the response, not on an HTTP status code.
//
// Classify checks for this interface BEFORE the sentinel ladder, so an
// implementor that returns ClassAuth dominates a wrap that otherwise looks
// like ErrNotFound. Use this power carefully — the sentinel mapping is the
// norm and implementing ClassifiedError is opting out of it.
type ClassifiedError interface {
	error
	Class() ErrorClass
}

// ErrAllKeysInvalidated is returned by KeyPool.GetKey when every configured
// token has been marked invalid by a 401 response. Maps to ClassAuth so
// callers stop retrying — no amount of backoff repairs a bad credential.
var ErrAllKeysInvalidated = errors.New("all API keys invalidated")

// ErrWrongEntityKind is returned when the API responds with the right
// status code (200 OK) but the wrong shape — specifically, when
// FetchIssueByNumber receives a 200 for a number that turns out to be a
// pull request, or vice versa. GitHub shares the issue/PR number space,
// so /issues/{N} returning a PR-shaped response is routine: it means
// "this N is not an issue, try /pulls/{N} instead."
//
// Mapped to ClassSkip so gap-fill loops continue past the item instead of
// bailing. Before the sentinel, this was a raw fmt.Errorf that escaped as
// ClassFatal-equivalent (v0.17.2 regression — see issue 4 / heudiconv).
var ErrWrongEntityKind = errors.New("wrong entity kind")

// ClassifyError returns the class a caller should use to decide what to
// do with err. Nil errors are ClassOK; unknown errors are ClassFatal.
//
// Lookup order:
//  1. nil → ClassOK
//  2. errors.As into ClassifiedError interface (GraphQL errors, custom types)
//  3. context.Canceled / context.DeadlineExceeded → ClassTransient
//  4. errors.Is against the package sentinels (ErrNotModified, ErrNotFound,
//     ErrForbidden, ErrGone, ErrWrongEntityKind) → their fixed class
//  5. everything else → ClassFatal
//
// The function is safe to call on wrapped errors; it uses errors.Is/As
// internally.
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ClassOK
	}

	var ce ClassifiedError
	if errors.As(err, &ce) {
		return ce.Class()
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassTransient
	}

	switch {
	case errors.Is(err, ErrNotModified):
		return ClassNotModified
	case errors.Is(err, ErrNotFound),
		errors.Is(err, ErrForbidden),
		errors.Is(err, ErrGone),
		errors.Is(err, ErrWrongEntityKind):
		return ClassSkip
	case errors.Is(err, ErrAllKeysInvalidated):
		return ClassAuth
	}

	return ClassFatal
}
