// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package httpserver builds the http.Servers aveloxis listens with — the
// monitor, the api and the web GUI — bounded by one knob, aveloxis.json's
// http_timeout_seconds (NET-6, 2026-09-29: the three servers set no
// timeouts, so a client that trickled its headers or never read its
// response held a goroutine and a socket forever).
//
// The bound is a BACKSTOP, not the place to tune latency: the operator's
// rule is that aveloxis never holds the shortest timeout in the chain,
// because a cut inside aveloxis is harder to troubleshoot than nginx's.
// Tune nginx (proxy_read_timeout and friends) below it. When aveloxis's
// bound is reached anyway it is never silent: the request's context is
// cancelled (so its database query is), the client gets a 503 naming the
// knob, and a WARN names the request.
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// DefaultTimeout is http_timeout_seconds' default. Every nginx timeout
// defaults to 60 s, and the slowest API statement measured on kate over
// 2026-09-22..28 was 59.9 s (held under nginx's default); three minutes
// stays above both, so nginx — tuned shorter — is the bound an operator
// sees first.
const DefaultTimeout = 180 * time.Second

// WriteMargin is how far the socket write deadline sits past the handler
// bound, so the 503 the bound answers (one small write) can always be
// written; ten seconds covers a slow client link for it.
const WriteMargin = 10 * time.Second

// timeoutBody is the 503 body a request past the bound gets.
const timeoutBody = "request exceeded http_timeout_seconds\n"

// New returns a server bounded by timeout: the header and body reads and
// the idle keep-alive (which outlasts nginx's 60 s upstream keep-alive at
// the default, so nginx closes idle upstream connections first) are
// timeout; each request's handler is bounded by timeout (Bound); the
// socket write deadline is timeout + WriteMargin. A bare WriteTimeout
// would neither cancel the handler nor say anything (NET-6 review r1 F1).
func New(addr string, h http.Handler, timeout time.Duration, logger *slog.Logger, component string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           Bound(h, timeout, logger, component),
		ReadHeaderTimeout: timeout,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout + WriteMargin,
		IdleTimeout:       timeout,
	}
}

// Bound runs h under http.TimeoutHandler: at timeout the request's context
// is cancelled and the client gets a 503 naming the knob, and one WARN
// names the component, the request and the bound. The responses are
// buffered until the handler returns, which every aveloxis handler already
// does (none streams or hijacks).
func Bound(h http.Handler, timeout time.Duration, logger *slog.Logger, component string) http.Handler {
	th := http.TimeoutHandler(h, timeout, timeoutBody)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		th.ServeHTTP(rec, r)
		if rec.status == http.StatusServiceUnavailable && rec.timedOut {
			logger.Warn("request exceeded http_timeout_seconds — tune nginx below it, or raise it if the request is legitimate",
				"component", component, "method", r.Method, "path", truncate(r.URL.Path, 200),
				"elapsed", time.Since(start).Round(time.Millisecond), "bound", timeout)
		}
	})
}

// RequestEnded reports whether a failure is the request's own end — its
// context is done (the client left, or Bound fired: http_timeout_seconds,
// whose WARN reports it) — or a write refused after Bound fired
// (http.ErrHandlerTimeout). Such a failure is not logged at ERROR/WARN.
//
// It is decided by the REQUEST's context, never by the error's type (NET-6
// review r5 F1: classifying by errors.Is(err, context.DeadlineExceeded)
// read a database connect timeout on a live request — pgx's ConnectTimeout
// wraps DeadlineExceeded — as "the request ended", so a DB outage became
// silent empty 200s: SR-5).
func RequestEnded(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, http.ErrHandlerTimeout) || ctx.Err() != nil
}

// LogFailure logs a request's failure at level — unless RequestEnded, which
// drops it to Debug. The one failure logger for request paths in api, web
// and monitor (NET-6 review r4 F1, r5 F2; pinned by
// scripts/TestRequestHandlersLogThroughLogFailure).
func LogFailure(ctx context.Context, logger *slog.Logger, level slog.Level, err error, msg string, args ...any) {
	if RequestEnded(ctx, err) {
		level = slog.LevelDebug
	}
	logger.Log(context.Background(), level, msg, args...)
}

// statusRecorder notes the status written and whether the body is the
// bound's own 503 (a handler's own 503 is not a timeout).
type statusRecorder struct {
	http.ResponseWriter
	status   int
	timedOut bool
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	if s.status == http.StatusServiceUnavailable && string(b) == timeoutBody {
		s.timedOut = true
	}
	return s.ResponseWriter.Write(b)
}

// truncate bounds a request-derived string for the log (the path is
// client-controlled); strconv.Quote escapes control characters.
func truncate(s string, n int) string {
	if len(s) > n {
		s = s[:n] + "…"
	}
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}
