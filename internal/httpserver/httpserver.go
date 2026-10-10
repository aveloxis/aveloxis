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
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultTimeout is http_timeout_seconds' default. Every nginx timeout
// defaults to 60 s, and the slowest API statement measured on kate over
// 2026-09-22..28 was 59.9 s (held under nginx's default); three minutes
// stays above both, so nginx — tuned shorter — is the bound an operator
// sees first.
const DefaultTimeout = 180 * time.Second

// WriteMargin is how far the server's WriteTimeout sits past the bound. It
// is the FALLBACK write deadline: when a response flushes, Bound gives it a
// fresh window of the bound (statusRecorder.flushStarts, NET-6 review r8
// F2), so the margin only matters if that window cannot be set — which is
// logged. Ten seconds covers one small write (the bound's 503) on a slow
// link.
const WriteMargin = 10 * time.Second

// timeoutBody is the 503 body a request past the bound gets.
const timeoutBody = "request exceeded http_timeout_seconds\n"

// New returns a server bounded by timeout: the header and body reads and
// the idle keep-alive (which outlasts nginx's 60 s upstream keep-alive at
// the default, so nginx closes idle upstream connections first) are
// timeout; each request's handler is bounded by timeout (Bound), and its
// response, once flushed, gets its own write window of timeout. The
// server's WriteTimeout (timeout + WriteMargin, counted from the request's
// headers) is the fallback until the flush. A bare WriteTimeout would
// neither cancel the handler nor say anything (NET-6 review r1 F1).
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
		rec := &statusRecorder{ResponseWriter: w, window: timeout}
		th.ServeHTTP(rec, r)
		if rec.windowErr != nil {
			logger.Warn("response write window could not be set — the response had only the server's write timeout, counted from the request's headers",
				"component", component, "path", truncate(r.URL.Path, 200), "error", rec.windowErr)
		}
		if rec.writeErr != nil {
			// NET-6 review r8 F2: the flush has its own window (see
			// statusRecorder.flushStarts); a write that still times out
			// is a client reading too slowly for a large body — said, never
			// a silently truncated 200. Any other write error is the client
			// leaving (nobody to tell).
			var ne net.Error
			if errors.Is(rec.writeErr, os.ErrDeadlineExceeded) || (errors.As(rec.writeErr, &ne) && ne.Timeout()) {
				logger.Warn("response write timed out — the client read the response too slowly; it was truncated",
					"component", component, "method", r.Method, "path", truncate(r.URL.Path, 200),
					"status", rec.status, "window", timeout, "error", rec.writeErr)
			} else {
				logger.Debug("response write failed — the client left", "component", component, "path", truncate(r.URL.Path, 200), "error", rec.writeErr)
			}
		}
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
	// window is the bound: the fresh write window the flush gets.
	window   time.Duration
	flushing bool
	writeErr error
	// windowErr is why the flush's own window could not be set (Bound
	// logs it: the response then has only the server's WriteTimeout).
	windowErr error
}

// flushStarts gives the response its own write window when TimeoutHandler
// begins copying the buffered response out (NET-6 review r8 F2): the
// server's write deadline counts from the request's headers, so a handler
// finishing near the bound left only WriteMargin to write a large body.
func (s *statusRecorder) flushStarts() {
	if s.flushing {
		return
	}
	s.flushing = true
	s.windowErr = http.NewResponseController(s.ResponseWriter).SetWriteDeadline(time.Now().Add(s.window))
}

func (s *statusRecorder) WriteHeader(code int) {
	s.flushStarts()
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.flushStarts()
	if s.status == 0 {
		s.status = http.StatusOK
	}
	if s.status == http.StatusServiceUnavailable && string(b) == timeoutBody {
		s.timedOut = true
	}
	n, err := s.ResponseWriter.Write(b)
	if err != nil && s.writeErr == nil {
		s.writeErr = err
	}
	return n, err
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

// ClientIP is the request's client address: the peer, or — only when the
// peer IS trustedProxy (canonical form, compared byte for byte) — the
// RIGHTMOST X-Forwarded-For entry, the one that proxy appended (entries to
// its left are the client's own claims). The api's per-IP limit and the
// web's sign-up quota both read it (one rule, SR-17; v0.29.89).
func ClientIP(r *http.Request, trustedProxy string) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if trustedProxy == "" || host != trustedProxy {
		return peer
	}
	// Every header line, joined: a proxy that appends a separate line (not
	// a comma) must not leave a client-supplied first line deciding.
	xff := strings.Join(r.Header.Values("X-Forwarded-For"), ",")
	if strings.TrimSpace(xff) == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	if ip := net.ParseIP(strings.TrimSpace(parts[len(parts)-1])); ip != nil {
		return ip
	}
	return peer
}
