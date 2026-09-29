// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package httpserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuf is a goroutine-safe log sink.
type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestNewSetsEveryTimeout — NET-6 (2026-09-29): the monitor, api and web
// http.Server literals set no timeouts. The reads and the idle keep-alive
// are the knob; the socket write deadline is a backstop WriteMargin past
// it, so the 503 the handler bound answers can always be written.
func TestNewSetsEveryTimeout(t *testing.T) {
	const d = 42 * time.Second
	srv := New(":0", http.NotFoundHandler(), d, quiet(), "test")
	if srv.ReadHeaderTimeout != d || srv.ReadTimeout != d || srv.IdleTimeout != d || srv.WriteTimeout != d+WriteMargin {
		t.Errorf("header %v read %v idle %v write %v; want %v ×3 and %v", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout, srv.WriteTimeout, d, d+WriteMargin)
	}
}

// TestDefaultIsNeverTheShortestBound — the operator's rule (2026-09-29):
// aveloxis's bound is a backstop, never the shortest in the chain; nginx
// is tuned shorter. Every nginx timeout defaults to 60 s, and the slowest
// API statement measured on kate (2026-09-22..28) was 59.9 s.
func TestDefaultIsNeverTheShortestBound(t *testing.T) {
	const nginxDefault = 60 * time.Second
	if DefaultTimeout <= nginxDefault {
		t.Errorf("DefaultTimeout %v must exceed nginx's %v defaults", DefaultTimeout, nginxDefault)
	}
}

// TestSlowHeadersAreCut: a client that sends a partial request and stops
// is disconnected near the bound.
func TestSlowHeadersAreCut(t *testing.T) {
	addr := serve(t, New("", http.NotFoundHandler(), 200*time.Millisecond, quiet(), "test"))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n"); err != nil { // headers never end
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	_, err = io.ReadAll(c)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the server kept a half-sent request open past its header bound")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the half-sent request was held %v against a 200 ms bound", took)
	}
}

// TestHandlerInsideTheBoundAnswers — the slow-query constraint: a handler
// that finishes inside the bound answers normally.
func TestHandlerInsideTheBoundAnswers(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})
	resp := get(t, serve(t, New("", h, 2*time.Second, quiet(), "test")))
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "done" {
		t.Errorf("got %d %q", resp.StatusCode, body)
	}
}

// TestRequestPastTheBoundIsCancelledAnsweredAndLogged — NET-6 review r1 F1:
// a bare WriteTimeout neither cancels the handler (its query ran on,
// holding a pool connection) nor tells anyone (the client got an EOF when
// the handler finished; aveloxis logged nothing) — the silent cut the
// operator's rule exists to avoid. At the bound the handler's context is
// cancelled (pgx cancels the query), the client gets a 503 naming the
// knob, and one WARN names the component, the request and the bound.
func TestRequestPastTheBoundIsCancelledAnsweredAndLogged(t *testing.T) {
	cancelled := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			cancelled <- r.Context().Err()
		case <-time.After(5 * time.Second):
			cancelled <- nil
		}
	})
	logs := &lockedBuf{}
	const bound = 300 * time.Millisecond
	addr := serve(t, New("", h, bound, slog.New(slog.NewTextHandler(logs, nil)), "api"))
	start := time.Now()
	resp := get(t, addr)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "http_timeout_seconds") {
		t.Errorf("past the bound: %d %q; want 503 naming http_timeout_seconds", resp.StatusCode, body)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the client waited %v against a %v bound", took, bound)
	}
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the handler's context must be cancelled at the bound (so its query is), got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never observed a cancellation")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), "request exceeded http_timeout_seconds") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	l := logs.String()
	if !strings.Contains(l, "level=WARN") || !strings.Contains(l, "request exceeded http_timeout_seconds") ||
		!strings.Contains(l, "component=api") || !strings.Contains(l, "path=/slow") {
		t.Errorf("one WARN must name the component, the request and the bound:\n%s", l)
	}
}

func get(t *testing.T, addr string) *http.Response {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if _, err := io.WriteString(c, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("no response: %v", err)
	}
	return resp
}

func serve(t *testing.T, srv *http.Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// TestOwnServiceUnavailableIsNotATimeout — NET-6 review r2 F3: handlers
// answer their own 503s ("store unavailable", "session lookup failed");
// those must not read as the bound firing.
func TestOwnServiceUnavailableIsNotATimeout(t *testing.T) {
	logs := &lockedBuf{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
	})
	resp := get(t, serve(t, New("", h, 2*time.Second, slog.New(slog.NewTextHandler(logs, nil)), "api")))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if strings.Contains(logs.String(), "request exceeded") {
		t.Errorf("a handler's own 503 was logged as the bound firing:\n%s", logs.String())
	}
}

// TestAbandonedRequestIsNotATimeout — r2 F3: a client that leaves makes
// TimeoutHandler write a bodiless 503; that is not the bound firing.
func TestAbandonedRequestIsNotATimeout(t *testing.T) {
	logs := &lockedBuf{}
	done := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(done)
	})
	addr := serve(t, New("", h, 5*time.Second, slog.New(slog.NewTextHandler(logs, nil)), "api"))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, "GET /x HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	_ = c.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never saw the client leave")
	}
	time.Sleep(100 * time.Millisecond)
	if strings.Contains(logs.String(), "request exceeded") {
		t.Errorf("an abandoned request was logged as the bound firing:\n%s", logs.String())
	}
}

// TestRequestEnded — the one classifier the error helpers share. NET-6
// review r5 F1: it classified by the ERROR alone, and a database connect
// timeout on a live request (pgx's ConnectTimeout wraps
// context.DeadlineExceeded) read as "the request ended" — a DB outage
// became silent empty 200s. The request's own context decides; an error of
// any type on a live request is a failure.
func TestRequestEnded(t *testing.T) {
	live := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	deadline := errors.Join(errors.New("failed to connect: dial error: timeout"), context.DeadlineExceeded)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"connect timeout on a live request is a FAILURE", live, deadline, false},
		{"cancelled error on a live request is a failure", live, context.Canceled, false},
		{"the request's context is done", done, deadline, true},
		{"the request's context is done, any error", done, errors.New("conn closed"), true},
		{"a write after the bound", live, http.ErrHandlerTimeout, true},
		{"no error", done, nil, false},
		{"a real failure on a live request", live, errors.New("relation does not exist"), false},
	} {
		if got := RequestEnded(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: RequestEnded = %v; want %v", tc.name, got, tc.want)
		}
	}
}

// TestLogFailure — the shared failure logger: the request's own end drops
// to Debug; the same error on a live request keeps its level.
func TestLogFailure(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	done, cancel := context.WithCancel(context.Background())
	cancel()
	LogFailure(done, logger, slog.LevelError, context.DeadlineExceeded, "lookup failed", "error", context.DeadlineExceeded)
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("an ended request logged ERROR:\n%s", logs.String())
	}
	LogFailure(context.Background(), logger, slog.LevelError, context.DeadlineExceeded, "lookup failed", "error", context.DeadlineExceeded)
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a connect timeout on a live request must stay ERROR:\n%s", logs.String())
	}
}
