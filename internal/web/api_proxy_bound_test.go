// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// TestAPIProxyUnderTheBound — NET-6 review r2 F1: the proxy's own header
// wait equalled the web's request bound and always lost the race, so the
// documented 502 never happened and one slow chart logged three WARNs, one
// blaming the api. The proxy now has no wait of its own — the proxied
// request carries the web request's context, so the web's bound governs —
// and its error line is skipped when that bound (or the client) ended the
// request. A dead api still fails fast: 502 and the proxy's WARN.
func TestAPIProxyUnderTheBound(t *testing.T) {
	const bound = 500 * time.Millisecond
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	apiLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	apiSrv := httpserver.New("", slow, 10*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)), "api")
	go func() { _ = apiSrv.Serve(apiLn) }()
	t.Cleanup(func() { _ = apiSrv.Close() })

	webLogs := &syncBuf{}
	run := func(apiURL string) (*http.Response, time.Duration) {
		logger := slog.New(slog.NewTextHandler(webLogs, nil))
		s := New(nil, config.WebConfig{APIInternalURL: apiURL}, nil, "", logger)
		s.sessions["proxy-test"] = &Session{UserID: 1, LoginName: "proxy-test", ExpiresAt: time.Now().Add(time.Hour)}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := httpserver.New("", s.Handler(), bound, logger, "web")
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
		start := time.Now()
		req, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/api/v1/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "proxy-test"})
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp, time.Since(start)
	}

	resp, took := run("http://" + apiLn.Addr().String())
	_ = resp.Body.Close()
	time.Sleep(100 * time.Millisecond)
	l := webLogs.String()
	if resp.StatusCode != http.StatusServiceUnavailable || took > 3*time.Second {
		t.Errorf("a slow api behind the web: %d after %v; want the web's 503 near %v", resp.StatusCode, took, bound)
	}
	if strings.Count(l, "request exceeded http_timeout_seconds") != 1 || strings.Contains(l, "api reverse proxy error") {
		t.Errorf("one slow chart must log the bound's one WARN and no proxy error:\n%s", l)
	}

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadURL := "http://" + dead.Addr().String()
	_ = dead.Close()
	resp, took = run(deadURL)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway || took > 2*time.Second || !strings.Contains(webLogs.String(), "api reverse proxy error") {
		t.Errorf("a dead api must still fail fast with 502 and the proxy's WARN: %d after %v", resp.StatusCode, took)
	}
}

// TestAPIProxyHasNoWaitOfItsOwn — NET-6 review r3 M4: re-adding the old
// 15 s ResponseHeaderTimeout survived, because the chain test's 500 ms bound
// never let a 15 s wait race it. The proxy's transport must set none.
func TestAPIProxyHasNoWaitOfItsOwn(t *testing.T) {
	s := New(nil, config.WebConfig{APIInternalURL: "http://127.0.0.1:8383"}, nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	rp, ok := s.apiProxy.(*httputil.ReverseProxy)
	if !ok {
		t.Fatalf("apiProxy is %T; this pin reads its transport", s.apiProxy)
	}
	tr, ok := rp.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("the proxy transport is %T", rp.Transport)
	}
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("the /api proxy waits %v on its own; the web's http_timeout_seconds bound must govern it", tr.ResponseHeaderTimeout)
	}
}

// TestOAuthFailureOnAnEndedRequest — NET-6 review r3 F3: with
// http_timeout_seconds below the 30 s callback bound, the request's bound
// fired first and logOAuthFailure logged ERROR naming the 30 s bound. When
// the request itself has ended, the failure is Debug (the bound's WARN says
// so); the callback bound's own expiry stays ERROR.
func TestOAuthFailureOnAnEndedRequest(t *testing.T) {
	var logs strings.Builder
	s := &Server{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	s.logOAuthFailure(ended, "github", "exchange", fmt.Errorf("x: %w", context.DeadlineExceeded))
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("an ended request's OAuth failure logged ERROR:\n%s", logs.String())
	}
	s.logOAuthFailure(context.Background(), "github", "exchange", fmt.Errorf("x: %w", context.DeadlineExceeded))
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "bound=30s") {
		t.Errorf("the callback bound's own expiry must stay ERROR naming it:\n%s", logs.String())
	}
}

// errEmailLookup answers every lookup with one error.
type errEmailLookup struct{ err error }

func (e errEmailLookup) GetUserEmail(context.Context, int) (string, error) { return "", e.err }
func (e errEmailLookup) GetUserLivePendingEmail(context.Context, int) (string, error) {
	return "", e.err
}

// TestDashboardEmailGateOnAnEndedRequest — NET-6 review r5 F2: the gate
// logs through a `logger` parameter, which the handler-logging ratchet did
// not see; a request ended by http_timeout_seconds logged WARN "could not
// read the user's email … context deadline exceeded". The request's end is
// Debug; the same error on a live request is still WARN (r5 F1).
func TestDashboardEmailGateOnAnEndedRequest(t *testing.T) {
	err := fmt.Errorf("q: %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name     string
		ended    bool
		wantWarn bool
	}{{"ended request", true, false}, {"live request", false, true}} {
		var logs strings.Builder
		ctx := context.Background()
		if tc.ended {
			c, cancel := context.WithCancel(ctx)
			cancel()
			ctx = c
		}
		r := httptest.NewRequest(http.MethodGet, "/dashboard", nil).WithContext(ctx)
		dashboardEmailGate(ctx, errEmailLookup{err}, confirmationPolicy{}, slog.New(slog.NewTextHandler(&logs, nil)), r, 7)
		if got := strings.Contains(logs.String(), "level=WARN"); got != tc.wantWarn {
			t.Errorf("%s: WARN logged = %v; want %v:\n%s", tc.name, got, tc.wantWarn, logs.String())
		}
	}
}

// TestConfirmationSendFailureIsLoggedAfterTheClientLeft — NET-6 review r6
// F2: the SMTP send takes no context, so its failure can never be the
// request's end; wrapped in LogFailure it dropped to Debug whenever nginx
// had already given up on the request (an SMTP dial can outlast nginx's
// 60 s). It is a WARN regardless.
func TestConfirmationSendFailureIsLoggedAfterTheClientLeft(t *testing.T) {
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	var logs strings.Builder
	r := httptest.NewRequest("POST", "/account/email", strings.NewReader("email=user%40example.com")).WithContext(gone)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	st := &fakeEmailStore{}
	m := &fakeConfirmMailer{site: "https://aveloxis.io", enabled: true, sendErr: errors.New("dial tcp: i/o timeout")}
	submitAccountEmail(gone, st, confirmationPolicy{mailer: m}, slog.New(slog.NewTextHandler(&logs, nil)), r, &Session{UserID: 7, LoginName: "alice"})
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "failed to send confirmation email") {
		t.Errorf("an SMTP failure must be a WARN even after the client left:\n%s", logs.String())
	}
}

// TestUserEmailsCallbackBoundIsNotSilent — NET-6 review r6 F1: the
// /user/emails read runs on the callback's own 30 s context; classifying
// its failure by THAT context hid the callback bound's expiry on a live
// request at Debug (the login then completed with no email and no log).
// The request's context decides; the forge read keeps the bounded one.
func TestUserEmailsCallbackBoundIsNotSilent(t *testing.T) {
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(stall.Close)
	client := stall.Client()
	client.Transport = rewriteTo(stall.URL, client.Transport)
	var logs strings.Builder
	live := context.Background()
	bounded, cancel := context.WithTimeout(live, 50*time.Millisecond)
	defer cancel()
	if got := fetchGitHubPrimaryEmail(live, bounded, client, slog.New(slog.NewTextHandler(&logs, nil))); got != "" {
		t.Fatalf("got %q from a stalled forge", got)
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("the callback bound expiring on a live request must be a WARN:\n%s", logs.String())
	}
}

// rewriteTo sends every request to base (the /user/emails URL is fixed).
func rewriteTo(base string, next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		u, _ := url.Parse(base)
		r2 := r.Clone(r.Context())
		r2.URL.Scheme, r2.URL.Host = u.Scheme, u.Host
		return next.RoundTrip(r2)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// ctxClearStore fails the clean-up when its context is done, like a real
// store call on a cancelled context.
type ctxClearStore struct{ *fakeEmailStore }

func (c ctxClearStore) ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.fakeEmailStore.ClearUserPendingEmailIf(ctx, userID, email)
}

// TestPendingEmailIsClearedAfterTheClientLeft — NET-6 review r7 F1: when
// the confirmation send fails after the request ended (an SMTP dial can
// outlast nginx and the bound), the clean-up ran on the cancelled request
// context, failed at once, and was logged at Debug — leaving the pending
// address set, so the dashboard kept announcing a link that was never
// sent. The clean-up belongs to a write that already committed: it runs
// detached from the request, bounded on its own.
func TestPendingEmailIsClearedAfterTheClientLeft(t *testing.T) {
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	var logs strings.Builder
	r := httptest.NewRequest("POST", "/account/email", strings.NewReader("email=user%40example.com")).WithContext(gone)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	st := ctxClearStore{&fakeEmailStore{}}
	m := &fakeConfirmMailer{site: "https://aveloxis.io", enabled: true, sendErr: errors.New("dial tcp: i/o timeout")}
	submitAccountEmail(gone, st, confirmationPolicy{mailer: m}, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), r, &Session{UserID: 7, LoginName: "alice"})
	if len(st.clears) != 1 || strings.Contains(logs.String(), "failed to clear the pending email") {
		t.Errorf("the pending address must be cleared even after the client left (clears %v):\n%s", st.clears, logs.String())
	}
}
