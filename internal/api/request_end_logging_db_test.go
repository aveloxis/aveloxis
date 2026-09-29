// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoEndpointBlamesTheRequestsEnd (AVELOXIS_TEST_DB) — NET-6 review r4
// F1: three rounds routed error helpers through httpserver.RequestEnded one
// site at a time, and handlers that log store errors themselves ("compare
// series failed", "snapshot failed", …) still logged ERROR when
// http_timeout_seconds or a departed client ended the request — invisible
// to the token ratchets because they never name the context errors. This
// pins the BEHAVIOR on every route: after a warm pass (which also caches
// the tokens, so auth does not fail first), each route runs with its
// request context already past its deadline, then already cancelled, and no
// ERROR or WARN may blame the request's own end.
func TestNoEndpointBlamesTheRequestsEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	fx := seedSmokeFixture(t, ctx, store)
	logs := &lockedBuffer{}
	srv, err := NewWithOptions(store, slog.New(slog.NewTextHandler(logs, nil)), Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	var end func(context.Context) (context.Context, context.CancelFunc)
	h := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if end != nil {
			c, cancel := end(r.Context())
			defer cancel()
			r = r.WithContext(c)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	fill := smokeFill(fx)
	do := func(route string, rc smokeRecipe) {
		method, path, _ := strings.Cut(route, " ")
		url := ts.URL + fill.Replace(path)
		if rc.query != "" {
			url += "?" + fill.Replace(rc.query)
		}
		var body io.Reader
		if rc.body != "" {
			body = strings.NewReader(fill.Replace(rc.body))
		}
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			t.Fatal(err)
		}
		if rc.auth != "" {
			req.Header.Set("Authorization", "Bearer "+fx.tokens[rc.auth])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	routes := 0
	for route, rc := range smokeRecipes {
		if rc.skip == "" && !strings.HasSuffix(route, "?") {
			do(route, rc) // warm: caches the tokens
			routes++
		}
	}
	srctest.MinCount(t, "routes driven", routes, 50)
	for name, fn := range map[string]func(context.Context) (context.Context, context.CancelFunc){
		"past its deadline (http_timeout_seconds)": func(c context.Context) (context.Context, context.CancelFunc) {
			return context.WithDeadline(c, time.Now().Add(-time.Second))
		},
		"cancelled (the client left)": func(c context.Context) (context.Context, context.CancelFunc) {
			c, cancel := context.WithCancel(c)
			cancel()
			return c, cancel
		},
	} {
		mark := len(logs.String())
		end = fn
		for route, rc := range smokeRecipes {
			if rc.skip == "" && !strings.HasSuffix(route, "?") {
				do(route, rc)
			}
		}
		end = nil
		for _, line := range strings.Split(logs.String()[mark:], "\n") {
			if (strings.Contains(line, "level=ERROR") || strings.Contains(line, "level=WARN")) &&
				(strings.Contains(line, "context deadline exceeded") || strings.Contains(line, "context canceled")) {
				t.Errorf("request %s: a handler blamed the request's own end:\n%s", name, line)
			}
		}
	}
}
