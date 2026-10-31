// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package monitor

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// countingStore records the prioritize calls that reach the store.
type countingStore struct {
	fakeMonitorStore
	prioritized []int64
}

func (c *countingStore) PrioritizeRepo(_ context.Context, id int64) error {
	c.prioritized = append(c.prioritized, id)
	return nil
}

// The monitor's one state change takes no credential, so a page on another
// site could post it from the operator's browser (0.29.125 ASVS review,
// V4.3.3): a foreign Origin or a cross-site fetch is refused before the
// store, same-host and header-less requests go through.
func TestPrioritizeRefusesCrossSiteRequests(t *testing.T) {
	cases := []struct {
		name     string
		headers  map[string]string
		wantCode int
	}{
		{"no browser headers", nil, http.StatusOK},
		{"same host", map[string]string{"Origin": "http://127.0.0.1:5555"}, http.StatusOK},
		{"foreign origin", map[string]string{"Origin": "https://evil.example.net"}, http.StatusForbidden},
		{"foreign referer", map[string]string{"Referer": "https://evil.example.net/x"}, http.StatusForbidden},
		{"cross-site fetch", map[string]string{"Origin": "http://127.0.0.1:5555", "Sec-Fetch-Site": "cross-site"}, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingStore{}
			var logs strings.Builder
			s := newServer(store, slog.New(slog.NewTextHandler(&logs, nil)), Options{})
			r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:5555/api/prioritize/7", nil)
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d\n%s", w.Code, tc.wantCode, w.Body.String())
			}
			reached := len(store.prioritized)
			if tc.wantCode == http.StatusOK && (reached != 1 || store.prioritized[0] != 7) {
				t.Errorf("store prioritized %v; want [7]", store.prioritized)
			}
			if tc.wantCode == http.StatusForbidden {
				if reached != 0 {
					t.Errorf("a refused request still reached the store: %v", store.prioritized)
				}
				if !strings.Contains(logs.String(), "cross-site prioritize refused") || !strings.Contains(logs.String(), "evil.example.net") && !strings.Contains(logs.String(), "127.0.0.1:5555") {
					t.Errorf("the refusal is not logged with its origin:\n%s", logs.String())
				}
			}
		})
	}
}

// Every monitor answer carries nosniff (ASVS V14.4.4): a page, JSON, the
// icon, a 404 and the mux's own 405.
func TestEveryMonitorAnswerCarriesNosniff(t *testing.T) {
	s := newServer(fakeMonitorStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/"},
		{http.MethodGet, "/api/stats"},
		{http.MethodGet, "/icon.png"},
		{http.MethodGet, "/no/such/page"},
		{http.MethodGet, "/api/prioritize/7"}, // 405: the mux answers, not a handler
		{http.MethodPost, "/api/prioritize/7"},
	} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s %s (%d): X-Content-Type-Options = %q, want nosniff", tc.method, tc.path, w.Code, got)
		}
	}
}
