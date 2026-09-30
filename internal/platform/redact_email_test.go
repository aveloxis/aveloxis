// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRedactEmail — v0.29.71 (personal data in INFO logs): an address in a
// log line keeps enough to recognise it (the first character and the
// domain) and nothing more.
func TestRedactEmail(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"jane.doe@example.org", "j***@example.org"},
		{"  ops@aveloxis.io ", "o***@aveloxis.io"},
		{"a@b", "a***@b"},
		{"@example.org", "***@example.org"},
		{"not-an-address", "***"},
		{"", ""},
		{"x@y@z.org", "x***@z.org"},
	} {
		if got := RedactEmail(tc.in); got != tc.want {
			t.Errorf("RedactEmail(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestClientErrorsCarryNoSearchedAddress — the HTTP client's errors embed
// the request URL, and they reach log lines as the error attribute: a
// rejected user search (GitHub answers 422 for some addresses) named the
// author's email in the resolver's log. The error keeps its sentinel and
// the path, not the address.
func TestClientErrorsCarryNoSearchedAddress(t *testing.T) {
	for _, status := range []int{http.StatusUnprocessableEntity, http.StatusNotFound, http.StatusBadRequest, http.StatusGone} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"x"}`))
		}))
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
		_, err := c.Get(context.Background(), "/search/users?q=jane.doe%40example.org+in:email&per_page=1")
		srv.Close()
		if err == nil {
			t.Fatalf("%d: want an error", status)
		}
		if strings.Contains(err.Error(), "jane.doe") || strings.Contains(err.Error(), "example.org") {
			t.Errorf("%d: the error carries the searched address: %v", status, err)
		}
		if !strings.Contains(err.Error(), "/search/users?q=***") {
			t.Errorf("%d: the error must still name the request: %v", status, err)
		}
	}
}
