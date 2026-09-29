// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// Worklist item 82: prelim's probe is a web HEAD, so a 451 reaches the
// scheduler with no body. FetchRepoNotice asks the REST API once for the
// block object. SR-16: three answers — a notice, a definitive "none"
// (2xx), and an error that is neither.
func TestFetchRepoNotice(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name       string
		status     int
		body       string
		wantNotice bool
		wantErr    bool
	}{
		{"legal block", http.StatusUnavailableForLegalReasons,
			`{"message":"Repository access blocked","block":{"reason":"dmca","html_url":"https://github.com/github/dmca/x.md"}}`, true, false},
		{"reachable", http.StatusOK, `{"full_name":"o/r"}`, false, false},
		{"not found is not a notice", http.StatusNotFound, `{"message":"Not Found"}`, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var path string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path = r.URL.Path
				w.WriteHeader(c.status)
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()
			client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
			n, ok, err := client.FetchRepoNotice(context.Background(), "o", "r")
			if path != "/repos/o/r" {
				t.Errorf("requested %q, want /repos/o/r", path)
			}
			if ok != c.wantNotice || (err != nil) != c.wantErr {
				t.Fatalf("ok=%v err=%v; want ok=%v err=%v", ok, err, c.wantNotice, c.wantErr)
			}
			if ok && (n.Message != "Repository access blocked" || n.Reason != "dmca" || n.URL != "https://github.com/github/dmca/x.md") {
				t.Errorf("notice = %+v", n)
			}
			if ok && err != nil {
				t.Errorf("a notice is an answer, not an error: %v", err)
			}
			if c.wantErr && !errors.Is(err, platform.ErrNotFound) {
				t.Errorf("err = %v, want the 404 passed through", err)
			}
		})
	}
}
