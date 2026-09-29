// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestCommitLookupNotFoundIsTheSentinel — old problem O4 (SR-5):
// githubCommitLookup read any error whose TEXT contained "not found" as a
// definitive 404 ("commit not on GitHub", nil, nil). A 422 whose URL
// happened to contain those words was swallowed that way — a rejected
// request read as a definitive answer. Only platform.ErrNotFound is "not
// on GitHub"; anything else is returned.
func TestCommitLookupNotFoundIsTheSentinel(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name    string
		status  int
		sha     string
		wantErr bool
	}{
		{"404 is not on GitHub", http.StatusNotFound, "abc123", false},
		{"422 whose URL contains the words", http.StatusUnprocessableEntity, "abc not found", true},
	} {
		keys := platform.NewKeyPool([]string{"x"}, lg)
		r := NewCommitResolver(nil, keys, "", lg)
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { w.WriteHeader(tc.status) }))
		r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
		author, err := r.githubCommitLookup(context.Background(), "o", "r", tc.sha)
		ts.Close()
		if (err != nil) != tc.wantErr || author != nil {
			t.Errorf("%s: author=%v err=%v; want error=%v", tc.name, author, err, tc.wantErr)
		}
	}
}
