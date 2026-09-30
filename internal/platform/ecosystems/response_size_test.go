// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package ecosystems

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestErrorResponsesAreNotDataSizes — v0.29.71 whole-branch review F5: only
// a 200's body is a data response; an error answer is never a size mark.
func TestErrorResponsesAreNotDataSizes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "not found")
	}))
	t.Cleanup(srv.Close)
	platform.ResetResponseSizeMarksForTest(t, "ecosystems")
	var logs bytes.Buffer
	c := New(Options{BaseURL: srv.URL, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if _, err := c.LookupPackages(context.Background(), "https://github.com/o/r"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "response size high-water mark") {
		t.Errorf("an error body was counted as a data response:\n%s", logs.String())
	}
}
