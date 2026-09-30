// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package depsdev

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

// TestErrorResponsesAreNotDataSizes — v0.29.71 whole-branch review F5: the
// body was counted for every status, so a 404's empty body could be logged
// as the source's first "response size high-water mark" (bytes=0). Only a
// 200's body is a data response, as in the forge REST client. The source's
// mark is reset first (fix review V2: an earlier test's 200 had set one, so
// an unread 404 reporting 0 bytes was no new maximum and the reverted code
// passed).
func TestErrorResponsesAreNotDataSizes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "not found")
	}))
	t.Cleanup(srv.Close)
	platform.ResetResponseSizeMarksForTest(t, "depsdev")
	var logs bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	c := New(Options{BaseURL: srv.URL})
	if _, err := c.GetPackageVersions(context.Background(), "o", "r"); err != nil {
		t.Fatal(err)
	}
	// The package-detail read is the second count site (fix review round 2:
	// its revert mutant passed while only the first was driven). Its 404 is
	// an error; the answer does not matter here, only that nothing is noted.
	_, _ = c.fetchPackageTimestamps(context.Background(), "NPM", "x")
	if strings.Contains(logs.String(), "response size high-water mark") {
		t.Errorf("a 404 body was counted as a data response:\n%s", logs.String())
	}
}
