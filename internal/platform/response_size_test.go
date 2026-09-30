// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestResponseSizeHighWaterMark — operator, 2026-09-30: no size limit on
// data responses, but log the largest. One INFO line per source each time a
// response is larger than any before it from that source.
func TestResponseSizeHighWaterMark(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	src := "test-source-" + t.Name()
	ResetResponseSizeMarksForTest(t, src, src+"-other")
	for _, n := range []int64{100, 50, 100, 300, 200} {
		NoteResponseSize(logger, src, "https://example.org/x?q=a%40b.org", n)
	}
	if got := strings.Count(logs.String(), "response size high-water mark"); got != 2 {
		t.Errorf("want 2 lines (100, then 300), got %d:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "bytes=300") || !strings.Contains(logs.String(), "previous_max=100") {
		t.Errorf("the line must carry the new size and the previous maximum:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "a%40b") {
		t.Error("the URL must be redacted")
	}
	logs.Reset()
	NoteResponseSize(logger, src+"-other", "", 10)
	if !strings.Contains(logs.String(), "response size high-water mark") {
		t.Error("each source keeps its own maximum")
	}
}

// TestRESTResponsesReportTheirSize — the forge REST client's data bodies
// are counted as they are read and reported when closed.
func TestRESTResponsesReportTheirSize(t *testing.T) {
	body := strings.Repeat("x", 12345)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ResetResponseSizeMarksForTest(t, "github-rest")
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
	resp, err := c.Get(context.Background(), "/repos/o/r/issues")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if !strings.Contains(logs.String(), "response size high-water mark") || !strings.Contains(logs.String(), "bytes=12345") {
		t.Errorf("a REST data body must report its size:\n%s", logs.String())
	}
}

// TestGraphQLResponseSizeNamesItsForge — whole-branch review F2: GitLab's
// GraphQL goes through the same client, and its sizes were logged as
// github-graphql, sharing GitHub's maximum (a large GitLab answer hid later
// GitHub marks). Each forge keeps its own source, as REST does.
func TestGraphQLResponseSizeNamesItsForge(t *testing.T) {
	for _, tc := range []struct {
		style AuthStyle
		want  string
	}{{AuthGitHub, "source=github-graphql"}, {AuthGitLab, "source=gitlab-graphql"}} {
		ResetResponseSizeMarksForTest(t, "github-graphql", "gitlab-graphql")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":{"hello":"world"}}`)
		}))
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, tc.style)
		var got struct {
			Hello string `json:"hello"`
		}
		if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
			t.Fatal(err)
		}
		srv.Close()
		if !strings.Contains(logs.String(), tc.want) {
			t.Errorf("auth style %v: the size line must carry %s; log:\n%s", tc.style, tc.want, logs.String())
		}
	}
}
