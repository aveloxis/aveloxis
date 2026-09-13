// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestExtractNextLink(t *testing.T) {
	const gh = "https://api.github.com"
	const gl = "https://gitlab.com/api/v4"
	tests := []struct {
		name, base, request, header, want string
		refused, malformed                bool
	}{
		{name: "link header with next rel", base: gh,
			header: `<https://api.github.com/repos/octocat/hello/issues?page=2>; rel="next", <https://api.github.com/repos/octocat/hello/issues?page=5>; rel="last"`,
			want:   "/repos/octocat/hello/issues?page=2"},
		{name: "link header without next rel", base: gh,
			header: `<https://api.github.com/repos/octocat/hello/issues?page=1>; rel="prev", <https://api.github.com/repos/octocat/hello/issues?page=5>; rel="last"`},
		{name: "empty link header", base: gh},
		{name: "multiple rels picks next", base: gh,
			header: `<https://api.github.com/repos/o/r?page=1>; rel="first", <https://api.github.com/repos/o/r?page=3>; rel="next", <https://api.github.com/repos/o/r?page=10>; rel="last"`,
			want:   "/repos/o/r?page=3"},
		// v0.29.12.
		{name: "GitLab continuation is made relative to the /api/v4 base", base: gl,
			header: `<https://gitlab.com/api/v4/projects/123/issues?page=2&per_page=100>; rel="next"`,
			want:   "/projects/123/issues?page=2&per_page=100"},
		{name: "encoded project path survives", base: gl,
			header: `<https://gitlab.com/api/v4/projects/group%2Fname/issues?page=2>; rel="next"`,
			want:   "/projects/group%2Fname/issues?page=2"},
		{name: "relative continuation resolves against the request", base: gh, request: gh + "/repos/o/r/issues?page=1",
			header: `<?page=2>; rel="next"`,
			want:   "/repos/o/r/issues?page=2"},
		{name: "another host", base: gh, header: `<https://evil.example/repos/o/r/issues?page=2>; rel="next"`, refused: true},
		{name: "opaque userinfo form (the reviewer's leak)", base: gh, request: gh + "/repos/o/r/issues", header: `<https:@evil.example/x?page=2>; rel="next"`, refused: true},
		{name: "opaque dot form", base: gh, request: gh + "/repos/o/r/issues", header: `<https:.evil.example/x>; rel="next"`, refused: true},
		{name: "scheme downgrade", base: gh, header: `<http://api.github.com/repos/o/r/issues?page=2>; rel="next"`, refused: true},
		{name: "same host outside the API base path", base: gl, header: `<https://gitlab.com/-/oauth/x?page=2>; rel="next"`, refused: true},
		{name: "relative continuation with no request to resolve against", base: gh, header: `</repos/o/r/issues?page=2>; rel="next"`, refused: true},
		// A malformed continuation is not an off-host one (review round 3).
		{name: "malformed continuation is an error, not an off-host refusal", base: gh, header: `<https://api.github.com/x%zz>; rel="next"`, malformed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			if tt.header != "" {
				resp.Header.Set("Link", tt.header)
			}
			if tt.request != "" {
				req, err := http.NewRequest(http.MethodGet, tt.request, nil)
				if err != nil {
					t.Fatal(err)
				}
				resp.Request = req
			}
			got, err := extractNextLink(resp, tt.base)
			if tt.malformed {
				if err == nil || errors.Is(err, ErrOffHostRefused) {
					t.Fatalf("extractNextLink() = (%q, %v), want a parse error that is not ErrOffHostRefused", got, err)
				}
				return
			}
			if tt.refused {
				if !errors.Is(err, ErrOffHostRefused) {
					t.Fatalf("extractNextLink() = (%q, %v), want ErrOffHostRefused", got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("extractNextLink() = (%q, %v), want (%q, nil)", got, err, tt.want)
			}
		})
	}
}

func TestSetQueryParam(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		key   string
		value string
		want  string
	}{
		{
			name:  "adds new param to path without query",
			path:  "/repos/o/r/issues",
			key:   "page",
			value: "2",
			want:  "/repos/o/r/issues?page=2",
		},
		{
			name:  "replaces existing param",
			path:  "/repos/o/r/issues?page=1&per_page=100",
			key:   "page",
			value: "3",
			want:  "/repos/o/r/issues?per_page=100&page=3",
		},
		{
			name:  "adds param to existing query string",
			path:  "/repos/o/r/issues?per_page=100",
			key:   "page",
			value: "5",
			want:  "/repos/o/r/issues?per_page=100&page=5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := setQueryParam(tt.path, tt.key, tt.value)
			if got != tt.want {
				t.Errorf("setQueryParam(%q, %q, %q) = %q, want %q", tt.path, tt.key, tt.value, got, tt.want)
			}
		})
	}
}

func TestTruncateBody_HTMLStripping(t *testing.T) {
	// Simulates the GitHub "Whoa there!" HTML response.
	input := `<html><head><title>Bad request</title></head><body><div class="c"><h1>Whoa there!</h1><p>You have sent an invalid request.</p></div></body></html>`
	got := truncateBody(input, 100)
	// Should have no HTML tags.
	if strings.Contains(got, "<") || strings.Contains(got, ">") {
		t.Errorf("truncateBody still contains HTML tags: %q", got)
	}
	if !strings.Contains(got, "Whoa there!") {
		t.Errorf("truncateBody lost content: %q", got)
	}
}

func TestTruncateBody_Truncation(t *testing.T) {
	input := strings.Repeat("a", 500)
	got := truncateBody(input, 50)
	if len(got) > 54 { // 50 + "..."
		t.Errorf("truncateBody length = %d, want <= 53", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("truncateBody should end with '...': %q", got)
	}
}

func TestTruncateBody_ShortStringUnchanged(t *testing.T) {
	input := "short error message"
	got := truncateBody(input, 100)
	if got != input {
		t.Errorf("truncateBody(%q) = %q, want unchanged", input, got)
	}
}

func TestTruncateBody_WhitespaceCollapsed(t *testing.T) {
	input := "line1\n\n\tline2   line3"
	got := truncateBody(input, 100)
	if got != "line1 line2 line3" {
		t.Errorf("truncateBody whitespace = %q, want %q", got, "line1 line2 line3")
	}
}

func TestEnsurePerPage(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "no query string",
			path: "/repos/o/r/issues",
			want: "/repos/o/r/issues?per_page=100",
		},
		{
			name: "existing query string without per_page",
			path: "/repos/o/r/issues?state=all",
			want: "/repos/o/r/issues?state=all&per_page=100",
		},
		{
			name: "already has per_page",
			path: "/repos/o/r/issues?per_page=50",
			want: "/repos/o/r/issues?per_page=50",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensurePerPage(tt.path)
			if got != tt.want {
				t.Errorf("ensurePerPage(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNextPageGitHub(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Link", `<https://api.github.com/repos/o/r/issues?page=3>; rel="next"`)
	got, err := nextPageGitHub(resp, "/repos/o/r/issues", "https://api.github.com")
	if err != nil || got != "/repos/o/r/issues?page=3" {
		t.Errorf("nextPageGitHub() = %q, want path with page=3", got)
	}
}

func TestNextPageGitLab_XNextPage(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Next-Page", "4")
	got, err := nextPageGitLab(resp, "/projects/123/issues?per_page=100", "https://gitlab.com/api/v4")
	if err != nil || !strings.Contains(got, "page=4") {
		t.Errorf("nextPageGitLab() = %q, want page=4", got)
	}
}

func TestNextPageGitLab_EmptyXNextPage(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Next-Page", "")
	got, err := nextPageGitLab(resp, "/projects/123/issues", "https://gitlab.com/api/v4")
	if err != nil || got != "" {
		t.Errorf("nextPageGitLab() with empty X-Next-Page = %q, want empty", got)
	}
}

func TestNextPageGitLab_ZeroPage(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("X-Next-Page", "0")
	got, err := nextPageGitLab(resp, "/projects/123/issues", "https://gitlab.com/api/v4")
	if err != nil || got != "" {
		t.Errorf("nextPageGitLab() with X-Next-Page=0 = %q, want empty", got)
	}
}

func TestNextPageGitLab_FallbackToLink(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Link", `<https://gitlab.com/api/v4/projects/123/issues?page=2>; rel="next"`)
	// v0.29.12: relative to the client's /api/v4 base. This test used to pin
	// "/api/v4/projects/123/issues?page=2" — the doubled path HTTPClient.Get
	// then requested as /api/v4/api/v4/….
	got, err := nextPageGitLab(resp, "/projects/123/issues", "https://gitlab.com/api/v4")
	if err != nil || got != "/projects/123/issues?page=2" {
		t.Errorf("nextPageGitLab() Link fallback = (%q, %v), want /projects/123/issues?page=2", got, err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{
			name:   "numeric value",
			header: "30",
			want:   30 * time.Second,
		},
		{
			name:   "empty returns default 60s",
			header: "",
			want:   60 * time.Second,
		},
		{
			name:   "non-numeric returns default 60s",
			header: "not-a-number",
			want:   60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			if tt.header != "" {
				resp.Header.Set("Retry-After", tt.header)
			}
			got := parseRetryAfter(resp)
			if got != tt.want {
				t.Errorf("parseRetryAfter() = %v, want %v", got, tt.want)
			}
		})
	}
}
