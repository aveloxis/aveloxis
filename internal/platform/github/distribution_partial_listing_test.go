// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestListRootManifestsSubdirFailureIsAnError (v0.29.55 review round 3, the
// class sweep of "a non-answer must not be recorded as an answer"): a
// first-level directory listing that failed for any reason — a body that
// would not decode, retries exhausted, an empty key pool — was skipped with a
// bare `continue` and the partial list returned with a nil error, which the
// scanner stored as the repo's complete manifest snapshot (probe: root lists
// package.json + packages/, packages/ is a cut-off 200 -> [package.json]
// err=nil). A not-found directory is an answer and is still skipped.
func TestListRootManifestsSubdirFailureIsAnError(t *testing.T) {
	root := `[{"type":"file","name":"package.json","path":"package.json"},{"type":"dir","name":"packages","path":"packages"}]`
	for name, tc := range map[string]struct {
		sub     func(w http.ResponseWriter)
		wantErr bool
	}{
		"cut-off body": {func(w http.ResponseWriter) { _, _ = w.Write([]byte(`[{"type":"fi`)) }, true},
		"not found":    {func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) }, false},
	} {
		t.Run(name, func(t *testing.T) {
			c := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/repos/o/r/contents/packages" {
					tc.sub(w)
					return
				}
				_, _ = w.Write([]byte(root))
			}))
			got, err := c.ListRootManifests(context.Background(), "o", "r")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %v, nil — a directory listing that failed without an answer was dropped silently", got)
				}
				if platform.IsDefinitiveAnswer(err) {
					t.Fatalf("err %v is a definitive answer", err)
				}
				return
			}
			if err != nil || len(got) != 1 {
				t.Fatalf("got %v, %v — a 404 directory is an answer: skip it and keep the rest", got, err)
			}
		})
	}
}

// TestFetchManifestContentBadEncodingIsAnError: content that does not
// base64-decode is not "no content"; returning "" stored the manifest with
// its declared package name blanked.
func TestFetchManifestContentBadEncodingIsAnError(t *testing.T) {
	c := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"encoding":"base64","content":"!!not base64!!"}`))
	}))
	if content, err := c.FetchManifestContent(context.Background(), "o", "r", "package.json"); err == nil {
		t.Fatalf("got %q, nil — undecodable content was reported as no content", content)
	}
}
