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
		// Copilot review 5237013602 on PR #209: the client's own off-host
		// refusal is not an answer about the directory; skipping it as one
		// stored a partial list.
		"off-host redirect": {func(w http.ResponseWriter) {
			w.Header().Set("Location", "https://elsewhere.example/repos/o/r/contents/packages")
			w.WriteHeader(http.StatusMovedPermanently)
		}, true},
		// Review round 6: a rejected request is an answer too
		// (platform.IsDefinitiveAnswer). Returning it as an error threw
		// the whole list away while the scanner — which counts it as an
		// answer — still stored the scan as complete, wiping the root
		// package.json that was read fine.
		"rejected (422)": {func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"message":"Validation Failed"}`))
		}, false},
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
				t.Fatalf("got %v, %v — an answer about the directory (404, 422): skip it and keep the rest", got, err)
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

// TestContentsPathsAreEscaped (v0.29.55 review rounds 5 and 6): file and
// directory names come from the repository tree and may contain %, #, ? or
// spaces. The Contents API paths were built unescaped: "100%" failed to parse
// as a URL (no request sent — a non-answer, so since v0.29.55 a scan failure
// and a strike every cycle); "C#" and "a?b" requested "C" and "a" (usually a
// 404 that silently skipped the directory's manifests, or a scan failure when
// the object at "C" did not decode); "a?b c" drew a 400. Every segment must
// be escaped.
func TestContentsPathsAreEscaped(t *testing.T) {
	c := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path { // the decoded path: what GitHub sees
		case "/repos/o/r/contents":
			_, _ = w.Write([]byte(`[{"type":"dir","name":"100%","path":"100%"},{"type":"dir","name":"C#","path":"C#"},{"type":"dir","name":"a?b c","path":"a?b c"},{"type":"file","name":"C","path":"C"}]`))
		case "/repos/o/r/contents/100%":
			_, _ = w.Write([]byte(`[{"type":"file","name":"package.json","path":"100%/package.json"}]`))
		case "/repos/o/r/contents/C#":
			_, _ = w.Write([]byte(`[{"type":"file","name":"Cargo.toml","path":"C#/Cargo.toml"}]`))
		case "/repos/o/r/contents/a?b c":
			_, _ = w.Write([]byte(`[{"type":"file","name":"setup.py","path":"a?b c/setup.py"}]`))
		case "/repos/o/r/contents/My%Lib/package.json":
			_, _ = w.Write([]byte(`{"encoding":"base64","content":"eyJuYW1lIjoieCJ9"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	got, err := c.ListRootManifests(context.Background(), "o", "r")
	if err != nil || len(got) != 3 {
		t.Fatalf("ListRootManifests = %v, %v — want the three manifests inside 100%%, C# and \"a?b c\"", got, err)
	}
	content, err := c.FetchManifestContent(context.Background(), "o", "r", "My%Lib/package.json")
	if err != nil || content != `{"name":"x"}` {
		t.Fatalf("FetchManifestContent = %q, %v — want the file under My%%Lib", content, err)
	}
}
