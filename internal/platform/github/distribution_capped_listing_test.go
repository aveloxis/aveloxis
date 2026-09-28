// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestListRootManifestsPastTheContentsCap pins worklist item 24: the
// Contents API silently returns at most 1,000 entries for a directory
// (live: DefinitelyTyped/types), and the partial listing was stored as
// complete. A root listing at the cap is re-read through the Git Trees API
// (no such cap; a `truncated` flag at 100k), so a manifest past the 1,000th
// entry is still found; a truncated tree is a non-answer (the scan fails
// and the snapshot is kept), never a partial listing stored as complete.
func TestListRootManifestsPastTheContentsCap(t *testing.T) {
	var contents strings.Builder
	contents.WriteString("[")
	for i := 0; i < contentsCap; i++ {
		if i > 0 {
			contents.WriteString(",")
		}
		fmt.Fprintf(&contents, `{"type":"file","name":"a%04d.txt","path":"a%04d.txt"}`, i, i)
	}
	contents.WriteString("]")
	treesHit := 0
	handler := func(truncated bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/repos/x/y/contents" || r.URL.Path == "/repos/x/y/contents/":
				_, _ = w.Write([]byte(contents.String()))
			case r.URL.Path == "/repos/x/y/git/trees/HEAD" && r.URL.RawQuery == "":
				// Exactly HEAD, non-recursive (review round 1: a recursive
				// read is the mode GitHub truncates, and a whole-repository
				// payload per capped root; the branch is not assumed).
				treesHit++
				fmt.Fprintf(w, `{"sha":"t","truncated":%v,"tree":[{"path":"a0000.txt","type":"blob"},{"path":"zz-late","type":"tree"},{"path":"package.json","type":"blob"}]}`, truncated)
			case r.URL.Path == "/repos/x/y/contents/zz-late":
				// The first-level walk depends on tree → "dir": a monorepo
				// past the cap would otherwise lose every packages/*/manifest.
				_, _ = w.Write([]byte(`[{"type":"file","name":"go.mod","path":"zz-late/go.mod"}]`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}
	manifests, err := testGHClient(t, handler(false)).ListRootManifests(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("ListRootManifests: %v", err)
	}
	if treesHit != 1 {
		t.Errorf("the Git Trees API was read %d times; want 1 (the root listing was at the Contents cap)", treesHit)
	}
	found := map[string]bool{}
	for _, m := range manifests {
		found[m.ManifestPath] = true
	}
	if !found["package.json"] {
		t.Errorf("package.json past the 1,000th entry was not found: %+v", manifests)
	}
	if !found["zz-late/go.mod"] {
		t.Errorf("the manifest under the directory the tree listed was not found (tree entries must map to directories the walk descends): %+v", manifests)
	}
	// A truncated tree is a non-answer.
	_, err = testGHClient(t, handler(true)).ListRootManifests(context.Background(), "x", "y")
	if err == nil || platform.IsDefinitiveAnswer(err) || errors.Is(err, platform.ErrNotModified) {
		t.Errorf("a truncated root tree = %v; want a non-answer error (the scan fails and keeps its snapshot)", err)
	}
	// So is any DEFINITIVE answer from the tree read (review round 5): the
	// Contents API just listed 1,000 entries, so a 404 or 409 on the tree
	// cannot mean "no files" — read as such it completed the scan and wiped
	// the snapshot. A 304 (unsolicited: GetJSON is ETag-free) stays in the
	// loop as a regression guard on the common non-answer path (round 10:
	// the arm's own 304 clause no longer decided anything and was removed).
	for _, status := range []int{http.StatusNotFound, http.StatusConflict, http.StatusGone, http.StatusNotModified} {
		treesHit = 0
		gone := func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/x/y/contents" || r.URL.Path == "/repos/x/y/contents/" {
				_, _ = w.Write([]byte(contents.String()))
				return
			}
			w.WriteHeader(status)
		}
		manifests, err := testGHClient(t, http.HandlerFunc(gone)).ListRootManifests(context.Background(), "x", "y")
		if err == nil || platform.IsDefinitiveAnswer(err) || len(manifests) != 0 {
			t.Errorf("a %d on the root tree after a capped listing = (%d manifests, %v); want a non-answer error", status, len(manifests), err)
		}
	}
}

// TestListRepoPackagesWarnsOnAFullPage: one page of packagesPageSize per
// package type is read by decision (worklist item 24); a full page is
// logged as possibly incomplete for that owner and type, never silent
// (review round 6 of items 22–25).
func TestListRepoPackagesWarnsOnAFullPage(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, nil))
	full := make([]map[string]any, packagesPageSize)
	for i := range full {
		full[i] = map[string]any{"name": fmt.Sprintf("pkg%d", i), "package_type": "npm", "version_count": 1, "repository": map[string]any{"name": "other"}}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "package_type=npm") {
			_ = json.NewEncoder(w).Encode(full)
			return
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	c := &Client{http: platform.NewHTTPClient(server.URL, keys, logger, platform.AuthGitHub), logger: logger}
	if _, err := c.ListRepoPackages(context.Background(), "o", "r"); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(log.String(), "packages listing may be incomplete"); n != 1 {
		t.Errorf("a full npm page logged the incomplete WARN %d times; want exactly 1 (the other types' empty pages log nothing):\n%s", n, log.String())
	}
	if !strings.Contains(log.String(), "package_type=npm") || !strings.Contains(log.String(), "owner=o") {
		t.Errorf("the WARN must name the owner and package type:\n%s", log.String())
	}
}

// TestUnsolicited304IsANonAnswerForEveryReader (review round 8 of items
// 22–25): the readers never solicit a 304 (GetJSON is ETag-free), so one
// that arrives is a misbehaving intermediary's and says nothing about the
// repository. Through the round-7 code emptyAnswer read it as "nothing
// here" — the snapshot-wipe the ETag bypass exists to prevent. Every reader
// now returns it as a non-definitive error the scanner strikes on.
func TestUnsolicited304IsANonAnswerForEveryReader(t *testing.T) {
	c := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) }))
	ctx := context.Background()
	check := func(what string, n int, err error) {
		t.Helper()
		if err == nil || platform.IsDefinitiveAnswer(err) || !errors.Is(err, platform.ErrNotModified) || n != 0 {
			t.Errorf("%s on an unsolicited 304 = (%d rows, %v); want zero rows and a non-definitive ErrNotModified", what, n, err)
		}
	}
	rows, err := c.ListReleaseAssetExtensions(ctx, "o", "r")
	check("release assets", len(rows), err)
	pkgs, err := c.ListRepoPackages(ctx, "o", "r")
	check("packages", len(pkgs), err)
	manifests, err := c.ListRootManifests(ctx, "o", "r")
	check("root manifests", len(manifests), err)
	content, err := c.FetchManifestContent(ctx, "o", "r", "package.json")
	check("manifest content", len(content), err)
}

// TestUnsolicited304IsANonAnswerAtTheFallbackSites (round 9): the all-304
// fixture above never reaches two of the six emptyAnswer sites — the org
// packages fallback (taken only after a user 404) and the first-level
// directory read (reached only after the root listed a directory). Each is
// pinned on its own path so a local widening there cannot drift back.
func TestUnsolicited304IsANonAnswerAtTheFallbackSites(t *testing.T) {
	ctx := context.Background()
	check := func(what string, n int, err error) {
		t.Helper()
		if err == nil || platform.IsDefinitiveAnswer(err) || !errors.Is(err, platform.ErrNotModified) || n != 0 {
			t.Errorf("%s = (%d rows, %v); want zero rows and a non-definitive ErrNotModified", what, n, err)
		}
	}
	// user 404 → the org endpoint answers 304.
	orgC := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/users/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	pkgs, err := orgC.ListRepoPackages(ctx, "o", "r")
	check("org packages fallback on an unsolicited 304", len(pkgs), err)
	// the root lists one directory → the directory read answers 304.
	dirC := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/o/r/contents" || r.URL.Path == "/repos/o/r/contents/" {
			_, _ = w.Write([]byte(`[{"name":"sub","path":"sub","type":"dir"}]`))
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	manifests, err := dirC.ListRootManifests(ctx, "o", "r")
	check("first-level directory on an unsolicited 304", len(manifests), err)
}
