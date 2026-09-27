// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestLiveRootTreeAcceptsHEAD is the network canary paired with
// TestListRootManifestsPastTheContentsCap (worklist item 24; review round
// 1, L8): the Contents-cap fallback reads `git/trees/HEAD`, non-recursive,
// and maps blobs to files and trees to directories. GitHub's documentation
// says the trees endpoint takes "the SHA1 value or ref (branch or tag)
// name"; this asks the live API whether HEAD is such a ref, and whether a
// non-recursive read of a large repository's root is neither truncated nor
// the whole tree. It calls fetchRootTree directly, so it proves the
// endpoint's contract, not the cap trigger (the unit fixture pins that).
// Skipped unless AVELOXIS_TEST_NETWORK=1 and a token is set.
func TestLiveRootTreeAcceptsHEAD(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the root-tree canary")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := New("https://api.github.com", platform.NewKeyPool([]string{tok}, logger), logger)
	entries, err := client.fetchRootTree(t.Context(), "golang", "go")
	if err != nil {
		t.Fatalf("git/trees/HEAD on golang/go: %v", err)
	}
	var files, dirs int
	for _, e := range entries {
		switch e.Type {
		case "file":
			files++
		case "dir":
			dirs++
		}
		if strings.Contains(e.Path, "/") {
			// A non-recursive root read lists direct children only; a nested
			// path means GitHub read the tree recursively (review round 2:
			// Name == Path by construction, so that comparison could not
			// fire).
			t.Errorf("a root entry has a nested path %q — the tree was read recursively", e.Path)
		}
	}
	if files == 0 || dirs == 0 {
		t.Errorf("golang/go's root tree mapped to %d files and %d directories; want both (README.md, src/)", files, dirs)
	}
}
