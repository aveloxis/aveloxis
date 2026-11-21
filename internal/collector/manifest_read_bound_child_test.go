// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A reader that opens a file OTHER than the one the walk handed it does so
// through the manifest package, which the walk's own size check does not
// see: a crate's Cargo.toml reads its workspace root's. In the child every
// such read is bound by the room its memory limit leaves (0.29.214;
// manifest.SetReadBound), so a workspace root far larger than the limit is
// refused by its length and the crate is read without it. Before, the root
// was read whole and the child ended at its memory limit, which cost the
// repository its whole analysis.
func TestTheChildBoundsAFileAReaderOpensForItself(t *testing.T) {
	dir := writeClone(t, map[string]string{
		"crates/member/Cargo.toml": "[package]\nname = \"member\"\nversion = \"0.1.0\"\n\n[dependencies]\nserde = \"1.0.200\"\nanyhow = { workspace = true }\n",
	})
	// The workspace root: 300 MiB by its length (sparse on disk; a few
	// bytes in a git pack), which is not a manifest the walk reads here
	// because the walk's own check refuses it first.
	root := filepath.Join(dir, "Cargo.toml")
	if err := os.WriteFile(root, []byte("[workspace]\nmembers = [\"crates/*\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(root, 300<<20); err != nil {
		t.Fatal(err)
	}
	limits := ManifestLimits{CPU: testLimits().CPU, HeapBytes: 64 << 20}
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir}, limits, &mapPomSource{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("the child did not finish: %v", err)
	}
	if peak := run.heapPeak; peak > 64<<20 {
		t.Errorf("the child's heap peaked at %d MiB under a 64 MiB limit", peak>>20)
	}
	got := map[string]string{}
	for _, d := range run.decl.Deps {
		if d.ManifestPath == "crates/member/Cargo.toml" {
			got[d.Name] = d.Version
		}
	}
	// The member is read; what it inherits from the root it could not read
	// has no version.
	if got["serde"] != "1.0.200" {
		t.Errorf("the member's own dependency: %v", got)
	}
	if v, ok := got["anyhow"]; !ok || v != "" {
		t.Errorf("the dependency inherited from the unread workspace root: %q (present %v); want it listed with no version", v, ok)
	}
	failed := false
	for _, f := range run.decl.Failed {
		failed = failed || f.Path == "Cargo.toml"
	}
	if !failed {
		t.Errorf("the oversized root is not recorded as a manifest that was not read: %+v", run.decl.Failed)
	}

	// The wiring: the child installs the bound where it installs its limit,
	// and removes it (a test may run the child's code in its own process).
	code := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ManifestChildMain("))
	if !strings.Contains(code, "manifest.SetReadBound(cloneReadRoom)") {
		t.Error("ManifestChildMain does not install the manifest package's read bound")
	}
}

// cloneReadRoom is the room the limit leaves, and never a value that reads
// as "no bound".
func TestCloneReadRoomIsWhatTheLimitLeaves(t *testing.T) {
	t.Cleanup(func() { cloneReadHeapLimit.Store(0) })
	cloneReadHeapLimit.Store(0)
	if got := cloneReadRoom("requirements.txt", -1); got != 0 {
		t.Errorf("with no limit: %d; want 0 (the child always has one)", got)
	}
	heap := processHeapBytes()
	cloneReadHeapLimit.Store(heap + 64<<20)
	if got := cloneReadRoom("requirements.txt", 1<<20); got <= 0 || got > 64<<20+heap {
		t.Errorf("with 64 MiB above the heap: %d", got)
	}
	// The room is in bytes of THE FILE: a kind that costs more to read has
	// less of it (0.29.215). A pnpm-lock.yaml costs fourteen times its
	// length and a manifest three, so the same heap leaves the manifest
	// between four and five times the length.
	manifestRoom, pnpmRoom := cloneReadRoom("requirements.txt", 1<<10), cloneReadRoom("pnpm-lock.yaml", 1<<10)
	if pnpmRoom <= 0 || manifestRoom < 4*pnpmRoom || manifestRoom > 5*pnpmRoom {
		t.Errorf("room for a manifest %d, for a pnpm-lock.yaml %d; want between four and five times", manifestRoom, pnpmRoom)
	}
	if readCost("a/b/pnpm-lock.yaml", 100) != 1400 || readCost("yarn.lock", 100) != 300 || readCost("requirements.txt", 100) != 300 ||
		readCost("uv.lock", 100) != 800 || readCost("x", 1<<62) != 1<<62 {
		t.Errorf("readCost: pnpm %d, yarn %d, a manifest %d, an unmeasured lockfile %d", readCost("pnpm-lock.yaml", 100), readCost("yarn.lock", 100), readCost("requirements.txt", 100), readCost("uv.lock", 100))
	}
	cloneReadHeapLimit.Store(1) // far under the heap: no room
	if got := cloneReadRoom("requirements.txt", 1); got > 0 {
		t.Errorf("with the limit under the heap: %d; want none", got)
	}
}
