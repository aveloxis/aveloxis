// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/manifest"
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
	// less of it (0.29.215). A pnpm-lock.yaml costs 46 times its length and
	// a yarn.lock 14, so the same heap leaves the yarn.lock a little over
	// three times the length.
	yarnRoom, pnpmRoom := cloneReadRoom("yarn.lock", 1<<10), cloneReadRoom("pnpm-lock.yaml", 1<<10)
	if pnpmRoom <= 0 || yarnRoom < 32*pnpmRoom/10 || yarnRoom > 34*pnpmRoom/10 {
		t.Errorf("room for a yarn.lock %d, for a pnpm-lock.yaml %d; want 46/14 times", yarnRoom, pnpmRoom)
	}
	// The factor: what was measured, half again, rounded UP to a whole
	// number; a kind with no measurement is priced as the costliest of its
	// class.
	for path, want := range map[string]int64{
		"a/b/pnpm-lock.yaml": 46, // 30.41 measured
		"yarn.lock":          14, // 9.14
		"Package.resolved":   6,  // 3.40: 5.1 rounds up
		"requirements.txt":   100,
		"pom.xml":            15,
		"setup.py":           100, // no measurement: the costliest manifest's
		"x":                  100,
	} {
		if got := readCostFactor(path); got != want {
			t.Errorf("readCostFactor(%s) = %d, want %d", path, got, want)
		}
		if got := readCost(path, 100); got != 100*want {
			t.Errorf("readCost(%s, 100) = %d, want %d", path, got, 100*want)
		}
	}
	if readCost("x", 1<<62) != 1<<62 {
		t.Error("the cost of a length past what can be multiplied is not held at the top")
	}
	if readCostMarginNum != 3 || readCostMarginDen != 2 {
		t.Errorf("the margin is %d/%d; the factors above are for three halves", readCostMarginNum, readCostMarginDen)
	}
	cloneReadHeapLimit.Store(1) // far under the heap: no room
	if got := cloneReadRoom("requirements.txt", 1); got > 0 {
		t.Errorf("with the limit under the heap: %d; want none", got)
	}
}

// Every lockfile kind the roster reads has a measured cost of its own: a new
// kind is measured when it is added, not priced as the costliest of the
// others by default (and a measurement names a kind that exists).
func TestEveryLockfileKindHasAMeasuredCost(t *testing.T) {
	examined := 0
	for kind, spec := range lockfileKinds {
		if spec.binaryOnly {
			continue // never read: only its presence is recorded
		}
		examined++
		if readCostMeasured[kind] <= 100 {
			t.Errorf("%s has no measured read cost (readCostMeasured): measure its shapes through the child and add it", kind)
		}
	}
	if examined < 18 {
		t.Fatalf("%d lockfile kinds examined: the roster has more", examined)
	}
	for kind, cost := range readCostMeasured {
		if cost <= 100 {
			t.Errorf("%s is measured at %d hundredths: reading a file costs at least its length", kind, cost)
		}
	}
}

// The configuration page's cost tables are the code's table (review round
// 10: nothing held them together): every measured kind has a row, and the
// row's measured cost, factor and largest file at the default limit
// (config.DefaultManifestReadMemoryMB, read here and not copied) are
// what readCostMeasured and readCostFactor give.
func TestTheConfigurationPagesCostTableIsTheCodes(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "getting-started", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	// A Maven or Gradle build file is never read past the build-file
	// limit, whatever its factor allows: its cell says so, whole.
	buildFiles := map[string]bool{"build.gradle": true, "pom.xml": true}
	seen := map[string]int{}
	for _, line := range strings.Split(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) != 6 || !strings.HasPrefix(strings.TrimSpace(cells[1]), "`") {
			continue
		}
		kind := strings.Trim(strings.TrimSpace(cells[1]), "`")
		measured, ok := readCostMeasured[kind]
		if !ok {
			continue
		}
		seen[kind]++
		factor := readCostFactor(kind)
		largest := fmt.Sprintf("%.1f MiB", float64(config.DefaultManifestReadMemoryMB)/float64(factor))
		if buildFiles[kind] {
			largest = fmt.Sprintf("%d MiB (the build-file limit; %s by the factor)", manifest.MaxBuildFileBytes>>20, largest)
		}
		want := []string{fmt.Sprintf("%.2f", float64(measured)/100), fmt.Sprint(factor), largest}
		for i, w := range want {
			if got := strings.TrimSpace(cells[2+i]); got != w {
				t.Errorf("configuration.md, row %s, column %d: %q, the code gives %q", kind, 2+i, got, w)
			}
		}
	}
	if head := fmt.Sprintf("Largest file read at %d MiB", config.DefaultManifestReadMemoryMB); strings.Count(string(doc), head) != 2 {
		t.Errorf("configuration.md's two cost tables are not headed %q", head)
	}
	// Each kind once (review round 11: a count of rows let a row written
	// twice stand in for a kind with none).
	for kind := range readCostMeasured {
		if seen[kind] != 1 {
			t.Errorf("configuration.md has %d rows for %s; want exactly one", seen[kind], kind)
		}
	}
	for kind := range buildFiles {
		if _, measured := readCostMeasured[kind]; !measured {
			t.Errorf("%s is named here as a build file and has no measured cost", kind)
		}
	}
}
