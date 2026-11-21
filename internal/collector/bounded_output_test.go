// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// boundedLines takes a subprocess's output a line at a time and stops the
// subprocess past its bound: a subprocess that never stops writing is
// ended, not waited for.
func TestBoundedLinesStopsASubprocessPastItsBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Under the bound: every line, in order, and the exit status.
	var got []string
	_, err := boundedLines(exec.CommandContext(ctx, "sh", "-c", "printf 'a\\nb\\nc\\n'"), 1<<20, func(l string) error {
		got = append(got, l)
		return nil
	})
	if err != nil || strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("lines %v, err %v", got, err)
	}

	// `yes` writes for ever. Past the byte bound it is stopped.
	n := 0
	began := time.Now()
	_, err = boundedLines(exec.CommandContext(ctx, "yes"), 64<<10, func(string) error { n++; return nil })
	if !errors.Is(err, errOutputPastBound) {
		t.Fatalf("an endless writer: err = %v, want errOutputPastBound", err)
	}
	if n == 0 || n > 64<<10 || time.Since(began) > 20*time.Second {
		t.Errorf("an endless writer: %d lines taken in %s", n, time.Since(began))
	}

	// The caller's own refusal stops it too, and is what is returned.
	mine := errors.New("enough")
	n = 0
	_, err = boundedLines(exec.CommandContext(ctx, "yes"), 1<<30, func(string) error {
		if n++; n == 100 {
			return mine
		}
		return nil
	})
	if !errors.Is(err, mine) || n != 100 {
		t.Errorf("the caller's refusal: err %v after %d lines", err, n)
	}

	// One line longer than any line taken.
	_, err = boundedLines(exec.CommandContext(ctx, "sh", "-c", "head -c 2000000 /dev/zero | tr '\\0' 'x'"), 1<<30, func(string) error { return nil })
	if !errors.Is(err, errOutputPastBound) {
		t.Errorf("a 2 MB line: err = %v, want errOutputPastBound", err)
	}

	// A failing subprocess: its exit error, and the head of its stderr.
	stderr, err := boundedLines(exec.CommandContext(ctx, "sh", "-c", "echo out; echo why >&2; exit 3"), 1<<20, func(string) error { return nil })
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 || stderr != "why" {
		t.Errorf("a failing subprocess: err %v, stderr %q", err, stderr)
	}
}

func TestBoundedCaptureAndCappedOutputHoldABoundedAmount(t *testing.T) {
	c := newBoundedCapture()
	chunk := []byte(strings.Repeat("x", 4096) + "\n")
	fmt.Fprint(c, "HEAD\n")
	for i := 0; i < 5000; i++ { // 20 MB
		if n, err := c.Write(chunk); n != len(chunk) || err != nil {
			t.Fatal(n, err)
		}
	}
	fmt.Fprint(c, "TAIL")
	s := c.String()
	if len(s) > 2*boundedCaptureBytes+200 || !strings.HasPrefix(s, "HEAD") || !strings.HasSuffix(s, "TAIL") {
		t.Errorf("kept %d bytes; head %q tail %q", len(s), s[:8], s[len(s)-8:])
	}

	o := &cappedOutput{max: 10}
	o.Write([]byte("12345"))
	if o.Over() || o.String() != "12345" {
		t.Errorf("under the cap: over %v, %q", o.Over(), o.String())
	}
	if n, _ := o.Write([]byte("67890abc")); n != 8 || !o.Over() || o.Len() != 10 {
		t.Errorf("past the cap: wrote %d, over %v, kept %d", n, o.Over(), o.Len())
	}
}

// Review round 8 (B8-1): two go.mod files, each naming the same N modules,
// every one replaced by the one local directory, make `go mod graph` print
// N*N edges with no network. The worker took all of it (228 KB became 2.75
// GB). The expansion is now abandoned at goModGraphLines as INCOMPLETE,
// with nothing of it kept.
func TestAGoModuleGraphLargerThanIsTakenIsAbandonedAsIncomplete(t *testing.T) {
	const n = 800 // 800 * 800 edges is past goModGraphLines
	if n*n <= goModGraphLines {
		t.Fatalf("the fixture's %d edges do not pass the bound %d", n*n, goModGraphLines)
	}
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOFLAGS", "-mod=mod")
	dir := t.TempDir()
	var req, rep strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&req, "\tx.example/m%d v1.0.0\n", i)
		fmt.Fprintf(&rep, "\tx.example/m%d v1.0.0 => ./testdata/m\n", i)
	}
	// Under testdata, which module discovery skips: one module to expand.
	if err := os.MkdirAll(filepath.Join(dir, "testdata", "m"), 0o755); err != nil {
		t.Fatal(err)
	}
	main := "module x.example/main\n\ngo 1.21\n\nrequire (\n" + req.String() + ")\n\nreplace (\n" + rep.String() + ")\n"
	sub := "module x.example/m\n\ngo 1.21\n\nrequire (\n" + req.String() + ")\n"
	for path, text := range map[string]string{"go.mod": main, "testdata/m/go.mod": sub} {
		if err := os.WriteFile(filepath.Join(dir, path), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var log strings.Builder
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(&log, nil)), TransitiveLockfiles: true}
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	pkgs, edges, complete := ac.scanGoModGraph(context.Background(), 1, dir, map[string]bool{})
	grew := int64(heap()) - int64(before)
	if complete || pkgs != nil || edges != nil {
		t.Errorf("complete %v, %d packages, %d edges; want an incomplete expansion with nothing kept", complete, len(pkgs), len(edges))
	}
	if !strings.Contains(log.String(), "larger than is taken for one repository") {
		t.Errorf("the log does not say why:\n%s", log.String())
	}
	if grew > 8<<20 {
		t.Errorf("the worker's live heap grew %d MiB over an abandoned expansion", grew>>20)
	}

	// The toolchain is STOPPED at the bound, not read to its end and then
	// discarded: one module's expansion takes one line past the bound and
	// holds no more edges than lines it took.
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	_, gotEdges, ok := ac.goModGraphOne(context.Background(), 1, goBin, dir, dir, map[string]bool{}, nil, nil, &lines)
	if ok || lines != goModGraphLines+1 || len(gotEdges) > goModGraphLines {
		t.Errorf("one module: ok %v, %d lines taken (the bound is %d), %d edges held", ok, lines, goModGraphLines, len(gotEdges))
	}
}

// An honest module is expanded as before: this module's own graph.
func TestAnHonestGoModuleGraphIsStillTakenWhole(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module x.example/solo\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOPROXY", "off")
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), TransitiveLockfiles: true}
	pkgs, edges, complete := ac.scanGoModGraph(context.Background(), 1, dir, map[string]bool{})
	if !complete || len(pkgs) != 0 {
		t.Errorf("a module with no requirement: complete %v, %d packages, %d edges", complete, len(pkgs), len(edges))
	}
}

// A tool's output file is read only when it is within what is taken, into
// one buffer of its size.
func TestAToolsOutputFileIsReadWithinItsBound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readToolOutputWithin(path, 100); err != nil || len(got) != 100 {
		t.Errorf("a file of exactly the bound: %d bytes, %v", len(got), err)
	}
	if got, err := readToolOutputWithin(path, 99); !errors.Is(err, errOutputPastBound) || got != nil {
		t.Errorf("a file one byte past the bound: %d bytes, %v; want it refused unread", len(got), err)
	}
	if _, err := readToolOutputWithin(dir, 1<<20); err == nil {
		t.Error("a directory was read as a tool's output")
	}
	if _, err := readToolOutputWithin(filepath.Join(dir, "absent"), 1<<20); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a file that is not there: %v", err)
	}
	// The scancode report is read through it, at its own bound.
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func decodeScancodeReport("))
	if !strings.Contains(body, "readToolOutputWithin(outputPath, scancodeOutputMaxBytes)") {
		t.Error("the scancode report is not read within scancodeOutputMaxBytes")
	}
}

// The worker's own walk of a clone: every entry once, SkipDir honoured, and
// it ENDS, as a failure, at its bound on entries, at its bound on depth, and
// at the first directory it cannot read (a walk that passed over what it
// could not read would report a part as the whole).
func TestTheWorkersWalkOfACloneIsBounded(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{"a/one", "a/two", "skip/hidden", "b/c/three"} {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	walk := func(max int) ([]string, error) {
		var seen []string
		err := walkBounded(dir, max, func(path string, entry fs.DirEntry) error {
			rel, _ := filepath.Rel(dir, path)
			seen = append(seen, filepath.ToSlash(rel))
			if entry.IsDir() && entry.Name() == "skip" {
				return filepath.SkipDir
			}
			return nil
		})
		sort.Strings(seen)
		return seen, err
	}
	// Seven entries: a, a/one, a/two, b, b/c, b/c/three, skip.
	want := []string{"a", "a/one", "a/two", "b", "b/c", "b/c/three", "skip"}
	if seen, err := walk(7); err != nil || !reflect.DeepEqual(seen, want) {
		t.Errorf("walked %v (%v); want %v", seen, err, want)
	}
	if _, err := walk(6); !errors.Is(err, errWalkPastBound) {
		t.Errorf("a walk of seven entries bounded at six: %v", err)
	}
	stop := errors.New("stop")
	if err := walkBounded(dir, 100, func(string, fs.DirEntry) error { return stop }); !errors.Is(err, stop) {
		t.Errorf("a visit's error: %v; want it to end the walk", err)
	}

	deep := t.TempDir()
	if err := os.MkdirAll(filepath.Join(deep, strings.Repeat("d/", walkDepth+1)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := walkBounded(deep, 1<<20, func(string, fs.DirEntry) error { return nil }); !errors.Is(err, errWalkPastBound) {
		t.Errorf("directories nested %d deep: %v; want the walk ended at its depth", walkDepth+1, err)
	}
	atDepth := t.TempDir()
	if err := os.MkdirAll(filepath.Join(atDepth, strings.Repeat("d/", walkDepth)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := walkBounded(atDepth, 1<<20, func(string, fs.DirEntry) error { return nil }); err != nil {
		t.Errorf("directories nested exactly %d deep: %v", walkDepth, err)
	}

	if os.Geteuid() != 0 {
		locked := filepath.Join(dir, "b", "c")
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
		if _, err := walk(100); err == nil {
			t.Error("a directory that cannot be read was passed over")
		}
	}
}

// The scorecard's report is held up to its bound, and a report past it is a
// failed run, not a truncated document handed to the decoder.
func TestAScorecardReportPastItsBoundIsAFailure(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func invokeScorecard("))
	for _, needle := range []string{"&cappedOutput{max: scorecardOutputMaxBytes}", "if stdout.Over() {", "errOutputPastBound"} {
		if !strings.Contains(body, needle) {
			t.Errorf("the scorecard run does not hold %q", needle)
		}
	}
	out := &cappedOutput{max: 4}
	if n, err := out.Write([]byte("abcd")); n != 4 || err != nil || out.Over() {
		t.Errorf("four bytes into a bound of four: %d, %v, over %v", n, err, out.Over())
	}
	if n, err := out.Write([]byte("e")); n != 1 || err != nil || !out.Over() || out.Len() != 4 {
		t.Errorf("a fifth byte: wrote %d, %v, over %v, holds %d; want it taken as written, dropped, and marked", n, err, out.Over(), out.Len())
	}
}
