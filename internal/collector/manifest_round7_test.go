// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 7 of the child that reads a clone (0.29.214).

// --- the Go test-only classifier reads a bounded prefix ----------------

func TestGoImportsAreReadFromABoundedPrefix(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, c := range map[string]struct {
		src  string
		want string
		cut  bool
	}{
		"forms":       {"// c\npackage x\n\nimport \"a/b\"\nimport n \"c\"\nimport . \"d\"\nimport _ \"e\"\nimport (\n\t\"f\"\n\tg \"h/i\"\n\t`raw`\n)\n\nfunc F() {}\nimport \"late\"\n", "a/b c d e f h/i raw", false},
		"none":        {"package x\n\nvar V = 1\n", "", false},
		"only":        {"package x\nimport \"a\"\n", "a", false},
		"not-go":      {"this is not go\n", "", false},
		"unfinished":  {"package x\nimport (\n\t\"a\"\n", "a", false}, // the whole file: nothing more could follow
		"long-header": {"/*" + strings.Repeat("x", goImportPrefixBytes) + "*/\npackage x\nimport \"a\"\n", "", true},
		"long-tail":   {"package x\nimport \"a\"\nvar V = `" + strings.Repeat("x", 2*goImportPrefixBytes) + "`\n", "a", false},
		"cut-between": {"package x\n" + strings.Repeat("import \"a\"\n", goImportPrefixBytes/11+10), "", true},
		// The prefix ends three letters into the word "import".
		"cut-mid-word": {"package x\n//" + strings.Repeat("p", goImportPrefixBytes-16) + "\nimport \"b\"\n", "", true},
		"cut-in-list":  {"package x\nimport (\n" + strings.Repeat("\t\"a\"\n", goImportPrefixBytes/5) + ")\n", "", true},
	} {
		got, cut := goImports(write(name+".go", c.src))
		if name == "cut-in-list" || name == "cut-between" {
			got = nil // some thousands of "a": only that it was cut matters
		}
		if strings.Join(got, " ") != c.want || cut != c.cut {
			t.Errorf("%s: imports %q, cut short %v; want %q, %v", name, got, cut, c.want, c.cut)
		}
	}
	// The read itself stops at the prefix whatever the file's size.
	big := write("big.go", "package x\n"+strings.Repeat("// pad\n", goImportPrefixBytes))
	data, whole, err := readCloneFilePrefix(big, goImportPrefixBytes)
	if err != nil || whole || len(data) != goImportPrefixBytes {
		t.Errorf("a file of %d bytes was read to %d (whole %v, %v); want %d and not whole", 7*goImportPrefixBytes, len(data), whole, err, goImportPrefixBytes)
	}
}

// A module imported only from tests is test scope; one whose only non-test
// importer could not be read to the end of its imports is NOT relabelled,
// and nor is anything else: that file may import any of them.
func TestTheGoClassifierDoesNotGuessPastAFileItCouldNotRead(t *testing.T) {
	deps := func() []model.LibyearDep {
		return []model.LibyearDep{
			{Name: "example.com/testonly", Manager: "go", Type: "runtime"},
			{Name: "example.com/used", Manager: "go", Type: "runtime"},
			{Name: "example.com/used/v2", Manager: "go", Type: "runtime"},
			{Name: "example.com/nowhere", Manager: "go", Type: "runtime"},
		}
	}
	dir := writeClone(t, map[string]string{
		"a.go":      "package a\nimport \"example.com/used/pkg\"\n",
		"a_test.go": "package a\nimport (\n\t\"example.com/testonly/assert\"\n\t\"example.com/used/v2/x\"\n)\n",
	})
	got := classifyGoModTestOnlyDeps(dir, deps())
	if got[0].Type != model.ScopeTest || got[1].Type != "runtime" || got[2].Type != model.ScopeTest || got[3].Type != "runtime" {
		t.Fatalf("scopes %q %q %q %q; want test, runtime, test (the /v2 module is its own), runtime", got[0].Type, got[1].Type, got[2].Type, got[3].Type)
	}
	// A non-test file whose imports are past the prefix.
	if err := os.WriteFile(filepath.Join(dir, "huge.go"), []byte("/*"+strings.Repeat("x", goImportPrefixBytes)+"*/\npackage a\nimport \"example.com/testonly/assert\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i, d := range classifyGoModTestOnlyDeps(dir, deps()) {
		if d.Type != "runtime" {
			t.Errorf("dependency %d was relabelled %q beside a file whose imports could not be read", i, d.Type)
		}
	}
}

// Modules times imports is not a product: 40,000 of each took 21 s when
// every module was compared with every import (review round 7).
func TestTheGoClassifierLooksEachImportUpOnce(t *testing.T) {
	// Four test files of 10,000 imports each: one file's import block is
	// inside the prefix that is read.
	const n, files = 40000, 4
	srcs := map[string]string{}
	deps := make([]model.LibyearDep, n)
	for f := 0; f < files; f++ {
		var src strings.Builder
		src.WriteString("package a\nimport (\n")
		for i := f * n / files; i < (f+1)*n/files; i++ {
			deps[i] = model.LibyearDep{Name: fmt.Sprintf("example.com/m%d", i), Manager: "go", Type: "runtime"}
			fmt.Fprintf(&src, "\t\"example.com/m%d/pkg/sub\"\n", i)
		}
		src.WriteString(")\n")
		srcs[fmt.Sprintf("a%d_test.go", f)] = src.String()
	}
	dir := writeClone(t, srcs)
	began := time.Now()
	got := classifyGoModTestOnlyDeps(dir, deps)
	took := time.Since(began)
	for i, d := range got {
		if d.Type != model.ScopeTest {
			t.Fatalf("module %d is %q; every one is imported from the test file alone", i, d.Type)
		}
	}
	// A product of 40,000 by 40,000 is seconds on any machine; a lookup
	// each is milliseconds. The bound is generous to a loaded host.
	if took > 3*time.Second {
		t.Errorf("%d modules and %d imports took %s", n, n, took)
	}
}

// --- a file is not read when its size alone passes the limit -----------

// sparse makes a file of size bytes that occupies none.
func sparse(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// A manifest and a lockfile far larger than the memory limit are each ONE
// file that could not be read; the rest of the clone is read, and the child
// never holds them (review round 7: a 6 GiB file took the child to 2 GiB at
// a 256 MiB limit, and the analysis was lost).
func TestAFileLargerThanTheMemoryLeftIsOneFailedReadAndTheRestIsRead(t *testing.T) {
	dir := writeClone(t, map[string]string{
		"package.json":  `{"dependencies":{"express":"^4.0.0"}}`,
		"web/yarn.lock": "# yarn lockfile v1\n\nleft-pad@^1.3.0:\n  version \"1.3.0\"\n",
	})
	const limit = 64 << 20
	sparse(t, filepath.Join(dir, "big", "requirements.txt"), 4<<30)
	sparse(t, filepath.Join(dir, "big", "yarn.lock"), 4<<30)
	sparse(t, filepath.Join(dir, "big", "notes.bin"), 4<<30) // no manifest: not a failure
	var logged bytes.Buffer
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5},
		ManifestLimits{CPU: 20 * time.Second, HeapBytes: limit}, nil, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil {
		t.Fatalf("the reader: %v\n%s", err, logged.String())
	}
	if run.decl.Found != 2 || run.decl.Parsed != 1 || len(run.decl.Failed) != 1 ||
		run.decl.Failed[0] != (model.ManifestFailure{Path: "big/requirements.txt", Kind: "requirements.txt", Reason: model.ManifestFailedRead}) {
		t.Errorf("the declarations: found %d, read %d, failed %+v; want the 4 GiB requirements.txt as the one failed read", run.decl.Found, run.decl.Parsed, run.decl.Failed)
	}
	if len(run.decl.Deps) != 1 || run.decl.Deps[0].Name != "express" {
		t.Errorf("the manifest beside it was not read: %+v", run.decl.Deps)
	}
	locks := heldLockfiles(*run.locks)
	if len(locks) != 1 || locks[0].Path != "web/yarn.lock" || len(run.locks.Failed) != 1 ||
		run.locks.Failed[0] != (model.ManifestFailure{Path: "big/yarn.lock", Kind: "yarn.lock", Reason: model.ManifestFailedRead, TooLarge: true}) {
		t.Errorf("the lockfiles: %d read, failed %+v; want web/yarn.lock read and the 4 GiB one failed", len(locks), run.locks.Failed)
	}
	if !run.present["big/yarn.lock"] || !run.present["web/yarn.lock"] || len(run.present) != 2 || !run.presentKnown {
		t.Errorf("the lockfiles listed: %v (known %v)", run.present, run.presentKnown)
	}
	// What the child had resident at most: its limit and what the runtime
	// keeps, never the files. (Go's own floor is some tens of mebibytes.)
	if run.maxRSS > 4*limit {
		t.Errorf("the child had %d MiB resident under a %d MiB limit beside three 4 GiB files", run.maxRSS>>20, limit>>20)
	}
	for _, want := range []string{"manifest is larger than the memory left to read it in", "lockfile is larger than the memory left to read it in", "big/requirements.txt", "big/yarn.lock"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("the log does not say %q:\n%s", want, logged.String())
		}
	}
	t.Logf("child: heap peak %d MiB, resident peak %d MiB, under a %d MiB limit", run.heapPeak>>20, run.maxRSS>>20, limit>>20)
}

// The rule itself: nothing is too large outside the child; inside it, a
// file is when its size is more than the limit leaves.
func TestWhatIsTooLargeToRead(t *testing.T) {
	if cloneFileTooLarge("requirements.txt", 1<<40) {
		t.Fatal("a process that is not the reading child refused a file for its size")
	}
	// The limit is set from this TEST process's heap, which holds whatever
	// the tests before this one left to be collected. The function collects
	// before it refuses, so the room it then sees can be much more than was
	// left here (0.29.215's closing run: 64 MiB was read "with 32 MiB left"
	// after a collection freed more than its cost). Collect first, and ask
	// about a size no collection can make room for.
	runtime.GC()
	runtime.GC()
	cloneReadHeapLimit.Store(processHeapBytes() + 32<<20)
	t.Cleanup(func() { cloneReadHeapLimit.Store(0) })
	// (A requirements.txt is taken to cost 100 times its length and a
	// yarn.lock 14: readCostFactor.)
	if cloneFileTooLarge("requirements.txt", 64<<10) {
		t.Error("64 KiB of requirements.txt was refused with 32 MiB left")
	}
	if !cloneFileTooLarge("requirements.txt", 1<<20) {
		t.Error("a mebibyte of requirements.txt, which costs 100, was not refused with 32 MiB left")
	}
	if cloneFileTooLarge("yarn.lock", 1<<20) {
		t.Error("a mebibyte of yarn.lock, which costs 14, was refused with 32 MiB left")
	}
	if !cloneFileTooLarge("requirements.txt", 1<<40) {
		t.Error("a tebibyte was not refused with 32 MiB left")
	}
}

// One item too long to hand over fails ITS file, by name, and nothing else
// (review round 7: the child exited with status 64 and said nothing, and all
// three passes were lost every cycle).
func TestAnItemTooLongToSendFailsItsFileByName(t *testing.T) {
	long := strings.Repeat("<", 3*manifestItemLimit) // escaped six-fold, past a frame
	dir := writeClone(t, map[string]string{
		"package.json":     `{"dependencies":{"express":"^4.0.0"}}`,
		"big/package.json": `{"dependencies":{"` + long + `":"1.0.0"}}`,
		"yarn.lock":        "# yarn lockfile v1\n\nleft-pad@^1.3.0:\n  version \"1.3.0\"\n",
		"big/yarn.lock":    "# yarn lockfile v1\n\n\"" + strings.Repeat("<", 3*manifestItemLimit) + "@^1.0.0\":\n  version \"1.0.0\"\n",
	})
	var logged bytes.Buffer
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5},
		ManifestLimits{CPU: 20 * time.Second, HeapBytes: 512 << 20}, nil, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil {
		t.Fatalf("the reader: %v", err)
	}
	if len(run.decl.Failed) != 1 || run.decl.Failed[0].Path != "big/package.json" || run.decl.Failed[0].Reason != model.ManifestFailedParse || len(run.decl.Deps) != 1 {
		t.Errorf("the declarations: failed %+v, %d read", run.decl.Failed, len(run.decl.Deps))
	}
	if got := heldLockfiles(*run.locks); len(got) != 1 || got[0].Path != "yarn.lock" || len(run.locks.Failed) != 1 || run.locks.Failed[0].Path != "big/yarn.lock" {
		t.Errorf("the lockfiles: %d read, failed %+v", len(got), run.locks.Failed)
	}
	for _, want := range []string{"a declaration too long to store", "an entry too long to store"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("the log does not say %q", want)
		}
	}
}

// --- what the parent holds, and what it refuses ------------------------

// An honest repository of many lockfiles is read under a memory limit
// smaller than all of them together, and this process holds their heads,
// not their entries (review round 7: refused as a protocol error past the
// limit, and 673 MiB held for fifteen yarn.lock files).
// manyHonestLockfiles is how many lockfiles of 4,000 entries the stand-in
// child of that test sends: 200, some 30 MiB on the wire; under the race
// detector 70, some 10 MiB, which this process checks entry by entry at a
// fifth of the speed. Either is several times what it may hold.
func manyHonestLockfiles() int {
	if manifestRaceEnabled {
		return 70
	}
	return 200
}

func TestManyLockfilesAreReadUnderALimitSmallerThanTheirSumAndKeptOnDisk(t *testing.T) {
	dir := writeClone(t, nil)
	misbehaving(t, "r7-many-honest-lockfiles")
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassLockfiles}, Dir: dir},
		ManifestLimits{CPU: 20 * time.Second, HeapBytes: 8 << 20}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("%d lockfiles of 4,000 entries under an 8 MiB limit: %v", manyHonestLockfiles(), err)
	}
	defer run.close()
	runtime.GC()
	runtime.ReadMemStats(&after)
	// Several times the 4 MiB this process may hold of it: 16 MiB at least
	// in the ordinary run, 8 MiB under the smaller count of the race run.
	floor := int64(16 << 20)
	if manifestRaceEnabled {
		floor = 8 << 20
	}
	if run.locks.spool.written < floor {
		t.Fatalf("the fixture is %d bytes on the wire: too small to show anything", run.locks.spool.written)
	}
	if held := int64(after.HeapAlloc) - int64(before.HeapAlloc); held > 4<<20 {
		t.Errorf("this process holds %d bytes of a lockfile pass of %d on the wire: the entries are to be on disk", held, run.locks.spool.written)
	}
	if run.locks.Parsed != nil || run.locks.parsedCount() != manyHonestLockfiles() {
		t.Errorf("lockfiles held in memory: %d; counted: %d", len(run.locks.Parsed), run.locks.parsedCount())
	}
	// Read back one at a time, twice (the inventory and the lockfile phase
	// each do), in the child's order, whole.
	for pass := 0; pass < 2; pass++ {
		n, entries := 0, 0
		if err := run.locks.each(func(pl parsedLockfile) error {
			if want := fmt.Sprintf("app%d/yarn.lock", n); pl.Path != want || len(pl.Result.Entries) != 4000 || pl.Result.Kind != "yarn.lock" {
				t.Fatalf("lockfile %d read back as %q with %d entries", n, pl.Path, len(pl.Result.Entries))
			}
			if got, want := pl.Result.Entries[3999].Name, fmt.Sprintf("package-with-a-name-%d-3999", n); got != want {
				t.Fatalf("lockfile %d's last entry is %q, want %q", n, got, want)
			}
			n++
			entries += len(pl.Result.Entries)
			return nil
		}); err != nil || n != manyHonestLockfiles() || entries != 4000*manyHonestLockfiles() {
			t.Fatalf("read back: %d lockfiles, %d entries, %v", n, entries, err)
		}
	}
	run.close()
	if err := run.locks.each(func(parsedLockfile) error { return nil }); err == nil {
		t.Error("a result that was let go was read back")
	}
}

// What a child may not send, added in 0.29.214; each is a named failure and
// nothing it sent is used.
func TestAChildThatMisbehavesRound7(t *testing.T) {
	dir := writeClone(t, nil)
	for mode, c := range map[string]struct{ pass, want string }{
		"r7-path-dot":                          {manifestPassDeclarations, "not a path inside the clone"},
		"r7-path-double":                       {manifestPassDeclarations, "not a path inside the clone"},
		"r7-path-is-dot":                       {manifestPassDeclarations, "not a path inside the clone"},
		"r7-path-escape":                       {manifestPassDeclarations, "not a path inside the clone"},
		"r7-path-not-text":                     {manifestPassDeclarations, "not a path inside the clone"},
		"r7-tree-root":                         {manifestPassDeclarations, "a Maven tree's root is not a path inside the clone"},
		"r7-repository-kind":                   {manifestPassDeclarations, "a declared repository is not one the pass records"},
		"r7-reason":                            {manifestPassDeclarations, "a reason this program does not have"},
		"r7-failed-carrying":                   {manifestPassDeclarations, "a failed manifest is not one the pass records"},
		"r7-counts-overflow":                   {manifestPassDeclarations, "do not add up"},
		"r7-name-count-zero":                   {manifestPassNames, "not one the roster gives"},
		"r7-name-count-overflow":               {manifestPassNames, "do not add up to a count"},
		"r7-present-not-a-lockfile":            {manifestPassNames, "a lockfile listed is not one the roster has"},
		"r7-present-twice":                     {manifestPassNames, "not once"},
		"r7-edges-fewer":                       {manifestPassLockfiles, "fewer entries or edges than it said"},
		"r7-entry-of-a-finished-lockfile":      {manifestPassLockfiles, "one the child had finished"},
		"r7-lockfiles-larger-than-their-files": {manifestPassLockfiles, "times the 1048576 bytes of the lockfiles it is of"},
		"r7-two-values-in-a-frame":             {manifestPassNames, "more than one value"},
	} {
		misbehaving(t, mode)
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{c.pass}, Dir: dir, RemotePomLimit: 5},
			ManifestLimits{CPU: 20 * time.Second, HeapBytes: 256 << 20}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		var child *errManifestChild
		if run.anyFinished() || !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadFailed || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: a pass finished: %v, error %v; want no result and a failed read that says %q", mode, run.anyFinished(), err, c.want)
		}
	}
}

// The one form a path is written in, on both sides.
func TestThePathFormOfThePasses(t *testing.T) {
	for p, want := range map[string]bool{
		"package.json": true, "a/b/pom.xml": true, "a b/c\td/x": true, "é/pom.xml": true,
		"": false, ".": false, "..": false, "../x": false, "a/../x": false, "a/./x": false, "a//x": false, "a/": false,
		"/abs": false, "a\x00b": false, "a\x1bb": false, "a\xffb": false, "./a": false,
	} {
		if wirePathOK(p) != want {
			t.Errorf("wirePathOK(%q) = %v", p, !want)
		}
		if s := storablePath(p); !wirePathOK(s) || (want && s != p) {
			t.Errorf("storablePath(%q) = %q", p, s)
		}
	}
}

// --- what the child is started with ------------------------------------

// The child's environment is empty and its working directory is the root,
// seen from INSIDE a child started the way every child is: a secret in this
// process's environment is not the child's (review round 7: the line that
// gives the child its environment could be deleted and every test passed).
func TestTheChildIsStartedWithNothingOfItsParents(t *testing.T) {
	t.Setenv("AVELOXIS_R7_SECRET", "hunter2-do-not-inherit")
	t.Setenv("PGPASSWORD", "hunter2-do-not-inherit")
	// A clone named by a relative path is made absolute for the child,
	// whose working directory is not this process's.
	parent := t.TempDir()
	if err := os.MkdirAll(filepath.Join(parent, "clone"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(parent)
	misbehaving(t, "r7-report-env")
	var logged bytes.Buffer
	if _, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: "clone"},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(&logged, nil))); err != nil {
		t.Fatal(err)
	}
	out := logged.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("the child was started with its parent's environment:\n%s", out)
	}
	wantDir, _ := filepath.EvalSymlinks(filepath.Join(parent, "clone"))
	gotDir := ""
	if i := strings.Index(out, "dir="); i >= 0 {
		gotDir, _ = filepath.EvalSymlinks(strings.Fields(out[i+4:])[0])
	}
	if !strings.Contains(out, "env=[]") || !strings.Contains(out, "cwd=/ ") || gotDir != wantDir {
		t.Errorf("the child's environment, working directory and clone: want env=[], cwd=/ and %s:\n%s", wantDir, out)
	}
}

// The real child reads a clone named by a relative path (review round 7: a
// relative TMPDIR made every reading fail).
func TestACloneNamedByARelativePathIsRead(t *testing.T) {
	parent := t.TempDir()
	t.Chdir(parent)
	if err := os.MkdirAll("clone", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("clone", "package.json"), []byte(`{"dependencies":{"a":"1"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: "clone"},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || len(run.decl.Deps) != 1 {
		t.Fatalf("a relative clone path: %v, %+v", err, run.decl)
	}
}

// On Linux the child is the program that is running, whatever is at its
// path now; elsewhere, the file at its path.
func TestTheChildIsTheRunningProgram(t *testing.T) {
	path := func() (string, error) { return "/opt/aveloxis/bin/aveloxis", nil }
	if got, err := manifestChildProgram("linux", path); err != nil || got != "/proc/self/exe" {
		t.Errorf("linux: %q, %v; want /proc/self/exe, the running program", got, err)
	}
	for _, goos := range []string{"darwin", "freebsd", "windows"} {
		if got, err := manifestChildProgram(goos, path); err != nil || got != "/opt/aveloxis/bin/aveloxis" {
			t.Errorf("%s: %q, %v; want the executable's path", goos, got, err)
		}
	}
	if _, err := manifestChildProgram("darwin", func() (string, error) { return "", errors.New("unknown") }); err == nil {
		t.Error("a path that is not known was not an error")
	}
	exe, args, err := manifestChildCommand()
	want, _ := manifestChildProgram(runtime.GOOS, os.Executable)
	if err != nil || exe != want || len(args) != 1 || args[0] != ManifestChildArg {
		t.Errorf("the command is %q %v, %v; want %q and the one argument", exe, args, err, want)
	}
}

// --- the log, and the clock --------------------------------------------

// A child's log is written up to a count and a size, and the rest is one
// line (review round 7: 200 MiB of log from one analysis).
func TestAChildsLogIsBounded(t *testing.T) {
	dir := writeClone(t, nil)
	misbehaving(t, "r7-log-flood")
	var logged bytes.Buffer
	if _, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(&logged, nil))); err != nil {
		t.Fatal(err)
	}
	out := logged.String()
	if n := strings.Count(out, "msg=flood"); n != manifestLogRecords {
		t.Errorf("%d of the child's 3,000 records were logged; want %d", n, manifestLogRecords)
	}
	if len(out) > manifestLogBytes+(256<<10) || strings.Contains(out, "yyyy") {
		t.Errorf("%d bytes were logged", len(out))
	}
	if !strings.Contains(out, "the rest of its records were dropped") || !strings.Contains(out, "dropped=2001") || strings.Count(out, "were dropped") != 1 {
		t.Errorf("the one line about what was dropped is missing or says another count")
	}
}

// And by size, when the records are few and large: five of 400 KiB are two
// logged and three dropped.
func TestAChildsLogIsBoundedBySizeToo(t *testing.T) {
	dir := writeClone(t, nil)
	misbehaving(t, "r7-log-large")
	var logged bytes.Buffer
	if _, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(&logged, nil))); err != nil {
		t.Fatal(err)
	}
	out := logged.String()
	if n := strings.Count(out, "msg=large"); n != 2 || !strings.Contains(out, "dropped=3") {
		t.Errorf("%d of five 400 KiB records were logged (%d bytes of log); want 2, and 3 said to be dropped", n, len(out))
	}
}

// The clock allowance is a valid duration for every CPU limit a config may
// hold (review round 7: the largest accepted value overflowed it to a
// negative one), and the default follows the default limit.
func TestTheClockAllowanceForEveryLimitAConfigMayHold(t *testing.T) {
	for _, seconds := range []int{1, config.DefaultManifestReadCPUSeconds, config.MaxManifestReadCPUSeconds} {
		cpu := time.Duration(seconds) * time.Second
		if got, want := manifestWallAllowance(cpu), time.Minute+4*cpu; got != want || got <= cpu {
			t.Errorf("%d s: allowance %s, want %s", seconds, got, want)
		}
	}
	if config.DefaultManifestReadCPUSeconds != 180 || config.MaxManifestReadCPUSeconds != 86400 {
		t.Errorf("the default and the most are %d and %d; the operator set 180 (2026-11-20) and the most is a day", config.DefaultManifestReadCPUSeconds, config.MaxManifestReadCPUSeconds)
	}
}

// --- across the wire ---------------------------------------------------

type scriptedPomSource struct {
	outcome manifest.RemoteOutcome
	asked   int
}

func (s *scriptedPomSource) RemotePom(string, string, string, []string) (*manifest.RemotePom, manifest.RemoteOutcome) {
	s.asked++
	return nil, s.outcome
}

// What the parent's pom source answers is what the reader in the child is
// told: refused is not "not found" (the round-6 rule that withholds a
// version rests on the difference).
func TestARefusedPomIsRefusedInTheChildToo(t *testing.T) {
	pom := `<project><modelVersion>4.0.0</modelVersion>
<parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>1</version><relativePath/></parent>
<artifactId>app</artifactId>
<dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies></project>`
	dir := writeClone(t, map[string]string{"pom.xml": pom})
	for outcome, want := range map[manifest.RemoteOutcome]string{
		manifest.RemoteRefused:  model.VersionReasonInheritedRepositoryRefused,
		manifest.RemoteNotFound: model.VersionReasonInheritedNotFound,
		manifest.RemoteFailed:   model.VersionReasonInheritedLookupFailed,
	} {
		src := &scriptedPomSource{outcome: outcome}
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
			ManifestLimits{}, src, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil || src.asked == 0 {
			t.Fatalf("outcome %d: %v, asked %d", outcome, err, src.asked)
		}
		got := ""
		for _, d := range run.decl.Deps {
			if d.Name == "g:x" {
				got = d.VersionReason
			}
		}
		if got != want {
			t.Errorf("the parent answered %d and g:x's reason is %q, want %q: %+v", outcome, got, want, run.decl.Deps)
		}
	}
}

// --- how the reading of the lockfiles ended ----------------------------

type recordedReadings struct{ got []LockfileReading }

func (r *recordedReadings) RecordLockfileReading(_ context.Context, _ int64, reading LockfileReading) error {
	r.got = append(r.got, reading)
	return nil
}

func TestHowTheLockfileReadingEndedIsSaidInOnePlace(t *testing.T) {
	rec := &recordedReadings{}
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), LockfileReadings: rec}
	ctx := context.Background()
	for _, c := range []struct {
		err  error
		want []LockfileReading
	}{
		{&errManifestChild{Attempt: model.InventoryAttemptReadMemoryLimit, detail: "x"}, []LockfileReading{{NotCompleted: model.InventoryAttemptReadMemoryLimit}}},
		{&errManifestChild{Attempt: model.InventoryAttemptReadCPULimit, detail: "x"}, []LockfileReading{{NotCompleted: model.InventoryAttemptReadCPULimit}}},
		{&errManifestChild{Attempt: model.InventoryAttemptReadTimedOut, detail: "x"}, []LockfileReading{{NotCompleted: model.InventoryAttemptReadTimedOut}}},
		{&errManifestChild{Attempt: model.InventoryAttemptReadFailed, detail: "x"}, []LockfileReading{{NotCompleted: model.InventoryAttemptReadFailed}}},
		{&errManifestRoot{errors.New("gone")}, []LockfileReading{{NotCompleted: model.InventoryAttemptRootUnreadable}}},
		// A binary of another version is said once for the process, and a
		// shutdown is not a reading that ended: neither is recorded.
		{&errManifestChild{Attempt: model.InventoryAttemptReadFailed, detail: "x", Stale: true}, nil},
		{context.Canceled, nil},
	} {
		rec.got = nil
		// The reader was reading the lockfiles when it stopped.
		if err := ac.scanLockfiles(ctx, 7, "", lockfileSet{WalkErr: c.err, Started: true}, &AnalysisResult{}); err == nil {
			t.Fatalf("%v: unread lockfiles were not an error", c.err)
		}
		if fmt.Sprint(rec.got) != fmt.Sprint(c.want) {
			t.Errorf("%v: recorded %+v, want %+v", c.err, rec.got, c.want)
		}
		// It was stopped BEFORE it reached them (0.29.215): whatever
		// stopped the pass ahead, the lockfiles' own reason is that they
		// were not reached. A clone whose root cannot be read, and the two
		// that record nothing, are as above.
		var child *errManifestChild
		if !errors.As(c.err, &child) || child.Stale {
			continue
		}
		rec.got = nil
		if err := ac.scanLockfiles(ctx, 7, "", lockfileSet{WalkErr: c.err}, &AnalysisResult{}); err == nil {
			t.Fatalf("%v: unread lockfiles were not an error", c.err)
		}
		if want := []LockfileReading{{NotCompleted: model.LockfileUnreadNotReached}}; fmt.Sprint(rec.got) != fmt.Sprint(want) {
			t.Errorf("%v, the lockfiles not reached: recorded %+v, want %+v", c.err, rec.got, want)
		}
	}
}

// --- the surviving mutants of round 7, each driven ----------------------

type slowPomSource struct{ wait time.Duration }

func (s slowPomSource) RemotePom(string, string, string, []string) (*manifest.RemotePom, manifest.RemoteOutcome) {
	time.Sleep(s.wait)
	return nil, manifest.RemoteNotFound
}

// The time this process spends answering a pom is not the child's: a child
// whose one request took longer than the whole clock allowance to answer is
// not stopped by the clock. (Two seconds of allowance, so that the child's
// own start and reading fit it under the race detector too.)
func TestTheClockDoesNotCountTheTimeSpentAnsweringTheChild(t *testing.T) {
	savedAllow, savedEvery := manifestWallAllowance, manifestWallCheckEvery
	manifestWallAllowance = func(time.Duration) time.Duration { return 2 * time.Second }
	manifestWallCheckEvery = 20 * time.Millisecond
	t.Cleanup(func() { manifestWallAllowance, manifestWallCheckEvery = savedAllow, savedEvery })
	dir := writeClone(t, map[string]string{"pom.xml": `<project><modelVersion>4.0.0</modelVersion>
<parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>1</version><relativePath/></parent>
<artifactId>app</artifactId></project>`})
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
		ManifestLimits{}, slowPomSource{wait: 3 * time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || !run.finished[manifestPassDeclarations] {
		t.Fatalf("a child that waited 3 s for its parent's answer under a 2 s clock: %v", err)
	}
}

// The CPU time a process has used is its time in the kernel too: a process
// that spends its time in system calls is as busy as one that computes.
func TestCPUTimeCountsTimeInTheKernel(t *testing.T) {
	zero, err := os.Open("/dev/zero")
	if err != nil {
		t.Fatalf("/dev/zero: %v", err)
	}
	defer zero.Close()
	null, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	system := func() time.Duration {
		var ru syscallRusage
		getrusageSelf(&ru)
		return ru.system()
	}
	sysBefore, cpuBefore := system(), processCPUTime()
	buf := make([]byte, 64)
	for system()-sysBefore < 200*time.Millisecond {
		for i := 0; i < 20000; i++ {
			if _, err := zero.Read(buf); err != nil {
				t.Fatal(err)
			}
			if _, err := null.Write(buf); err != nil {
				t.Fatal(err)
			}
		}
	}
	sys, cpu := system()-sysBefore, processCPUTime()-cpuBefore
	if cpu < sys {
		t.Errorf("%s of CPU time counted over a stretch with %s in the kernel alone", cpu, sys)
	}
}

// On Linux the child's own high-water mark is the VmHWM line of
// /proc/self/status, in kibibytes (0.29.223). getrusage's ru_maxrss is not
// the child's there: the kernel carries the mark of the process image that
// existed before the exec into it, so every child reported the memory of
// the process that started it. CI showed it: under a 64 MiB limit a child
// with a 1 MiB heap "had" 823 MiB resident, and two children reading a
// 1 MB and a 3 MB lockfile both "had" exactly 1,442 MiB. Those tests show
// it on Linux only when the test process is itself large (review round 15:
// on a small one the old figure stayed under their bound); a pin that does
// not depend on that is planned work. This one holds the reading of the
// line, and TestTheResidentMarkOnLinuxIsTheStatusFilesOrUnknown the choice
// of source, on every system.
func TestTheHighWaterMarkIsReadFromTheStatusFile(t *testing.T) {
	const status = "Name:\taveloxis\nVmPeak:\t 1234567 kB\nVmHWM:\t   51234 kB\nVmRSS:\t   40000 kB\n"
	if got, ok := statusHighWaterBytes(status); !ok || got != 51234<<10 {
		t.Errorf("VmHWM of 51234 kB read as %d bytes (found %v); want %d", got, ok, 51234<<10)
	}
	for name, s := range map[string]string{
		"no such line":                          "Name:\taveloxis\nVmRSS:\t 40000 kB\n",
		"another unit":                          "VmHWM:\t 51234 MB\n",
		"no number":                             "VmHWM:\t kB\n",
		"a negative number":                     "VmHWM:\t -5 kB\n",
		"zero":                                  "VmHWM:\t 0 kB\n",
		"a longer key":                          "VmHWMX:\t 51234 kB\n",
		"the key inside a line":                 "XVmHWM:\t 51234 kB\n",
		"empty":                                 "",
		"a number past what a byte count holds": "VmHWM:\t 9223372036854775807 kB\n",
	} {
		if got, ok := statusHighWaterBytes(s); ok {
			t.Errorf("%s: read as %d bytes; want not found, so that nothing is reported", name, got)
		}
	}
}

// On Linux the figure is the status file's line or NOTHING (0, unknown):
// never getrusage's, which is another process's mark there (review round 15,
// P-2: a Linux child whose status file could not be read fell back to it in
// silence). Elsewhere it is getrusage's, in bytes.
func TestTheResidentMarkOnLinuxIsTheStatusFilesOrUnknown(t *testing.T) {
	good := func() (string, error) { return "VmHWM:\t 2048 kB\n", nil }
	unreadable := func() (string, error) { return "", errors.New("no /proc") }
	noLine := func() (string, error) { return "Name:\tx\n", nil }
	rusage := func() int64 { return 777 << 20 }
	for name, c := range map[string]struct {
		goos   string
		status func() (string, error)
		want   int64
	}{
		"linux, the line":                 {"linux", good, 2048 << 10},
		"linux, no status file":           {"linux", unreadable, 0},
		"linux, a status file without it": {"linux", noLine, 0},
		"darwin":                          {"darwin", good, 777 << 20},
		"freebsd":                         {"freebsd", unreadable, 777 << 20},
	} {
		if got := residentHighWater(c.goos, c.status, rusage); got != c.want {
			t.Errorf("%s: %d bytes; want %d", name, got, c.want)
		}
	}
}

// The most memory the process has had resident is in bytes on every
// platform (the system reports kibibytes on Linux and bytes on macOS).
func TestMaxRSSIsInBytes(t *testing.T) {
	got := processMaxRSS()
	if got < 4<<20 || got > 64<<30 {
		t.Errorf("this test process has had %d bytes resident at most: not a number of bytes for a Go test binary", got)
	}
}

// A manifest whose path cannot be stored is recorded under a spelling that
// can, and carries no row: that instruction crosses from the real child.
func TestNoCarryCrossesFromTheRealChild(t *testing.T) {
	dir := writeClone(t, map[string]string{"a\x01b/package.json": `{"dependencies":{"a":"1"}}`, "package.json": `{"dependencies":{"b":"1"}}`})
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(run.decl.Failed) != 1 || !run.decl.Failed[0].NoCarry || run.decl.Failed[0].Path != "ab/package.json" || len(run.decl.Deps) != 1 {
		t.Errorf("failed %+v, deps %+v; want the unstorable path's manifest failed under ab/package.json, carrying nothing", run.decl.Failed, run.decl.Deps)
	}
}

// What the lockfile pass sends is held to a multiple of the lockfiles' own
// size (lockfileWireFactor). An honest file of the format with the most on
// the wire for its size (Gemfile.lock: every dependency line is an edge that
// repeats its parent), and of two others, is within a quarter of it.
func TestTheLockfilePassIsNoLargerThanItsLockfilesTimesTheFactor(t *testing.T) {
	gemfile := fill(2<<20, "GEM\n  remote: https://rubygems.org/\n  specs:\n", "\nPLATFORMS\n  ruby\n\nDEPENDENCIES\n  pkg-0\n\nBUNDLED WITH\n   2.4.10\n", func(i int) string {
		return fmt.Sprintf("    pkg-%d (%d.%d.%d)\n      pkg-%d (~> 1.0)\n      pkg-%d (>= 2.0, < 4)\n", i, i%9, i%20, i%30, i+1, i+2)
	})
	for name, body := range map[string]string{
		"Gemfile.lock":      gemfile,
		"pnpm-lock.yaml":    honestLockfile("pnpm-lock.yaml", 2<<20),
		"package-lock.json": honestLockfile("package-lock.json", 2<<20),
	} {
		dir := writeClone(t, map[string]string{name: body})
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassLockfiles}, Dir: dir},
			ManifestLimits{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil || run.locks.parsedCount() != 1 {
			t.Fatalf("%s: %v", name, err)
		}
		ratio := float64(run.locks.spool.written) / float64(len(body))
		run.close()
		if ratio > lockfileWireFactor/4 || ratio < 0.05 {
			t.Errorf("%s of %d bytes is %.2f times its size on the wire; the factor is %d", name, len(body), ratio, lockfileWireFactor)
		}
		t.Logf("%s: %.2f times its size on the wire", name, ratio)
	}
}

// A lockfile the child says it read is a regular file of the clone, as this
// process finds it: one that is not there is not a lockfile the pass read.
func TestALockfileTheChildNamesIsAFileOfTheClone(t *testing.T) {
	dir := writeClone(t, nil)
	misbehaving(t, "r7-many-honest-lockfiles")
	manifestLockfileSize = cloneLockfileSize // misbehaving's cleanup puts back what it found
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassLockfiles}, Dir: dir},
		ManifestLimits{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if run.anyFinished() || err == nil || !strings.Contains(err.Error(), "is not a file of the clone") {
		t.Errorf("lockfiles named that are in no clone: finished %v, %v", run.anyFinished(), err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "sub", "yarn.lock"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := cloneLockfileSize(dir, "sub/yarn.lock"); err == nil {
		t.Error("a directory was taken for a lockfile")
	}
}

// A Maven tree's root in a directory whose name cannot be stored crosses in
// a spelling that can: the reading is not refused for it. (The module's own
// path is storable; its parent, which the walk reads on its own, is the
// tree's root.)
func TestATreeRootedAtAnUnstorablePathCrosses(t *testing.T) {
	parent := `<project><modelVersion>4.0.0</modelVersion>
<parent><groupId>org.example</groupId><artifactId>grandparent</artifactId><version>1</version><relativePath/></parent>
<groupId>org.example</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>
<repositories><repository><id>r</id><url>https://repo.example/maven</url></repository></repositories></project>`
	module := `<project><modelVersion>4.0.0</modelVersion>
<parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>1</version><relativePath>../bad\x01dir/pom.xml</relativePath></parent>
<artifactId>app</artifactId>
<dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies></project>`
	dir := writeClone(t, map[string]string{"bad\x01dir/pom.xml": parent, "mod/pom.xml": module})
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
		ManifestLimits{}, &scriptedPomSource{outcome: manifest.RemoteFailed}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("a module whose parent pom is under a directory named with a control character: %v", err)
	}
	if len(run.decl.NotWhole)+len(run.decl.TreeHosts)+len(run.decl.Repositories) == 0 {
		t.Fatalf("the fixture names no tree and no repository: %+v", run.decl)
	}
	for _, g := range run.decl.NotWhole {
		if !wirePathOK(g.Root) {
			t.Errorf("a tree's root crossed as %q", g.Root)
		}
	}
	for _, h := range run.decl.TreeHosts {
		if !wirePathOK(h.Root) {
			t.Errorf("a tree's root crossed as %q", h.Root)
		}
	}
	for _, r := range run.decl.Repositories {
		if !wirePathOK(r.ManifestPath) {
			t.Errorf("a declared repository's manifest crossed as %q", r.ManifestPath)
		}
	}
	t.Logf("not whole: %+v; hosts: %+v; repositories: %+v; failed: %+v", run.decl.NotWhole, run.decl.TreeHosts, run.decl.Repositories, run.decl.Failed)
}
