// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// The lockfiles the name pass LISTS are exactly the lockfiles the lockfile
// pass READS or records as failed under their own path (review round 8:
// they were two walks with two copies of the rules, and nothing held them
// equal). Every form one of them might take differently is in the tree:
// under node_modules, vendor and .git; a symbolic link; a path that cannot
// be stored; a binary lockfile; an empty one.
func TestTheLockfilesListedAreTheLockfilesRead(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, text string) {
		t.Helper()
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const yarn = "# yarn lockfile v1\n\nleft-pad@^1.0.0:\n  version \"1.3.0\"\n"
	write("app/yarn.lock", yarn)
	write("empty/yarn.lock", "")
	write("web/bun.lockb", "\x00binary")
	write("node_modules/x/yarn.lock", yarn)
	write("vendor/y/yarn.lock", yarn)
	write(".git/z/yarn.lock", yarn)
	write("bad\x01dir/yarn.lock", yarn)
	if err := os.MkdirAll(filepath.Join(dir, "linked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "app", "yarn.lock"), filepath.Join(dir, "linked", "yarn.lock")); err != nil {
		t.Fatal(err)
	}

	present, err := presentLockfiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, p := range present {
		listed = append(listed, p.Path)
	}
	sort.Strings(listed)

	var read []string
	failed, err := walkLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), func(pl parsedLockfile) { read = append(read, pl.Path) })
	if err != nil {
		t.Fatal(err)
	}
	var unstorable []model.ManifestFailure
	for _, f := range failed {
		if f.NoCarry {
			unstorable = append(unstorable, f)
			continue
		}
		read = append(read, f.Path)
	}
	sort.Strings(read)

	want := []string{"app/yarn.lock", "empty/yarn.lock", "web/bun.lockb"}
	if !reflect.DeepEqual(listed, want) {
		t.Errorf("listed %v; want %v", listed, want)
	}
	if !reflect.DeepEqual(read, want) {
		t.Errorf("read or failed under their own path %v; want %v, the ones listed", read, want)
	}
	// The lockfile whose path cannot be stored is a lockfile of the
	// repository that failed: recorded, under its sanitized path, carrying
	// nothing. Until 0.29.215 it was passed over in silence, and a
	// repository whose only lockfile was such a one was recorded as having
	// none.
	if len(unstorable) != 1 || unstorable[0].Path != "baddir/yarn.lock" || unstorable[0].Reason != model.ManifestFailedRead || unstorable[0].Kind != "yarn.lock" {
		t.Errorf("the lockfile at a path that cannot be stored: %+v; want one failure under baddir/yarn.lock", unstorable)
	}
}

// A sanitized path that is also a real lockfile's is not said twice: the
// parent takes a lockfile named twice for a reader that is not this
// program, and refuses the whole reading.
func TestAnUnstorableLockfileIsNotNamedOverARealOne(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{"baddir/yarn.lock", "bad\x01dir/yarn.lock", "bad\x02dir/yarn.lock"} {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("# yarn lockfile v1\n\na@1:\n  version \"1\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]int{}
	failed, err := walkLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), func(pl parsedLockfile) { seen[pl.Path]++ })
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range failed {
		seen[f.Path]++
	}
	if len(seen) != 1 || seen["baddir/yarn.lock"] != 1 {
		t.Errorf("named: %v; want baddir/yarn.lock once", seen)
	}
}

// A lockfile not read for its size is one FAILED lockfile, the others are
// read, the snapshot is replaced with its previous rows kept, and the
// reading is recorded as one that did not complete, for want of memory
// (which is what the repository's page cautions about), with an Error line
// that names the setting. Through the real child, from the configuration
// file's value.
func TestALockfileTooLargeToReadIsOneFailureAndTheReadingSaysSo(t *testing.T) {
	f := lockFixture(t, "locks-toolarge")
	ctx := context.Background()
	rec := &recordedReadings{}
	f.ac.LockfileReadings = rec
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	f.write(t, "big/package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "big/package-lock.json", goodNpmLock)
	f.analyse(t)
	before := lockfileRows(t, f)

	// The second lockfile grows past what a 64 MiB reader can take a
	// package-lock.json of (2.6 times its length); the first changes.
	f.write(t, "big/package-lock.json", honestLockfile("package-lock.json", 40<<20))
	f.write(t, "package-lock.json", strings.ReplaceAll(goodNpmLock, "4.18.2", "4.19.0"))
	SetManifestLimits(limitsFromConfig(t, `"manifest_read_memory_mb": 64`))
	t.Cleanup(func() { installedManifestLimits.Store(nil) })

	read := f.ac.readClone(ctx, f.repoID, f.dir)
	defer read.close()
	locks := read.lockfiles()
	if locks.WalkErr != nil {
		t.Fatalf("the reading was stopped (%v): a file too large to read is not read, and the reading goes on", locks.WalkErr)
	}
	var tooLarge []string
	for _, fl := range locks.Failed {
		if fl.TooLarge && fl.Reason == model.ManifestFailedRead {
			tooLarge = append(tooLarge, fl.Path)
		}
	}
	if !reflect.DeepEqual(tooLarge, []string{"big/package-lock.json"}) || locks.parsedCount() != 1 {
		t.Fatalf("too large: %v, parsed %d; want the one large lockfile failed for its size and the other read", tooLarge, locks.parsedCount())
	}
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, read, f.ac.lockfilesInUse(ctx, f.repoID, locks, read.declaredKeys()), &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	var log recorded
	quiet := f.ac.logger
	f.ac.logger = log.logger()
	err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{})
	f.ac.logger = quiet
	if err != nil {
		t.Fatalf("the lockfile phase: %v; want the snapshot replaced", err)
	}
	inv := inventoryRows(t, f)
	if e := inv["package.json:express"]; e.Version != "4.19.0" || e.Source != versionuse.SourceLocked {
		t.Errorf("express beside the lockfile that was read = %+v; want the version it now locks", e)
	}
	if e := inv["big/package.json:express"]; e.Version != "4.18.2" || e.Source != versionuse.SourceLocked {
		t.Errorf("express beside the lockfile too large to read = %+v; want the version its previous rows lock", e)
	}
	keptBig, freshSmall := 0, false
	for _, r := range lockfileRows(t, f) {
		if r.Path == "big/package-lock.json" {
			keptBig++
		}
		if r.Path == "package-lock.json" && r.Version == "4.19.0" {
			freshSmall = true
		}
	}
	wantBig := 0
	for _, r := range before {
		if r.Path == "big/package-lock.json" {
			wantBig++
		}
	}
	if keptBig != wantBig || wantBig == 0 || !freshSmall {
		t.Errorf("rows of the large lockfile: %d, were %d; the other lockfile's rows fresh: %v", keptBig, wantBig, freshSmall)
	}
	if n := len(rec.got); n != 2 || rec.got[n-1] != (LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true}) {
		t.Errorf("the readings recorded: %+v; want the second not completed, at the memory limit, with lockfiles", rec.got)
	}
	said := false
	for _, line := range log.lines {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "too large for the memory left") {
			said = true
			for _, want := range []string{"first=big/package-lock.json", "raise collection.manifest_read_memory_mb", "manifest_read_memory_mb=64"} {
				if !strings.Contains(line, want) {
					t.Errorf("the Error line lacks %q: %s", want, line)
				}
			}
		}
	}
	if !said {
		t.Errorf("no Error line says a lockfile was too large to read: %q", log.lines)
	}
}

// A lockfile the repository deleted is forgotten whole when the lockfiles
// go unread, the row that names it included: a binary lockfile and an empty
// one give no package row, and until 0.29.215 the snapshot went on naming
// them (review round 8), which now decides whether the page says the
// lockfiles are "behind".
func TestADeletedLockfileWithNoPackageRowsIsForgottenToo(t *testing.T) {
	f := lockFixture(t, "locks-gone-empty")
	ctx := context.Background()
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	f.write(t, "web/bun.lockb", "\x00binary")
	f.write(t, "empty/yarn.lock", "")
	f.analyse(t)
	paths := func() []string {
		t.Helper()
		got, err := f.store.GetRepoLockfilePaths(ctx, f.repoID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := paths(); !reflect.DeepEqual(got, []string{"empty/yarn.lock", "package-lock.json", "web/bun.lockb"}) {
		t.Fatalf("the snapshot names %v", got)
	}
	for _, gone := range []string{"web/bun.lockb", "empty/yarn.lock"} {
		if err := os.Remove(filepath.Join(f.dir, filepath.FromSlash(gone))); err != nil {
			t.Fatal(err)
		}
	}
	// The lockfiles go unread; the name pass had listed the one that is
	// left.
	unread := lockfileSet{WalkErr: &errManifestChild{Attempt: model.InventoryAttemptReadMemoryLimit, detail: "stopped"}, Started: true,
		PresentKnown: true, Present: map[string]bool{"package-lock.json": true}}
	if err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, unread, &AnalysisResult{}); err == nil {
		t.Fatal("unread lockfiles were not an error")
	}
	if got := paths(); !reflect.DeepEqual(got, []string{"package-lock.json"}) {
		t.Errorf("after the lockfiles went unread the snapshot names %v; want only the lockfile still in the clone", got)
	}
}

// A repository whose only lockfile is there and could not be parsed HAS
// lockfiles: the reading that found it is recorded so (round 8: counting
// only the lockfiles that parsed would have recorded "none").
func TestARepositoryWhoseOnlyLockfileFailedHasLockfiles(t *testing.T) {
	f := lockFixture(t, "locks-onlyfailed")
	rec := &recordedReadings{}
	f.ac.LockfileReadings = rec
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", `{"lockfileVersion": 3, "packages": `)
	f.analyse(t)
	if len(rec.got) != 1 || rec.got[0] != (LockfileReading{Completed: true, Lockfiles: true}) {
		t.Errorf("recorded %+v; want a completed reading of a repository WITH lockfiles", rec.got)
	}
}

// When the lockfiles the child read cannot be KEPT by this process (no
// temporary file can be made), that is the lockfile pass's own failure, on
// this host: the names and the declarations the child finished stand, and
// the failure says where the file was to be. Until 0.29.215 it was reported
// as the reader breaking the protocol, and all three passes were discarded
// (review round 8). A clone without lockfiles keeps nothing, and is whole.
func TestLockfilesThatCannotBeKeptFailOnlyTheLockfilePass(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	with := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n", "package.json": `{"dependencies":{"left-pad":"1.0.0"}}`,
		"yarn.lock": "# yarn lockfile v1\n\nleft-pad@1.0.0:\n  version \"1.0.0\"\n"})
	without := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n"})
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "not-there"))

	run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: with, RemotePomLimit: 5}, ManifestLimits{}, nil, quiet)
	if run == nil {
		t.Fatalf("nothing of the reading was kept: %v", err)
	}
	defer run.close()
	for _, pass := range []string{manifestPassNames, manifestPassDeclarations} {
		if perr := run.passErr(pass, err); perr != nil {
			t.Errorf("the %s pass: %v; want it to stand", pass, perr)
		}
	}
	if len(run.decl.Deps) != 2 {
		t.Errorf("declarations kept: %+v", run.decl.Deps)
	}
	var child *errManifestChild
	perr := run.passErr(manifestPassLockfiles, err)
	if !errors.As(perr, &child) || child.Attempt != model.InventoryAttemptReadFailed || child.Stale ||
		!strings.Contains(perr.Error(), "could not be kept on this host") || !strings.Contains(perr.Error(), "not-there") {
		t.Errorf("the lockfile pass: %v; want its own failure, naming the directory", perr)
	}
	// The lockfile phase is told the pass had begun: the reason recorded
	// is this failure's, not "not reached".
	read := &cloneRead{run: run, runErr: err, passes: manifestPassesAll}
	if set := read.lockfiles(); set.WalkErr == nil || !set.Started || !set.PresentKnown || !set.Present["yarn.lock"] {
		t.Errorf("the lockfile set: %+v", set)
	}

	run2, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: without, RemotePomLimit: 5}, ManifestLimits{}, nil, quiet)
	if err != nil || run2.passErr(manifestPassLockfiles, err) != nil {
		t.Errorf("a clone without lockfiles, where none can be kept: %v, %v; want a whole reading", err, run2.passErr(manifestPassLockfiles, err))
	}
	run2.close()
}

// The file the lockfile pass is kept in has no name from the moment it is
// made, and its descriptor is let go on every way out of a reading: one
// that finished, a shutdown, a reader stopped at a limit, a read-back that
// fails (review round 8: removing the unlink, or the close, failed no
// test).
func TestTheLockfileSpoolLeavesNothingBehind(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n", "a/yarn.lock": "# yarn lockfile v1\n\nleft-pad@1.0.0:\n  version \"1.0.0\"\n",
		"b/package-lock.json": `{"lockfileVersion":3,"packages":{"":{"name":"x"},"node_modules/a":{"version":"1.0.0"}}}`})
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	descriptors := func() int {
		entries, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatalf("this test counts open descriptors in /dev/fd: %v", err)
		}
		return len(entries)
	}
	before := descriptors()
	// Forty readings, ten of each ending; under the race detector, where
	// each starts a child that takes about a second, three of each. A leak
	// is one per reading either way.
	readings := 40
	if manifestRaceEnabled {
		readings = 12
	}
	for i := 0; i < readings; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		limits := ManifestLimits{}
		switch i % 4 {
		case 1:
			cancel() // a shutdown
		case 2:
			limits = ManifestLimits{CPU: time.Nanosecond, HeapBytes: 1 << 30} // stopped at once
		}
		run, _ := runManifestChild(ctx, manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, limits, nil, quiet)
		if i%4 == 0 {
			if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
				t.Fatalf("while a finished reading is held, the temporary directory holds %d files (%v); want none: the file has no name", len(left), err)
			}
		}
		if i%4 == 3 && run != nil && run.locks != nil {
			_ = run.locks.each(func(parsedLockfile) error { return errors.New("stop") })
		}
		run.close()
		cancel()
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Errorf("%d files left in the temporary directory", len(left))
	}
	// Other tests' descriptors come and go; a leak here is one per reading.
	if after := descriptors(); after-before > readings/4 {
		t.Errorf("open descriptors went from %d to %d over %d readings", before, after, readings)
	}
}

// A binary lockfile is named and never read: its size earns the spool no
// room (review round 8: a large bun.lockb beside one small lockfile let the
// reader send sixteen times the binary's size).
func TestABinaryLockfileEarnsTheSpoolNoRoom(t *testing.T) {
	check := newManifestCheck("/clone")
	check.lockfileSize = func(string, string) (int64, error) { return 1 << 30, nil }
	pending := newManifestPending(manifestPassLockfiles)
	defer pending.discard()
	part := &manifestPart{Lockfiles: []manifestWireLockfile{
		{Path: "web/bun.lockb", Kind: "bun.lockb", Ecosystem: lockfileKinds["bun.lockb"].ecosystem},
	}}
	if err := check.part(manifestPassLockfiles, part, pending); err != nil {
		t.Fatal(err)
	}
	if pending.lockBytes != 0 {
		t.Errorf("a binary lockfile of 1 GiB allowed the spool %d bytes of lockfiles", pending.lockBytes)
	}
	part = &manifestPart{Lockfiles: []manifestWireLockfile{
		{Path: "yarn.lock", Kind: "yarn.lock", Ecosystem: lockfileKinds["yarn.lock"].ecosystem},
	}}
	if err := check.part(manifestPassLockfiles, part, pending); err != nil {
		t.Fatal(err)
	}
	if pending.lockBytes != 1<<30 {
		t.Errorf("a text lockfile of 1 GiB allowed the spool %d bytes of lockfiles", pending.lockBytes)
	}
}

// A reader stopped before it finished the passes ahead of the lockfiles had
// not reached them; one stopped with those passes finished had (the reason
// the lockfile phase records turns on it).
func TestWhetherTheLockfilePassHadBegunWhenTheReaderStopped(t *testing.T) {
	// A run stopped on its first pass: nothing finished.
	run := &manifestRun{finished: map[string]bool{}, rootErr: map[string]error{}, passFail: map[string]error{}}
	var err error = &errManifestChild{Attempt: model.InventoryAttemptReadCPULimit, detail: "stopped"}
	read := &cloneRead{run: run, runErr: err, passes: manifestPassesAll}
	if set := read.lockfiles(); set.WalkErr == nil || set.Started {
		t.Errorf("stopped on the first pass: %+v; want the lockfiles not begun", set)
	}
	// The same stop with the passes ahead finished is a stop in the
	// lockfile pass.
	run.finished[manifestPassNames], run.finished[manifestPassDeclarations] = true, true
	if set := read.lockfiles(); set.WalkErr == nil || !set.Started {
		t.Errorf("stopped with the passes ahead finished: %+v; want the lockfiles begun", set)
	}
	// With the Maven store unavailable the declarations are not asked for,
	// and are not waited on.
	run.finished[manifestPassDeclarations] = false
	read.passes = []string{manifestPassNames, manifestPassLockfiles}
	if set := read.lockfiles(); !set.Started {
		t.Errorf("declarations not asked for: %+v; want the lockfiles begun", set)
	}
}

// The Go test-only classifier reads at most maxGoFilesClassified files, and
// past that relabels NOTHING (a file it did not read may hold the one
// import that makes a module a runtime dependency). Exactly at the bound it
// still classifies (review round 8: the bound moved by one, or its "nothing
// is relabelled" dropped, failed no test).
func TestTheGoClassifierAtItsFileBound(t *testing.T) {
	dir := t.TempDir()
	body := []byte("package x\n\nimport _ \"example.com/onlytests/pkg\"\n")
	link := func(i int) {
		t.Helper()
		sub := filepath.Join(dir, "d"+strconv.Itoa(i/1000))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, "f"+strconv.Itoa(i)+"_test.go"), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < maxGoFilesClassified; i++ {
		link(i)
	}
	deps := func() []model.LibyearDep {
		return []model.LibyearDep{{Manager: "go", Name: "example.com/onlytests", Type: model.ScopeRuntime}}
	}
	if got := classifyGoModTestOnlyDeps(dir, deps()); got[0].Type != model.ScopeTest {
		t.Errorf("with exactly %d .go files the module is %q; want it classified (test)", maxGoFilesClassified, got[0].Type)
	}
	link(maxGoFilesClassified)
	if got := classifyGoModTestOnlyDeps(dir, deps()); got[0].Type != model.ScopeRuntime {
		t.Errorf("with one .go file more than the bound the module is %q; want nothing relabelled", got[0].Type)
	}
}
