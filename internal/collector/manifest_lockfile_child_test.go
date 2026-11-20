// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// The lockfile pass behind the child's boundary (0.29.213).

// sortedLockfiles puts a lockfile set in one order: a JSON or YAML
// lockfile's entries come out of a map, so two readings of the same file
// differ in order and in nothing else.
func sortedLockfiles(set lockfileSet) lockfileSet {
	out := lockfileSet{WalkErr: set.WalkErr}
	for _, pl := range set.Parsed {
		res := *pl.Result
		res.Entries = append([]LockfileEntry(nil), res.Entries...)
		res.Edges = append([]LockfileEdge(nil), res.Edges...)
		sort.Slice(res.Entries, func(i, j int) bool { return fmt.Sprint(res.Entries[i]) < fmt.Sprint(res.Entries[j]) })
		sort.Slice(res.Edges, func(i, j int) bool { return fmt.Sprint(res.Edges[i]) < fmt.Sprint(res.Edges[j]) })
		if len(res.Entries) == 0 {
			res.Entries = nil
		}
		if len(res.Edges) == 0 {
			res.Edges = nil
		}
		out.Parsed = append(out.Parsed, parsedLockfile{Path: pl.Path, Result: &res})
	}
	sort.Slice(out.Parsed, func(i, j int) bool { return out.Parsed[i].Path < out.Parsed[j].Path })
	out.Failed = append([]model.ManifestFailure(nil), set.Failed...)
	sort.Slice(out.Failed, func(i, j int) bool { return out.Failed[i].Path < out.Failed[j].Path })
	return out
}

func lockfileSetsDiffer(a, b lockfileSet) string {
	a, b = sortedLockfiles(a), sortedLockfiles(b)
	if len(a.Parsed) != len(b.Parsed) {
		return fmt.Sprintf("%d lockfiles parsed against %d", len(a.Parsed), len(b.Parsed))
	}
	for i := range a.Parsed {
		if a.Parsed[i].Path != b.Parsed[i].Path || !reflect.DeepEqual(a.Parsed[i].Result, b.Parsed[i].Result) {
			return fmt.Sprintf("%s:\n in process %+v\n the child  %+v", a.Parsed[i].Path, a.Parsed[i].Result, b.Parsed[i].Result)
		}
	}
	if !reflect.DeepEqual(a.Failed, b.Failed) {
		return fmt.Sprintf("failed: %+v against %+v", a.Failed, b.Failed)
	}
	return ""
}

// The child's lockfile pass returns exactly what the in-process walk
// returns, for every format, for the malformed files, and for text that is
// not UTF-8. The clone holds every lockfile fixture under its roster name,
// each in a directory of its own, the malformed ones among them, and
// lockfiles that say things JSON cannot carry as they are.
func TestTheChildsLockfilesAreTheWalksLockfiles(t *testing.T) {
	files := map[string]string{}
	fixtures, err := os.ReadDir(filepath.Join("testdata", "lockfiles"))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, f := range fixtures {
		// "package-lock.v3.json" is a package-lock.json; "empty.poetry.lock"
		// a poetry.lock; "Package.resolved.v1" a Package.resolved.
		kind := ""
		for k := range lockfileKinds {
			name := f.Name()
			stem, ext := strings.TrimSuffix(k, filepath.Ext(k)), filepath.Ext(k)
			if name == k || strings.HasSuffix(name, "."+k) || (strings.HasPrefix(name, stem+".v") && strings.HasSuffix(name, ext)) || strings.HasPrefix(name, k+".v") {
				if len(k) > len(kind) {
					kind = k
				}
			}
		}
		if kind == "" {
			t.Fatalf("testdata/lockfiles/%s is of no roster kind: name it so that this corpus can place it", f.Name())
		}
		body, err := os.ReadFile(filepath.Join("testdata", "lockfiles", f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Join("fixtures", f.Name(), kind)] = string(body)
		kinds[kind] = true
	}
	for k := range lockfileKinds {
		if !kinds[k] {
			t.Errorf("no fixture of %s in testdata/lockfiles", k)
		}
	}
	// What a lockfile's author can write that a JSON string cannot hold as
	// it is, or that a path check must not mangle.
	files["odd/yarn.lock"] = "\"bad\xff\xfename@^1\":\n  version \"1.0.\x01\"\n  dependencies:\n    \"<child>&\" \"^2 \\u0000\"\n\n\"\x00nul@1\":\n  version \"2.0.0\"\n"
	files["odd/Gemfile.lock"] = "GEM\n  remote: https://rubygems.org/\n  specs:\n    r\xe9sum\xe9 (1.0.0)\n      \x00dep (>= 1)\n    plain (2.0.0)\n\nDEPENDENCIES\n  plain\n"
	files["odd/gradle.lockfile"] = "g:a\xc3:1.0=compileClasspath\ncom.example:lib:2.0=runtimeClasspath\n"
	files["odd dir \u00e9/sub/poetry.lock"] = "[[package]]\nname = \"a\"\nversion = \"1.0\"\ncategory = \"dev\"\n"
	files["aliases/package-lock.json"] = `{"lockfileVersion":3,"packages":{"":{"dependencies":{"x":"1"}},
		"node_modules/x":{"version":"1.0.0","dependencies":{"y":"^2"}},
		"node_modules/alias":{"version":"npm:real@3.0.0"},
		"node_modules/gitdep":{"version":"github:o/r#abc"},
		"node_modules/range":{"version":">= 2"},
		"node_modules/devonly":{"version":"4.0.0","dev":true}}}`
	files["empty/yarn.lock"] = ""
	files["empty/pnpm-lock.yaml"] = ""
	files["vendor/yarn.lock"] = "skipped@1:\n  version \"1.0.0\"\n"
	files["node_modules/p/package-lock.json"] = `{"lockfileVersion":3,"packages":{"node_modules/skipped":{"version":"1.0.0"}}}`
	dir := writeClone(t, files)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := collectLockfiles(dir, quiet)
	if want.WalkErr != nil || len(want.Parsed) < len(lockfileKinds) || len(want.Failed) < 2 {
		t.Fatalf("the corpus is not what this test needs: %d parsed, %d failed, %v", len(want.Parsed), len(want.Failed), want.WalkErr)
	}
	entries, edges, odd := 0, 0, 0
	for _, pl := range want.Parsed {
		entries += len(pl.Result.Entries)
		edges += len(pl.Result.Edges)
		for _, e := range pl.Result.Entries {
			if s := e.Name + e.Version; !utf8.ValidString(s) || strings.ContainsAny(s, "\x00\x01") {
				odd++
			}
		}
	}
	if entries < 60 || edges < 15 || odd < 3 {
		t.Fatalf("the corpus is thin: %d entries, %d edges, %d entries that are not plain text", entries, edges, odd)
	}
	for _, passes := range [][]string{{manifestPassLockfiles}, manifestPassesAll} {
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: passes, Dir: dir, RemotePomLimit: 5}, testLimits(), nil, quiet)
		if err != nil {
			t.Fatalf("%v: %v", passes, err)
		}
		if err := run.passErr(manifestPassLockfiles, nil); err != nil {
			t.Fatalf("%v: %v", passes, err)
		}
		if diff := lockfileSetsDiffer(want, *run.locks); diff != "" {
			t.Errorf("%v: the child's lockfiles are not the walk's: %s", passes, diff)
		}
	}
}

// One child does the three passes, and each is what its own child gives.
func TestOneChildReadsTheNamesTheDeclarationsAndTheLockfiles(t *testing.T) {
	dir := writeClone(t, map[string]string{
		"package.json":       `{"dependencies":{"left-pad":"^1.3.0"}}`,
		"requirements.txt":   "requests==2.31.0\n",
		"yarn.lock":          "left-pad@^1.3.0:\n  version \"1.3.0\"\n",
		"sub/pnpm-lock.yaml": "packages:\n  /a@1.0.0: {}\n",
		"bad/Pipfile.lock":   "{not json",
	})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	all, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, testLimits(), nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	for _, pass := range manifestPassesAll {
		one, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{pass}, Dir: dir, RemotePomLimit: 5}, testLimits(), nil, quiet)
		if err != nil || all.passErr(pass, nil) != nil {
			t.Fatalf("%s: %v, %v", pass, err, all.passErr(pass, nil))
		}
		switch pass {
		case manifestPassNames:
			if !reflect.DeepEqual(one.names, all.names) || len(all.names) == 0 {
				t.Errorf("names: %v against %v", all.names, one.names)
			}
		case manifestPassDeclarations:
			if !reflect.DeepEqual(one.decl, all.decl) || len(all.decl.Deps) != 2 {
				t.Errorf("declarations: %+v against %+v", all.decl, one.decl)
			}
		case manifestPassLockfiles:
			if diff := lockfileSetsDiffer(*one.locks, *all.locks); diff != "" || len(all.locks.Parsed) != 2 || len(all.locks.Failed) != 1 {
				t.Errorf("lockfiles: %s (%d parsed, %d failed)", diff, len(all.locks.Parsed), len(all.locks.Failed))
			}
		}
	}
	// A pass that was not asked for has no result, and says so.
	only, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassNames}, Dir: dir}, testLimits(), nil, quiet)
	if err != nil || only.passErr(manifestPassLockfiles, nil) == nil || only.locks != nil {
		t.Errorf("a pass not asked for: %v, %v", err, only.passErr(manifestPassLockfiles, nil))
	}
}

// What the parent holds a child's lockfiles to: each wrong thing is a named
// failure, and nothing the child sent is used.
func TestAChildThatMisbehavesWithLockfilesIsANamedFailure(t *testing.T) {
	dir := writeClone(t, nil)
	for mode, want := range map[string]string{
		"lock-outside":              "not one the roster has",
		"lock-not-its-kind":         "not one the roster has",
		"lock-unknown-kind":         "not one the roster has",
		"lock-ecosystem":            "not one the roster has",
		"lock-twice":                "not once",
		"lock-twice-in-one-piece":   "not once",
		"lock-count-fewer":          "do not add up",
		"pom-in-the-lockfiles":      "a child does not send",
		"lock-entry-of-none":        "belongs to no lockfile sent",
		"lock-edge-of-none":         "belongs to no lockfile sent",
		"lock-more-than-said":       "more entries than it said",
		"lock-more-edges-than-said": "more edges than it said",
		"lock-fewer-than-said":      "fewer entries or edges than it said",
		"lock-count":                "do not add up",
		"lock-negative":             "counts are not counts",
		"lock-binary-with-entries":  "counts are not counts",
		"lock-scope":                "a scope no parser gives",
		"lock-entry-ecosystem":      "an ecosystem its lockfile does not hold",
		"lock-failed-and-parsed":    "a failed lockfile is not one the pass records",
		"lock-failed-carrying":      "a failed lockfile is not one the pass records",
		"lock-failed-reason":        "a failed lockfile is not one the pass records",
		"lock-with-declarations":    "the lockfile pass sent manifest results",
	} {
		misbehaving(t, mode)
		// With a pom source at hand: poms are answered while the
		// declarations are read, and at no other time.
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassLockfiles}, Dir: dir, RemotePomLimit: 5},
			ManifestLimits{CPU: 20 * time.Second, HeapBytes: 8 << 20}, &mapPomSource{other: manifest.RemoteNotFound}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		var child *errManifestChild
		if run.anyFinished() || !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadFailed || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: a pass finished: %v, error %v; want no result and a failed read that says %q", mode, run.anyFinished(), err, want)
		}
	}
}

// A child stopped part-way leaves the passes it had finished whole, and the
// rest with the reason it was stopped; one that sent what this protocol does
// not send leaves nothing, the finished passes included.
func TestAStoppedChildLeavesItsFinishedPassesWhole(t *testing.T) {
	dir := writeClone(t, nil)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	limits := ManifestLimits{CPU: 20 * time.Second, HeapBytes: 8 << 20}
	req := manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}

	misbehaving(t, "two-passes-then-the-memory-limit")
	run, err := runManifestChild(context.Background(), req, limits, nil, quiet)
	var child *errManifestChild
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit {
		t.Fatalf("stopped at its memory limit in the lockfiles: %v", err)
	}
	if e := run.passErr(manifestPassNames, err); e != nil || run.names["Python"]["requests"] != 2 {
		t.Errorf("the names had finished: %v, %v", e, run.names)
	}
	if e := run.passErr(manifestPassDeclarations, err); e != nil || len(run.decl.Deps) != 1 {
		t.Errorf("the declarations had finished: %v", e)
	}
	if e := run.passErr(manifestPassLockfiles, err); !errors.As(e, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit || run.locks != nil {
		t.Errorf("the lockfiles had not finished, and half of one had arrived: error %v, lockfiles %v; want the memory limit and none", e, run.locks)
	}

	misbehaving(t, "one-pass-of-three")
	run, err = runManifestChild(context.Background(), req, limits, nil, quiet)
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadFailed {
		t.Fatalf("gone after the names: %v", err)
	}
	if run.passErr(manifestPassNames, err) != nil || run.passErr(manifestPassDeclarations, err) == nil || run.passErr(manifestPassLockfiles, err) == nil {
		t.Errorf("gone after the names: names %v, declarations %v, lockfiles %v", run.passErr(manifestPassNames, err), run.passErr(manifestPassDeclarations, err), run.passErr(manifestPassLockfiles, err))
	}

	misbehaving(t, "three-passes-then-rubbish")
	run, err = runManifestChild(context.Background(), req, limits, nil, quiet)
	if err == nil || run.anyFinished() || run.names != nil || run.decl != nil {
		t.Errorf("rubbish after two whole passes: error %v, finished %v; want nothing kept", err, run.anyFinished())
	}
	for _, pass := range manifestPassesAll {
		if run.passErr(pass, err) == nil {
			t.Errorf("rubbish after two whole passes: the %s stand", pass)
		}
	}
}

// A lockfile of a million packages crosses the wire, in pieces that each fit
// a frame, and arrives whole.
func TestALockfileOfAMillionPackagesCrosses(t *testing.T) {
	const packages = 1_000_000
	var b strings.Builder
	for i := 0; i < packages; i++ {
		fmt.Fprintf(&b, "com.example.group%d:artifact-%d:1.%d.0=compileClasspath,runtimeClasspath\n", i%977, i, i%50)
	}
	dir := writeClone(t, map[string]string{"gradle.lockfile": b.String()})
	run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, ManifestLimits{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	got := run.locks.Parsed
	if len(got) != 1 || len(got[0].Result.Entries) != packages {
		t.Fatalf("%d lockfiles; the first with %d entries, want one with %d", len(got), len(got[0].Result.Entries), packages)
	}
	if first, last := got[0].Result.Entries[0], got[0].Result.Entries[packages-1]; first.Name != "com.example.group0:artifact-0" || last.Version != fmt.Sprintf("1.%d.0", (packages-1)%50) {
		t.Errorf("the entries did not arrive in order: first %+v, last %+v", first, last)
	}
	t.Logf("a million packages: child CPU %s, heap peak %d MiB", run.cpu.Round(time.Millisecond), run.heapPeak>>20)
}

// The whole phase, for an analysis whose reader was stopped while it read
// the lockfiles (here by collection.manifest_read_memory_mb, from the
// configuration file, at a lockfile larger than the limit allows): the
// declarations it had already handed over are used and the inventory is
// replaced; no lockfile row changes; the inventory's rows go on reporting
// the versions those rows lock; and the coverage record says which limit.
func TestAnAnalysisWhoseLockfilesWereNotReadKeepsThemAndSaysWhy(t *testing.T) {
	f := lockFixture(t, "locks-unread")
	ctx := context.Background()
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	f.analyse(t)
	if got, rows := inventoryRows(t, f)["package.json:express"], lockfileRows(t, f); got.Version != "4.18.2" || len(rows) != 2 {
		t.Fatalf("the first analysis: express %+v, lockfile rows %+v", got, rows)
	}
	// Stored beside the lockfile's rows, as the Go build list's are: a row
	// that is no lockfile's, and locks nothing in the inventory.
	if _, err := f.store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.repo_lockfile_packages
		(repo_id, ecosystem, package_name, resolved_version, lockfile_path, direct, data_source, data_collection_date)
		VALUES ($1, 'go', 'example.com/m', 'v1.5.0', 'go.mod', TRUE, 'analysis', NOW()),
		       ($1, 'bioconductor', 'AAAbioc', '3.0', 'r/renv.lock', FALSE, 'analysis', NOW()),
		       ($1, 'cran', 'zoo', '1.8', 'r/renv.lock', FALSE, 'analysis', NOW())
		ON CONFLICT (repo_id, lockfile_path, package_name, resolved_version) DO NOTHING`, f.repoID); err != nil {
		t.Fatal(err)
	}
	rowsBefore := lockfileRows(t, f)
	// What lockfiles that went unread offer the inventory: the stored rows
	// of each lockfile, in its own ecosystem, and nothing else's.
	offered := map[string]string{}
	for _, lf := range f.ac.lockfilesInUse(ctx, f.repoID, lockfileSet{WalkErr: errors.New("unread")}) {
		var names []string
		for name := range lf.Versions {
			names = append(names, name)
		}
		sort.Strings(names)
		offered[lf.Path] = lf.Ecosystem + ": " + strings.Join(names, ",")
	}
	if want := map[string]string{"package-lock.json": "npm: npm|body-parser,npm|express", "r/renv.lock": "cran: cran|zoo"}; !reflect.DeepEqual(offered, want) {
		t.Errorf("unread lockfiles offer %v; want %v", offered, want)
	}
	f.write(t, "go.mod", "module example.com/app\n\ngo 1.21\n\nrequire example.com/m v1.0.0\n")

	// The next analysis meets a second manifest, and a lockfile that takes
	// more memory to read than the limit allows.
	f.write(t, "requirements.txt", "requests==2.31.0\n")
	var big strings.Builder
	for i := 0; big.Len() < 96<<20; i++ {
		fmt.Fprintf(&big, "com.example.group%d:artifact-%d:1.%d.0=compileClasspath,runtimeClasspath\n", i%977, i, i%50)
	}
	f.write(t, "big/gradle.lockfile", big.String())
	SetManifestLimits(limitsFromConfig(t, `"manifest_read_memory_mb": 64`))
	t.Cleanup(func() { installedManifestLimits.Store(nil) })

	read := f.ac.readClone(ctx, f.repoID, f.dir)
	var child *errManifestChild
	if !errors.As(read.runErr, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit {
		t.Fatalf("the reader: %v; want it stopped at the memory limit", read.runErr)
	}
	locks := read.lockfiles()
	if locks.WalkErr == nil || len(locks.Parsed)+len(locks.Failed) != 0 {
		t.Fatalf("the lockfiles of a reader stopped while reading them: %+v", locks)
	}
	if err := f.ac.scanDependencies(ctx, f.repoID, f.dir, read, &AnalysisResult{}); err != nil {
		t.Errorf("the names had been read: %v", err)
	}
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, read, f.ac.lockfilesInUse(ctx, f.repoID, locks), &AnalysisResult{}); err != nil {
		t.Fatalf("the declarations had been read: %v", err)
	}
	inv := inventoryRows(t, f)
	if _, replaced := inv["requirements.txt:requests"]; !replaced {
		t.Errorf("the inventory was not replaced: %v", inv)
	}
	if e := inv["package.json:express"]; e.Version != "4.18.2" || e.Source != versionuse.SourceLocked {
		t.Errorf("express = %+v; want it still at the version its stored lockfile rows lock", e)
	}
	if m, ok := inv["go.mod:example.com/m"]; !ok || m.Version != "v1.0.0" || m.Source == versionuse.SourceLocked {
		t.Errorf("the Go module = %+v (in the inventory: %v); want the version go.mod requires: the build list's stored row is no lockfile's", m, ok)
	}
	var log recorded
	quiet := f.ac.logger
	f.ac.logger = log.logger()
	err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{})
	f.ac.logger = quiet
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit || !strings.Contains(err.Error(), "collection.manifest_read_memory_mb allows (64 MiB)") {
		t.Errorf("the lockfile phase: %v; want the memory limit, by its setting and value", err)
	}
	if after := lockfileRows(t, f); !reflect.DeepEqual(after, rowsBefore) {
		t.Errorf("the lockfile rows changed: %+v, were %+v", after, rowsBefore)
	}
	// The inventory WAS replaced, so no attempt is recorded (a successful
	// replace leaves the attempt columns as they were, and none had been
	// recorded for this repository): the coverage record must not read as a
	// failed analysis over a fresh inventory. It is logged, at Error, with
	// the repository, the reason, the limit in force and what to do.
	cov, found, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil || !found || cov.LastAttemptResult != "" || cov.LastAttemptAt != nil {
		t.Errorf("the coverage record's last attempt is %q at %v (found %v, %v); want none: the inventory was replaced", cov.LastAttemptResult, cov.LastAttemptAt, found, err)
	}
	said := false
	for _, line := range log.lines {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "the lockfiles were not read") {
			said = true
			for _, want := range []string{fmt.Sprintf("repo_id=%d", f.repoID), "result=" + model.InventoryAttemptReadMemoryLimit, "raise collection.manifest_read_memory_mb", "manifest_read_memory_mb=64", "manifest_read_cpu_seconds=120"} {
				if !strings.Contains(line, want) {
					t.Errorf("the Error line lacks %q: %s", want, line)
				}
			}
		}
	}
	if !said {
		t.Errorf("no Error line says the lockfiles were not read: %q", log.lines)
	}

	// With room, the same clone is read whole.
	SetManifestLimits(limitsFromConfig(t, `"manifest_read_memory_mb": 2048`))
	roomy := f.ac.readClone(ctx, f.repoID, f.dir).lockfiles()
	if roomy.WalkErr != nil || len(roomy.Parsed) != 2 {
		t.Errorf("with room: %v, %d lockfiles", roomy.WalkErr, len(roomy.Parsed))
	}
}

// Which stored rows stand in for lockfiles that went unread: a lockfile's
// own, and not the Go build list's rows stored beside them, nor a
// renv.lock's Bioconductor packages.
func TestWhichStoredRowsLockADependency(t *testing.T) {
	for _, c := range []struct {
		row  model.RepoLockfilePackage
		want bool
	}{
		{model.RepoLockfilePackage{Ecosystem: "npm", LockfilePath: "web/package-lock.json"}, true},
		{model.RepoLockfilePackage{Ecosystem: "cran", LockfilePath: "renv.lock"}, true},
		{model.RepoLockfilePackage{Ecosystem: bioconductorEcosystem, LockfilePath: "renv.lock"}, false},
		{model.RepoLockfilePackage{Ecosystem: "go", LockfilePath: "go.mod"}, false},
		{model.RepoLockfilePackage{Ecosystem: "go", LockfilePath: "svc/go.mod"}, false},
		{model.RepoLockfilePackage{Ecosystem: "npm", LockfilePath: ""}, false},
	} {
		if got := storedRowLocksADependency(c.row); got != c.want {
			t.Errorf("%+v: %v, want %v", c.row, got, c.want)
		}
	}
}

// A shutdown while the clone is read is no attempt: nothing is recorded and
// every phase returns the cancellation.
func TestAShutdownWhileTheCloneIsReadRecordsNothing(t *testing.T) {
	f := lockFixture(t, "locks-shutdown")
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	read := f.ac.readClone(ctx, f.repoID, f.dir)
	locks := read.lockfiles()
	for phase, err := range map[string]error{
		"dependencies": f.ac.scanDependencies(ctx, f.repoID, f.dir, read, &AnalysisResult{}),
		"lockfiles":    f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{}),
	} {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s: %v; want the cancellation", phase, err)
		}
	}
	if _, found, err := f.store.GetRepoDependencyCoverage(context.Background(), f.repoID); err != nil || found {
		t.Errorf("a shutdown left a coverage record (found %v, %v)", found, err)
	}
}

// collectLockfiles is the lockfile pass run here, in this process, and
// gathered: what the tests of the walk and of the lockfile phase read with,
// and what the child's result is compared against.
func collectLockfiles(workDir string, logger *slog.Logger) lockfileSet {
	var out []parsedLockfile
	failed, walkErr := walkLockfiles(workDir, logger, func(pl parsedLockfile) { out = append(out, pl) })
	return lockfileSet{Parsed: out, Failed: failed, WalkErr: walkErr}
}

// What reads a clone's files is called from the child's serving functions
// and from nowhere else in the program: the analysis has only what the child
// sent (TestAnAnalysisWhoseLockfilesWereNotReadKeepsThemAndSaysWhy and
// TestAnAnalysisStoppedAtTheLimitKeepsItsInventoryAndSaysWhy drive that; this
// names the next call that would read a clone in the worker).
func TestTheCloneIsReadOnlyByTheChild(t *testing.T) {
	sources := srctest.PackageFiles(t, "internal/collector", 40)
	for call, home := range map[string]string{
		"readManifestNames(":        "func (c *manifestChild) serveNames(",
		"readManifestDeclarations(": "func (c *manifestChild) serveDeclarations(",
		"walkLockfiles(":            "func (c *manifestChild) serveLockfiles(",
		"ParseLockfile(":            "func walkLockfiles(",
		"manifest.NewWalk(":         "func readManifestDeclarations(",
	} {
		inHome := strings.Count(srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", home)), call)
		total := 0
		for _, src := range sources {
			code := srctest.StripGoComments(src)
			total += strings.Count(code, call) - strings.Count(code, "func "+call)
		}
		if inHome < 1 || total != inHome {
			t.Errorf("%s is called %d times in internal/collector, %d of them in %s: a clone's files are read by the child, in that function alone", call, total, inHome, home)
		}
	}
	analyze := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (ac *AnalysisCollector) AnalyzeRepo("))
	if strings.Count(analyze, "ac.readClone(") != 1 || !strings.Contains(analyze, "read.lockfiles()") {
		t.Error("AnalyzeRepo reads its clone once, through readClone, and takes the lockfiles from that reading")
	}
}

// The program on disk is another version than the one running (a `go
// install`, or an upgrade, under a serve that was not stopped): the real
// child refuses the request, by name; nothing is read and nothing stored;
// the attempt is recorded as a failed read; and the process says what to do
// at Error ONCE, however many analyses meet it.
func TestAChildOfAnotherVersionIsRefusedAndSaidOnce(t *testing.T) {
	f := lockFixture(t, "other-version")
	ctx := context.Background()
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	if err := f.ac.scanDependencies(ctx, f.repoID, f.dir, f.ac.readClone(ctx, f.repoID, f.dir), &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	f.analyse(t)
	inventory, names, locks := f.inventory(t), f.dependencyNames(t), lockfileRows(t, f)
	if len(inventory) != 1 || len(names) != 1 || len(locks) != 2 {
		t.Fatalf("the first analysis stored %v, %v, %v", inventory, names, locks)
	}

	// The running program is now "another version" than the file on disk.
	const running = "0.0.1-running"
	saved := manifestParentVersion
	manifestParentVersion = running
	manifestStaleSaid.Store(false)
	t.Cleanup(func() { manifestParentVersion = saved; manifestStaleSaid.Store(false) })
	f.write(t, "requirements.txt", "requests==2.31.0\n")
	var log recorded
	quiet := f.ac.logger
	f.ac.logger = log.logger()
	t.Cleanup(func() { f.ac.logger = quiet })
	for analysis := 0; analysis < 2; analysis++ {
		read := f.ac.readClone(ctx, f.repoID, f.dir)
		var child *errManifestChild
		want := fmt.Sprintf("is version %s, this process is version %s: restart serve", db.ToolVersion, running)
		if !errors.As(read.runErr, &child) || !child.Stale || child.Attempt != model.InventoryAttemptReadFailed || !strings.Contains(read.runErr.Error(), want) || read.run.anyFinished() {
			t.Fatalf("the reader: %v; want a failed read that says %q, and nothing read", read.runErr, want)
		}
		result := &AnalysisResult{}
		lockSet := read.lockfiles()
		for phase, err := range map[string]error{
			"dependencies": f.ac.scanDependencies(ctx, f.repoID, f.dir, read, result),
			"libyear":      f.ac.scanLibyear(ctx, f.repoID, f.dir, read, f.ac.lockfilesInUse(ctx, f.repoID, lockSet), result),
			"lockfiles":    f.ac.scanLockfiles(ctx, f.repoID, f.dir, lockSet, result),
		} {
			if !errors.As(err, &child) || !child.Stale {
				t.Errorf("%s: %v; want the version refusal", phase, err)
			}
			result.Errors = append(result.Errors, err)
		}
		logAnalysisPhaseErrors(f.ac.logger, f.repoID, result.Errors)
	}
	if a, b, c := f.inventory(t), f.dependencyNames(t), lockfileRows(t, f); !reflect.DeepEqual(a, inventory) || !reflect.DeepEqual(b, names) || !reflect.DeepEqual(c, locks) {
		t.Errorf("something was stored: inventory %v, names %v, lockfile rows %v", a, b, c)
	}
	if cov, found := f.coverage(t); !found || cov.LastAttemptResult != model.InventoryAttemptReadFailed {
		t.Errorf("the coverage record says %q (found %v); want %q", cov.LastAttemptResult, found, model.InventoryAttemptReadFailed)
	}
	// Two analyses, three phases each: one line at Error, none at Warn.
	var loud []string
	for _, line := range log.lines {
		if strings.Contains(line, "level=ERROR") || strings.Contains(line, "level=WARN") {
			loud = append(loud, line)
		}
	}
	if len(loud) != 1 || !strings.Contains(loud[0], "level=ERROR") {
		t.Fatalf("two analyses logged %d lines at Warn or Error; want one, at Error:\n%s", len(loud), strings.Join(loud, "\n"))
	}
	for _, want := range []string{"binary_version=" + db.ToolVersion, "running_version=" + running, "restart serve", "binary="} {
		if !strings.Contains(loud[0], want) {
			t.Errorf("the Error line lacks %q: %s", want, loud[0])
		}
	}
}

func TestWhatToDoAboutLockfilesThatWentUnread(t *testing.T) {
	for attempt, want := range map[string]string{
		model.InventoryAttemptReadMemoryLimit: "raise collection.manifest_read_memory_mb",
		model.InventoryAttemptReadCPULimit:    "raise collection.manifest_read_cpu_seconds",
		model.InventoryAttemptReadTimedOut:    "raise collection.manifest_read_cpu_seconds",
		model.InventoryAttemptReadFailed:      "see the error",
	} {
		if got := lockfilesUnreadAdvice(attempt); !strings.Contains(got, want) {
			t.Errorf("%s: %q; want it to say %q", attempt, got, want)
		}
	}
}
