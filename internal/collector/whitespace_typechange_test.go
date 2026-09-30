// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestWhitespaceTypeChangeKeepsFilesAligned — v0.29.71 (the kate log,
// 2026-09-30: LadybirdBrowser/ladybird's whitespace phase refused "1 of
// 431553 stats matched no stored commit row"). A file whose type changes
// (a symlink replaced by a regular file) is ONE numstat line but TWO patch
// sections (a delete and a new file), so the walker's positional pairing
// shifted every later file by one: their adjusted counts were written to
// the previous file's row, and only the last file (falling back to its
// plain b-path) went unmatched. Real git output, not a fixture.
func TestWhitespaceTypeChangeKeepsFilesAligned(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "core.symlinks=true"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q")
	write("a.md", "one\ntwo\n")
	if err := os.Symlink("a.md", filepath.Join(dir, "m.md")); err != nil {
		t.Skip("symlinks unsupported here")
	}
	write("z.md", "zeta\n")
	git("add", "-A")
	git("commit", "-q", "-m", "one")
	if err := os.Remove(filepath.Join(dir, "m.md")); err != nil {
		t.Fatal(err)
	}
	write("m.md", "first line\nsecond line\nthird line\n")
	write("z.md", "zeta\nanother line here\n")
	git("add", "-A")
	git("commit", "-q", "-m", "two")
	head := strings.TrimSpace(git("rev-parse", "HEAD"))

	logOut := git(append([]string{"-c", "diff.noprefix=true"}, whitespaceLogArgs(dir, "HEAD^!")...)...) // the walk's own flags (review C4); a host noprefix must not matter
	stats := collectStats(t, bytes.NewReader([]byte(logOut)))[head]
	if m := stats["m.md"]; m.Added != 3 || m.Removed != 1 {
		t.Errorf("m.md (the type change) = +%d -%d, want +3 -1 (numstat's one line)", m.Added, m.Removed)
	}
	if z := stats["z.md"]; z.Added != 1 || z.Removed != 0 {
		t.Errorf("z.md = +%d -%d, want +1 -0 — a shifted pairing gives it m.md's counts", z.Added, z.Removed)
	}
	if len(stats) != 2 {
		t.Errorf("want one stat per numstat line (2), got %d: %+v", len(stats), stats)
	}
}

// TestWhitespaceMisalignedPairingFails — the guard for any shape not
// handled: a patch section whose path is not its numstat name fails the
// walk, naming the commit, instead of writing counts to the wrong rows.
func TestWhitespaceMisalignedPairingFails(t *testing.T) {
	err := parseWhitespaceLog(fixtureLog(
		"\x1eaaaa000000000000000000000000000000000000",
		"1\t0\ta.txt",
		"1\t0\tb.txt",
		"",
		"diff --git a/b.txt b/b.txt",
		"--- a/b.txt",
		"+++ b/b.txt",
		"@@ -1,0 +2,1 @@",
		"+bee",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,0 +2,1 @@",
		"+ay",
	), func(whitespaceCommit) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "aaaa000000000000000000000000000000000000") {
		t.Errorf("a misaligned commit must fail the walk and name the commit, got %v", err)
	}
	// Renames match through their arrow form, spaces and all.
	if err := parseWhitespaceLog(fixtureLog(
		"\x1ebbbb000000000000000000000000000000000000",
		"1\t0\tdir/{old name.txt => new name.txt}",
		"0\t0\t{x => y}/f.txt",
		"0\t0\tp.txt => q/p.txt",
		"",
		"diff --git a/dir/old name.txt b/dir/new name.txt",
		"@@ -1,0 +2,1 @@",
		"+line",
		"diff --git a/x/f.txt b/y/f.txt",
		"similarity index 100%",
		"diff --git a/p.txt b/q/p.txt",
		"similarity index 100%",
	), func(whitespaceCommit) error { return nil }); err != nil {
		t.Errorf("renames must pair with their arrow-form numstat names: %v", err)
	}
}

// TestWhitespacePairsEveryNumstatNameShape — v0.29.71 review: the pairing
// check rejected correct pairings, which fails the walk forever (worse than
// the positional pairing it guards). Real git names: a rename whose sides
// git quotes separately (non-ASCII, a quote character) with no brace form,
// and files whose NAMES contain " => " or "{p => q}" (not renames). Each
// commit must parse with its stats under the numstat names.
func TestWhitespacePairsEveryNumstatNameShape(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "core.quotePath=true"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	write := func(name, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q")
	write("é.md", "a shared first line of text\nsecond\n")
	write(`dd/q"x`, "another shared line here\nsecond\n")
	write("a => b.txt", "arrow file\n")
	write("{p => q}", "brace file\n")
	// Review round 2: braces in a directory NAME before git's own brace
	// form (cookiecutter/copier templates), both empty-side brace forms
	// (the doubled-slash collapse), and two brace pairs in one rename.
	write("{{x}}/src/app.py", "template module\n")
	write("p/f1", "moves down a level\n")
	write("p/q2/f2", "moves up a level\n")
	write("d/w/f", "dir and file rename\n")
	write("x/a}b", "a closing brace in the name\n")
	write("d2/a => b", "an arrow in the old name\n")
	write("d3/abc/f", "bytes shared past the last shared slash\n")
	git("add", "-A")
	git("commit", "-q", "-m", "one")
	git("mv", "é.md", "ü.md")
	git("mv", `dd/q"x`, "dd/qq")
	git("mv", "{{x}}/src/app.py", "{{x}}/src/main.py")
	if err := os.MkdirAll(filepath.Join(dir, "p", "q"), 0o755); err != nil {
		t.Fatal(err)
	}
	git("mv", "p/f1", "p/q/f1")
	git("mv", "p/q2/f2", "p/f2")
	if err := os.MkdirAll(filepath.Join(dir, "d", "v"), 0o755); err != nil {
		t.Fatal(err)
	}
	git("mv", "d/w/f", "d/v/g")
	git("mv", "x/a}b", "x/c}b")
	git("mv", "d2/a => b", "d2/c")
	if err := os.MkdirAll(filepath.Join(dir, "d3", "abd"), 0o755); err != nil {
		t.Fatal(err)
	}
	git("mv", "d3/abc/f", "d3/abd/f")
	write("a => b.txt", "arrow file\nmore\n")
	write("{p => q}", "brace file\nmore\n")
	git("add", "-A")
	git("commit", "-q", "-m", "two")

	logOut := git(append([]string{"-c", "diff.noprefix=true"}, whitespaceLogArgs(dir, "HEAD^!")...)...) // the walk's own flags (review C4); a host noprefix must not matter
	var got map[string]whitespaceFileStat
	err := parseWhitespaceLog(bytes.NewReader([]byte(logOut)), func(c whitespaceCommit) error {
		got = map[string]whitespaceFileStat{}
		for _, f := range c.Files {
			got[f.Filename] = f
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a correct pairing was rejected: %v\n%s", err, logOut)
	}
	if len(got) != 11 {
		t.Errorf("want 11 stats, got %d: %+v", len(got), got)
	}
	if f := got["a => b.txt"]; f.Added != 1 {
		t.Errorf(`"a => b.txt" (a file name, not a rename) = %+v, want +1`, f)
	}
	if f := got["{p => q}"]; f.Added != 1 {
		t.Errorf(`"{p => q}" (a file name) = %+v, want +1`, f)
	}
}

// TestNumstatNameMatchIsNotCubic — review round 3: the brace readings were
// every "{", " => ", "}" triple, all built before any was compared, so one
// hostile path of repeated "{ => }" (git limits no tree path length, and a
// bare-clone walk never touches the filesystem) made the walker allocate
// hundreds of GB. The brace form is now rendered from the header's paths the
// way git renders it (pprint_rename) and compared, so the work is about
// linear in the name. A 4 KB adversarial name must stay small.
func TestNumstatNameMatchIsNotCubic(t *testing.T) {
	name := strings.Repeat("{ => }", 4096/6)
	header := "a/" + name + " b/" + name
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ok := numstatNameMatchesHeader(name, header)
	runtime.ReadMemStats(&after)
	if !ok {
		t.Error("a literal name (not a rename) must match its own header")
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 16<<20 {
		t.Errorf("matching a 4 KB name allocated %d bytes, want under 16 MiB (the triple enumeration allocated tens of GB)", got)
	}
	if numstatNameMatchesHeader(name, "a/x b/y") {
		t.Error("a 4 KB name must not match an unrelated header")
	}

	// Review round 4: a path of many " b/" components makes EVERY space a
	// valid header split; rendering a string per split made that quadratic
	// (a 4 KB non-match allocated 24 MB). Matching and non-matching.
	bpath := "x/" + strings.Repeat(" b/", 4096/3) + "f"
	bheader := "a/" + bpath + " b/" + bpath
	runtime.GC()
	runtime.ReadMemStats(&before)
	okMatch := numstatNameMatchesHeader(bpath, bheader)
	noMatch := numstatNameMatchesHeader("unrelated.txt", bheader)
	runtime.ReadMemStats(&after)
	if !okMatch || noMatch {
		t.Errorf("the \" b/\"-component path: match %v (want true), unrelated %v (want false)", okMatch, noMatch)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 16<<20 {
		t.Errorf("a 4 KB \" b/\"-component header allocated %d bytes, want under 16 MiB", got)
	}
}

// TestGitRenameMatchesIsExact — review round 5: a name that is git's
// rendering plus trailing bytes is another file's name, not this pairing's.
func TestGitRenameMatchesIsExact(t *testing.T) {
	header := "a/d/f b/dd/f"
	if !numstatNameMatchesHeader("{d => dd}/f", header) {
		t.Error(`"{d => dd}/f" must match "a/d/f b/dd/f"`)
	}
	for _, name := range []string{"{d => dd}/f4", "x{d => dd}/f", "{d => dd}/"} {
		if numstatNameMatchesHeader(name, header) {
			t.Errorf("%q must not match %q", name, header)
		}
	}
}

// TestWhitespaceWalkArgsIgnoreHostDiffConfig — v0.29.71 whole-branch review
// (C2, C4): host git config beyond the prefixes changes the -p layout the
// pairing check reads. diff.submodule=log prints a submodule change with no
// "diff --git" header, so every later section paired with the wrong
// numstat line and the walk failed on every cycle for any repository with
// a submodule in its history (a failure stamps no marker); color.ui=always
// hides every header, so nothing matched and the marker was stamped over
// nothing; a textconv driver rewrites the -p lines while numstat still
// counts the file's own (fix review V4: an external diff tool, named here
// first, is inert for git log without --ext-diff). The walk's own argument
// list (whitespaceLogArgs) must neutralise all of them.
func TestWhitespaceWalkArgsIgnoreHostDiffConfig(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	run := func(extra []string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append(extra, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return string(out)
	}
	base := []string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com"}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run(base, "init", "-q")
	write(".gitattributes", "*.txt diff=dbl\n")
	write("a.txt", "one\n")
	write("z.txt", "zeta\n")
	run(base, "add", ".gitattributes", "a.txt", "z.txt")
	run(base, "commit", "-q", "-m", "one")
	first := strings.TrimSpace(run(base, "rev-parse", "HEAD"))
	// A submodule link between two ordinary files in path order.
	run(base, "update-index", "--add", "--cacheinfo", "160000,"+first+",m-sub")
	write("a.txt", "one\ntwo\n")
	write("z.txt", "zeta\neta\ntheta\n")
	run(base, "add", "a.txt", "z.txt")
	run(base, "commit", "-q", "-m", "two")
	head := strings.TrimSpace(run(base, "rev-parse", "HEAD"))

	hostile := []string{"-c", "diff.submodule=log", "-c", "color.ui=always", "-c", "diff.noprefix=true", "-c", "diff.dbl.textconv=sed p"}
	out := run(hostile, whitespaceLogArgs(dir, "HEAD")...)
	got := map[string]map[string]whitespaceFileStat{}
	err := parseWhitespaceLog(strings.NewReader(out), func(c whitespaceCommit) error {
		got[c.Hash] = map[string]whitespaceFileStat{}
		for _, f := range c.Files {
			got[c.Hash][f.Filename] = f
		}
		return nil
	})
	if err != nil {
		t.Fatalf("host diff config broke the walk: %v\n%q", err, out)
	}
	if a := got[head]["a.txt"]; a.Added != 1 || a.Removed != 0 {
		t.Errorf("a.txt = +%d -%d, want +1 -0", a.Added, a.Removed)
	}
	if z := got[head]["z.txt"]; z.Added != 2 || z.Removed != 0 {
		t.Errorf("z.txt = +%d -%d, want +2 -0 (a shifted pairing gives it another file's counts)", z.Added, z.Removed)
	}
	if len(got) != 2 {
		t.Errorf("want both commits walked, got %d", len(got))
	}
}
