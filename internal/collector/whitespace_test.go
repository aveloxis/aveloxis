// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.105 — whitespace measurement (Workstream C of the 2026-08-19
// fill audit). commits.cmt_whitespace was 0% populated while
// SUM(cmt_whitespace) feeds six live aggregate queries. The walker
// implements Augur's algorithm (analyzecommit.py) over
// `git log --numstat -p`:
//   - an added line whose content strips to empty → whitespace (not added)
//   - an added line whose stripped content equals a just-removed line's
//     stripped content AND len > 8 → whitespace-only reformat:
//     removed--, whitespace++ (not added)
//   - otherwise added++
//
// Filenames come from the NUMSTAT block (paired positionally with the
// patch sections) so the UPDATE join hits the exact cmt_filename the
// facade stored — including git's arrow/brace rename forms that a
// patch-derived path could never reproduce.
package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// buildWhitespaceFixture crafts a git-log-style output stream
// (--format=%x1e%H --numstat -p) without needing git.
func fixtureLog(parts ...string) io.Reader {
	return strings.NewReader(strings.Join(parts, "\n") + "\n")
}

func collectStats(t *testing.T, r io.Reader) map[string]map[string]whitespaceFileStat {
	t.Helper()
	out := map[string]map[string]whitespaceFileStat{}
	err := parseWhitespaceLog(r, func(c whitespaceCommit) error {
		m := map[string]whitespaceFileStat{}
		for _, f := range c.Files {
			m[f.Filename] = f
		}
		out[c.Hash] = m
		return nil
	})
	if err != nil {
		t.Fatalf("parseWhitespaceLog: %v", err)
	}
	return out
}

func TestWhitespaceBlankAddedLine(t *testing.T) {
	stats := collectStats(t, fixtureLog(
		"\x1eaaaa000000000000000000000000000000000000",
		"3\t0\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"new file mode 100644",
		"--- /dev/null",
		"+++ b/a.txt",
		"@@ -0,0 +1,3 @@",
		"+line one content",
		"+",
		"+line two content",
	))
	f := stats["aaaa000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 2 || f.Removed != 0 || f.Whitespace != 1 {
		t.Fatalf("blank-added: got added=%d removed=%d ws=%d, want 2/0/1", f.Added, f.Removed, f.Whitespace)
	}
}

func TestWhitespaceReformatPair(t *testing.T) {
	// Reindent of a >8-char line: Augur counts removed--, whitespace++.
	stats := collectStats(t, fixtureLog(
		"\x1ebbbb000000000000000000000000000000000000",
		"1\t1\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,1 +1,1 @@",
		"-  a long enough content line",
		"+      a long enough content line",
	))
	f := stats["bbbb000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 0 || f.Removed != 0 || f.Whitespace != 1 {
		t.Fatalf("reformat: got added=%d removed=%d ws=%d, want 0/0/1", f.Added, f.Removed, f.Whitespace)
	}
}

func TestWhitespaceShortPairIsNotReformat(t *testing.T) {
	// len <= 8 stripped content never matches the reformat rule
	// (Augur's guard against trivial-line false positives).
	stats := collectStats(t, fixtureLog(
		"\x1ecccc000000000000000000000000000000000000",
		"1\t1\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,1 +1,1 @@",
		"-  x = 1",
		"+      x = 1",
	))
	f := stats["cccc000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 1 || f.Removed != 1 || f.Whitespace != 0 {
		t.Fatalf("short pair: got added=%d removed=%d ws=%d, want 1/1/0", f.Added, f.Removed, f.Whitespace)
	}
}

func TestWhitespaceRenameUsesNumstatFilename(t *testing.T) {
	// The stored cmt_filename for renames is numstat's arrow form —
	// the walker must key file stats by the NUMSTAT name (positional
	// pairing), never the patch's "+++ b/" path.
	stats := collectStats(t, fixtureLog(
		"\x1edddd000000000000000000000000000000000000",
		"1\t0\tdir/{old.txt => new.txt}",
		"",
		"diff --git a/dir/old.txt b/dir/new.txt",
		"similarity index 90%",
		"rename from dir/old.txt",
		"rename to dir/new.txt",
		"--- a/dir/old.txt",
		"+++ b/dir/new.txt",
		"@@ -1,0 +2,1 @@",
		"+an appended content line",
	))
	f, ok := stats["dddd000000000000000000000000000000000000"]["dir/{old.txt => new.txt}"]
	if !ok {
		t.Fatal("rename stats must be keyed by the numstat arrow filename (the stored cmt_filename)")
	}
	if f.Added != 1 {
		t.Fatalf("rename: got added=%d, want 1", f.Added)
	}
}

func TestWhitespaceBinaryAndMultiCommit(t *testing.T) {
	stats := collectStats(t, fixtureLog(
		"\x1eeeee000000000000000000000000000000000000",
		"-\t-\tblob.bin",
		"1\t0\ta.txt",
		"",
		"diff --git a/blob.bin b/blob.bin",
		"Binary files a/blob.bin and b/blob.bin differ",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,0 +2,1 @@",
		"+content line",
		"\x1effff000000000000000000000000000000000000",
		"0\t1\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -2,1 +1,0 @@",
		"-content line",
	))
	if f := stats["eeee000000000000000000000000000000000000"]["blob.bin"]; f.Added != 0 || f.Whitespace != 0 {
		t.Fatalf("binary file must count 0/0/0, got %+v", f)
	}
	if f := stats["eeee000000000000000000000000000000000000"]["a.txt"]; f.Added != 1 {
		t.Fatalf("first commit a.txt: got %+v", f)
	}
	if f := stats["ffff000000000000000000000000000000000000"]["a.txt"]; f.Removed != 1 || f.Added != 0 {
		t.Fatalf("second commit a.txt: got %+v", f)
	}
}

// --- wiring pins ------------------------------------------------------------

func TestFacadeRunsWhitespacePhase(t *testing.T) {
	src, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "runWhitespacePhase(") {
		t.Error("CollectRepo must run the whitespace phase after parseGitLog")
	}
	ws, err := os.ReadFile("whitespace.go")
	if err != nil {
		t.Fatalf("whitespace.go must exist: %v", err)
	}
	w := string(ws)
	// Incremental on a stamped marker, full walk when the marker is
	// empty; a vanished marker (force-push) falls back to full.
	if !strings.Contains(w, "GetWhitespaceHead(") || !strings.Contains(w, "SetWhitespaceHead(") {
		t.Error("whitespace phase must read/stamp repos.whitespace_head_hash")
	}
	if !strings.Contains(w, "--numstat") || !strings.Contains(w, "-p") {
		t.Error("the walker must run git log with --numstat AND -p (positional filename pairing)")
	}
}

// --- fixture-repo end-to-end (gated on AVELOXIS_TEST_DB; needs git) ---------

func TestWhitespaceWalkEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	// Build a real git repo with hand-computable expectations.
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=WS Test", "GIT_AUTHOR_EMAIL=ws@test.example",
			"GIT_COMMITTER_NAME=WS Test", "GIT_COMMITTER_EMAIL=ws@test.example",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-b", "main")
	// Commit 1: two content lines + one blank → Augur: added=2, ws=1.
	if err := os.WriteFile(filepath.Join(dir, "a.txt"),
		[]byte("first content line\n\nsecond content line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "a.txt")
	run("commit", "-m", "c1")
	// Commit 2: reindent line 1 (>8 chars stripped) → added=0, removed=0, ws=1.
	if err := os.WriteFile(filepath.Join(dir, "a.txt"),
		[]byte("    first content line\n\nsecond content line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "a.txt")
	run("commit", "-m", "c2")

	fc := NewFacadeCollector(store, logger, t.TempDir())
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGenericGit,
		GitURL:   "https://fixture.invalid/avwstest/repo",
		Owner:    "avwstest", Name: "repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commit_parents WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.commits WHERE repo_id=$1`, repoID)
		_, _ = store.Pool().Exec(cctx, `DELETE FROM aveloxis_data.repos WHERE repo_id=$1`, repoID)
	})

	// Numstat pass first (the rows the walker updates), then the walk.
	result := &FacadeResult{}
	if err := fc.parseGitLog(ctx, repoID, dir, result); err != nil {
		t.Fatalf("parseGitLog: %v", err)
	}
	updated, head, err := fc.runWhitespaceWalk(ctx, repoID, dir, "")
	if err != nil {
		t.Fatalf("runWhitespaceWalk: %v", err)
	}
	if updated == 0 || head == "" {
		t.Fatalf("walk returned updated=%d head=%q", updated, head)
	}

	type row struct{ added, removed, ws int }
	rows := map[string]row{}
	rws, err := store.Pool().Query(ctx, `
		SELECT m.cmt_msg, COALESCE(cmt_added,0), COALESCE(cmt_removed,0), COALESCE(cmt_whitespace,0)
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.commit_messages m
		  ON m.repo_id = c.repo_id AND m.cmt_hash = c.cmt_commit_hash
		WHERE c.repo_id=$1 AND c.cmt_filename='a.txt'`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rws.Next() {
		var msg string
		var r row
		if err := rws.Scan(&msg, &r.added, &r.removed, &r.ws); err != nil {
			t.Fatal(err)
		}
		rows[strings.TrimSpace(msg)] = r
	}
	rws.Close()
	if r := rows["c1"]; r.added != 2 || r.removed != 0 || r.ws != 1 {
		t.Errorf("c1: got %+v, want added=2 removed=0 ws=1 (blank added line is whitespace)", r)
	}
	if r := rows["c2"]; r.added != 0 || r.removed != 0 || r.ws != 1 {
		t.Errorf("c2: got %+v, want 0/0/1 (reindent of a >8-char line is a whitespace reformat)", r)
	}

	// Marker stamped → a second walk with the marker range is a no-op.
	var marker string
	if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(whitespace_head_hash,'') FROM aveloxis_data.repos WHERE repo_id=$1`, repoID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != head {
		t.Fatalf("marker not stamped: %q vs head %q", marker, head)
	}
}

// v0.27.106 (PR #184 review, finding 4): readCappedLine is built on
// ReadSlice fragments so a single mega-line (minified bundle, data
// blob) never materializes in memory — ReadString would have allocated
// the whole physical line before truncating. This drives a line larger
// than the bufio buffer (1 MiB) through the parser and expects clean,
// correctly-counted output.
func TestWhitespaceMegaLineIsCappedNotAllocated(t *testing.T) {
	mega := "+" + strings.Repeat("x", 3*1024*1024) // 3 MiB added line
	stats := collectStats(t, fixtureLog(
		"\x1eabab000000000000000000000000000000000000",
		"2\t0\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -0,0 +1,2 @@",
		mega,
		"+normal content line",
	))
	f := stats["abab000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 2 || f.Whitespace != 0 {
		t.Fatalf("mega-line file: got added=%d ws=%d, want 2/0", f.Added, f.Whitespace)
	}
}

func TestWhitespaceDuplicateRemovalsConsumeOncePerOccurrence(t *testing.T) {
	// v0.27.113 (Copilot round 9): wsCheck became an occurrence-count
	// multiset (map[string]int) for O(1) reformat lookup. This pins the
	// MULTISET semantics the old list had: two identical removed lines
	// satisfy exactly two matching additions; a third identical addition
	// finds the multiset exhausted and counts as Added. A future
	// "simplification" to a set (map[string]bool) breaks this — the
	// second addition would stop matching (or over-match), diverging
	// from Augur's list-consume behavior.
	stats := collectStats(t, fixtureLog(
		"\x1effff000000000000000000000000000000000000",
		"3\t2\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,2 +1,3 @@",
		"-  duplicated long content line",
		"-  duplicated long content line",
		"+      duplicated long content line",
		"+      duplicated long content line",
		"+      duplicated long content line",
	))
	f := stats["ffff000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 1 || f.Removed != 0 || f.Whitespace != 2 {
		t.Fatalf("duplicate reformat: got added=%d removed=%d ws=%d, want 1/0/2", f.Added, f.Removed, f.Whitespace)
	}
}

func TestWhitespaceShortRunePairIsNotReformat(t *testing.T) {
	// v0.27.117 (Copilot round 11, suppressed): Augur's Python len()
	// counts CODE POINTS; Go's len() counts bytes. "ドイツ語です" is 6
	// runes (Augur: fails the >8 guard) but 18 bytes (a byte-length
	// check would pass it and diverge from Augur's numbers). The pair
	// below must count as add+remove, NOT as a whitespace reformat.
	stats := collectStats(t, fixtureLog(
		"\x1eeeee000000000000000000000000000000000000",
		"1\t1\ta.txt",
		"",
		"diff --git a/a.txt b/a.txt",
		"--- a/a.txt",
		"+++ b/a.txt",
		"@@ -1,1 +1,1 @@",
		"-  ドイツ語です",
		"+      ドイツ語です",
	))
	f := stats["eeee000000000000000000000000000000000000"]["a.txt"]
	if f.Added != 1 || f.Removed != 1 || f.Whitespace != 0 {
		t.Fatalf("short rune pair: got added=%d removed=%d ws=%d, want 1/1/0", f.Added, f.Removed, f.Whitespace)
	}
}
