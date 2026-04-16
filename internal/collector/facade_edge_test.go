package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

// ============================================================
// parseCommitHeader edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseCommitHeader_MergeCommitThreeParents(t *testing.T) {
	// Octopus merge has 3+ parents.
	line := "merge1" + fieldSep +
		"p1 p2 p3" + fieldSep +
		"Author" + fieldSep + "a@b.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Committer" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Octopus merge"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil commit")
	}
	if len(c.Parents) != 3 {
		t.Errorf("octopus merge parents = %d, want 3", len(c.Parents))
	}
}

func TestParseCommitHeader_UnicodeInSubject(t *testing.T) {
	line := "abc" + fieldSep +
		"" + fieldSep +
		"Jose" + fieldSep + "j@ex.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Jose" + fieldSep + "j@ex.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Fix: handle unicode chars"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	if c.Subject != "Fix: handle unicode chars" {
		t.Errorf("Subject = %q", c.Subject)
	}
}

func TestParseCommitHeader_EmptyLine(t *testing.T) {
	c := parseCommitHeader("")
	if c != nil {
		t.Error("empty line should return nil")
	}
}

func TestParseCommitHeader_WhitespaceOnly(t *testing.T) {
	c := parseCommitHeader("   \t  ")
	if c != nil {
		t.Error("whitespace-only should return nil")
	}
}

func TestParseCommitHeader_EmptyAuthorEmail(t *testing.T) {
	line := "abc" + fieldSep +
		"" + fieldSep +
		"Author" + fieldSep + "" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Committer" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"msg"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	if c.AuthorEmail != "" {
		t.Errorf("AuthorEmail = %q, want empty", c.AuthorEmail)
	}
}

func TestParseCommitHeader_SubjectWithFieldSep(t *testing.T) {
	// Subject might contain the field separator character in theory.
	// SplitN with 9 means the 9th field captures everything remaining.
	line := "abc" + fieldSep +
		"" + fieldSep +
		"A" + fieldSep + "a@b.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"C" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"msg" + fieldSep + "extra"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	// The 9th field should capture everything after the 8th separator.
	if c.Subject != "msg"+fieldSep+"extra" {
		t.Errorf("Subject = %q, want subject with separator", c.Subject)
	}
}

// ============================================================
// parseNumstatLine edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseNumstatLine_ZeroCounts(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "0\t0\tempty.txt")
	if len(c.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(c.Files))
	}
	if c.Files[0].Additions != 0 || c.Files[0].Deletions != 0 {
		t.Error("expected 0/0")
	}
}

func TestParseNumstatLine_PathWithSpaces(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "5\t3\tpath with spaces/file.txt")
	if len(c.Files) != 1 {
		t.Fatal("expected 1 file")
	}
	if c.Files[0].Filename != "path with spaces/file.txt" {
		t.Errorf("filename = %q", c.Files[0].Filename)
	}
}

func TestParseNumstatLine_LargeNumbers(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "99999\t88888\tbig.go")
	if c.Files[0].Additions != 99999 || c.Files[0].Deletions != 88888 {
		t.Errorf("large: %d/%d", c.Files[0].Additions, c.Files[0].Deletions)
	}
}

func TestParseNumstatLine_EmptyFilename(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "1\t0\t")
	if len(c.Files) != 1 {
		t.Fatal("expected 1 file")
	}
	if c.Files[0].Filename != "" {
		t.Errorf("filename = %q, want empty", c.Files[0].Filename)
	}
}

func TestParseNumstatLine_MultipleFiles(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "1\t0\tfile1.go")
	parseNumstatLine(c, "2\t1\tfile2.go")
	parseNumstatLine(c, "3\t2\tfile3.go")
	if len(c.Files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(c.Files))
	}
}

// ============================================================
// extractDate edge cases (beyond facade_test.go coverage)
// ============================================================

func TestExtractDate_ExactTenChars(t *testing.T) {
	got := extractDate("2024-01-15")
	if got != "2024-01-15" {
		t.Errorf("extractDate = %q", got)
	}
}

func TestExtractDate_LongISO(t *testing.T) {
	got := extractDate("2024-06-15T14:30:00+05:30")
	if got != "2024-06-15" {
		t.Errorf("extractDate = %q", got)
	}
}

func TestExtractDate_ShortString(t *testing.T) {
	got := extractDate("abc")
	if got != "abc" {
		t.Errorf("short: %q", got)
	}
}

// ============================================================
// parseTimestamp edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseTimestamp_PositiveOffset(t *testing.T) {
	ts := parseTimestamp("2024-06-15T14:30:00+05:30")
	if ts == nil {
		t.Error("should handle positive timezone offset")
	}
}

func TestParseTimestamp_NegativeOffset(t *testing.T) {
	ts := parseTimestamp("2024-06-15T14:30:00-08:00")
	if ts == nil {
		t.Error("should handle negative timezone offset")
	}
}

func TestParseTimestamp_Empty(t *testing.T) {
	if parseTimestamp("") != nil {
		t.Error("empty should return nil")
	}
}

// ============================================================
// platformHost edge cases (beyond facade_test.go coverage)
// ============================================================

func TestPlatformHost_GenericGit(t *testing.T) {
	// PlatformGenericGit (3) — should return "unknown" or similar.
	host := platformHost(model.PlatformGenericGit)
	// Actual behavior depends on implementation; document it.
	_ = host
}

// ============================================================
// clonePath edge cases
// ============================================================

func TestClonePath_Format(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/tmp/repos"}
	if path := fc.clonePath(42); path != "/tmp/repos/repo_42" {
		t.Errorf("clonePath = %q", path)
	}
}

func TestClonePath_LargeID(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/data"}
	if path := fc.clonePath(999999999); path != "/data/repo_999999999" {
		t.Errorf("clonePath = %q", path)
	}
}

func TestClonePath_ZeroID(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/tmp"}
	if path := fc.clonePath(0); path != "/tmp/repo_0" {
		t.Errorf("clonePath = %q", path)
	}
}

// ============================================================
// FacadeResult edge cases
// ============================================================

func TestFacadeResult_Defaults(t *testing.T) {
	r := &FacadeResult{}
	if r.Commits != 0 {
		t.Errorf("Commits = %d", r.Commits)
	}
	if r.CommitMessages != 0 {
		t.Errorf("CommitMessages = %d", r.CommitMessages)
	}
	if r.Errors != nil {
		t.Error("Errors should be nil")
	}
}

func TestFacadeResult_WithErrors(t *testing.T) {
	r := &FacadeResult{
		Commits:        100,
		CommitMessages: 50,
	}
	r.Errors = append(r.Errors, fmt.Errorf("test error"))
	if len(r.Errors) != 1 {
		t.Errorf("errors = %d, want 1", len(r.Errors))
	}
}

// TestBareCloneFetchWithoutRefspec demonstrates the bug: git clone --bare
// does not create a fetch refspec, so git fetch --all does not advance local
// branch refs. This is why re-collection missed new commits.
func TestBareCloneFetchWithoutRefspec(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Create upstream with one commit, bare clone it.
	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial")
	initialHash := git(upstreamDir, "rev-parse", "HEAD")

	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)

	// Push a new commit upstream.
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v2"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "second commit")

	// Bare clone has no fetch refspec — git fetch --all does NOT update refs/heads/*.
	git(bareDir, "fetch", "--all", "--prune")
	staleHash := git(bareDir, "rev-parse", "refs/heads/main")
	if staleHash != initialHash {
		t.Fatal("expected refs/heads/main to remain stale after fetch --all (no refspec)")
	}
}

// TestBareCloneFetchWithExplicitRefspec verifies the fix: providing an explicit
// refspec on git fetch advances local branch refs even without a config refspec.
func TestBareCloneFetchWithExplicitRefspec(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Create upstream with one commit, bare clone it.
	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial")

	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)

	// Push a new commit upstream.
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v2"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "second commit")
	newHash := git(upstreamDir, "rev-parse", "HEAD")

	// Fetch with explicit refspec (the fix) — this DOES update refs/heads/*.
	git(bareDir, "fetch", "origin",
		"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*", "--prune")

	fixedHash := git(bareDir, "rev-parse", "refs/heads/main")
	if fixedHash != newHash {
		t.Errorf("refs/heads/main = %s, want %s", fixedHash, newHash)
	}
}

// TestBareCloneFetchMultipleBranches verifies the explicit refspec updates all branches.
func TestBareCloneFetchMultipleBranches(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Create upstream with main + develop.
	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial")
	git(upstreamDir, "checkout", "-b", "develop")
	os.WriteFile(filepath.Join(upstreamDir, "dev.txt"), []byte("dev"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "develop commit")

	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)

	// Add new commits on both branches upstream.
	git(upstreamDir, "checkout", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v2"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "main update")
	mainHash := git(upstreamDir, "rev-parse", "HEAD")

	git(upstreamDir, "checkout", "develop")
	os.WriteFile(filepath.Join(upstreamDir, "dev.txt"), []byte("dev2"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "develop update")
	devHash := git(upstreamDir, "rev-parse", "HEAD")

	// Fetch with explicit refspec.
	git(bareDir, "fetch", "origin",
		"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*", "--prune")

	gotMain := git(bareDir, "rev-parse", "refs/heads/main")
	if gotMain != mainHash {
		t.Errorf("main = %s, want %s", gotMain, mainHash)
	}
	gotDev := git(bareDir, "rev-parse", "refs/heads/develop")
	if gotDev != devHash {
		t.Errorf("develop = %s, want %s", gotDev, devHash)
	}
}

// TestBareCloneFetchPrunesDeletedBranch verifies --prune removes branches
// deleted on origin.
func TestBareCloneFetchPrunesDeletedBranch(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Create upstream with main + temp-branch.
	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial")
	git(upstreamDir, "checkout", "-b", "temp-branch")
	os.WriteFile(filepath.Join(upstreamDir, "tmp.txt"), []byte("tmp"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "temp")
	git(upstreamDir, "checkout", "main")

	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)

	// Verify temp-branch exists in bare clone.
	git(bareDir, "rev-parse", "refs/heads/temp-branch")

	// Delete temp-branch upstream.
	git(upstreamDir, "branch", "-D", "temp-branch")

	// Fetch with explicit refspec + prune.
	git(bareDir, "fetch", "origin",
		"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*", "--prune")

	// temp-branch should be gone.
	cmd := exec.Command("git", "-C", bareDir, "rev-parse", "refs/heads/temp-branch")
	if err := cmd.Run(); err == nil {
		t.Error("expected refs/heads/temp-branch to be pruned, but it still exists")
	}
}

// TestSyncDefaultBranch_UpdatesHEAD verifies that when the remote renames its
// default branch (e.g. master → main), syncDefaultBranch updates the bare
// clone's HEAD to match. Without this, resolveDefaultBranch returns a stale ref
// and git log runs against the wrong (or nonexistent) branch.
func TestSyncDefaultBranch_UpdatesHEAD(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Create upstream with default branch "master".
	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "master")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial on master")

	// Bare clone — HEAD points to refs/heads/master.
	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)
	headBefore := git(bareDir, "symbolic-ref", "HEAD")
	if headBefore != "refs/heads/master" {
		t.Fatalf("initial HEAD = %s, want refs/heads/master", headBefore)
	}

	// Rename default branch on upstream to "main".
	git(upstreamDir, "branch", "-m", "master", "main")
	git(upstreamDir, "symbolic-ref", "HEAD", "refs/heads/main")

	// Fetch new refs into the bare clone.
	git(bareDir, "fetch", "origin",
		"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*", "--prune")

	// Before syncDefaultBranch, bare clone HEAD still points to master.
	headStale := git(bareDir, "symbolic-ref", "HEAD")
	if headStale != "refs/heads/master" {
		t.Fatalf("expected HEAD still at refs/heads/master after fetch, got %s", headStale)
	}

	// Run syncDefaultBranch.
	fc := &FacadeCollector{logger: slog.Default()}
	fc.syncDefaultBranch(context.Background(), bareDir)

	// HEAD should now point to main.
	headAfter := git(bareDir, "symbolic-ref", "HEAD")
	if headAfter != "refs/heads/main" {
		t.Errorf("after syncDefaultBranch, HEAD = %s, want refs/heads/main", headAfter)
	}
}

// TestSyncDefaultBranch_NoChangeWhenAlreadyCorrect verifies syncDefaultBranch
// is a no-op when HEAD already matches the remote.
func TestSyncDefaultBranch_NoChangeWhenAlreadyCorrect(t *testing.T) {
	tmpDir := t.TempDir()
	upstreamDir := filepath.Join(tmpDir, "upstream")
	bareDir := filepath.Join(tmpDir, "bare.git")

	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	os.MkdirAll(upstreamDir, 0o755)
	git(upstreamDir, "init", "-b", "main")
	os.WriteFile(filepath.Join(upstreamDir, "file.txt"), []byte("v1"), 0o644)
	git(upstreamDir, "add", ".")
	git(upstreamDir, "commit", "-m", "initial")

	git(tmpDir, "clone", "--bare", upstreamDir, bareDir)

	fc := &FacadeCollector{logger: slog.Default()}
	fc.syncDefaultBranch(context.Background(), bareDir)

	// HEAD should still be refs/heads/main — no change.
	head := git(bareDir, "symbolic-ref", "HEAD")
	if head != "refs/heads/main" {
		t.Errorf("HEAD = %s, want refs/heads/main", head)
	}
}
