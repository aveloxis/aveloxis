// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
)

// v0.29.58 (2026-09-22 log review, finding 2): 585 facade jobs in one
// eight-hour run warned "git log exited with error: exit status 128" and
// nothing else — every one of them an EMPTY repository (a bare clone whose
// default branch has no commits). Two defects: git's stderr was discarded,
// so the operator could not tell an empty repo from a corrupt clone; and
// an empty repository is not a failure at all.

// TestParseGitLogEmptyRepositoryIsNotAnError pins that a bare clone with
// no commits on its default branch completes the numstat pass with zero
// commits, no error, and an INFO line that names the condition.
func TestParseGitLogEmptyRepositoryIsNotAnError(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	if out, err := exec.Command("git", "init", "--bare", "-q", dir).CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v: %s", err, out)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	f := NewFacadeCollector(nil, logger, t.TempDir())

	result := &FacadeResult{}
	if err := f.parseGitLog(context.Background(), 1, dir, result); err != nil {
		t.Fatalf("parseGitLog on an empty bare repo returned an error: %v", err)
	}
	if result.Commits != 0 || len(result.Errors) != 0 {
		t.Fatalf("empty repo: commits=%d errors=%d, want 0/0", result.Commits, len(result.Errors))
	}
	if !result.EmptyDefaultBranch {
		t.Fatal("result.EmptyDefaultBranch not set — CollectRepo would run the whitespace phase against an unborn ref")
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "no commits on its default branch") {
		t.Fatalf("expected an INFO line naming the empty repository, got:\n%s", logs)
	}
	if strings.Contains(logs, "level=WARN") || strings.Contains(logs, "level=ERROR") {
		t.Fatalf("an empty repository must not log at WARN or ERROR:\n%s", logs)
	}
}

// TestParseGitLogErrorCarriesGitStderr pins that a real git log failure
// reaches the log with git's own diagnostic, not just the exit status. A
// directory that is not a repository is the simplest such failure: the
// empty-branch probe cannot claim it (rev-parse fails with 128, not the
// "no such ref" exit 1), so git log runs and fails.
func TestParseGitLogErrorCarriesGitStderr(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	notARepo := t.TempDir()

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	f := NewFacadeCollector(nil, logger, t.TempDir())

	err := f.parseGitLog(context.Background(), 1, notARepo, &FacadeResult{})
	if err == nil {
		t.Fatal("parseGitLog on a non-repository directory returned nil")
	}
	if !strings.Contains(err.Error(), "not a git repository") {
		t.Fatalf("error does not carry git's stderr: %v", err)
	}
}

// TestExitStderrExtractsTheToolsDiagnostic pins the shared helper every
// cmd.Output()/Wait() error site uses to surface stderr (facade git log,
// whitespace git log -p, the go toolchain in gomod_graph): an ExitError's
// captured stderr is returned trimmed and bounded; anything else yields "".
func TestExitStderrExtractsTheToolsDiagnostic(t *testing.T) {
	cmd := exec.Command("sh", "-c", "echo 'fatal: something specific' >&2; exit 3")
	_, err := cmd.Output()
	if err == nil {
		t.Fatal("expected a non-zero exit")
	}
	if got := exitStderr(err); got != "fatal: something specific" {
		t.Fatalf("exitStderr = %q, want the trimmed stderr", got)
	}
	if got := exitStderr(context.Canceled); got != "" {
		t.Fatalf("exitStderr(non-exit error) = %q, want empty", got)
	}
	long := exec.Command("sh", "-c", "head -c 10000 /dev/zero | tr '\\0' x >&2; exit 1")
	_, err = long.Output()
	if got := exitStderr(err); len(got) > stderrTailMax {
		t.Fatalf("exitStderr returned %d bytes, want at most %d", len(got), stderrTailMax)
	}
}

// TestStderrCaptureNeverShortWrites — review round 1 (high): a Write that
// returns fewer bytes than it was given makes os/exec's copy goroutine
// fail with io.ErrShortWrite and report a SUCCESSFUL git log as failed.
// A subprocess that writes past the cap and exits 0 must still run clean,
// with the capture holding exactly the cap.
func TestStderrCaptureNeverShortWrites(t *testing.T) {
	cap := &stderrCapture{}
	cmd := exec.Command("sh", "-c", "head -c 5000 /dev/zero | tr '\\0' x >&2; for i in 1 2 3 4 5; do echo more >&2; done; exit 0")
	cmd.Stderr = cap
	if err := cmd.Run(); err != nil {
		t.Fatalf("a successful subprocess with >cap stderr failed: %v", err)
	}
	if got := len(cap.String()); got != stderrTailMax {
		t.Fatalf("captured %d bytes, want exactly the cap %d", got, stderrTailMax)
	}
}

// TestWhitespaceWalkEmptyRepositoryIsNotAnError — review round 1 (high):
// the facade's empty path handed the repository to the whitespace phase,
// whose rev-parse fails 128 on an unborn ref, so the WARN moved rather
// than disappeared. The walk itself must recognise the empty repository
// (RewalkWhitespace reaches it without CollectRepo's gate).
func TestWhitespaceWalkEmptyRepositoryIsNotAnError(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	if out, err := exec.Command("git", "init", "--bare", "-q", dir).CombinedOutput(); err != nil {
		t.Fatalf("git init --bare: %v: %s", err, out)
	}
	var logBuf bytes.Buffer
	f := NewFacadeCollector(nil, slog.New(slog.NewTextHandler(&logBuf, nil)), t.TempDir())

	updated, head, err := f.runWhitespaceWalk(context.Background(), 1, dir, "")
	if err != nil {
		t.Fatalf("whitespace walk on an empty bare repo returned an error: %v", err)
	}
	if updated != 0 || head != "" {
		t.Fatalf("updated=%d head=%q, want 0 and no head to stamp", updated, head)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "no commits on the default branch") {
		t.Fatalf("expected the INFO line naming the empty repository, got:\n%s", logs)
	}
	if strings.Contains(logs, "level=WARN") || strings.Contains(logs, "level=ERROR") {
		t.Fatalf("an empty repository must not log at WARN or ERROR:\n%s", logs)
	}
}

// TestWhitespaceWalkRevParseErrorCarriesGitStderr pins the same stderr
// rule on the walk's rev-parse (review round 1): a broken clone names its
// cause.
func TestWhitespaceWalkRevParseErrorCarriesGitStderr(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	f := NewFacadeCollector(nil, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), t.TempDir())
	_, _, err := f.runWhitespaceWalk(context.Background(), 1, t.TempDir(), "")
	if err == nil {
		t.Fatal("whitespace walk on a non-repository returned nil")
	}
	if !strings.Contains(err.Error(), "not a git repository") {
		t.Fatalf("error does not carry git's stderr: %v", err)
	}
}
