// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.105 ultrareview finding (bug_001, verified real): runWhitespaceWalk
// called cmd.Wait() without draining stdout when parseWhitespaceLog bailed
// early on a DB flush error — git blocks on a full ~64KiB pipe, Wait()
// blocks on git's exit, and the worker wedges silently in a syscall
// (exactly the pattern Go's exec.StdoutPipe docs warn about). The fix
// cancels a derived context on the parse-error path so git is killed and
// Wait() returns. parseGitLog (facade.go) had the same latent shape on a
// scanner error and gets the same guard.
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
)

// TestWhitespaceWalkErrorDoesNotDeadlock reproduces the reviewed failure
// shape: the flush errors mid-stream (closed pool) while git still has
// far more than a pipe buffer's worth of patch text to write. Pre-fix,
// runWhitespaceWalk never returns; post-fix it surfaces the flush error
// promptly.
func TestWhitespaceWalkErrorDoesNotDeadlock(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	// Close the pool NOW — every flush will error immediately.
	store.Close()

	// Fixture: commit 1 is tiny (its flush trips the error at
	// flushEvery=1); commit 2 carries a multi-MB patch so git is still
	// mid-write — far past the kernel pipe buffer — when the parse
	// loop abandons the stream.
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
	// git log streams NEWEST FIRST — so the big patch must live in the
	// OLDER commit: the tiny newest commit streams first, its flush
	// errors at the next commit boundary, and git is still mid-write on
	// the older commit's ~12 MB patch (far past bufio's 1 MiB + the
	// kernel's ~64 KiB pipe) when the parse loop abandons the stream.
	big := strings.Repeat("padding line of some length for the patch stream\n", 240_000) // ~12 MB
	if err := os.WriteFile(filepath.Join(dir, "big.txt"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-m", "c1-big-old")
	if err := os.WriteFile(filepath.Join(dir, "small.txt"), []byte("one line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-m", "c2-small-new")

	old := whitespaceFlushEvery
	whitespaceFlushEvery = 1
	t.Cleanup(func() { whitespaceFlushEvery = old })

	fc := NewFacadeCollector(store, logger, t.TempDir())
	done := make(chan error, 1)
	go func() {
		_, _, werr := fc.runWhitespaceWalk(ctx, 999_999_999, dir, "")
		done <- werr
	}()
	select {
	case werr := <-done:
		if werr == nil {
			t.Fatal("walk must surface the flush error (pool is closed)")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runWhitespaceWalk DEADLOCKED: cmd.Wait() with an undrained stdout pipe — the parse-error path must kill git before waiting")
	}
}
