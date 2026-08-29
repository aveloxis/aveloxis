// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Pass 35 (v0.28.18): a `git fetch` killed by shutdown was read as
// "fetch failed, re-cloning" — the persistent bare clone was deleted
// and the re-clone on the dead ctx failed, so the next cycle paid a
// from-scratch clone (kernel-class: GBs, hours). Under a done ctx the
// fetch must return ctx.Err() and leave the clone alone.
func TestEnsureCloneKeepsTheBareCloneWhenShutdownKillsTheFetch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	src := filepath.Join(root, "src")
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@x", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@x")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	run(src, "init", "-q")
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(src, "add", "f")
	run(src, "commit", "-q", "-m", "one")
	bare := filepath.Join(root, "bare.git")
	run(root, "clone", "-q", "--bare", "--", src, bare)

	f := &FacadeCollector{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // shutdown already landed when the fetch starts
	err := f.ensureClone(ctx, src, bare)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureClone under a done ctx must return the context error, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(bare, "HEAD")); statErr != nil {
		t.Fatalf("the bare clone must survive a shutdown-killed fetch, got %v", statErr)
	}
}
