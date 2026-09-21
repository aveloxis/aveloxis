// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.29.57 fix-review round 2: the scancode worker was the third subprocess
// boundary handed a stored repo_git, and the one without the refusal — its
// clone put the URL on git's command line and, on failure, git's own "unable
// to access '<url>'" into the WARN. runOne refuses before any I/O: nothing
// cloned, a strike recorded so the row is not reclaimed every tick, the
// ERROR redacted.
func TestScancodeRunOneRefusesARepoURLWithUserinfo(t *testing.T) {
	var logs bytes.Buffer
	fake := &fakeScancodeStore{}
	dir := t.TempDir()
	w := NewScancodeWorker(fake, slog.New(slog.NewTextHandler(&logs, nil)), ScancodeWorkerOptions{CloneDir: dir})
	w.bookkeeping.Add(1) // the dispatcher's count, which runOne releases
	w.runOne(context.Background(), db.ScancodeJob{
		RepoID: 777, RepoOwner: "owner", RepoName: "name",
		RepoGit: "https://user:s3cret@github.com/owner/name",
	})
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a refused URL created %d entries under the clone dir", len(entries))
	}
	if len(fake.failures) != 1 || fake.failures[0] != 777 {
		t.Errorf("failures recorded = %v, want [777] — without a strike the row is reclaimed every tick", fake.failures)
	}
	if len(fake.completed) != 0 || len(fake.skipped) != 0 {
		t.Errorf("a refused row was completed (%v) or skipped (%v)", fake.completed, fake.skipped)
	}
	if bytes.Contains(logs.Bytes(), []byte("s3cret")) {
		t.Errorf("the refusal logged the credential: %s", logs.String())
	}
	if !bytes.Contains(logs.Bytes(), []byte("level=ERROR")) || !bytes.Contains(logs.Bytes(), []byte("repo_id=777")) {
		t.Errorf("the refusal must be logged at ERROR naming the repo: %s", logs.String())
	}
}

// Copilot review 5267193512: a job handed to a runner as shutdown began
// reached the refusal with a cancelled context, and the strike it recorded
// counted a `stop serve` toward the sideline. Shutdown before the check is a
// clean release: lock cleared, no strike.
func TestScancodeRunOneShutdownBeforeURLCheckIsACleanRelease(t *testing.T) {
	fake := &fakeScancodeStore{}
	w := NewScancodeWorker(fake, slog.New(slog.NewTextHandler(io.Discard, nil)), ScancodeWorkerOptions{CloneDir: t.TempDir()})
	w.bookkeeping.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.runOne(ctx, db.ScancodeJob{RepoID: 778, RepoOwner: "o", RepoName: "n", RepoGit: "https://user:s3cret@github.com/o/n"})
	if len(fake.failures) != 0 {
		t.Errorf("a shutdown recorded a strike: %v", fake.failures)
	}
	if len(fake.cleared) != 1 || fake.cleared[0] != 778 {
		t.Errorf("the lock was not cleared on shutdown: cleared=%v", fake.cleared)
	}
}
