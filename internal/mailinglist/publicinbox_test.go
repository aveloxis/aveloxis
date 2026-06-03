// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Compile-time assertion that PublicInbox satisfies ArchiveSource.
var _ ArchiveSource = (*PublicInbox)(nil)

// makeInboxRepo builds a public-inbox-style git repo: each message is the
// blob "m" in its own commit, dated in 2026-03.
func makeInboxRepo(t *testing.T, dir string, messages []string) {
	t.Helper()
	run := func(env []string, args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), env...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run(nil, "init", "--quiet")
	run(nil, "config", "user.email", "t@example.org")
	run(nil, "config", "user.name", "T")
	for i, m := range messages {
		if err := os.WriteFile(filepath.Join(dir, "m"), []byte(m), 0o644); err != nil {
			t.Fatal(err)
		}
		run(nil, "add", "m")
		date := "2026-03-0" + string(rune('1'+i)) + "T00:00:00"
		run([]string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date}, "commit", "--quiet", "-m", "msg")
	}
}

func TestPublicInboxFetchMonthFromGitArchive(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	tmp := t.TempDir()
	// archiveURL(inbox) = baseURL + "/linux-pci/git/0.git" — build the fixture there.
	repo := filepath.Join(tmp, "linux-pci", "git", "0.git")
	patchMsg := "Message-ID: <p1@vger>\r\nFrom: Dev <dev@example.org>\r\n" +
		"Subject: [PATCH v2 1/2] pci: fix foo\r\nDate: Sun, 01 Mar 2026 00:00:00 +0000\r\n\r\nthe patch body\n"
	reviewMsg := "Message-ID: <r1@vger>\r\nFrom: Bjorn <bjorn@example.org>\r\n" +
		"Subject: Re: [PATCH v2 1/2] pci: fix foo\r\nDate: Mon, 02 Mar 2026 00:00:00 +0000\r\n\r\nOn ...\nReviewed-by: Bjorn <bjorn@example.org>\n"
	makeInboxRepo(t, repo, []string{patchMsg, reviewMsg})

	pi := NewPublicInbox(tmp, filepath.Join(tmp, "clones"))
	msgs, _, err := pi.FetchMonth(context.Background(), "linux-pci@vger.kernel.org", "2026-03")
	if err != nil {
		t.Fatalf("FetchMonth: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	// The generalization proof: the kernel ruleset classifies these into the
	// PR-equivalent + review-equivalent.
	kernel, err := LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	sys := kernel["lore_public_inbox"]
	classes := map[string]bool{}
	for _, m := range msgs {
		c := sys.Classify(Message{Subject: m.Subject, Sender: m.Sender, Body: m.Body})
		classes[c.Class] = true
	}
	if !classes[ClassPatchSubmission] {
		t.Error("expected a patch_submission from the [PATCH] message")
	}
	if !classes[ClassReview] {
		t.Error("expected a review from the Reviewed-by reply")
	}

	// FirstMonth resolves from the archive.
	if fm, _ := pi.FirstMonth(context.Background(), "linux-pci@vger.kernel.org"); fm != "2026-03" {
		t.Errorf("FirstMonth = %q, want 2026-03", fm)
	}
}
