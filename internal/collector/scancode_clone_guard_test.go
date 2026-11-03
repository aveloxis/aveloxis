// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// H-3 (summary/59 §5, the hotfix release): the scancode worker's shallow
// clone skipped the two guards the facade clone has — validateGitURL
// (refuses file://, option injection, missing schemes) and
// GIT_TERMINAL_PROMPT=0 (no credential prompt from a background process).
// It now builds its command through scancodeCloneCmd, which applies both.

func TestScancodeCloneCmdRefusesUnsafeURLs(t *testing.T) {
	for _, u := range []string{"file:///etc", "-oProxyCommand=touch /tmp/x", "--upload-pack=x", "ext::sh -c x", "", "github.com/o/r"} {
		if cmd, err := scancodeCloneCmd(context.Background(), u, "/tmp/dir"); err == nil || cmd != nil {
			t.Errorf("scancodeCloneCmd(%q) = %v, %v; want a refusal and no command", u, cmd, err)
		}
	}
}

func TestScancodeCloneCmdUsesTheCloneEnvironment(t *testing.T) {
	// A loopback stub (trusted by the package's TestMain): the command's
	// git endpoint walk must not reach a live forge from a unit test (L10
	// round 3 of 0.29.132); a 404 there keeps the stored URL.
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	u := srv.URL + "/chaoss/augur"
	cmd, err := scancodeCloneCmd(context.Background(), u, "/tmp/dir")
	if err != nil {
		t.Fatal(err)
	}
	// -c http.followRedirects=false: git never follows a redirect the
	// outbound-address rule's probe did not see (0.29.132).
	want := []string{"git", "-c", gitNoRedirects, "clone", "--depth", "1", "--", u, "/tmp/dir"}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("args = %q, want %q", cmd.Args, want)
	}
	for _, kv := range []string{"GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1"} {
		if !slices.Contains(cmd.Env, kv) {
			t.Errorf("clone env lacks %s", kv)
		}
	}
}

// The worker's clone goes through scancodeCloneCmd; no second spelling of
// the git clone command remains in prepareClone.
func TestScancodePrepareCloneUsesTheGuardedCommand(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/collector/scancode_worker.go"))
	body := srctest.FuncBody(t, src, "func (w *ScancodeWorker) prepareClone(")
	if !strings.Contains(body, "scancodeCloneCmd(") {
		t.Fatal("prepareClone must build its clone with scancodeCloneCmd")
	}
	if strings.Contains(body, `"clone"`) {
		t.Fatal("prepareClone spells its own git clone command; use scancodeCloneCmd")
	}
}
