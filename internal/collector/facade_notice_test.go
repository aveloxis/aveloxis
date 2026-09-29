// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// Worklist item 82: a repository disabled by GitHub staff is refused at
// the clone with git's `remote: Access to this repository has been
// disabled by GitHub staff.` — the only place that text appears (and the
// only source on GitLab). The facade carries it on the clone error.

func TestRemoteNotice(t *testing.T) {
	cases := []struct {
		name, stderr, want string
	}{
		{"staff disabled 403",
			"Cloning into bare repository '/x'...\nremote: Access to this repository has been disabled by GitHub staff.\nfatal: unable to access 'https://github.com/o/r/': The requested URL returned error: 403\n",
			"Access to this repository has been disabled by GitHub staff."},
		{"legal block 451",
			"remote: Repository access blocked\nfatal: unable to access 'https://github.com/o/r/': The requested URL returned error: 451\n",
			"Repository access blocked"},
		{"several remote lines are joined, blank ones dropped",
			"remote: first line\nremote:\nremote:   second line  \nfatal: unable to access 'u': The requested URL returned error: 403\n",
			"first line second line"},
		{"CRLF line ends",
			"remote: disabled\r\nfatal: unable to access 'u': The requested URL returned error: 403\r\n",
			"disabled"},
		// Not a refusal: a transient server error's remote text is not the
		// forge's notice about the repository.
		{"500 is not a refusal",
			"remote: Internal Server Error\nfatal: unable to access 'u': The requested URL returned error: 500\n", ""},
		{"404 is the gone path, not a notice",
			"remote: Repository not found.\nfatal: repository 'https://github.com/o/r/' not found\n", ""},
		{"403 without remote text", "fatal: unable to access 'u': The requested URL returned error: 403\n", ""},
		// Progress chatter on a successful-looking fetch is never a notice.
		{"sideband progress", "remote: Enumerating objects: 5, done.\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, ok := remoteNotice(c.stderr)
			if c.want == "" {
				if ok {
					t.Errorf("remoteNotice = %+v, want none", n)
				}
				return
			}
			if !ok || n.Message != c.want || n.Reason != "" || n.URL != "" {
				t.Errorf("remoteNotice = %+v, %v; want Message %q only", n, ok, c.want)
			}
		})
	}
}

// TestFreshCloneCarriesTheRemoteNotice drives real git against a server
// that refuses the clone the way GitHub refuses a disabled repository: a
// 403 with a text/plain body, which git prints as `remote:` lines.
func TestFreshCloneCarriesTheRemoteNotice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Access to this repository has been disabled by GitHub staff.\n"))
	}))
	defer srv.Close()
	f := NewFacadeCollector(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	err := f.freshClone(context.Background(), srv.URL+"/o/r.git", filepath.Join(t.TempDir(), "r"))
	if err == nil {
		t.Fatal("clone of a refused repository succeeded")
	}
	n, ok := platform.NoticeOf(err)
	if !ok || n.Message != "Access to this repository has been disabled by GitHub staff." {
		t.Errorf("NoticeOf(%v) = %+v, %v", err, n, ok)
	}
}

// TestCollectRepoReportsTheCloneOutcome — review round 1 F4: whether the
// forge answered the clone is decided by the clone, not by the whole facade
// (a git log failure after a successful clone is not a refusal). CloneOK is
// set when the clone or fetch succeeded, and only then.
func TestCollectRepoReportsTheCloneOutcome(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	// An empty bare repository served over git's dumb HTTP protocol.
	bare := t.TempDir()
	for _, args := range [][]string{{"init", "--bare", "-q", bare}, {"-C", bare, "update-server-info"}} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	ok := httptest.NewServer(http.StripPrefix("/o/r.git", http.FileServer(http.Dir(bare))))
	defer ok.Close()
	f := NewFacadeCollector(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	res, err := f.CollectRepo(context.Background(), 1, ok.URL+"/o/r.git")
	if res == nil || !res.CloneOK {
		t.Errorf("a clone that succeeded: CloneOK=false (err %v)", err)
	}

	refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Access to this repository has been disabled by GitHub staff.\n"))
	}))
	defer refused.Close()
	res, err = f.CollectRepo(context.Background(), 2, refused.URL+"/o/r.git")
	if err == nil || res == nil || res.CloneOK {
		t.Errorf("a refused clone: CloneOK=%v err=%v; want false and an error", res != nil && res.CloneOK, err)
	}
}
