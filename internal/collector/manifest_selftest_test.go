// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// serve's self-test of the manifest reader says ONE line an operator can
// look for, and it is a line that can be the other one (0.29.215: the check
// the 0.29.214 note gave could not fail).
func TestTheManifestReaderSelfTestSaysWhetherItAnswered(t *testing.T) {
	var log recorded
	if err := ManifestReaderSelfTest(context.Background(), log.logger()); err != nil {
		t.Fatalf("the self-test of this program: %v", err)
	}
	self, _ := os.Executable()
	ok := ""
	for _, line := range log.lines {
		if strings.Contains(line, `msg="manifest reader ok"`) {
			ok = line
		}
	}
	for _, want := range []string{"level=INFO", "program=" + self, "version=" + db.ToolVersion, "build=" + programBuild(self), "manifest_read_cpu_seconds=", "manifest_read_memory_mb="} {
		if !strings.Contains(ok, want) {
			t.Errorf("the line lacks %q: %q (of %q)", want, ok, log.lines)
		}
	}

	// serve runs it at its start (and only serve: every command loads the
	// configuration, and a migrate or a web process reads no manifest).
	if body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runServe(")); !strings.Contains(body, "manifestReaderSelfTest(ctx, logger)") {
		t.Error("serve does not run the manifest reader's self-test at its start")
	}
	if body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func manifestReaderSelfTest(")); !strings.Contains(body, "collector.ManifestReaderSelfTest(ctx, logger)") {
		t.Error("cmd/aveloxis's self-test is not the collector's")
	}

	// A reader that cannot be started.
	saved, savedFallback := manifestChildCommand, manifestChildFallback
	t.Cleanup(func() { manifestChildCommand, manifestChildFallback = saved, savedFallback })
	gone := filepath.Join(t.TempDir(), "no-such-program")
	manifestChildCommand = func() (string, []string, error) { return gone, []string{ManifestChildArg}, nil }
	log = recorded{}
	err := ManifestReaderSelfTest(context.Background(), log.logger())
	var child *errManifestChild
	if !errors.As(err, &child) || child.Attempt != model.InventoryAttemptReadFailed {
		t.Fatalf("the self-test of a program that is not there: %v", err)
	}
	failed := ""
	for _, line := range log.lines {
		if strings.Contains(line, "manifest reader self-test FAILED") {
			failed = line
		}
		if strings.Contains(line, `msg="manifest reader ok"`) {
			t.Errorf("a reader that could not be started was said to be ok: %s", line)
		}
	}
	for _, want := range []string{"level=ERROR", "program=" + gone, "could not be started", "restart serve"} {
		if !strings.Contains(failed, want) {
			t.Errorf("the line lacks %q: %q", want, failed)
		}
	}

	// A shutdown is not a failure of the reader, and says nothing.
	manifestChildCommand = saved
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log = recorded{}
	if err := ManifestReaderSelfTest(ctx, log.logger()); !errors.Is(err, context.Canceled) || len(log.lines) != 0 {
		t.Errorf("a self-test under a shutdown: %v, said %q", err, log.lines)
	}
}

// A program of this VERSION that is another FILE is not this program: the
// reader refuses, as it does another version, and the analysis says which
// build each is (0.29.215: a `go install` of the same version under a
// running serve was accepted).
func TestAReaderOfAnotherBuildIsRefused(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n"})
	saved := manifestParentBuild
	t.Cleanup(func() { manifestParentBuild = saved; manifestStaleSaid.Store(false) })
	self, _ := os.Executable()
	own := programBuild(self)
	if own == "" {
		t.Fatal("this program's build could not be told")
	}
	if got := saved(); got != own {
		t.Fatalf("this process asks as build %q; its file is %q", got, own)
	}

	manifestParentBuild = func() string { return "1.1" }
	_, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, ManifestLimits{}, nil, quiet)
	var child *errManifestChild
	if !errors.As(err, &child) || !child.Stale ||
		!strings.Contains(err.Error(), "is version "+db.ToolVersion+" build "+own+", this process is version "+db.ToolVersion+" build 1.1: restart serve") {
		t.Fatalf("asked as another build: %v; want a refusal that names both builds", err)
	}
	// A process that could not tell its own build asks without one, and is
	// answered: nothing is compared.
	manifestParentBuild = func() string { return "" }
	if _, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, ManifestLimits{}, nil, quiet); err != nil {
		t.Errorf("asked with no build: %v", err)
	}
}

// What a file's build is: it changes when the file is written again, even
// to the same length.
func TestAProgramsBuildChangesWhenItsFileIsReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "program")
	if err := os.WriteFile(path, []byte("one"), 0o700); err != nil {
		t.Fatal(err)
	}
	first := programBuild(path)
	later := time.Now().Add(3 * time.Second)
	if err := os.WriteFile(path, []byte("two"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if second := programBuild(path); first == "" || second == "" || first == second {
		t.Errorf("builds %q and %q; want two that differ", first, second)
	}
	if got := programBuild(filepath.Join(t.TempDir(), "absent")); got != "" {
		t.Errorf("the build of a file that is not there: %q", got)
	}
}

// When the running program's link cannot be started, the program's own path
// is; it is said once; and when neither can be, the failure names both.
func TestTheReaderFallsBackToThisProgramsPath(t *testing.T) {
	dir := writeClone(t, map[string]string{"requirements.txt": "requests==2.31.0\n"})
	self, _ := os.Executable()
	// What is fallen back to, and from what alone.
	if got, err := manifestChildFallback(procSelfExe); err != nil || got != self {
		t.Errorf("after the link fails: %q, %v; want this program's path", got, err)
	}
	if got, err := manifestChildFallback(self); err != nil || got != "" {
		t.Errorf("after this program's own path fails: %q, %v; want nothing to fall back to", got, err)
	}

	saved, savedFallback := manifestChildCommand, manifestChildFallback
	t.Cleanup(func() {
		manifestChildCommand, manifestChildFallback = saved, savedFallback
		manifestFallbackSaid.Store(false)
	})
	gone := filepath.Join(t.TempDir(), "no-such-link")
	manifestChildCommand = func() (string, []string, error) { return gone, []string{ManifestChildArg}, nil }
	asked := ""
	manifestChildFallback = func(failed string) (string, error) { asked = failed; return self, nil }
	manifestFallbackSaid.Store(false)
	var log recorded
	for i := 0; i < 2; i++ {
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, ManifestLimits{}, nil, log.logger())
		if err != nil || len(run.decl.Deps) != 1 {
			t.Fatalf("reading %d through the fallback: %v", i, err)
		}
		run.close()
	}
	if asked != gone {
		t.Errorf("the fallback was asked about %q; want the program that failed", asked)
	}
	said := 0
	for _, line := range log.lines {
		if strings.Contains(line, "is started from this program's path instead") {
			said++
			for _, want := range []string{"level=WARN", "link=" + gone, "program=" + self} {
				if !strings.Contains(line, want) {
					t.Errorf("the line lacks %q: %s", want, line)
				}
			}
		}
	}
	if said != 1 {
		t.Errorf("the fallback was said %d times over two readings; want once", said)
	}

	alsoGone := filepath.Join(t.TempDir(), "no-such-program")
	manifestChildFallback = func(string) (string, error) { return alsoGone, nil }
	_, err := runManifestChild(context.Background(), manifestRequest{Passes: manifestPassesAll, Dir: dir, RemotePomLimit: 5}, ManifestLimits{}, nil, log.logger())
	if err == nil || !strings.Contains(err.Error(), gone) || !strings.Contains(err.Error(), alsoGone) {
		t.Errorf("neither could be started: %v; want both named", err)
	}
}
