// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGoBinDirAsksTheGoTool (batch 4a review rounds 16–17): install-tools
// exits non-zero when a tool lands off PATH, so its message must name the
// directory `go install` really used. cmd/go reads GOBIN and GOPATH through
// its env file as well as the process environment (`go env -w GOBIN=…`),
// so the answer comes from `go env GOBIN GOPATH` when go is on PATH; the
// environment rule (GOBIN, else the first GOPATH entry's bin, else
// ~/go/bin) is the fallback when it is not. A result that is not absolute
// is refused — `go install` refuses it too, and `export PATH="bin:$PATH"`
// cannot work (LookPath rejects a relative PATH entry).
func TestGoBinDirAsksTheGoTool(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not on PATH")
	}
	ctx := context.Background()
	envFile := filepath.Join(t.TempDir(), "go.env")
	if err := os.WriteFile(envFile, []byte("GOBIN=/tmp/from-go-env-w\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOENV", envFile)
	t.Setenv("GOBIN", "")
	t.Setenv("GOPATH", "")
	if got, err := GoBinDir(ctx); err != nil || got != "/tmp/from-go-env-w" {
		t.Errorf("GOBIN set with go env -w: GoBinDir() = %q, %v; want the env file's /tmp/from-go-env-w", got, err)
	}
	t.Setenv("GOENV", "off")
	t.Setenv("GOBIN", "/opt/gobin")
	if got, err := GoBinDir(ctx); err != nil || got != "/opt/gobin" {
		t.Errorf("GOBIN in the environment: GoBinDir() = %q, %v; want /opt/gobin", got, err)
	}
	// GOPATH can come back empty (HOME unset, or the default ~/go equals
	// GOROOT): `go env` then prints "GOBIN\n\n" — two lines, the second
	// empty (round 18: trimming every trailing newline made it one).
	t.Setenv("HOME", "")
	t.Setenv("GOPATH", "")
	if got, err := GoBinDir(ctx); err != nil || got != "/opt/gobin" {
		t.Errorf("GOBIN set, GOPATH empty: GoBinDir() = %q, %v; want /opt/gobin", got, err)
	}
}

// TestGoBinDirIgnoresTheCwdModulesToolchain (round 18): with the default
// GOTOOLCHAIN=auto, `go env` run inside a module whose go line is newer
// than the installed toolchain tries to download one (this checkout pins
// go1.26.6); a directory probe must not, and must work offline.
func TestGoBinDirIgnoresTheCwdModulesToolchain(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not on PATH")
	}
	mod := t.TempDir()
	if err := os.WriteFile(filepath.Join(mod, "go.mod"), []byte("module example.com/future\n\ngo 1.99\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(mod)
	t.Setenv("GOTOOLCHAIN", "auto")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOENV", "off")
	t.Setenv("GOBIN", "/opt/gobin")
	if got, err := GoBinDir(context.Background()); err != nil || got != "/opt/gobin" {
		t.Errorf("inside a module asking for go 1.99: GoBinDir() = %q, %v; want /opt/gobin with no toolchain switch", got, err)
	}
}

// TestGoBinDirFallbackWithoutGo drives the environment rule with go absent
// from PATH, and the refusal of a non-absolute answer.
func TestGoBinDirFallbackWithoutGo(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir()) // no go here
	ctx := context.Background()
	sep := string(os.PathListSeparator)
	for _, tc := range []struct{ gobin, gopath, want string }{
		{"/opt/gobin", "/x" + sep + "/y", "/opt/gobin"},
		{"", "/x" + sep + "/y", filepath.Join("/x", "bin")},
		{"", "/only", filepath.Join("/only", "bin")},
		{"", "", filepath.Join(home, "go", "bin")},
	} {
		t.Setenv("GOBIN", tc.gobin)
		t.Setenv("GOPATH", tc.gopath)
		if got, err := GoBinDir(ctx); err != nil || got != tc.want {
			t.Errorf("GOBIN=%q GOPATH=%q: GoBinDir() = %q, %v; want %q", tc.gobin, tc.gopath, got, err, tc.want)
		}
	}
	for _, bad := range []struct{ gobin, gopath string }{{"relative/bin", ""}, {"", sep + "/y"}, {"", "~/go"}} {
		t.Setenv("GOBIN", bad.gobin)
		t.Setenv("GOPATH", bad.gopath)
		if got, err := GoBinDir(ctx); err == nil {
			t.Errorf("GOBIN=%q GOPATH=%q: GoBinDir() = %q; want an error (not absolute — go install refuses it)", bad.gobin, bad.gopath, got)
		}
	}
	src := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/tools.go"), "func installScorecardBinary("))
	if !strings.Contains(src, "GoBinDir(ctx)") || strings.Contains(src, `os.Getenv("GOPATH")`) {
		t.Error("installScorecardBinary must place scorecard in GoBinDir(ctx), the directory install-tools tells the operator to export")
	}
}

// TestScorecardShadowWarning (round 17): an upgrade writes scorecard to
// GoBinDir, and a copy an earlier install left elsewhere on PATH can come
// first — serve would keep running the old one while the upgrade reported
// success. The installer says so when PATH resolves the name elsewhere.
func TestScorecardShadowWarning(t *testing.T) {
	early, late := t.TempDir(), t.TempDir()
	for _, d := range []string{early, late} {
		if err := os.WriteFile(filepath.Join(d, "scorecard"), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", early+string(os.PathListSeparator)+late)
	if msg := shadowWarning("scorecard", filepath.Join(late, "scorecard")); !strings.Contains(msg, filepath.Join(early, "scorecard")) || !strings.Contains(msg, "first on PATH") {
		t.Errorf("a copy earlier on PATH: warning %q; want it named", msg)
	}
	if msg := shadowWarning("scorecard", filepath.Join(early, "scorecard")); msg != "" {
		t.Errorf("the written copy is the one PATH finds: warning %q; want none", msg)
	}
}

// TestGoBinDirCarriesGoStderr (round 19): a failed probe must say why — go's
// own stderr in the error ("everything that errors is logged"), not a bare
// "exit status 1". A go shim on PATH writes a reason and exits 1.
func TestGoBinDirCarriesGoStderr(t *testing.T) {
	bin := t.TempDir()
	shim := "#!/bin/sh\necho 'go: probe-shim-reason' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	_, err := GoBinDir(context.Background())
	if err == nil || !strings.Contains(err.Error(), "probe-shim-reason") {
		t.Errorf("a failing go env = %v; want go's stderr carried into the error", err)
	}
}
