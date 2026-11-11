// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommandInstallShadowedIsAnError (0.29.144 review F3): a tool installed
// by a command into its BinDir (scc's pinned go install) while another copy
// comes first on PATH must fail, naming both paths, as scorecard's install
// does: upgrade-tools would report "ok scc upgraded" and serve would keep
// running the other copy (a Homebrew scc ahead of the Go bin directory).
func TestCommandInstallShadowedIsAnError(t *testing.T) {
	trueBin, err := exec.LookPath("true")
	if err != nil {
		t.Skip("true not on PATH")
	}
	early, bin := t.TempDir(), t.TempDir()
	for _, dir := range []string{early, bin} {
		if err := os.WriteFile(filepath.Join(dir, "avx-shadow-tool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tool := ExternalTool{
		Name:        "avx-shadow-tool",
		CheckBinary: "avx-shadow-tool",
		InstallCmd:  trueBin, // the install "succeeds" and writes nothing new
		BinDir:      func(context.Context) (string, error) { return bin, nil },
	}

	t.Setenv("PATH", early+string(os.PathListSeparator)+bin)
	err = runToolInstall(context.Background(), tool)
	if !errors.Is(err, ErrInstallShadowed) {
		t.Fatalf("err = %v; want ErrInstallShadowed when another copy is first on PATH", err)
	}
	for _, p := range []string{filepath.Join(bin, "avx-shadow-tool"), filepath.Join(early, "avx-shadow-tool")} {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("err %q does not name %s", err, p)
		}
	}

	// Unshadowed: the installed copy is the one PATH finds.
	t.Setenv("PATH", bin+string(os.PathListSeparator)+early)
	if err := runToolInstall(context.Background(), tool); err != nil {
		t.Errorf("an unshadowed install failed: %v", err)
	}

	// A BinDir that cannot be resolved is an error, never "not shadowed"
	// (SR-5: a lookup error is not "no").
	errGoEnv := errors.New("go env failed")
	tool.BinDir = func(context.Context) (string, error) { return "", errGoEnv }
	if err := runToolInstall(context.Background(), tool); !errors.Is(err, errGoEnv) {
		t.Errorf("an install whose directory cannot be resolved reports that failure, got %v", err)
	}

	// A tool without a BinDir (the installer chooses, as pipx does) is not
	// checked here.
	tool.BinDir = nil
	t.Setenv("PATH", early+string(os.PathListSeparator)+bin)
	if err := runToolInstall(context.Background(), tool); err != nil {
		t.Errorf("a tool with no BinDir is not shadow-checked: %v", err)
	}
}

// scc is installed by a command into GoBinDir, so it gets the shadow check,
// and the installed binary must report the pinned version.
func TestSCCInstallIsShadowCheckedAndVerified(t *testing.T) {
	for _, tool := range ExternalTools() {
		if tool.Name == "scc" {
			if tool.InstallFunc != nil || tool.BinDir == nil || tool.VerifyInstalled == nil {
				t.Errorf("scc must install by command into its BinDir (shadow-checked) and verify the installed version: InstallFunc set %v, BinDir nil %v, VerifyInstalled nil %v",
					tool.InstallFunc != nil, tool.BinDir == nil, tool.VerifyInstalled == nil)
			}
			return
		}
	}
	t.Fatal("scc missing from ExternalTools")
}

// A command install that succeeded runs the tool's VerifyInstalled on the
// binary it wrote, and its failure is the install's (0.29.144 review round 2:
// an install that "succeeded" but delivered another version is a failure).
func TestCommandInstallRunsVerifyInstalledOnTheWrittenBinary(t *testing.T) {
	trueBin, err := exec.LookPath("true")
	if err != nil {
		t.Skip("true not on PATH")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "avx-verify-tool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+filepath.Dir(trueBin))
	errWrong := errors.New("wrong build")
	var verified string
	tool := ExternalTool{
		Name:        "avx-verify-tool",
		CheckBinary: "avx-verify-tool",
		InstallCmd:  trueBin,
		BinDir:      func(context.Context) (string, error) { return bin, nil },
		VerifyInstalled: func(_ context.Context, path string) error {
			verified = path
			return errWrong
		},
	}
	if err := runToolInstall(context.Background(), tool); !errors.Is(err, errWrong) {
		t.Errorf("a failed verification fails the install, got %v", err)
	}
	if verified != filepath.Join(bin, "avx-verify-tool") {
		t.Errorf("VerifyInstalled ran on %q, want the binary the install wrote", verified)
	}
	tool.VerifyInstalled = func(context.Context, string) error { return nil }
	if err := runToolInstall(context.Background(), tool); err != nil {
		t.Errorf("a verified install succeeds: %v", err)
	}
}

func TestVerifySCCPin(t *testing.T) {
	dir := t.TempDir()
	write := func(version string) string {
		p := filepath.Join(dir, "scc")
		if err := os.WriteFile(p, []byte("#!/bin/sh\necho 'scc version "+version+"'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if err := verifySCCPin(context.Background(), write(sccPinReports)); err != nil {
		t.Errorf("the pinned build verifies: %v", err)
	}
	err := verifySCCPin(context.Background(), write("4.1.0"))
	if !errors.Is(err, ErrInstallWrongVersion) || !strings.Contains(err.Error(), "4.1.0") || !strings.Contains(err.Error(), sccPinReports) {
		t.Errorf("another version fails with ErrInstallWrongVersion naming both: %v", err)
	}
	if err := verifySCCPin(context.Background(), filepath.Join(dir, "missing")); err == nil {
		t.Error("an unreadable binary does not verify")
	}
}

// Another spelling of the same file is not a shadow (0.29.144 review round
// 2): a hard link here, a PATH entry that differs only in case on macOS's
// case-insensitive APFS in production. Both made upgrade-tools fail with
// ErrInstallShadowed although PATH runs the installed binary.
func TestShadowWarningAcceptsTheSameFileUnderAnotherPath(t *testing.T) {
	bin, other := t.TempDir(), t.TempDir()
	written := filepath.Join(bin, "avx-link-tool")
	if err := os.WriteFile(written, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(written, filepath.Join(other, "avx-link-tool")); err != nil {
		t.Fatalf("link the installed binary into a second directory: %v", err)
	}
	t.Setenv("PATH", other+string(os.PathListSeparator)+bin)
	if w := shadowWarning("avx-link-tool", written); w != "" {
		t.Errorf("a hard link to the installed binary is the installed binary: %s", w)
	}
	// A different file first on PATH is still a shadow.
	if err := os.Remove(filepath.Join(other, "avx-link-tool")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "avx-link-tool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if w := shadowWarning("avx-link-tool", written); w == "" {
		t.Error("a different file first on PATH is a shadow")
	}
}
