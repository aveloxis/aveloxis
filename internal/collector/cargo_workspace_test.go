// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFileAt(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCargoWorkspaceInheritedDepsTakeTheRootsSource — v0.29.71 (the
// v0.29.56 follow-up): `name.workspace = true` was looked up on crates.io by
// name without reading the workspace root, so a crate the root declares by
// path or git named whatever crates.io package shares the name (SR-6).
// Decided as a class: the nearest ancestor Cargo.toml with [workspace],
// never above the scan root, answers; a version there is a pinned registry
// dependency; path, git or another registry there is not a crates.io crate;
// no root or no declaration is not guessed at either.
func TestCargoWorkspaceInheritedDepsTakeTheRootsSource(t *testing.T) {
	root := t.TempDir()
	writeFileAt(t, filepath.Join(root, "Cargo.toml"), `[workspace]
members = ["crates/*"]

[workspace.dependencies]
serde = "1.0.190"
tokio = { version = "1.35", features = ["full"] }
localcrate = { path = "crates/localcrate" }
forked = { git = "https://github.com/x/forked" }
alt = { version = "2", registry = "internal" }

[dependencies]
serde = { workspace = true }
`)
	member := writeFileAt(t, filepath.Join(root, "crates", "app", "Cargo.toml"), `[package]
name = "app"

[dependencies]
serde.workspace = true
tokio = { workspace = true, features = ["rt"] }
localcrate.workspace = true
forked = { workspace = true }
alt.workspace = true
missing.workspace = true
plain = "0.3"
`)
	ws := newCargoWorkspaceIndex(root, nil)
	type want struct {
		version     string
		nonRegistry bool
	}
	check := func(label string, deps []libyearDep, wants map[string]want) {
		t.Helper()
		got := map[string]libyearDep{}
		for _, d := range deps {
			got[d.Name] = d
		}
		for name, w := range wants {
			d, ok := got[name]
			if !ok {
				t.Errorf("%s: %s missing from the result", label, name)
				continue
			}
			if d.Version != w.version || d.NonRegistry != w.nonRegistry {
				t.Errorf("%s: %s = version %q nonRegistry %v, want %q %v", label, name, d.Version, d.NonRegistry, w.version, w.nonRegistry)
			}
		}
		if len(got) != len(wants) {
			t.Errorf("%s: %d deps, want %d: %+v", label, len(got), len(wants), deps)
		}
	}
	check("member", parseCargoVersionsIn(ws, member), map[string]want{
		"serde":      {"1.0.190", false},
		"tokio":      {"1.35", false},
		"localcrate": {"", true},
		"forked":     {"", true},
		"alt":        {"2", true},
		"missing":    {"", true},
		"plain":      {"0.3", false},
	})
	// The root's own [dependencies] inherit from its own table.
	check("root", parseCargoVersionsIn(ws, filepath.Join(root, "Cargo.toml")), map[string]want{
		"serde": {"1.0.190", false},
	})

	// Bounded by the scan root: a [workspace] ABOVE it is never read, so an
	// inherited dep of a clone without its own root is not guessed at.
	outer := t.TempDir()
	writeFileAt(t, filepath.Join(outer, "Cargo.toml"), "[workspace]\n[workspace.dependencies]\nserde = \"1.0.1\"\n")
	clone := filepath.Join(outer, "clone")
	lone := writeFileAt(t, filepath.Join(clone, "Cargo.toml"), "[package]\nname = \"x\"\n[dependencies]\nserde.workspace = true\n")
	check("bounded", parseCargoVersionsIn(newCargoWorkspaceIndex(clone, nil), lone), map[string]want{
		"serde": {"", true},
	})
}

// TestCargoWorkspaceNearestRootDecides — review round 1 F2/F3: the nearest
// root answers, and an outer root never fills in for it (a cargo-fuzz
// `fuzz/Cargo.toml` with its own [workspace] inside a repository whose top
// root declares the crate); a root recognised by a [workspace.*] table alone
// counts; and an unreadable Cargo.toml stops the walk as unresolved and is
// logged — it is not "no root here" (SR-5), which would let an outer root
// answer (SR-6).
func TestCargoWorkspaceNearestRootDecides(t *testing.T) {
	root := t.TempDir()
	writeFileAt(t, filepath.Join(root, "Cargo.toml"), "[workspace]\n[workspace.dependencies]\nserde = \"1.0.190\"\n")
	writeFileAt(t, filepath.Join(root, "fuzz", "Cargo.toml"), "[workspace]\nmembers = [\".\"]\n[dependencies]\nserde.workspace = true\n")
	fuzz := filepath.Join(root, "fuzz", "Cargo.toml")
	var logs bytes.Buffer
	ws := newCargoWorkspaceIndex(root, slog.New(slog.NewTextHandler(&logs, nil)))
	for _, d := range parseCargoVersionsIn(ws, fuzz) {
		if d.Name == "serde" && (!d.NonRegistry || d.Version != "") {
			t.Errorf("the inner root does not declare serde; the outer root answered: %+v", d)
		}
	}

	// A root with only [workspace.dependencies] (no bare [workspace]).
	r2 := t.TempDir()
	writeFileAt(t, filepath.Join(r2, "Cargo.toml"), "[workspace.dependencies]\ntokio = \"1.35\"\n")
	m2 := writeFileAt(t, filepath.Join(r2, "app", "Cargo.toml"), "[dependencies]\ntokio.workspace = true\n")
	for _, d := range parseCargoVersionsIn(newCargoWorkspaceIndex(r2, nil), m2) {
		if d.Name == "tokio" && (d.NonRegistry || d.Version != "1.35") {
			t.Errorf("a [workspace.dependencies]-only root must answer: %+v", d)
		}
	}

	// An unreadable Cargo.toml (here a directory of that name) between the
	// member and an outer root that declares the crate.
	r3 := t.TempDir()
	writeFileAt(t, filepath.Join(r3, "Cargo.toml"), "[workspace]\n[workspace.dependencies]\nserde = \"1.0.1\"\n")
	if err := os.MkdirAll(filepath.Join(r3, "inner", "Cargo.toml"), 0o755); err != nil {
		t.Fatal(err)
	}
	m3 := writeFileAt(t, filepath.Join(r3, "inner", "sub", "Cargo.toml"), "[dependencies]\nserde.workspace = true\n")
	logs.Reset()
	for _, d := range parseCargoVersionsIn(newCargoWorkspaceIndex(r3, slog.New(slog.NewTextHandler(&logs, nil))), m3) {
		if d.Name == "serde" && (!d.NonRegistry || d.Version != "") {
			t.Errorf("an unreadable inner Cargo.toml let the outer root answer: %+v", d)
		}
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "Cargo.toml") {
		t.Errorf("an unreadable Cargo.toml must be logged; log:\n%s", logs.String())
	}
}

// TestCargoWorkspaceRootSymlinkIsNotRead — v0.29.71 whole-branch review
// (C1): the manifest walk refuses symlinks ("prevent traversal attacks"),
// and the workspace index read ancestor Cargo.toml files with a plain
// ReadFile, following them. A clone whose root Cargo.toml links outside
// the clone (to a host file, or /dev/zero, read forever) must not answer:
// a non-regular Cargo.toml is treated as unreadable — the walk stops, the
// inherited dependency stays unresolved (SR-6), and it is logged.
func TestCargoWorkspaceRootSymlinkIsNotRead(t *testing.T) {
	outside := t.TempDir()
	target := writeFileAt(t, filepath.Join(outside, "Cargo.toml"), "[workspace]\n[workspace.dependencies]\nserde = \"9.9.9\"\n")
	root := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "Cargo.toml")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	member := writeFileAt(t, filepath.Join(root, "crates", "a", "Cargo.toml"), "[dependencies]\nserde.workspace = true\n")
	var logs bytes.Buffer
	found := false
	for _, d := range parseCargoVersionsIn(newCargoWorkspaceIndex(root, slog.New(slog.NewTextHandler(&logs, nil))), member) {
		if d.Name != "serde" {
			continue
		}
		found = true
		if !d.NonRegistry || d.Version != "" {
			t.Errorf("a symlinked root Cargo.toml answered the member's inherited dependency: %+v", d)
		}
	}
	if !found {
		t.Fatal("the member's serde entry is missing — the fixture's premise")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "not a regular file") {
		t.Errorf("a non-regular Cargo.toml must be logged as such; log:\n%s", logs.String())
	}
}
