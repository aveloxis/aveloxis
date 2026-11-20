// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.44 (summary/19 P1) — behavioral tests for the dependency-scope
// relabeling. Eight ecosystems used to conflate dev/test/build/peer/
// optional dependencies as "runtime"; each fixture below is shaped
// like a real manifest (ground-truth rule: expected values derive
// from each package manager's own documentation, not from what our
// parser happens to emit).

package collector

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// scopeOf finds a dep by name and returns its Type; fails the test if
// the dep is absent (a scope test that silently skips a missing dep
// would pass against a parser that dropped the line entirely).
func scopeOf(t *testing.T, deps []model.LibyearDep, name string) string {
	t.Helper()
	for _, d := range deps {
		if d.Name == name {
			return d.Type
		}
	}
	t.Fatalf("dep %q not found in parse result (%d deps)", name, len(deps))
	return ""
}

func TestCargoScopeSections(t *testing.T) {
	dir := t.TempDir()
	toml := `[package]
name = "demo"

[dependencies]
serde = "1.0"

[dev-dependencies]
criterion = "0.5"

[build-dependencies]
cc = "1.0"
`
	if err := os.WriteFile(filepath.Join(dir, "Cargo.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	deps := parseCargoVersions(filepath.Join(dir, "Cargo.toml"))
	if got := scopeOf(t, deps, "serde"); got != "runtime" {
		t.Errorf("[dependencies] → %q, want runtime", got)
	}
	if got := scopeOf(t, deps, "criterion"); got != model.ScopeDev {
		t.Errorf("[dev-dependencies] → %q, want dev (Cargo's own section semantics)", got)
	}
	if got := scopeOf(t, deps, "cc"); got != model.ScopeBuild {
		t.Errorf("[build-dependencies] → %q, want build", got)
	}
	// NOTE: the per-dep table form ([dependencies.tokio] with a
	// version = "..." line) exits the section tracker and is not
	// parsed — a pre-existing parser limitation, unchanged by the
	// v0.27.44 scope work.
}
