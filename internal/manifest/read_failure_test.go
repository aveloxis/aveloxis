// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A manifest that cannot be read is a failure for EVERY kind (0.29.159).
// Before, only the two JSON kinds reported anything; the rest returned no
// declarations, which reads as a project with no dependencies.
func TestReadReportsAnUnreadableManifestForEveryKind(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: a file cannot be made unreadable")
	}
	dir := t.TempDir()
	examined := 0
	for _, k := range Roster() {
		if k.declared == nil || k.match != nil {
			continue // kinds with a fixed name; the three predicate kinds are below
		}
		examined++
		path := filepath.Join(dir, k.Name)
		if err := os.WriteFile(path, []byte("x"), 0o000); err != nil {
			t.Fatal(err)
		}
		got := NewWalk(dir, nil).Read(path, Options{DevBuildDeps: true, GitHubActionsDeps: true})
		_ = os.Chmod(path, 0o600)
		if !got.Claimed || got.Kind != k.Name || got.Failure == nil || got.Failure.Reason != model.ManifestFailedRead || len(got.Deps) != 0 {
			t.Errorf("%s unreadable: %+v (failure %+v); want claimed, a read failure and no declarations", k.Name, got, got.Failure)
		}
		_ = os.Remove(path)
	}
	if examined < 20 {
		t.Errorf("examined %d kinds; the roster shrank", examined)
	}
	for rel, wantKind := range map[string]string{
		"App.csproj":               "*.csproj",
		"requirements-dev.txt":     "requirements variant",
		".github/workflows/ci.yml": "GitHub Actions workflow",
	} {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o000); err != nil {
			t.Fatal(err)
		}
		got := NewWalk(dir, nil).Read(path, Options{DevBuildDeps: true, GitHubActionsDeps: true})
		_ = os.Chmod(path, 0o600)
		if !got.Claimed || got.Kind != wantKind || got.Failure == nil || got.Failure.Reason != model.ManifestFailedRead {
			t.Errorf("%s unreadable: %+v; want kind %q and a read failure", rel, got, wantKind)
		}
	}
}

// A file is a manifest only when a kind whose gate holds reads declarations
// from it: a lockfile, a Makefile and a gated kind with its knob off are not.
func TestReadClaimsOnlyWhatDeclaresDependencies(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) string {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	w := NewWalk(dir, nil)
	for _, rel := range []string{"yarn.lock", "Makefile", "README.md", "go.sum"} {
		if got := w.Read(write(rel, "x\n"), Options{DevBuildDeps: true, GitHubActionsDeps: true}); got.Claimed || got.Failure != nil || len(got.Deps) != 0 {
			t.Errorf("%s: %+v; want not claimed", rel, got)
		}
	}
	workflow := write(".github/workflows/ci.yml", "jobs:\n  b:\n    steps:\n      - uses: actions/checkout@v4\n")
	if got := w.Read(workflow, Options{}); got.Claimed {
		t.Errorf("a workflow with the Actions knob off: %+v; want not claimed", got)
	}
	if got := w.Read(workflow, Options{GitHubActionsDeps: true}); !got.Claimed || got.Kind != "GitHub Actions workflow" || len(got.Deps) != 1 || got.Failure != nil {
		t.Errorf("a workflow with the knob on: %+v", got)
	}
	broken := write("package.json", "{not json")
	if got := w.Read(broken, Options{}); !got.Claimed || got.Failure == nil || got.Failure.Reason != model.ManifestFailedParse {
		t.Errorf("a package.json that is not JSON: %+v; want a parse failure", got)
	}
}

// writeFixture writes one file in a fresh directory and returns its path.
func writeFixture(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func pathDir(p string) string { return filepath.Dir(p) }
