// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// DeclaredKind says whether the roster reads a file's declarations, and as
// which kind, from its PATH alone (0.29.214): it opens nothing, so it
// answers for a file that is too large to read, or is not there. It is the
// answer Walk.Read gives in Claimed and Kind for the same path and options.
func TestDeclaredKindIsWhatReadClaimsWithoutReading(t *testing.T) {
	dir := t.TempDir()
	for _, o := range []Options{{}, {DevBuildDeps: true}, {GitHubActionsDeps: true}, {DevBuildDeps: true, GitHubActionsDeps: true}} {
		for _, rel := range []string{"package.json", "sub/pom.xml", "build.gradle.kts", "Gemfile", "x/app.csproj", "requirements-dev.txt",
			".github/workflows/ci.yml", "Dockerfile", "README.md", "go.mod", "pyproject.toml", "mix.exs", "pubspec.yaml", "Cargo.toml", "notes.txt"} {
			path := filepath.Join(dir, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			read := NewWalk(dir, nil).Read(path, o)
			kind, claimed := DeclaredKind(path, o)
			if kind != read.Kind || claimed != read.Claimed {
				t.Errorf("%s under %+v: DeclaredKind = (%q, %v), Read claims (%q, %v)", rel, o, kind, claimed, read.Kind, read.Claimed)
			}
			// And for a file that does not exist: the path alone decides.
			if k2, c2 := DeclaredKind(filepath.Join(dir, "absent", filepath.FromSlash(rel)), o); k2 != kind || c2 != claimed {
				t.Errorf("%s: an absent file is answered (%q, %v), a present one (%q, %v)", rel, k2, c2, kind, claimed)
			}
		}
	}
	if kind, claimed := DeclaredKind(filepath.Join(dir, "package.json"), Options{}); !claimed || kind != "package.json" {
		t.Fatalf("package.json: (%q, %v)", kind, claimed)
	}
}

// A process that may hold only so much (the child that reads a clone)
// installs a bound, and EVERY read of a manifest in this package then stops
// at it: the readers that open the file themselves included. Without one,
// reads are whole, as they always were.
func TestAnInstalledReadBoundHoldsEveryReadOfAManifest(t *testing.T) {
	dir := t.TempDir()
	write := func(rel string, n int) string {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		body := make([]byte, n)
		for i := range body {
			body[i] = ' '
		}
		copy(body, `{"dependencies": {"a": "1.0.0"}}`)
		if err := os.WriteFile(p, body, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	small, large := write("small/package.json", 100), write("large/package.json", 5000)

	// No bound: both are read.
	for _, p := range []string{small, large} {
		if _, err := ReadManifest(p); err != nil {
			t.Fatalf("no bound: %v", err)
		}
	}
	room := int64(1000)
	var asked []int64
	restore := SetReadBound(func(size int64) int64 { asked = append(asked, size); return room })
	t.Cleanup(restore)
	if _, err := ReadManifest(small); err != nil {
		t.Errorf("a file inside the bound: %v", err)
	}
	// The bound is told the length of the file it is asked about, and -1
	// for a file with none to tell.
	if _, err := ReadManifest(filepath.Join(dir, "absent.json")); err == nil {
		t.Error("an absent file was read")
	}
	if len(asked) != 2 || asked[0] != 100 || asked[1] != -1 {
		t.Errorf("the bound was asked with %v; want the file's 100 bytes, then -1 for the absent one", asked)
	}
	var tooLarge *ManifestTooLargeError
	if _, err := ReadManifest(large); !errors.As(err, &tooLarge) || tooLarge.Max != 1000 || tooLarge.Size != 5000 {
		t.Errorf("a file past the bound: %v; want it refused by its length, unread", err)
	}
	// Through the roster: the file is a manifest that could not be read.
	if d := NewWalk(dir, nil).Read(large, Options{}); !d.Claimed || d.Failure == nil || !errors.As(d.Failure.Err, &tooLarge) || len(d.Deps) != 0 {
		t.Errorf("the roster's read of it: %+v", d)
	}
	if d := NewWalk(dir, nil).Read(small, Options{}); d.Failure != nil || len(d.Deps) != 1 {
		t.Errorf("the roster's read of the small one: %+v", d)
	}
	if _, _, _, err := Names(large); !errors.As(err, &tooLarge) {
		t.Errorf("the name reader: %v", err)
	}
	// The bound is asked at each read: the room left changes as the process
	// works. With none left, nothing is read, and nothing is read "whole".
	room = 0
	if _, err := ReadManifest(small); !errors.As(err, &tooLarge) {
		t.Errorf("with no room left a file was read: %v", err)
	}
	room = -5
	if _, err := ReadManifest(small); !errors.As(err, &tooLarge) {
		t.Errorf("a negative bound read a file whole: %v", err)
	}
	// Restored: whole again.
	restore()
	if _, err := ReadManifest(large); err != nil {
		t.Errorf("after the bound was removed: %v", err)
	}
}
