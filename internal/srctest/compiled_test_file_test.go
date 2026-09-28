// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCompiledTestFile drives every shape go/build leaves out silently —
// the `_` and `.` prefixes, a GOOS or GOARCH suffix (another platform's
// AND this one's), a `//go:build` that never holds, one that holds here,
// a `// +build` line — plus the shapes that must stay accepted: a plain
// file, a suffix that is not a platform, and a directive after the
// package clause (not a constraint; go test fails it loudly). The directory
// half (round 10): a file under testdata, under a `_`- or `.`-prefixed
// directory, or inside a nested module is never compiled by `go test ./...`
// from the root; a plain subdirectory is.
func TestCompiledTestFile(t *testing.T) {
	dir := t.TempDir()
	body := "package p\nimport \"testing\"\nfunc TestX(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/p\n\ngo 1.26\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, src string
		want      bool
	}{
		{"plain_test.go", body, true},
		{"h_foo_test.go", body, true},
		{"g_test.go", "package p\n\n//go:build never\nimport \"testing\"\nfunc TestX(t *testing.T) {}\n", true},
		{"_a_test.go", body, false},
		{".b_test.go", body, false},
		{"c_windows_test.go", body, false},
		{"c_linux_test.go", body, false},
		{"c_darwin_test.go", body, false},
		{"c_amd64_test.go", body, false},
		{"c_linux_amd64_test.go", body, false},
		{"d_test.go", "//go:build never\n\n" + body, false},
		{"e_test.go", "//go:build !windows\n\n" + body, false},
		{"f_test.go", "// +build ignore\n\n" + body, false},
		{"i_test.go", "//go:build never\npackage p\nimport \"testing\"\nfunc TestX(t *testing.T) {}\n", false},
	} {
		path := filepath.Join(dir, tc.name)
		if err := os.WriteFile(path, []byte(tc.src), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := CompiledTestFile(t, dir, path); got != tc.want {
			t.Errorf("%s: compiled on every platform = %v; want %v", tc.name, got, tc.want)
		}
	}
	for _, tc := range []struct {
		sub  string
		want bool
	}{
		{"ok", true},
		{"ok/deeper", true},
		{"testdata", false},
		{"ok/testdata/x", false},
		{"_old", false},
		{".hid", false},
		{"nested", false},
		{"nested/inner", false},
	} {
		d := filepath.Join(dir, filepath.FromSlash(tc.sub))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(tc.sub, "nested") {
			if err := os.WriteFile(filepath.Join(dir, "nested", "go.mod"), []byte("module example.com/nested\n\ngo 1.26\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		path := filepath.Join(d, "a_test.go")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := CompiledTestFile(t, dir, path); got != tc.want {
			t.Errorf("%s/a_test.go: compiled from the root = %v; want %v", tc.sub, got, tc.want)
		}
	}
}
