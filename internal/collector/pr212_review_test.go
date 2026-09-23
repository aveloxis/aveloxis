// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// PR #212 Copilot-style review (v0.29.66).

// TestRequirementsKeepTheNameOfANamedDirectReference — a PEP 508 direct
// reference names its package before the '@' ("requests @ git+https://…");
// the v0.29.62 name check dropped these lines whole instead of keeping the
// name. A bare URL or path names no package and stays out.
func TestRequirementsKeepTheNameOfANamedDirectReference(t *testing.T) {
	got := parseRequirementsTxt("requests @ git+https://github.com/psf/requests\n" +
		"pkg@https://example.com/pkg-1.0.whl\n" +
		"anyio[trio] @ https://x/y.whl\n" +
		"git+https://github.com/o/r@v1\n" +
		"./local-pkg\n")
	want := []string{"requests", "pkg", "anyio"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseRequirementsTxt = %v, want %v", got, want)
	}
}

// TestRequirementsParenthesizedSpecifier — "requests (>=2.0)" is valid PEP
// 508 (the '(' version_many ')' form Poetry 2.x writes). It was dropped by
// the name check; it names requests, with the specifier inside.
func TestRequirementsParenthesizedSpecifier(t *testing.T) {
	if got := parseRequirementsTxt("requests (>=2.0)\n"); !reflect.DeepEqual(got, []string{"requests"}) {
		t.Errorf("parseRequirementsTxt = %v, want [requests]", got)
	}
	if name, spec := splitPyNameSpec("requests (>=2.0,<3)"); name != "requests" || spec != ">=2.0,<3" {
		t.Errorf("splitPyNameSpec = %q, %q; want requests, >=2.0,<3", name, spec)
	}
}

// TestLockfileWithAByteOrderMarkParses — v0.29.62 decoded manifests
// through readManifest, but lockfiles were still read raw: a UTF-8 BOM
// failed the whole lockfile ("invalid character 'ï'").
func TestLockfileWithAByteOrderMarkParses(t *testing.T) {
	dir := t.TempDir()
	body := "\xEF\xBB\xBF" + `{"name":"x","lockfileVersion":3,"packages":{"":{"name":"x"},"node_modules/left-pad":{"version":"1.3.0"}}}`
	if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := collectLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(got) != 1 || got[0].Result == nil || len(got[0].Result.Entries) == 0 {
		t.Fatalf("a BOM-prefixed package-lock.json must parse: %+v", got)
	}
}

// TestSwiftURLFoldsWWW — https://www.github.com/o/r names the same
// repository as github.com; the exact-host check rejected it and the purl
// namespace kept "www.github.com", which OSV does not match.
func TestSwiftURLFoldsWWW(t *testing.T) {
	ref, ok := parseSwiftPackageURL("https://www.github.com/o/r")
	if !ok || ref.Host != "github.com" || ref.Namespace != "github.com/o" {
		t.Errorf("parseSwiftPackageURL(www) = %+v %v, want host github.com, namespace github.com/o", ref, ok)
	}
}

// TestEveryPythonInventoryReaderNamesDirectReferencesAlike — requirements.txt,
// PEP 621 dependencies and setup.py/setup.cfg install_requires share one
// name rule (SR-17): a named direct reference keeps its name, a
// parenthesized specifier ends the name, and a bare URL or path names
// nothing.
func TestEveryPythonInventoryReaderNamesDirectReferencesAlike(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"requests @ git+https://github.com/psf/requests", "requests"},
		{"requests (>=2.0)", "requests"},
		{"anyio[trio]>=4", "anyio"},
		{"https://example.com/pkg-1.0.whl", ""},
		{"./local-pkg", ""},
	} {
		if got := extractPyDepName(tc.in); got != tc.want {
			t.Errorf("extractPyDepName(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if got := extractPEP621DepName(`"` + tc.in + `",`); got != tc.want {
			t.Errorf("extractPEP621DepName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
