// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.45 (summary/19 P2) — behavioral tests for the dev/build
// dependency expansion. Fixtures are shaped from real manifests
// (numpy's pyproject build-system + test extras, a poetry-groups
// project, PEP 735, Pipfile dev-packages) per the ground-truth rule.

package collector

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestClassifyGoModTestOnlyDeps(t *testing.T) {
	dir := t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("main.go", `package main

import (
	"fmt"

	"github.com/example/runtimemod/pkg"
)

func main() { fmt.Println(pkg.V) }
`)
	mustWrite("main_test.go", `package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/example/runtimemod/pkg"
)

func TestMain2(t *testing.T) { assert.Equal(t, pkg.V, pkg.V) }
`)
	deps := []model.LibyearDep{
		{Name: "github.com/example/runtimemod", Version: "v1.0.0", Type: "runtime", Manager: "go"},
		{Name: "github.com/stretchr/testify", Version: "v1.9.0", Type: "runtime", Manager: "go"},
		{Name: "github.com/example/indirect", Version: "v0.3.0", Type: "runtime", Manager: "go"},
		{Name: "serde", Version: "1.0", Type: "runtime", Manager: "cargo"},
	}
	out := classifyGoModTestOnlyDeps(dir, deps)
	if got := scopeOf(t, out, "github.com/example/runtimemod"); got != "runtime" {
		t.Errorf("module imported from both test and non-test files → %q, want runtime", got)
	}
	if got := scopeOf(t, out, "github.com/stretchr/testify"); got != model.ScopeTest {
		t.Errorf("module imported ONLY from _test.go → %q, want test (the C1 classification)", got)
	}
	if got := scopeOf(t, out, "github.com/example/indirect"); got != "runtime" {
		t.Errorf("never-imported module → %q, want runtime (absence of evidence is not test evidence)", got)
	}
	if got := scopeOf(t, out, "serde"); got != "runtime" {
		t.Errorf("non-go dep must be untouched, got %q", got)
	}
}
