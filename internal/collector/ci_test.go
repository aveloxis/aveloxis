// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestCIWorkflowsExist verifies all required GitHub Actions workflow files are present.
func TestCIWorkflowsExist(t *testing.T) {
	// Find the repo root by walking up from the test directory.
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}

	required := map[string]string{
		"test.yml":            "Go tests on every push",
		"container-build.yml": "Docker/Podman build test on PRs",
		"docker-publish.yml":  "Docker image publish on main push",
		"codeql.yml":          "CodeQL security analysis on PRs",
		"lint.yml":            "Linting checks on PRs",
		"docs.yml":            "Sphinx docs build with warnings-as-errors on PRs",
	}

	for filename, purpose := range required {
		path := filepath.Join(root, ".github", "workflows", filename)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("missing CI workflow %s (%s)", filename, purpose)
		}
	}
}

// TestDockerfileExists verifies the Dockerfile is present for container builds.
func TestDockerfileExists(t *testing.T) {
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}
	path := filepath.Join(root, "Dockerfile")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("missing Dockerfile in repo root")
	}
}

// TestCIBadgesInREADME verifies the README has CI status badges.
func TestCIBadgesInREADME(t *testing.T) {
	root := srctest.Root(t)
	if root == "" {
		t.Skip("could not find repo root")
	}
	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("could not read README.md: %v", err)
	}
	readme := string(data)

	badges := []string{"test.yml", "lint.yml", "codeql.yml", "container-build.yml", "docker-publish.yml"}
	for _, badge := range badges {
		if !strings.Contains(readme, badge) {
			t.Errorf("README.md missing badge for %s", badge)
		}
	}
}
