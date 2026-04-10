package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCIWorkflowsExist verifies all required GitHub Actions workflow files are present.
func TestCIWorkflowsExist(t *testing.T) {
	// Find the repo root by walking up from the test directory.
	root := findRepoRoot(t)
	if root == "" {
		t.Skip("could not find repo root")
	}

	required := map[string]string{
		"test.yml":           "Go tests on every push",
		"container-build.yml": "Docker/Podman build test on PRs",
		"docker-publish.yml": "Docker image publish on main push",
		"codeql.yml":         "CodeQL security analysis on PRs",
		"lint.yml":           "Linting checks on PRs",
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
	root := findRepoRoot(t)
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
	root := findRepoRoot(t)
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

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
