// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The predicates of the manifest kinds that have no fixed file name.

// requirementsFileScope classifies a requirements-variant filename
// into a dependency scope. ok=false means the file is not a
// requirements variant (or is the exact "requirements.txt", which the
// walk's dedicated case already handles as runtime). Classification
// is by the ecosystem's own filename convention (open decision #6,
// operator-accepted): test → test; dev/lint/doc → dev;
// build/ci/release → build; bare/prod/base → runtime.
func requirementsFileScope(base, path string) (string, bool) {
	lower := strings.ToLower(base)
	if !strings.HasSuffix(lower, ".txt") {
		return "", false
	}
	if lower == "requirements.txt" {
		return "", false // the walks' requirements.txt case owns this, in any letter case (canonicalManifestName)
	}
	stem := strings.TrimSuffix(lower, ".txt")
	inRequirementsDir := filepath.Base(filepath.Dir(path)) == "requirements"
	isVariant := strings.HasPrefix(stem, "requirements") ||
		strings.HasSuffix(stem, "-requirements") ||
		strings.HasSuffix(stem, "_requirements")
	if !isVariant && !inRequirementsDir {
		return "", false
	}
	// Token classification on the stem with the requirements part
	// stripped: "requirements-dev" → "dev", "test_requirements" →
	// "test", requirements/ci.txt → "ci".
	token := strings.Trim(strings.ReplaceAll(strings.ReplaceAll(stem, "requirements", ""), "_", "-"), "-")
	switch {
	case strings.Contains(token, "test"):
		return model.ScopeTest, true
	case strings.Contains(token, "dev"), strings.Contains(token, "lint"), strings.Contains(token, "doc"):
		return model.ScopeDev, true
	case strings.Contains(token, "build"), strings.Contains(token, "ci"), strings.Contains(token, "release"):
		return model.ScopeBuild, true
	default:
		// bare requirements/foo.txt, requirements-prod.txt, base…
		return "runtime", true
	}
}

// isWorkflowPath reports whether path is a GitHub Actions workflow
// file (.github/workflows/*.yml|yaml).
func isWorkflowPath(path string) bool {
	dir := filepath.ToSlash(filepath.Dir(path))
	if !strings.HasSuffix(dir, ".github/workflows") {
		return false
	}
	return strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")
}
