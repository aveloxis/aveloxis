// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.20.16: every Go source file in the repo must carry the two
// SPDX header lines at the top:
//
//	// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
//	// SPDX-License-Identifier: MIT
//
// This test walks the repository and fails if any *.go file is
// missing either line. Matches the pattern used by the
// configuration-docs coverage test in internal/config — a CI-time
// drift detector that catches new files added without the header.
//
// Exclusions:
//   - .git/ and any hidden directory
//   - vendor/ (third-party code; license headers belong to the
//     upstream project)
//   - docs/_build/ (generated HTML output)
//   - node_modules/ (if ever present)
//
// To regenerate headers on missed files, run scripts/add_spdx.sh
// from the repo root (see that file for the apply logic).
const (
	spdxCopyrightLine = "// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard"
	spdxLicenseLine   = "// SPDX-License-Identifier: MIT"
)

func TestEveryGoFileHasSPDXHeader(t *testing.T) {
	// Walk up from this test's working directory to find the
	// repository root. We anchor on the presence of go.mod so
	// the test is robust to being run from any cwd.
	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("finding repo root: %v", err)
	}

	var missing []string

	err = filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") && name != "." {
				return filepath.SkipDir
			}
			switch name {
			case "vendor", "node_modules":
				return filepath.SkipDir
			}
			// docs/_build is generated HTML; nothing Go in there
			// but skip defensively.
			if strings.HasSuffix(path, "/docs/_build") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content := string(b)
		if !strings.Contains(content, spdxCopyrightLine) ||
			!strings.Contains(content, spdxLicenseLine) {
			rel, _ := filepath.Rel(repoRoot, path)
			missing = append(missing, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repo: %v", err)
	}

	if len(missing) > 0 {
		t.Errorf("%d Go file(s) missing SPDX header. Add these two lines at the top:\n  %s\n  %s\n\nMissing files:\n  %s",
			len(missing),
			spdxCopyrightLine,
			spdxLicenseLine,
			strings.Join(missing, "\n  "))
	}
}

// findRepoRoot walks up from the current working directory until
// it finds a go.mod file. Allows the test to be run from any cwd.
func findRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
