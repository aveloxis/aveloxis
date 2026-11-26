// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The two walks over a clone's manifests reject symlinks. A malicious repo
// can contain a symlink like requirements.txt -> /etc/passwd; without the
// check, os.ReadFile follows it and stores host file contents as dependency
// names. (0.29.213: the walks are manifest_pass.go's, run in a child;
// TestTheManifestWalksDoNotFollowSymlinks drives both through it.)
func TestScanDependenciesSkipsSymlinks(t *testing.T) {
	for _, fn := range []string{"func readManifestNames(", "func readManifestDeclarations("} {
		if code := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", fn)); !strings.Contains(code, "info.Mode()&os.ModeSymlink != 0") {
			t.Errorf("%s must check for os.ModeSymlink in its filepath.Walk callback to prevent symlink traversal attacks", fn)
		}
	}
}
