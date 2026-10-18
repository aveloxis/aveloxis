// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"path"
	"strings"
)

// SiteKey is the ONE spelling of a reviewed-site registry key (v0.29.98,
// stage 0.5 P-1): the package directory and the function, then any
// further discriminator the registry needs (a log message, a column).
// Registries used to key on the FILE, so every split of a file churned
// every entry naming it; a function keeps its name across a split, and
// the directory is as far as a key needs to reach. A registry that must
// tell two identical messages in two functions apart keeps the function
// (dir|func|message), never the file (L2 granularity).
//
//	srctest.SiteKey("internal/api", "handleAdminPrioritizeRepo")
//	srctest.SiteKey(srctest.SiteDir(rel), fn.Name.Name, msg)
func SiteKey(dir, fn string, extra ...string) string {
	parts := append([]string{dir, fn}, extra...)
	return strings.Join(parts, "|")
}

// SiteDir is the directory part of a repo-relative file path, in the
// forward-slash form registries key on.
func SiteDir(repoRelFile string) string {
	return path.Dir(strings.ReplaceAll(repoRelFile, "\\", "/"))
}
