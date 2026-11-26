// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
)

// regularFileInClone reports whether path is a regular file that is really
// inside the clone rooted at root: the one guard for every file the Maven
// and Gradle readers open because a build file named it (a parent's
// relativePath, a module, `apply from:`, a version catalogue).
//
// "Really" is the point: what is judged is the place the path resolves to
// once every symbolic link along it is followed. A committed link to a
// directory is otherwise a way out (`sym/secrets.gradle`, with `sym`
// pointing anywhere on the host), and the path as written says nothing
// about that. A link that stays inside the clone is the repository's own
// layout and is followed. The file itself must not be a link: that was
// always refused.
func regularFileInClone(root, path string) bool {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	// Both sides resolved: the clone's own root may sit behind a link
	// (macOS's /var, a mounted volume), which is not an escape.
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	return pathWithin(realRoot, realPath)
}

// pathWithin reports whether path is root or below it, as the two are
// written (so it is asked of resolved paths).
func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
