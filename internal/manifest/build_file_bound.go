// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"os"
)

// MaxBuildFileBytes bounds one Maven or Gradle build file that the tree
// readers open: a pom, a build or settings script, an applied script, a
// version catalogue, a gradle.properties. It is the bound the collector
// already puts on a pom it FETCHES (its mavenPomBodyLimit; the two are
// pinned equal by a test there): the largest pom in common use is a few
// hundred kilobytes, and a megabyte-scale one is not a pom. A file in the
// clone is no more trusted than one a repository serves.
//
// It is not ReadManifest's bound: that reader also reads lockfiles, and a
// large monorepo's lockfile is legitimately tens of megabytes.
const MaxBuildFileBytes int64 = 8 << 20

// errBuildFileTooLarge: the file is longer than MaxBuildFileBytes.
var errBuildFileTooLarge = errors.New("the build file is larger than a build file is (MaxBuildFileBytes); it was not read")

// regularBuildFile is regularFileInClone for a build file: in the clone, a
// regular file, and no longer than MaxBuildFileBytes.
func regularBuildFile(root, path string) bool {
	if !regularFileInClone(root, path) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.Size() <= MaxBuildFileBytes
}

// buildFileTooLarge reports a file that exists and is over the bound.
func buildFileTooLarge(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() > MaxBuildFileBytes
}
