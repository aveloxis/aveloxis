// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
)

// A build file in a clone is bounded as a pom that is fetched is: one
// figure, stated in two packages (the manifest package cannot import this
// one), and pinned equal here.
func TestABuildFileInTheCloneIsBoundedAsAFetchedPomIs(t *testing.T) {
	if manifest.MaxBuildFileBytes != mavenPomBodyLimit {
		t.Errorf("manifest.MaxBuildFileBytes = %d and mavenPomBodyLimit = %d; they are one bound", manifest.MaxBuildFileBytes, mavenPomBodyLimit)
	}
}
