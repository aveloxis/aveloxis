// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
)

// --- Go purl v-prefix ---

// --- manifest.CleanVersion fixes ---

// --- Go single-line require ---

// --- manifest.ParseRequirementsTxt ---

// --- NuGet case sensitivity ---

// --- package.json peer deps ---

// --- Cargo missing tables ---

// TestCargoParsesBuildDependencies verifies parseCargoVersions handles
// [build-dependencies] in addition to [dependencies] and [dev-dependencies]
// (build deps are needed for an accurate SBOM and vulnerability scan). A
// behavior test since v0.29.71, when the parser moved; it used to read
// analysis.go's source for the table name.
func TestCargoParsesBuildDependencies(t *testing.T) {
	deps := parseCargoVersions(writeManifest(t, "Cargo.toml", "[dependencies]\na = \"1\"\n[dev-dependencies]\nb = \"2\"\n[build-dependencies]\nc = \"3\"\n"))
	scope := map[string]string{}
	for _, d := range deps {
		scope[d.Name] = d.Type
	}
	if scope["a"] != "runtime" || scope["b"] != "dev" || scope["c"] != "build" {
		t.Errorf("scopes = %v, want a runtime, b dev, c build", scope)
	}
}
