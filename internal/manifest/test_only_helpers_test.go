// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"

	"github.com/aveloxis/aveloxis/internal/model"
)

// parseCargoVersions parses one lone Cargo manifest: its own directory is
// the only workspace root in scope (scanLibyear passes one index for the
// whole clone instead). The rules are parseCargoVersionsIn's:
// parseCargoVersions extracts crates.io deps from Cargo.toml through the
// shared table scanner (manifest_toml.go).
//
// v0.27.44 (summary/19 P1): [dev-dependencies] = tests/examples/benches,
// [build-dependencies] = build scripts. v0.27.71: a table carries a
// version only under an explicit version key; a workspace-inherited dep
// has none (the unpinned pathway). v0.29.56: git- and
// alternative-registry-sourced deps are not crates.io packages, and a
// path dep counts only with the version it is published as, so none of
// them is looked up on crates.io.
func parseCargoVersions(path string) []model.LibyearDep {
	return parseCargoVersionsIn(newCargoWorkspaceIndex(filepath.Dir(path), nil), path)
}
