// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// cargoWorkspaceIndex answers a workspace-inherited Cargo dependency
// (`name.workspace = true`, `name = { workspace = true }`) from the
// workspace root's [workspace.dependencies] (v0.29.71, the v0.29.56
// follow-up). Before, such a dependency was looked up on crates.io by name,
// so a crate the root declares by path or git named whatever crates.io
// package shares the name (SR-6). Decided as a class:
//   - the root is the nearest ancestor Cargo.toml (the manifest's own
//     directory first) with a [workspace] table, never above the scan root;
//   - the root's version is the dependency's, a pinned registry lookup;
//   - a path, git or other-registry declaration there is not a crates.io
//     crate, and neither is a name no root declares — Cargo refuses to
//     build either, and guessing is the defect.
//
// One index per scan, so a workspace of many crates reads its root once.
type cargoWorkspaceIndex struct {
	root string
	// byDir caches each directory's Cargo.toml: nil when the directory has
	// none or it has no [workspace] table.
	byDir map[string]map[string]tomlDepEntry
	seen  map[string]bool
	// unreadable marks a directory whose Cargo.toml exists but could not be
	// read: whether it is the root is unknown, so the walk stops there.
	unreadable map[string]bool
	// logger reports a Cargo.toml that could not be read (nil: discarded).
	logger *slog.Logger
}

func newCargoWorkspaceIndex(root string, logger *slog.Logger) *cargoWorkspaceIndex {
	return &cargoWorkspaceIndex{root: filepath.Clean(root), byDir: map[string]map[string]tomlDepEntry{},
		seen: map[string]bool{}, unreadable: map[string]bool{}, logger: logger}
}

// lookup returns the workspace root's declaration of name for the manifest
// at manifestPath; ok is false when no root in scope declares it.
func (w *cargoWorkspaceIndex) lookup(manifestPath, name string) (tomlDepEntry, bool) {
	dir := filepath.Clean(filepath.Dir(manifestPath))
	for {
		if rel, err := filepath.Rel(w.root, dir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return tomlDepEntry{}, false
		}
		deps := w.workspaceAt(dir)
		if w.unreadable[dir] {
			// Unknown is not "no root here" (SR-5): walking on would let an
			// outer root answer for a crate this one may declare otherwise.
			return tomlDepEntry{}, false
		}
		if deps != nil {
			e, ok := deps[name]
			return e, ok
		}
		if dir == w.root {
			return tomlDepEntry{}, false
		}
		dir = filepath.Dir(dir)
	}
}

// workspaceAt reads dir/Cargo.toml once and returns its
// [workspace.dependencies] when it has a [workspace] table (an empty,
// non-nil map when the table declares none), nil otherwise.
func (w *cargoWorkspaceIndex) workspaceAt(dir string) map[string]tomlDepEntry {
	if w.seen[dir] {
		return w.byDir[dir]
	}
	w.seen[dir] = true
	path := filepath.Join(dir, "Cargo.toml")
	// Lstat first (whole-branch review C1): the manifest walk refuses
	// symlinks against traversal, and a plain read here would follow one —
	// out of the clone, or into /dev/zero, read forever. A Cargo.toml that
	// exists but is not a regular file is unreadable: the walk stops there.
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("not a regular file (%s); not followed", info.Mode().Type())
	}
	var data []byte
	if err == nil {
		data, err = readManifest(path)
	}
	if err != nil {
		w.unreadable[dir] = true
		if w.logger != nil {
			w.logger.Warn("Cargo workspace root could not be read — its members' inherited dependencies are left unresolved",
				"path", path, "error", err)
		}
		return nil
	}
	content := string(data)
	isWorkspace := false
	for _, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(stripHashComment(raw))
		if line == "[workspace]" || strings.HasPrefix(line, "[workspace.") {
			isWorkspace = true
			break
		}
	}
	if !isWorkspace {
		return nil
	}
	deps := map[string]tomlDepEntry{}
	for _, e := range scanTOMLDepTables(content, map[string]bool{"[workspace.dependencies]": true}) {
		deps[e.Name] = e
	}
	w.byDir[dir] = deps
	return deps
}

// parseCargoVersionsIn is parseCargoVersions with workspace inheritance
// answered from ws (scanLibyear passes one index for the whole clone).
func parseCargoVersionsIn(ws *cargoWorkspaceIndex, path string) []libyearDep {
	data, err := readManifest(path)
	if err != nil {
		return nil
	}
	scopes := map[string]string{
		"[dependencies]":       "runtime",
		"[dev-dependencies]":   model.ScopeDev,
		"[build-dependencies]": model.ScopeBuild,
	}
	sections := map[string]bool{}
	for s := range scopes {
		sections[s] = true
	}
	var deps []libyearDep
	for _, e := range scanTOMLDepTables(string(data), sections) {
		src := e
		unresolved := false
		if e.Workspace {
			root, ok := ws.lookup(path, e.Name)
			src, unresolved = root, !ok
		}
		// A git or alternative-registry dep is not the crates.io crate of
		// that name; a path dep is only when it declares the version it
		// publishes as; an inherited dep no root declares is not guessed.
		nonRegistry := unresolved || src.Git || src.Registry || (src.Path && src.Version == "")
		deps = append(deps, libyearDep{Name: e.Name, Version: cleanVersion(src.Version), Requirement: e.Raw,
			Type: scopes[e.Section], Manager: "cargo", NonRegistry: nonRegistry})
	}
	return deps
}
