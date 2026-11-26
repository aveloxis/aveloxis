// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Fuzz targets for the collector's trust-boundary parsers. These are the
// exact code paths that have produced production incident classes:
// garbage manifest versions defeating OSV matching (v0.27.71), raw purls
// 400-ing whole scan batches (v0.27.73), lockfile formats arriving
// malformed/truncated (v0.27.11). All targets run under ClusterFuzzLite
// (see .clusterfuzzlite/build.sh) and, seeds-only, in ordinary `go test`.

// FuzzParseLockfile drives every lockfile format with hostile bytes.
// Seeds come from the committed v0.27.11 fixture corpus so coverage
// starts warm. Contract: never panic; parse errors are the designed
// degrade path (WARN + skip in scanLockfiles).
func FuzzParseLockfile(f *testing.F) {
	kinds := []string{
		"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "bun.lock",
		"bun.lockb", "poetry.lock", "Pipfile.lock", "uv.lock", "pdm.lock",
		"Cargo.lock", "Gemfile.lock", "composer.lock", "mix.lock",
		"pubspec.lock", "Package.resolved", "packages.lock.json",
		"gradle.lockfile", "stack.yaml.lock", "renv.lock",
	}
	if len(kinds) != len(lockfileKinds) {
		f.Fatalf("the fuzz list names %d lockfile kinds and the roster has %d: a kind that is not fuzzed", len(kinds), len(lockfileKinds))
	}
	// Seed every fixture under its kind (fixture filenames are the kind
	// name, optionally with a variant suffix like Package.resolved.v2).
	fixtures, _ := os.ReadDir(filepath.Join("testdata", "lockfiles"))
	for _, fx := range fixtures {
		data, err := os.ReadFile(filepath.Join("testdata", "lockfiles", fx.Name()))
		if err != nil {
			continue
		}
		for _, kind := range kinds {
			if strings.HasPrefix(fx.Name(), kind) {
				f.Add(kind, data)
				break
			}
		}
	}
	for _, kind := range kinds {
		f.Add(kind, []byte("{"))
		f.Add(kind, []byte("\x00\xff garbage: [unterminated"))
	}

	f.Fuzz(func(t *testing.T, kind string, data []byte) {
		res, err := ParseLockfile(kind, data)
		if err != nil {
			return
		}
		if res == nil {
			t.Fatal("ParseLockfile returned nil result with nil error")
		}
	})
}
