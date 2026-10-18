// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"path/filepath"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/spdx"
)

// Helpers production never calls (v0.29.98, stage 0.5 P-4 / COL-5): they
// moved out of the production files so `staticcheck -checks U1000
// -tests=false` (scripts/unused-ratchet.sh) sees only the deliberate seams.
// Each wraps or reads a production path; none carries logic of its own
// that production does not run.

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
func parseCargoVersions(path string) []libyearDep {
	return parseCargoVersionsIn(newCargoWorkspaceIndex(filepath.Dir(path), nil), path)
}

// sccLanguage is scc's per-language JSON record; production decodes the
// stream per file and never materializes it.
type sccLanguage struct {
	Name  string    `json:"Name"`
	Files []sccFile `json:"Files"`
}

// registryHostInterval returns the pacing interval for host (0 = unpaced).
func registryHostInterval(host string) time.Duration {
	registryPaceMu.Lock()
	defer registryPaceMu.Unlock()
	return registryHostIntervals[host]
}

// setRegistryHostIntervalForTest paces an httptest host; TEST-ONLY.
func setRegistryHostIntervalForTest(host string, d time.Duration) (restore func()) {
	registryPaceMu.Lock()
	old, had := registryHostIntervals[host]
	registryHostIntervals[host] = d
	registryPaceMu.Unlock()
	return func() {
		registryPaceMu.Lock()
		defer registryPaceMu.Unlock()
		if had {
			registryHostIntervals[host] = old
		} else {
			delete(registryHostIntervals, host)
		}
		delete(registryNextSlot, host)
		// The deep-queue report is throttled per host for a minute; a later
		// test reusing this httptest port would otherwise see no line.
		delete(registryPaceLastLog, host)
	}
}

func (c *ttlCache[V]) get(key string) (V, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !c.now().Before(e.expires) {
		var zero V
		return zero, nil, false
	}
	return e.val, e.err, true
}

func (c *ttlCache[V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// cdxLicenseFor is the raw-input convenience over cdxLicenseForNormalized
// (the production path normalizes once in makeCDXLicenses).
func cdxLicenseFor(raw, acknowledgement string, inEnum func(string) bool) cdxLicense {
	return cdxLicenseForNormalized(db.NormalizeLicenseToSPDX(raw), acknowledgement, inEnum)
}

func orNoAssertion(s string) string {
	if s == "" {
		return "NOASSERTION"
	}
	return s
}

// isSPDXLicense reports whether a string is an SPDX license identifier, as
// spelled by the official list (internal/spdx, worklist 53, SR-17).
func isSPDXLicense(license string) bool { return spdx.IsLicenseID(license) }
