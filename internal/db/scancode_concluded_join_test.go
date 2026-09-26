// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/spdx"
)

// TestScancodeConclusionKeepsEachFilesExpression — v0.29.67 review round 1:
// the whole-tree conclusion joined the distinct per-file expressions with a
// bare " AND ", so a file licensed "MIT OR Apache-2.0" next to one licensed
// "BSD-3-Clause" became "BSD-3-Clause AND MIT OR Apache-2.0", which parses as
// (BSD-3-Clause AND MIT) OR Apache-2.0. Both SBOMs carry this conclusion
// (CycloneDX evidence, SPDX licenseConcluded).
func TestScancodeConclusionKeepsEachFilesExpression(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repo := seedRepoForDeps(t, store, ctx, "aveloxis-it", "scancode-conclusion")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_scan.scancode_file_results WHERE repo_id = $1`, repo)
	})
	for _, f := range []struct{ path, lic string }{
		{"a.go", "BSD-3-Clause"},
		{"b.rs", "MIT OR Apache-2.0"},
		{"c.go", "BSD-3-Clause"}, // duplicates collapse
	} {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_scan.scancode_file_results (repo_id, path, detected_license_expression_spdx)
			VALUES ($1, $2, $3)`, repo, f.path, f.lic)
	}
	got, err := store.GetScancodeForSBOM(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	const want = "BSD-3-Clause AND (MIT OR Apache-2.0)"
	if got.ConcludedLicenseSPDX != want {
		t.Errorf("concluded = %q, want %q", got.ConcludedLicenseSPDX, want)
	}
	if !spdx.Valid(got.ConcludedLicenseSPDX) {
		t.Errorf("concluded %q must be a valid SPDX expression", got.ConcludedLicenseSPDX)
	}
}
