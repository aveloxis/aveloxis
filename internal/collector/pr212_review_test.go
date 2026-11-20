// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// PR #212 Copilot-style review (v0.29.66).

// TestLockfileWithAByteOrderMarkParses — v0.29.62 decoded manifests
// through manifest.ReadManifest, but lockfiles were still read raw: a UTF-8 BOM
// failed the whole lockfile ("invalid character 'ï'").
func TestLockfileWithAByteOrderMarkParses(t *testing.T) {
	dir := t.TempDir()
	body := "\xEF\xBB\xBF" + `{"name":"x","lockfileVersion":3,"packages":{"":{"name":"x"},"node_modules/left-pad":{"version":"1.3.0"}}}`
	if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := collectLockfiles(dir, slog.New(slog.NewTextHandler(io.Discard, nil))).Parsed
	if len(got) != 1 || got[0].Result == nil || len(got[0].Result.Entries) == 0 {
		t.Fatalf("a BOM-prefixed package-lock.json must parse: %+v", got)
	}
}
