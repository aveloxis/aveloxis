// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

// TestUTF16BOMDetection verifies that ParseDependencyFile detects UTF-16
// BOM and transcodes to UTF-8 before parsing. Some Windows-created
// requirements.txt files are UTF-16LE encoded.
func TestUTF16BOMDetection(t *testing.T) {
	// Behaviour, not shape (0.29.152: this read analysis.go for "0xff").
	// A Windows-created requirements.txt in UTF-16LE with its BOM must come
	// back as UTF-8, with no null byte for PostgreSQL to reject.
	path := filepath.Join(t.TempDir(), "requirements.txt")
	raw := []byte{0xff, 0xfe}
	for _, r := range "flask==2.0.0\n" {
		raw = append(raw, byte(r), 0)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "flask==2.0.0\n" {
		t.Errorf("ReadManifest of a UTF-16LE file = %q, want the UTF-8 text", got)
	}
}
