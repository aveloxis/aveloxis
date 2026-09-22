// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 3: CollectRepo refused a credentialed URL, but
// `aveloxis rewalk-whitespace` reaches ensureClone directly, so the clone
// log line and git's command line saw the credential. The refusal lives in
// ensureClone itself now — every clone path shares it.
func TestEnsureCloneRefusesUserinfo(t *testing.T) {
	var logs bytes.Buffer
	fc := NewFacadeCollector(nil, slog.New(slog.NewTextHandler(&logs, nil)), t.TempDir())
	path := filepath.Join(fc.repoDir, "probe")
	err := fc.ensureClone(context.Background(), "https://user:s3cret@github.com/owner/name", path)
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("ensureClone = %v, want platform.ErrURLUserinfo", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("a refused URL created the clone path")
	}
	if bytes.Contains(logs.Bytes(), []byte("s3cret")) {
		t.Errorf("ensureClone logged the credential: %s", logs.String())
	}
}
