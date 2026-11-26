// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// H-4 (summary/59 §5, the hotfix release): repo_dependencies is a snapshot
// table with no unique arbiter. When clearing the previous snapshot failed,
// scanDependencies logged a WARN and inserted the new snapshot on top, so
// every dependency row was duplicated. It now aborts the pass, as the
// libyear rotation has since v0.27.17; the next cycle retries.
func TestScanDependenciesAbortsWhenTheClearFails(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatal(err)
	}
	// A closed pool makes the clear fail for real (no fake store); a pass
	// that carried on would go on to insert over the un-cleared snapshot.
	store.Close()

	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "package.json"), []byte(`{"dependencies":{"left-pad":"1.0.0"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	ac := NewAnalysisCollector(store, lg, t.TempDir())
	err = ac.scanDependencies(ctx, 1, workDir, ac.readClone(ctx, 1, workDir), &AnalysisResult{})
	if err == nil {
		t.Fatal("scanDependencies returned nil after the snapshot clear failed; it must abort, not insert over the old rows")
	}
}
