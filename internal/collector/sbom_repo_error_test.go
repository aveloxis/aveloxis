// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestSBOMRepoLookupErrorNamesTheRepository pins PR #218 review A7:
// GenerateSBOMWithOptions returns the repo lookup's error unwrapped on the
// word that "the store names the repository", but GetRepoForSBOM named it
// only for not-found; any other failure came back bare.
func TestSBOMRepoLookupErrorNamesTheRepository(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.Close() // a live context and a failing store: not a not-found
	const repoID = 918273645
	_, err = GenerateSBOMWithOptions(ctx, store, repoID, FormatCycloneDX, SBOMOptions{})
	if err == nil {
		t.Fatal("an SBOM was generated from a closed store")
	}
	if errors.Is(err, db.ErrRepoNotFound) {
		t.Errorf("a store failure read as not-found: %v", err)
	}
	if !strings.Contains(err.Error(), "repo 918273645") {
		t.Errorf("err = %q; want it to name the repository", err)
	}
}
