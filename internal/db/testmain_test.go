// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/testdb"
)

// TestMain runs this package's DB-tier tests in a database of their own,
// created from AVELOXIS_TEST_DB, prepared as an established deployment before
// any test, checked with the data-verify battery after green tests, and
// dropped (internal/testdb), so rows an earlier or concurrent run left behind
// can never reach them. The hooks are testdb/prepare's Deployment and Verify,
// written here because that package imports this one; both call the same
// PrepareTestDeployment and VerifyFailures, and
// TestVerifyThisPackageFailsOnAFAILFinding drives the verify hook itself.
func TestMain(m *testing.M) {
	os.Exit(testdb.Main(m, prepareThisPackage, verifyThisPackage))
}

func prepareThisPackage(ctx context.Context, dsn string) error {
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	err = store.PrepareTestDeployment(ctx, testdb.BootstrapAdminLogin)
	store.Close()
	return err
}

func verifyThisPackage(ctx context.Context, dsn string) error {
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	err = VerifyFailures(store.RunDataVerification(ctx, VerifyOptions{}))
	store.Close()
	return err
}
