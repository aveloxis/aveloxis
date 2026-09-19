// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package prepare holds the hooks testdb.Main runs on a per-package test
// database: Deployment before the tests, Verify after them. It is separate
// from internal/testdb because it imports internal/db, and internal/db's own
// tests import internal/testdb.
package prepare

import (
	"context"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/testdb"
)

// Deployment makes a fresh test database look like an established deployment
// (db.PrepareTestDeployment): migrated, with its Default repo group and a
// bootstrap admin. Tests then depend on neither another package nor an earlier
// run having done any of it.
func Deployment(ctx context.Context, dsn string) error {
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	defer store.Close()
	return store.PrepareTestDeployment(ctx, testdb.BootstrapAdminLogin)
}

// Verify runs the data-verify invariant battery — the one `aveloxis
// data-verify` runs, with its defaults — over the database a package's tests
// left, and fails on its FAIL findings (db.VerifyFailures): a test that leaks
// rows breaking an invariant the battery rates FAIL (cached queue counts that
// disagree with the tables, duplicate 'Default' groups, case-variant duplicate
// repos, new cross-kind message collisions, batch/single stats disagreement)
// fails its own package. WARN findings, such as stranded repos, pass.
func Verify(ctx context.Context, dsn string) error {
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.DiscardHandler))
	if err != nil {
		return err
	}
	defer store.Close()
	return db.VerifyFailures(store.RunDataVerification(ctx, db.VerifyOptions{}))
}
