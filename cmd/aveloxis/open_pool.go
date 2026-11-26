// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

// openPool opens a pool with the library's defaults (pgxpool.New), through
// the one place this module opens pools (db.OpenPool; 0.29.214): the
// data-test, seed, perf and shadow-diff commands read and compare
// databases on pools of their own, apart from the store's.
func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return db.OpenPool(ctx, dsn)
}
