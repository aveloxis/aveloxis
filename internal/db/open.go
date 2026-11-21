// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"

	"github.com/aveloxis/aveloxis/internal/db/stmttrace"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every pool and every connection this module opens is opened in this file
// (0.29.214, review round 7). The store's own pool, the data-test and
// shadow-diff pools of cmd/aveloxis, the one-connection probes, and the
// pools tests build to hand to production functions all come through the
// four openers below, and each passes its configuration through
// withStatementSeam first.
//
// The reason is the write-policy judge (internal/testdb/writejudge): it is
// a tracer, and a tracer sees only the connections it is put on. While the
// store's pool was the only one that consulted the seam, production code
// run on any other pool was neither judged nor counted as executed, and
// eleven statement sites of the content and catalog comparisons stood on
// the "no test reaches it" baseline although tests ran them.
// scripts/pool_openers_test.go fails on a library constructor anywhere
// else in the module's non-test code, and in the test files of the DB-tier
// packages outside a short reviewed list.
//
// In a binary the seam is nil and each opener is the library call it is
// named for, with nothing added: OpenPool(ctx, s) is pgxpool.New(ctx, s).

// withStatementSeam gives the configuration's tracer to the one test seam
// on what is sent (stmttrace): nil in every binary; the DB-tier test
// harness sets it, to hand each statement to the write-policy judge.
func withStatementSeam(cc *pgx.ConnConfig) {
	if stmttrace.Wrap != nil {
		cc.Tracer = stmttrace.Wrap(cc.Tracer)
	}
}

// OpenPoolConfig is pgxpool.NewWithConfig.
func OpenPoolConfig(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	withStatementSeam(cfg.ConnConfig)
	return pgxpool.NewWithConfig(ctx, cfg)
}

// OpenPool is pgxpool.New: the library's defaults for the connection
// string, and none of the store's own settings (NewPostgresStore).
func OpenPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return OpenPoolConfig(ctx, cfg)
}

// OpenConnConfig is pgx.ConnectConfig.
func OpenConnConfig(ctx context.Context, cfg *pgx.ConnConfig) (*pgx.Conn, error) {
	withStatementSeam(cfg)
	return pgx.ConnectConfig(ctx, cfg)
}

// OpenConn is pgx.Connect.
func OpenConn(ctx context.Context, connString string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return OpenConnConfig(ctx, cfg)
}
