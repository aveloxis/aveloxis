// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ServerConnectionBudget reports the server's max_connections and the
// connections a normal role cannot use — superuser_reserved_connections
// plus, on PostgreSQL 16+, reserved_connections (the
// pg_use_reserved_connections reserve, default 0) — over ONE short-lived
// connection, before serve opens its pool (v0.29.58). The pool's ceiling
// is fixed at creation, so the budget has to be known first. A failure is
// returned, never guessed (SR-5): the caller sizes from demand alone and
// says so.
func ServerConnectionBudget(ctx context.Context, connString string) (maxConns, reserved int, err error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return 0, 0, fmt.Errorf("connecting for the connection budget: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	show := func(name string) (int, error) {
		var raw string
		if err := conn.QueryRow(ctx, "SHOW "+name).Scan(&raw); err != nil {
			return 0, fmt.Errorf("SHOW %s: %w", name, err)
		}
		n, perr := strconv.Atoi(raw)
		if perr != nil {
			return 0, fmt.Errorf("SHOW %s returned %q: %w", name, raw, perr)
		}
		return n, nil
	}
	if maxConns, err = show("max_connections"); err != nil {
		return 0, 0, err
	}
	if reserved, err = show("superuser_reserved_connections"); err != nil {
		return 0, 0, err
	}
	// reserved_connections exists from PostgreSQL 16; an older server
	// answers "unrecognized configuration parameter" (SQLSTATE 42704),
	// the one error that means "there is no such reserve".
	roleReserve, err := show("reserved_connections")
	if err != nil {
		if isUnknownParameter(err) {
			return maxConns, reserved, nil
		}
		return 0, 0, err
	}
	return maxConns, reserved + roleReserve, nil
}

// isUnknownParameter reports whether err is PostgreSQL's answer to SHOW
// of a parameter this server version does not have: SQLSTATE 42704
// (undefined_object, "unrecognized configuration parameter"). Only that
// code means "there is no such reserve"; every other failure is an error
// the caller must not read as an answer (SR-5).
func isUnknownParameter(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42704"
}

// DefaultPoolMaxConns is the pool ceiling every non-serve command opens
// (web, api, migrate, the one-shot heals): NewPostgresStore's default.
const DefaultPoolMaxConns = 20
