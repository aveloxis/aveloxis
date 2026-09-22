// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsUnknownParameterOnlyAccepts42704 drives the one error the budget
// probe may read as "this server has no such reserve" (review round 5):
// a wrapped 42704 is the answer; any other SQLSTATE, or a non-Postgres
// error, is a failure the caller must return (SR-5).
func TestIsUnknownParameterOnlyAccepts42704(t *testing.T) {
	unknown := fmt.Errorf("SHOW reserved_connections: %w", &pgconn.PgError{Code: "42704", Message: "unrecognized configuration parameter"})
	if !isUnknownParameter(unknown) {
		t.Fatal("a wrapped 42704 must read as an unknown parameter")
	}
	for _, e := range []error{
		fmt.Errorf("SHOW x: %w", &pgconn.PgError{Code: "42P01"}),
		fmt.Errorf("SHOW x: %w", &pgconn.PgError{Code: "57P03"}),
		errors.New("connection refused"),
		nil,
	} {
		if isUnknownParameter(e) {
			t.Errorf("%v must not read as an unknown parameter", e)
		}
	}
}
