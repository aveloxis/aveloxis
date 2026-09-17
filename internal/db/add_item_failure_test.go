// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestAddItemFailurePermanent: only an error in the item's own values stops a
// retry. A lost connection, a timeout, a full server, a deadlock, a concurrent
// delete or dedup, and any error that is not a Postgres error leave the item
// for a later pass (round-23 review: the database test only injected 40001, and
// treating a dropped connection, or every error that is not a Postgres error,
// as permanent passed every tier).
func TestAddItemFailurePermanent(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code} }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"string data right truncation (22001)", pg("22001"), true},
		{"invalid text representation (22P02)", pg("22P02"), true},
		{"not-null violation (23502)", pg("23502"), true},
		{"check violation (23514)", pg("23514"), true},
		{"wrapped check violation", fmt.Errorf("add-request item %q: %w", "u", pg("23514")), true},
		{"foreign key violation (23503)", pg("23503"), false},
		{"unique violation (23505)", pg("23505"), false},
		{"exclusion violation (23P01)", pg("23P01"), false},
		{"integrity constraint violation (23000)", pg("23000"), false},
		{"serialization failure (40001)", pg("40001"), false},
		{"deadlock (40P01)", pg("40P01"), false},
		{"admin shutdown (57P01)", pg("57P01"), false},
		{"query cancelled (57014)", pg("57014"), false},
		{"too many connections (53300)", pg("53300"), false},
		{"connection failure (08006)", pg("08006"), false},
		{"wrapped connection failure", fmt.Errorf("link repo: %w", pg("08006")), false},
		{"not a Postgres error", errors.New("conn closed"), false},
		{"unexpected EOF", io.ErrUnexpectedEOF, false},
		{"deadline exceeded", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		if got := addItemFailurePermanent(c.err); got != c.want {
			t.Errorf("addItemFailurePermanent(%s) = %v; want %v", c.name, got, c.want)
		}
	}
}
