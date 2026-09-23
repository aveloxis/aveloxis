// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestForgeIDTableMissingIsTheOnlyDegrade — v0.29.63 review (finding 6):
// the repo page may drop the forge-ID notice only when the table does not
// exist yet (42P01, an api ahead of its migrate). Pool exhaustion, a
// permission error or a broken connection must fail the stats instead of
// rendering a page without "this may affect statistics".
func TestForgeIDTableMissingIsTheOnlyDegrade(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"undefined_table", &pgconn.PgError{Code: "42P01"}, true},
		{"wrapped undefined_table", fmt.Errorf("list: %w", &pgconn.PgError{Code: "42P01"}), true},
		{"insufficient_privilege", &pgconn.PgError{Code: "42501"}, false},
		{"undefined_column", &pgconn.PgError{Code: "42703"}, false},
		{"pool/transport error", errors.New("failed to connect"), false},
		{"deadline", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		if got := forgeIDTableMissing(c.err); got != c.want {
			t.Errorf("%s: forgeIDTableMissing = %v, want %v", c.name, got, c.want)
		}
	}
}
