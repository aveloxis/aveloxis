// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// TestRunMigrationsRespectsMatviewMode pins that the matview block in
// RunMigrations switches on pg.matviewMode before either the rebuild or the
// create-if-missing branch fires. With that gate, `aveloxis migrate
// --skip-views` — and a deployment with collection.materialized_views off —
// runs schema DDL only, at no matview cost (v0.29.57).
func TestRunMigrationsRespectsMatviewMode(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func RunMigrations(")
	if fnIdx < 0 {
		t.Fatal("cannot find RunMigrations in migrate.go")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	body := rest[:end+1]

	if !strings.Contains(body, "switch pg.matviewMode") {
		t.Error("RunMigrations must switch on pg.matviewMode before invoking " +
			"CreateMaterializedViews or CreateMaterializedViewsIfNotExist. " +
			"Without that branch, --skip-views and materialized_views:false " +
			"have no effect.")
	}
}

// TestPostgresStoreHasSetMatviewMode pins the public setter migrateCmd and
// serve use, and — more importantly — that OFF is the zero value. A store
// nobody configured must build no views: that is what makes materialized
// views optional for a deployment, and what keeps every test from paying for
// twenty of them (v0.29.57).
func TestPostgresStoreHasSetMatviewMode(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) SetMatviewMode(") {
		t.Error("PostgresStore must define SetMatviewMode(MatviewMode) so " +
			"--skip-views and collection.materialized_views flow into the " +
			"matview gating in RunMigrations.")
	}
	if MatviewsOff != 0 {
		t.Errorf("MatviewsOff = %d, want 0: an unconfigured store must build no views", MatviewsOff)
	}
	var zero PostgresStore
	if zero.matviewMode != MatviewsOff {
		t.Error("the zero PostgresStore must be MatviewsOff — anything else makes views the silent default again")
	}
}
