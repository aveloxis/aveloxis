package db

import (
	"os"
	"strings"
	"testing"
)

// TestRunMigrationsRespectsMatviewSkip pins that the matview block in
// RunMigrations checks pg.matviewSkip before either the refresh or the
// create-if-not-exist branch fires. With this gate, `aveloxis migrate
// --skip-views` runs schema DDL only — no matview cost.
func TestRunMigrationsRespectsMatviewSkip(t *testing.T) {
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

	if !strings.Contains(body, "matviewSkip") {
		t.Error("RunMigrations must consult pg.matviewSkip before invoking " +
			"CreateMaterializedViews or CreateMaterializedViewsIfNotExist. " +
			"Without this branch, --skip-views has no effect.")
	}
}

// TestPostgresStoreHasSetMatviewSkip pins the public setter that
// migrateCmd uses to forward the --skip-views flag.
func TestPostgresStoreHasSetMatviewSkip(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) SetMatviewSkip(") {
		t.Error("PostgresStore must define SetMatviewSkip(bool) so the " +
			"--skip-views flag flows into matview-block gating in " +
			"RunMigrations.")
	}
}
