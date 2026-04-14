package db

import (
	"os"
	"strings"
	"testing"
)

// TestSchemaMetaTableExistsInDDL verifies the schema_meta table is defined
// in schema.sql. This single-row table tracks the schema version so that
// non-migrating commands (web, api) can warn when the DB is behind the binary.
func TestSchemaMetaTableExistsInDDL(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "aveloxis_ops.schema_meta") {
		t.Error("schema.sql must define aveloxis_ops.schema_meta table to track schema version")
	}
	if !strings.Contains(code, "schema_version") {
		t.Error("schema_meta table must have a schema_version column")
	}
}

// TestMigrateStampsSchemaVersion verifies that RunMigrations updates the
// schema_meta table with the current ToolVersion after all migrations complete.
func TestMigrateStampsSchemaVersion(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find RunMigrations function.
	idx := strings.Index(code, "func RunMigrations(")
	if idx < 0 {
		t.Fatal("cannot find RunMigrations function")
	}
	fnBody := code[idx:]

	// Must stamp the schema version.
	if !strings.Contains(fnBody, "schema_meta") {
		t.Error("RunMigrations must update aveloxis_ops.schema_meta with the current ToolVersion")
	}
	if !strings.Contains(fnBody, "ToolVersion") {
		t.Error("RunMigrations must write ToolVersion to schema_meta")
	}
}

// TestGetSchemaVersionExists verifies the PostgresStore has a method to
// read the current schema version from the database.
func TestGetSchemaVersionExists(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetSchemaVersion") {
		t.Error("migrate.go must define GetSchemaVersion to read the stored schema version")
	}
}

// TestCheckSchemaVersionExists verifies the PostgresStore has a method to
// check if the schema is up to date and log a warning if not.
func TestCheckSchemaVersionExists(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "CheckSchemaVersion") {
		t.Error("migrate.go must define CheckSchemaVersion for non-migrating commands " +
			"(web, api) to warn when the schema is behind the binary")
	}
}

// TestWebCommandChecksSchemaVersion verifies the web command calls
// CheckSchemaVersion on startup so users get a clear warning when the
// schema needs migrating.
func TestWebCommandChecksSchemaVersion(t *testing.T) {
	src, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the web command section — it has the "web does NOT run migrations" comment.
	idx := strings.Index(code, "web does NOT run migrations")
	if idx < 0 {
		t.Fatal("cannot find web command section in main.go")
	}
	// Look in a window around that comment for the schema check.
	start := idx - 200
	if start < 0 {
		start = 0
	}
	end := idx + 500
	if end > len(code) {
		end = len(code)
	}
	section := code[start:end]

	if !strings.Contains(section, "CheckSchemaVersion") {
		t.Error("web command must call CheckSchemaVersion on startup to warn when " +
			"the schema is behind the binary version. Users should see a clear message " +
			"to run 'aveloxis migrate' or restart 'aveloxis serve'.")
	}
}

// TestAPICommandChecksSchemaVersion verifies the api command calls
// CheckSchemaVersion on startup.
func TestAPICommandChecksSchemaVersion(t *testing.T) {
	src, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the runAPI function.
	idx := strings.Index(code, "func runAPI(")
	if idx < 0 {
		t.Fatal("cannot find runAPI function in main.go")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	if !strings.Contains(fnBody, "CheckSchemaVersion") {
		t.Error("api command must call CheckSchemaVersion on startup to warn when " +
			"the schema is behind the binary version")
	}
}
