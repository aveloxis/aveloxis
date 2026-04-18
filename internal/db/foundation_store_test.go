package db

import (
	"os"
	"strings"
	"testing"
)

// TestUpsertFoundationMembershipExists — store-level source contract: the
// importers command needs a way to record "this repo belongs to CNCF as a
// graduated project" (or equivalent). Without a persisted membership row,
// operators can't query "show me all CNCF Sandbox repos" after the fact.
func TestUpsertFoundationMembershipExists(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(f.Name())
		if err != nil {
			continue
		}
		if strings.Contains(string(data), "func (s *PostgresStore) UpsertFoundationMembership(") {
			found = true
			break
		}
	}
	if !found {
		t.Error("db package must expose UpsertFoundationMembership(ctx, foundation, status, projectName, homepage, repoURL) — " +
			"required so `aveloxis import-foundations` can persist which repos belong to CNCF/Apache at which maturity level")
	}
}

// TestFoundationMembershipSchemaInSchemaSQL — schema.sql must declare the
// aveloxis_ops.foundation_membership table. Defining the DDL at another
// location (e.g., inside Go code) would mean `aveloxis migrate` on a fresh
// DB creates it, but a direct psql schema dump wouldn't show it.
func TestFoundationMembershipSchemaInSchemaSQL(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "aveloxis_ops.foundation_membership") {
		t.Error("schema.sql must declare CREATE TABLE IF NOT EXISTS aveloxis_ops.foundation_membership " +
			"with PRIMARY KEY (foundation, project_name, repo_url) so `aveloxis migrate` " +
			"brings a fresh database up to the shape import-foundations needs")
	}
	if !strings.Contains(src, "PRIMARY KEY (foundation, project_name, repo_url)") &&
		!strings.Contains(src, "PRIMARY KEY(foundation, project_name, repo_url)") {
		t.Error("foundation_membership must use a composite PRIMARY KEY over (foundation, project_name, repo_url) — " +
			"a surrogate SERIAL would let duplicate (foundation, project, repo) rows stack up on repeated imports")
	}
}
