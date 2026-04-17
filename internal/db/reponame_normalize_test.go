package db

import (
	"os"
	"strings"
	"testing"
)

// TestUpsertRepoNormalizesName — source-level contract: UpsertRepo must
// normalize r.Name via model.NormalizeRepoName before the INSERT, so
// ".git"-suffixed names never reach the repos table. This is the single
// write-side fix that prevents every downstream 404 on /releases, /issues,
// /pulls, etc. when the slug is used in API URLs.
func TestUpsertRepoNormalizesName(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *PostgresStore) UpsertRepo(")
	if idx < 0 {
		t.Fatal("cannot find UpsertRepo in postgres.go")
	}
	// Examine the function body up to the closing of its main block.
	fnBody := code[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "NormalizeRepoName") {
		t.Error("UpsertRepo must call model.NormalizeRepoName(r.Name) before INSERT — " +
			"otherwise repo slugs with a '.git' suffix hit the DB and every API " +
			"call (/releases, /issues, /pulls) 404s")
	}
}

// TestUpdateRepoURLNormalizesName — same guarantee for the redirect path.
func TestUpdateRepoURLNormalizesName(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *PostgresStore) UpdateRepoURL(")
	if idx < 0 {
		t.Fatal("cannot find UpdateRepoURL in postgres.go")
	}
	fnBody := code[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "NormalizeRepoName") {
		t.Error("UpdateRepoURL must call model.NormalizeRepoName on the parsed name — " +
			"redirects should also produce clean slugs in the DB")
	}
}

// TestRepoNameGitCleanupMigrationPresent — one-time migration must be wired
// into RunMigrations to strip ".git" from existing repo_name values so
// databases populated before the normalize fix don't keep failing.
func TestRepoNameGitCleanupMigrationPresent(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "cleanupRepoNameGitSuffix") {
		t.Error("migrate.go must define and call cleanupRepoNameGitSuffix to strip " +
			"legacy '.git' suffixes from aveloxis_data.repos.repo_name — without this, " +
			"pre-existing rows keep producing 404s on API collection")
	}

	// Must be wired into RunMigrations.
	runIdx := strings.Index(code, "func RunMigrations(")
	if runIdx < 0 {
		t.Fatal("cannot find RunMigrations")
	}
	runBody := code[runIdx:]
	end := strings.Index(runBody, "\n}\n")
	if end > 0 {
		runBody = runBody[:end]
	}
	if !strings.Contains(runBody, "cleanupRepoNameGitSuffix") {
		t.Error("RunMigrations must invoke cleanupRepoNameGitSuffix during startup")
	}
}
