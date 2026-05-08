package db

import (
	"os"
	"strings"
	"testing"
)

// TestUpsertContributorBatchUpdatesCanonical pins that
// UpsertContributorBatch's DO UPDATE clause backfills cntrb_canonical
// when an incoming row brings a non-empty value.
//
// Pre-v0.19.8 the DO UPDATE clause omitted cntrb_canonical, so
// runEnrichment (which feeds UpsertContributorBatch with rows whose
// Canonical was set from EnrichContributor's GET /users/{login} call)
// silently dropped the canonical email on any existing row. The v0.19.7
// hotfix removed the per-job ResolveEmailsToCanonical call on the
// (correct but partial) claim that EnrichContributor + UpsertContributorBatch
// covered the same data flow. They didn't — UpsertContributorBatch's
// missing UPDATE clause was the gap. v0.19.8 closes it.
func TestUpsertContributorBatchUpdatesCanonical(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *PostgresStore) UpsertContributorBatch(")
	if fnIdx < 0 {
		t.Fatal("cannot find UpsertContributorBatch")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of UpsertContributorBatch")
	}
	body := rest[:end+1]

	// The DO UPDATE clause must include cntrb_canonical with the same
	// COALESCE / NULLIF preserve-existing pattern used for the other
	// fields. Pre-v0.19.8 the column was set on INSERT only.
	if !strings.Contains(body, "cntrb_canonical = COALESCE") {
		t.Error("UpsertContributorBatch's ON CONFLICT DO UPDATE clause must " +
			"include `cntrb_canonical = COALESCE(NULLIF(EXCLUDED.cntrb_canonical, ''), contributors.cntrb_canonical)` " +
			"so runEnrichment's per-tick re-enrichment of existing thin " +
			"contributors actually backfills the canonical email instead of " +
			"silently dropping it. Without this, the v0.19.7 removal of the " +
			"per-job ResolveEmailsToCanonical leaves a permanent regression " +
			"in canonical-email completeness.")
	}
}
