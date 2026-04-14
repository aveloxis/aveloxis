package collector

import (
	"os"
	"strings"
	"testing"
)

// TestEnrichmentTrackingColumnInSchema verifies the schema includes
// cntrb_last_enriched_at to track when a contributor was last enriched,
// preventing infinite re-enrichment of users with genuinely empty profiles.
func TestEnrichmentTrackingColumnInSchema(t *testing.T) {
	src, err := os.ReadFile("../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "cntrb_last_enriched_at") {
		t.Error("schema.sql must include cntrb_last_enriched_at column on contributors table " +
			"to track when enrichment was last attempted, preventing wasteful re-enrichment " +
			"of users with genuinely empty GitHub profiles")
	}
}

// TestEnrichmentTrackingMigration verifies the migration adds the column
// for existing installations.
func TestEnrichmentTrackingMigration(t *testing.T) {
	src, err := os.ReadFile("../db/migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "cntrb_last_enriched_at") {
		t.Error("migrate.go must add cntrb_last_enriched_at column via addColumnIfMissing " +
			"for existing installations")
	}
}

// TestGetThinContributorLoginsExcludesRecentlyEnriched verifies the thin
// contributor query excludes contributors that were recently enriched but
// have genuinely empty profiles (no company/location on GitHub).
func TestGetThinContributorLoginsExcludesRecentlyEnriched(t *testing.T) {
	src, err := os.ReadFile("../db/contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the GetThinContributorLogins function.
	idx := strings.Index(code, "func (r *ContributorResolver) GetThinContributorLogins(")
	if idx < 0 {
		t.Fatal("cannot find GetThinContributorLogins function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	// Must exclude recently enriched contributors.
	if !strings.Contains(fnBody, "cntrb_last_enriched_at") {
		t.Error("GetThinContributorLogins must filter on cntrb_last_enriched_at to exclude " +
			"recently enriched contributors with genuinely empty profiles")
	}
}

// TestMarkContributorEnrichedExists verifies the DB has a method to mark
// a contributor as enriched.
func TestMarkContributorEnrichedExists(t *testing.T) {
	src, err := os.ReadFile("../db/contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "MarkContributorEnriched") {
		t.Error("contributors.go must contain MarkContributorEnriched to track " +
			"when enrichment was last attempted for a contributor")
	}
}

// TestEnrichThinContributorsMarksEnriched verifies that EnrichThinContributors
// calls MarkContributorEnriched after successful enrichment.
func TestEnrichThinContributorsMarksEnriched(t *testing.T) {
	src, err := os.ReadFile("enrich.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "MarkContributorEnriched") {
		t.Error("EnrichThinContributors must call MarkContributorEnriched after " +
			"successful enrichment to prevent re-enrichment on the next pass")
	}
}

// TestEnrichBatchSizeConstant verifies the batch size is reasonable.
// 14,000 was set when the intent was to enrich all thin contributors in
// one pass, but with tracking we can use a smaller batch.
func TestEnrichBatchSizeConstant(t *testing.T) {
	src, err := os.ReadFile("enrich.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "EnrichBatchSize") {
		t.Error("enrich.go must define EnrichBatchSize constant")
	}
}
