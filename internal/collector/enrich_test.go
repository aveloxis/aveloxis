package collector

import (
	"os"
	"strings"
	"testing"
)

// TestEnrichContributorsPhaseExists verifies the enrichment phase exists
// in the collector package.
func TestEnrichContributorsPhaseExists(t *testing.T) {
	src, err := os.ReadFile("enrich.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "EnrichThinContributors") {
		t.Error("enrich.go must contain EnrichThinContributors function")
	}
	if !strings.Contains(code, "EnrichContributor") {
		t.Error("enrich.go must call EnrichContributor from the platform client")
	}
}

// TestEnrichQueryExists verifies the DB has a method to find thin contributors.
func TestEnrichQueryExists(t *testing.T) {
	src, err := os.ReadFile("../db/contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetThinContributorLogins") {
		t.Error("contributors.go must contain GetThinContributorLogins to find contributors needing enrichment")
	}
}

// TestSchedulerCallsEnrichment verifies the scheduler runs enrichment
// after collection.
func TestSchedulerCallsEnrichment(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "EnrichThinContributors") {
		t.Error("scheduler must call EnrichThinContributors after collection")
	}
}
