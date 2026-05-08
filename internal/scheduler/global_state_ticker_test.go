package scheduler

import (
	"os"
	"strings"
	"testing"
)

// readRunJob extracts the body of runJob from scheduler.go for source-contract
// pinning. The body ends at the next top-level "\nfunc " line.
func readRunJob(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	idx := strings.Index(src, "func (s *Scheduler) runJob(")
	if idx < 0 {
		t.Fatal("cannot find runJob in scheduler.go")
	}
	rest := src[idx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of runJob")
	}
	return rest[:end+1]
}

// readRunMethod extracts the body of (*Scheduler).Run from scheduler.go.
// Used to pin the ticker registration in the main loop.
func readRunMethod(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	idx := strings.Index(src, "func (s *Scheduler) Run(")
	if idx < 0 {
		t.Fatal("cannot find Run in scheduler.go")
	}
	rest := src[idx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of Run")
	}
	return rest[:end+1]
}

// TestRunJobDoesNotPopulateAffiliations pins that the per-job hot path
// no longer calls PopulateAffiliations. v0.19.7 moved this to a
// periodic singleton ticker after a production diagnostic on
// 2026-05-08 caught ShareLock contention on the
// `INSERT INTO contributor_affiliations` statement: with 120 workers,
// every completed repo collection triggered a full-table scan of
// contributors + a batch of single-row INSERTs racing on
// UNIQUE (ca_domain). The N-worker concurrent fan-out is the same
// architectural anti-pattern that drove v0.16.5 (dm_repo_*
// aggregates → bulk weekly) and v0.18.29 (EnrichThinContributors →
// periodic ticker).
func TestRunJobDoesNotPopulateAffiliations(t *testing.T) {
	body := readRunJob(t)
	if strings.Contains(body, "PopulateAffiliations(") {
		t.Error("runJob must NOT call PopulateAffiliations directly. " +
			"This is global state — moving it to a periodic singleton " +
			"ticker (runAffiliationsPopulation) eliminates the cross-worker " +
			"contention on UNIQUE (ca_domain) that triggered the v0.19.7 " +
			"hotfix. The per-job invocation is what the production deadlock " +
			"trace caught on 2026-05-08.")
	}
}

// TestRunCommitResolutionDoesNotResolveEmailsToCanonical pins that the
// per-job commit-resolution helper no longer calls
// ResolveEmailsToCanonical. v0.19.7 removed it as redundant with
// runEnrichment: EnrichContributor populates Canonical from the same
// GET /users/{login} endpoint, so the per-job pass was duplicate work
// AND another instance of the global-state-per-worker anti-pattern
// (selects ≤500 contributors fleet-wide, sleeps 100ms × 500, per-row
// UPDATEs cntrb_canonical and cntrb_last_enriched_at).
func TestRunCommitResolutionDoesNotResolveEmailsToCanonical(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	idx := strings.Index(src, "func (s *Scheduler) runCommitResolution(")
	if idx < 0 {
		t.Fatal("cannot find runCommitResolution")
	}
	rest := src[idx:]
	end := strings.Index(rest[1:], "\nfunc ")
	body := rest[:end+1]
	if strings.Contains(body, "ResolveEmailsToCanonical(") {
		t.Error("runCommitResolution must NOT call ResolveEmailsToCanonical. " +
			"runEnrichment (v0.18.29 periodic ticker) already populates " +
			"cntrb_canonical via EnrichContributor, which calls the same " +
			"GET /users/{login} endpoint. The per-job invocation is " +
			"duplicate work and re-introduces the global-state-per-worker " +
			"contention pattern v0.18.29 fixed for the sister function.")
	}
}

// TestSchedulerHasRunAffiliationsPopulation pins the new periodic
// singleton method that replaces the per-job PopulateAffiliations call.
func TestSchedulerHasRunAffiliationsPopulation(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *Scheduler) runAffiliationsPopulation(") {
		t.Error("scheduler.go must define (*Scheduler).runAffiliationsPopulation " +
			"as a periodic singleton task that calls store.PopulateAffiliations " +
			"once per tick, replacing the per-job call site that v0.19.7 removed.")
	}
}

// TestRunRegistersAffiliationsTicker pins that the Run method declares
// an affiliationsTicker and routes its tick to runAffiliationsPopulation.
func TestRunRegistersAffiliationsTicker(t *testing.T) {
	body := readRunMethod(t)
	if !strings.Contains(body, "affiliationsTicker") {
		t.Error("Run must declare an affiliationsTicker (time.NewTicker) " +
			"alongside enrichTicker and searchResolveTicker. Without it, " +
			"PopulateAffiliations never runs after the per-job call is " +
			"removed.")
	}
	if !strings.Contains(body, "runAffiliationsPopulation(ctx)") {
		t.Error("Run's select-loop must include a `case <-affiliationsTicker.C:` " +
			"branch that invokes runAffiliationsPopulation. Otherwise the " +
			"ticker fires but nothing handles it.")
	}
}

// TestConfigHasAffiliationIntervalKnob pins the operator-tunable knob
// for the affiliations-population cadence. Mirrors the existing
// EnrichInterval / SearchResolveInterval pattern so operators have one
// consistent way to tune periodic-task cadence.
func TestConfigHasAffiliationIntervalKnob(t *testing.T) {
	data, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "AffiliationIntervalMinutes") {
		t.Error("config.go must declare AffiliationIntervalMinutes on " +
			"CollectionConfig (json tag affiliation_interval_minutes), with " +
			"a duration accessor mirroring EnrichIntervalDuration.")
	}
	if !strings.Contains(src, "affiliation_interval_minutes") {
		t.Error("config.go must expose the affiliations-ticker cadence as a " +
			"JSON-configurable field named affiliation_interval_minutes.")
	}
}
