package collector

import (
	"os"
	"strings"
	"testing"
)

// TestPRChildModeGateInStagedPRs — source-contract test for the phase 1
// feature gate. When cfg.Collection.PRChildMode == "graphql", staged
// collection must call the batched GraphQL fetcher (FetchPRBatch) for
// PR children instead of the per-PR REST waterfall. The REST path
// stays intact as the fallback for mode="rest" (default) and for
// clients that don't implement the batch method (GitLab today).
//
// This test pins the wiring — the actual behavior is covered by
// integration tests that mock the client and the runtime test
// TestFetchPRBatch_HappyPath in the github package.
func TestPRChildModeGateInStagedPRs(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "PRChildMode") {
		t.Error("staged.go must reference PRChildMode to gate between REST and GraphQL paths — " +
			"without the gate, the equivalence test harness can't compare the two")
	}
	if !strings.Contains(code, "FetchPRBatch") {
		t.Error("staged.go must call FetchPRBatch when mode is 'graphql' — otherwise the gate has no effect")
	}
}

// TestPRChildModeGateInRefreshOpen — the refresh-open path iterates open
// PRs and fetches children. Same gate applies: graphql mode should use
// FetchPRBatch, rest mode keeps the existing per-PR waterfall.
//
// Without consistent gating across all three collection paths (main,
// refresh, gap fill), a repo collected on the GraphQL side would diff
// differently from the REST side only in certain columns, and we'd chase
// phantom failures in the equivalence tests.
func TestPRChildModeGateInRefreshOpen(t *testing.T) {
	src, err := os.ReadFile("refresh_open.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "PRChildMode") {
		t.Error("refresh_open.go must consult PRChildMode so GraphQL mode applies consistently to the refresh path")
	}
	if !strings.Contains(code, "FetchPRBatch") {
		t.Error("refresh_open.go must call FetchPRBatch in GraphQL mode")
	}
}

// TestPRChildModeGateInGapFill — same gate for gap fill. Gap fill
// fetches specific PR numbers; the batched fetcher is a natural fit.
func TestPRChildModeGateInGapFill(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "PRChildMode") {
		t.Error("gap_fill.go must consult PRChildMode so GraphQL mode applies to gap-fill too")
	}
	if !strings.Contains(code, "FetchPRBatch") {
		t.Error("gap_fill.go must call FetchPRBatch in GraphQL mode")
	}
}

// TestPRChildModeConfigExists — the config struct must declare the field
// with a sane default (rest) and the JSON tag that matches existing
// aveloxis.json conventions (snake_case).
func TestPRChildModeConfigExists(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "PRChildMode") {
		t.Error("CollectionConfig must declare PRChildMode to control the REST↔GraphQL gate")
	}
	if !strings.Contains(code, `json:"pr_child_mode"`) {
		t.Error("PRChildMode must have the json tag 'pr_child_mode' to match the snake_case convention in aveloxis.json")
	}
	// Default must be "rest" so existing deployments pick up v0.18.1 without
	// behavior change until operators explicitly opt in to GraphQL.
	if !strings.Contains(code, `"rest"`) {
		t.Error("config.DefaultConfig must default PRChildMode to \"rest\" — GraphQL is opt-in until validated in production")
	}
}
