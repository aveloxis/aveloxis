package collector

import (
	"fmt"
	"testing"
)

// ============================================================
// CollectResult edge cases
// ============================================================

func TestCollectResult_Defaults(t *testing.T) {
	r := &CollectResult{}
	if r.Issues != 0 || r.PullRequests != 0 || r.Messages != 0 ||
		r.Events != 0 || r.Releases != 0 || r.Contributors != 0 {
		t.Error("all counts should default to 0")
	}
	if r.Errors != nil {
		t.Error("Errors should be nil by default")
	}
}

func TestCollectResult_ErrorAccumulation(t *testing.T) {
	r := &CollectResult{}
	r.Errors = append(r.Errors, fmt.Errorf("error 1"))
	r.Errors = append(r.Errors, fmt.Errorf("error 2"))
	if len(r.Errors) != 2 {
		t.Errorf("errors = %d, want 2", len(r.Errors))
	}
}

func TestCollectResult_CountSummation(t *testing.T) {
	r := &CollectResult{
		Issues:       10,
		PullRequests: 20,
		Messages:     30,
		Events:       40,
		Releases:     5,
		Contributors: 15,
	}
	total := r.Issues + r.PullRequests + r.Messages + r.Events + r.Releases + r.Contributors
	if total != 120 {
		t.Errorf("total = %d, want 120", total)
	}
}

// ============================================================
// ClientForRepo edge cases (beyond collector_test.go coverage)
// ============================================================

func TestClientForRepo_NilGitHubClient(t *testing.T) {
	// ClientForRepo returns nil client without error when client is nil.
	// The caller is expected to check.
	client, owner, repo, err := ClientForRepo("https://github.com/org/repo", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client")
	}
	if owner != "org" || repo != "repo" {
		t.Errorf("owner=%q repo=%q", owner, repo)
	}
}

func TestClientForRepo_NilGitLabClient(t *testing.T) {
	client, owner, repo, err := ClientForRepo("https://gitlab.com/group/project", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client")
	}
	if owner != "group" || repo != "project" {
		t.Errorf("owner=%q repo=%q", owner, repo)
	}
}

// ============================================================
// Status constants
// ============================================================

func TestStatusConstants_Values(t *testing.T) {
	if StatusPending != "Pending" {
		t.Errorf("StatusPending = %q", StatusPending)
	}
	if StatusCollecting != "Collecting" {
		t.Errorf("StatusCollecting = %q", StatusCollecting)
	}
	if StatusSuccess != "Success" {
		t.Errorf("StatusSuccess = %q", StatusSuccess)
	}
	if StatusError != "Error" {
		t.Errorf("StatusError = %q", StatusError)
	}
	if StatusInitializing != "Initializing" {
		t.Errorf("StatusInitializing = %q", StatusInitializing)
	}
}

func TestPhaseConstants_Values(t *testing.T) {
	if PhasePrelim != "prelim" {
		t.Errorf("PhasePrelim = %q", PhasePrelim)
	}
	if PhasePrimary != "primary" {
		t.Errorf("PhasePrimary = %q", PhasePrimary)
	}
	if PhaseSecondary != "secondary" {
		t.Errorf("PhaseSecondary = %q", PhaseSecondary)
	}
	if PhaseFacade != "facade" {
		t.Errorf("PhaseFacade = %q", PhaseFacade)
	}
}

// ============================================================
// ResolveResult edge cases
// ============================================================

func TestResolveResult_Defaults(t *testing.T) {
	r := &ResolveResult{}
	if r.TotalCommits != 0 || r.ResolvedNoreply != 0 || r.ResolvedDBHit != 0 ||
		r.ResolvedAPI != 0 || r.ResolvedSearch != 0 || r.Unresolved != 0 ||
		r.ContribsCreated != 0 || r.ContribsUpdated != 0 ||
		r.AliasesCreated != 0 || r.Errors != 0 {
		t.Error("all ResolveResult fields should default to 0")
	}
}

func TestResolveResult_SumsCorrectly(t *testing.T) {
	r := &ResolveResult{
		TotalCommits:    100,
		ResolvedNoreply: 30,
		ResolvedDBHit:   40,
		ResolvedAPI:     10,
		ResolvedSearch:  5,
		Unresolved:      15,
	}
	resolved := r.ResolvedNoreply + r.ResolvedDBHit + r.ResolvedAPI + r.ResolvedSearch
	if resolved+r.Unresolved != r.TotalCommits {
		t.Errorf("resolved(%d) + unresolved(%d) != total(%d)",
			resolved, r.Unresolved, r.TotalCommits)
	}
}

// ============================================================
// VulnerabilityResult edge cases
// ============================================================

func TestVulnerabilityResult_Defaults(t *testing.T) {
	r := &VulnerabilityResult{}
	if r.TotalDepsScanned != 0 || r.VulnsFound != 0 {
		t.Error("defaults should be 0")
	}
}

func TestVulnerabilityResult_MultipleVulnsPerDep(t *testing.T) {
	// A single dep can have multiple vulns — VulnsFound can exceed TotalDepsScanned.
	r := &VulnerabilityResult{
		TotalDepsScanned: 10,
		VulnsFound:       25,
	}
	if r.TotalDepsScanned != 10 || r.VulnsFound != 25 {
		t.Error("fields not set correctly")
	}
}

// ============================================================
// SBOMFormat edge cases
// ============================================================

func TestSBOMFormat_String(t *testing.T) {
	if string(FormatCycloneDX) != "cyclonedx" {
		t.Errorf("FormatCycloneDX = %q", FormatCycloneDX)
	}
	if string(FormatSPDX) != "spdx" {
		t.Errorf("FormatSPDX = %q", FormatSPDX)
	}
}
