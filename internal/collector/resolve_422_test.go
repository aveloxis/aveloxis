package collector

import (
	"testing"
)

// TestResolveResult_ConsecutiveNotFound verifies that the resolver tracks
// consecutive 422/not-found errors and can detect when it should bail out.
func TestResolveResult_ShouldAbort422(t *testing.T) {
	// After 50 consecutive 422s, the resolver should abort — the commits
	// clearly don't belong to this repo.
	r := &ResolveResult{TotalCommits: 1000, Consecutive422: 50}
	if !r.ShouldAbort422() {
		t.Error("50 consecutive 422s should trigger abort")
	}
}

func TestResolveResult_ShouldNotAbort422_Low(t *testing.T) {
	r := &ResolveResult{TotalCommits: 1000, Consecutive422: 10}
	if r.ShouldAbort422() {
		t.Error("10 consecutive 422s should not trigger abort yet")
	}
}

func TestResolveResult_ShouldNotAbort422_Zero(t *testing.T) {
	r := &ResolveResult{}
	if r.ShouldAbort422() {
		t.Error("0 consecutive 422s should not trigger abort")
	}
}
