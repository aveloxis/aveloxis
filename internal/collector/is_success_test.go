package collector

import (
	"testing"
)

// v0.20.10 (Fix F): the pre-v0.20.10 ResolveResult.IsSuccess()
// used `r.KeyExhausted < r.TotalCommits/2` which, due to Go
// integer division, returned false for ANY single-commit job
// regardless of whether keys were actually exhausted. With
// TotalCommits=1, the threshold becomes 0, and `KeyExhausted < 0`
// is never true for the non-negative KeyExhausted value. Result:
// 569 single-commit jobs in the May 9–12 production log were
// logged at ERROR level as "commit resolution FAILED (no API
// keys available — most commits unresolved)" even though
// KeyExhausted was 0 in every case. The real key-exhaustion
// events drowned in the false-positive noise.
//
// The semantic intent (per the original docstring): failure if
// MORE THAN 50% of commits failed due to key exhaustion. Integer
// form: success iff `KeyExhausted * 2 <= TotalCommits`.

func TestIsSuccess_SingleCommitNoKeyExhaustion(t *testing.T) {
	// The dominant case in the May 9–12 log: 569 events with
	// total=1, key_exhausted=0. Must be success.
	r := &ResolveResult{TotalCommits: 1, KeyExhausted: 0}
	if !r.IsSuccess() {
		t.Error("IsSuccess() must return true for TotalCommits=1, KeyExhausted=0 — pre-v0.20.10 the integer-division bug returned false here, producing 569 false-positive ERROR log lines in the production log")
	}
}

func TestIsSuccess_SingleCommitKeyExhausted(t *testing.T) {
	// Genuine single-commit failure: 1 commit, 1 key-exhausted.
	r := &ResolveResult{TotalCommits: 1, KeyExhausted: 1}
	if r.IsSuccess() {
		t.Error("IsSuccess() must return false for TotalCommits=1, KeyExhausted=1 — the one and only commit failed due to key exhaustion")
	}
}

func TestIsSuccess_ZeroCommits(t *testing.T) {
	// Empty job — no work to do, success by definition.
	r := &ResolveResult{TotalCommits: 0}
	if !r.IsSuccess() {
		t.Error("IsSuccess() must return true for TotalCommits=0 — empty job is trivially successful")
	}
}

func TestIsSuccess_HalfExhausted(t *testing.T) {
	// 4 commits, 2 exhausted — exactly 50%. Per the original
	// docstring "more than 50% failed", this should NOT be a
	// failure. v0.20.10 puts the threshold at the 50% boundary
	// inclusively so half-exhausted counts as success.
	r := &ResolveResult{TotalCommits: 4, KeyExhausted: 2}
	if !r.IsSuccess() {
		t.Error("IsSuccess() must return true for TotalCommits=4, KeyExhausted=2 (exactly 50%) — the original docstring says 'more than 50%' is the failure threshold")
	}
}

func TestIsSuccess_MajorityExhausted(t *testing.T) {
	r := &ResolveResult{TotalCommits: 4, KeyExhausted: 3}
	if r.IsSuccess() {
		t.Error("IsSuccess() must return false for TotalCommits=4, KeyExhausted=3 (75% exhausted) — that's a real failure that the operator needs to see")
	}
}

func TestIsSuccess_DBHitOnlyNoAPIKey(t *testing.T) {
	// A real case from the log: every commit resolved via
	// db_hit (cache) so no API call was attempted. KeyExhausted
	// is 0. Must be success regardless of how many commits.
	r := &ResolveResult{TotalCommits: 50, ResolvedDBHit: 50, KeyExhausted: 0}
	if !r.IsSuccess() {
		t.Error("IsSuccess() must return true when all commits resolved via cache hits with zero API calls — KeyExhausted=0 means no key was even consulted")
	}
}
