// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Two purls that ask OSV one question (two refs of one GitHub Action) get
// one answer, whatever the cache does between them (review round 1). The
// cache was read once per PURL: when the entry expired or was evicted
// between the two reads, the first purl was answered from it and the second
// was neither answered nor asked, since it was not the first of its key.
// Its open findings then closed, with no answer behind the closing.
func TestTwoPurlsOfOneQuestionGetOneAnswer(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"results":[{"vulns":[{"id":"GHSA-live"}]}]}`))
	}))
	defer srv.Close()
	old := osvBatchURL
	osvBatchURL = srv.URL
	defer func() { osvBatchURL = old }()

	a, b := "pkg:githubactions/x/y@v1", "pkg:githubactions/x/y@v2"
	targets := map[string]model.VulnScanTarget{
		a: {OSVQueryName: "x/y", OSVQueryEcosystem: "GitHub Actions"},
		b: {OSVQueryName: "x/y", OSVQueryEcosystem: "GitHub Actions"},
	}
	t0 := time.Now()
	for _, expiresAtRead := range []int{2, 3} { // before the first purl's read, and between the two
		cache := NewOSVCache()
		n := 0
		cache.now = func() time.Time {
			n++
			if n >= expiresAtRead+1 { // call 1 is putAnswer
				return t0.Add(25 * time.Hour)
			}
			return t0
		}
		cache.putAnswer(osvQueryKey(a, targets), []string{"GHSA-cached"}, t0)
		calls = 0
		out, err := fetchOSVQueryAnswers(context.Background(), cache, []string{a, b}, targets, &VulnerabilityResult{}, osvFetchOptions{AskedAt: t0})
		if err != nil {
			t.Fatal(err)
		}
		idsA, okA := out.IDs[a]
		idsB, okB := out.IDs[b]
		if !okA || !okB {
			t.Fatalf("expiry at read %d: a answered %v, b answered %v; both must be", expiresAtRead, okA, okB)
		}
		if len(idsA) != 1 || len(idsB) != 1 || idsA[0] != idsB[0] {
			t.Errorf("expiry at read %d: a %v, b %v; one question, one answer", expiresAtRead, idsA, idsB)
		}
		if out.Rejected[b] || out.Truncated[b] {
			t.Errorf("expiry at read %d: b is marked rejected or truncated", expiresAtRead)
		}
		if calls > 1 {
			t.Errorf("expiry at read %d: OSV was asked %d times for one question", expiresAtRead, calls)
		}
	}
}

// And a target with no answer at all is incomplete, whatever the reason: a
// scan never closes a finding on a question nobody answered.
func TestATargetWithNoAnswerIsIncomplete(t *testing.T) {
	answers := osvQueryAnswers{IDs: map[string][]string{"pkg:npm/a@1": {}}, Rejected: map[string]bool{"pkg:npm/r@1": true}, Truncated: map[string]bool{"pkg:npm/t@1": true}}
	answers.IDs["pkg:npm/t@1"] = []string{"GHSA-1"}
	for purl, want := range map[string]bool{"pkg:npm/a@1": false, "pkg:npm/r@1": true, "pkg:npm/t@1": true, "pkg:npm/never@1": true} {
		if got := answers.incomplete(purl); got != want {
			t.Errorf("incomplete(%s) = %v, want %v", purl, got, want)
		}
	}
}
