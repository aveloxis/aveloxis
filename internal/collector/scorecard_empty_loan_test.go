// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.10 — an empty token loan never reaches remote mode. KeyPool.LendTokens
// lends only usable keys, so ScorecardTokens returns "" when every GitHub key
// is quarantined or on a secondary-limit cooldown. Remote mode was still
// selected and scorecard ran with GITHUB_TOKEN= set to nothing. Probed
// 2026-09-13 with the installed binary: it logs "GitHub token env var is not
// set", goes unauthenticated, hits the rate limit and logs
// "Rate limit exceeded. Waiting 46m50s to retry" — so the attempt held a
// subprocess slot for the whole scorecard_timeout_minutes before any local
// fallback (review comment on PR #203).

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestScorecardEmptyLoanWithCloneRunsLocalOnly(t *testing.T) {
	argsLog, _ := installFakeScorecard(t, `printf '%s' '`+fakeScorecardJSON+`'`)
	store := &fakeScorecardStore{}
	var rlHits atomic.Int64
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { rlHits.Add(1) }))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(),
		RemotePrimary: true, // a GitHub repo...
		Timeout:       time.Minute,
		GithubToken:   "", // ...but every key is resting, so nothing was lent
		RateLimitURL:  rl.URL,
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	args := readLines(t, argsLog)
	if len(args) != 1 || !strings.HasPrefix(args[0], "--local ") {
		t.Fatalf("invocations = %q, want exactly one --local call — a --repo run with no token sleeps through the whole timeout", args)
	}
	if res.Mode != "local" || res.APICalls != 0 {
		t.Errorf("result mode=%q api_calls=%d, want local/0", res.Mode, res.APICalls)
	}
	if rlHits.Load() != 0 {
		t.Errorf("rate_limit probe fired %d times; there is no token to instrument", rlHits.Load())
	}
	if len(store.snapshot()) == 0 {
		t.Error("the local result must still be persisted (subject to the D9 gate)")
	}
}

func TestScorecardEmptyLoanWithoutCloneFailsFast(t *testing.T) {
	argsLog, _ := installFakeScorecard(t, `printf '%s' '`+fakeScorecardJSON+`'`)
	store := &fakeScorecardStore{}
	var rlHits atomic.Int64
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { rlHits.Add(1) }))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     "", // analysis failed, or the bulk run-scorecard pass
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "",
		RateLimitURL:  rl.URL,
	}, quietLogger())
	if !errors.Is(err, ErrScorecardNoToken) {
		t.Fatalf("err = %v, want ErrScorecardNoToken — no token and no clone must fail fast with a named cause, not run remote and not skip silently", err)
	}
	if res != nil {
		t.Errorf("res = %v, want nil", res)
	}
	if args := readLines(t, argsLog); len(args) != 0 {
		t.Errorf("scorecard was invoked %q; nothing may run without a token or a clone", args)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Errorf("persisted %v on the fail-fast path", calls)
	}
	if rlHits.Load() != 0 {
		t.Errorf("rate_limit probe fired %d times", rlHits.Load())
	}
}
