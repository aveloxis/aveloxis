// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 (Copilot review 5260961848 on PR #210): refusing REMOTE mode on a
// non-public github.base_url (review 5260880711) left the token in the local
// fallback. invokeScorecard exports the loan as GITHUB_TOKEN on every
// invocation, and local mode rewrites the clone's origin to the repo URL so
// the subprocess can reach the forge for its API-dependent checks — which on
// this deployment means an Enterprise token handed to a process that picks
// its own host, exactly what the remote refusal was for. Local mode still
// runs (the pure-local checks need no token); the loan does not travel.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestScorecardNonPublicHostNeverLendsTheTokenToLocalMode(t *testing.T) {
	const enterprise = "https://ghe.example.invalid/api/v3"
	for _, tc := range []struct {
		name          string
		base          string
		remotePrimary bool
		wantToken     string
	}{
		// The arm the review named: remote refused, clone present.
		{"github repo on an Enterprise host", enterprise, true, ""},
		// The local-only arm on the same deployment: the loan is still an
		// Enterprise token and the subprocess still picks its own host.
		{"local-only platform on an Enterprise host", enterprise, false, ""},
		// The contrast that pins the predicate: on public GitHub the token
		// belongs to the host the subprocess will reach, so local mode
		// keeps it (v0.27.5 behaviour).
		{"local-only platform on public GitHub", "", false, "ghe-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			argsLog, envLog := installFakeScorecard(t,
				`case "$1" in --local) printf '%s' '`+fakeScorecardJSON+`';; *) exit 3;; esac`)
			store := &fakeScorecardStore{}
			res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
				RepoURL:       "https://github.com/augurlabs/augur",
				LocalPath:     t.TempDir(),
				RemotePrimary: tc.remotePrimary,
				Timeout:       time.Minute,
				GithubToken:   "ghe-secret",
				APIBaseURL:    tc.base,
			}, quietLogger())
			if err != nil {
				t.Fatalf("RunScorecard: %v", err)
			}
			if res == nil || res.Mode != "local" {
				t.Fatalf("result = %+v, want a local-mode result", res)
			}
			args := readLines(t, argsLog)
			if len(args) != 1 || !strings.HasPrefix(args[0], "--local ") {
				t.Fatalf("invocations = %q, want exactly one --local call", args)
			}
			env := readLines(t, envLog)
			if len(env) != 1 || env[0] != tc.wantToken {
				t.Errorf("GITHUB_TOKEN seen by scorecard = %q, want %q", env, tc.wantToken)
			}
		})
	}
}

// TestRunScorecardLendsTheTokenThroughOneName pins the structure the fix
// depends on: every invocation and every "was a token lent" check inside
// RunScorecard reads lentToken, never opts.GithubToken, so relaxing the
// remote-mode guard later cannot re-lend the token to a subprocess arm the
// gate no longer covers (round 7 of the 5260961848 fixes: the remote arm
// and the empty-loan check still named opts.GithubToken, equivalent only
// because the guard returned first).
func TestRunScorecardLendsTheTokenThroughOneName(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/scorecard.go"), "func RunScorecard("))
	if n := strings.Count(body, "lentToken := opts.GithubToken"); n != 1 {
		t.Fatalf("RunScorecard must derive lentToken from opts.GithubToken exactly once, found %d", n)
	}
	// ...and that derivation is the ONLY reference to opts.GithubToken in
	// the body: a re-lend past the host gate in any spelling
	// (`lentToken = opts.GithubToken`, `lentToken = strings.TrimSpace(opts.GithubToken)`,
	// a gofmt line break after `=`) needs a second reference, and every
	// call still naming lentToken would hide it (rounds 9–10).
	if n := strings.Count(body, "opts.GithubToken"); n != 1 {
		t.Fatalf("opts.GithubToken is referenced only by the lentToken derivation, found %d references", n)
	}
	if !strings.Contains(body, "if lentToken == \"\"") {
		t.Error("the empty-loan check must read lentToken, the value the arms lend")
	}
	// Every invocation lends lentToken — the argument itself, not the
	// absence of some other name (a copy taken before the derivation would
	// slip past an absence check).
	calls := 0
	for rest := body; ; {
		i := strings.Index(rest, "invokeScorecard(")
		if i < 0 {
			break
		}
		rest = rest[i+len("invokeScorecard("):]
		end := strings.Index(rest, ")")
		if end < 0 {
			t.Fatal("unterminated invokeScorecard call")
		}
		args := strings.Split(rest[:end], ",")
		if len(args) != 8 {
			t.Fatalf("invokeScorecard call has %d arguments, want 8 (token is the 7th): %q", len(args), rest[:end])
		}
		if got := strings.TrimSpace(args[6]); got != "lentToken" {
			t.Errorf("invokeScorecard call lends %q, want lentToken — the host gate must cover every arm", got)
		}
		calls++
	}
	if calls < 3 {
		t.Fatalf("expected the three invocation arms (local, remote, fallback), found %d", calls)
	}
}

// The no-clone half of the non-public arm: nothing runs, nothing is lent,
// and the result is nil, nil (the repo is simply not scored; the arm WARNs).
func TestScorecardNonPublicHostWithoutCloneRunsNothing(t *testing.T) {
	argsLog, envLog := installFakeScorecard(t, `exit 3`)
	store := &fakeScorecardStore{}
	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     "",
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "ghe-secret",
		APIBaseURL:    "https://ghe.example.invalid/api/v3",
	}, quietLogger())
	if res != nil || err != nil {
		t.Fatalf("RunScorecard = %+v, %v; want nil, nil — no clone and remote refused means not scored, not failed", res, err)
	}
	if args := readLines(t, argsLog); len(args) != 0 {
		t.Errorf("scorecard was invoked %q; the non-public arm without a clone must run nothing", args)
	}
	if env := readLines(t, envLog); len(env) != 0 {
		t.Errorf("a token reached the subprocess environment: %q", env)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Errorf("persist calls = %v, want none", calls)
	}
}
