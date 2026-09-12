// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// 2026-09-12 operator rule: each scorecard run either delivers the full
// check set or leaves the prior complete set in place. Before this,
// persistScorecard rotated the current rows to history UNCONDITIONALLY
// and inserted whatever the run produced — so a 15-minute remote
// timeout followed by a successful local fallback REPLACED an 18-check
// remote set with an 11-check local one, and 10,214 repos on chaoss.tv
// came to hold the 7-check deficit (Branch-Protection, CII-Best-
// Practices, CI-Tests, Code-Review, Contributors, Maintained,
// Signed-Releases: zero rows in local mode). A consumer reading the
// scorecard table cannot tell "this project has no code review" from
// "the run that could measure code review timed out".
//
// The discriminator is the stored scorecard_mode, not the check count:
// remote is the complete set; local is by construction a subset;
// GitLab/generic-git repos are local-only, so local-over-local is their
// normal replace path with no special case.

package collector

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// runLocalFallback drives RunScorecard through a failed remote attempt
// into a successful local one against a store pre-seeded with the given
// current mode — the exact production shape that produced the deficit.
func runLocalFallback(t *testing.T, store *fakeScorecardStore) (*ScorecardResult, error) {
	t.Helper()
	installFakeScorecard(t,
		`case "$1" in --repo) exit 1;; --local) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	return RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(),
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "tok1",
	}, quietLogger())
}

// TestScorecardLocalRunNeverReplacesStoredRemoteSet is the red test for
// the deficit: a stored REMOTE set must survive a local-mode run
// untouched — no rotation, no insert — and the run must still report
// what happened (mode local, the attempt cost is real) so the phase log
// stays honest.
func TestScorecardLocalRunNeverReplacesStoredRemoteSet(t *testing.T) {
	store := &fakeScorecardStore{storedMode: "remote", storedFound: true}
	res, err := runLocalFallback(t, store)
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	if res == nil || res.Mode != "local" {
		t.Fatalf("result = %+v, want a local-mode result (the attempt happened; only the write is refused)", res)
	}
	if !res.Discarded {
		t.Error("result.Discarded must be true so the phase log can say the run was not written")
	}
	for _, c := range store.snapshot() {
		if c == "rotate" || strings.HasPrefix(c, "insert:") {
			t.Fatalf("persist calls = %v — a local (partial) run must NEVER rotate or insert over a stored remote (complete) set", store.snapshot())
		}
	}
}

// TestScorecardReplaceMatrix pins the other three cells: remote replaces
// anything (best obtainable), local replaces local (no better data
// exists — the GitLab/generic normal path), local stores over nothing.
func TestScorecardReplaceMatrix(t *testing.T) {
	cases := []struct {
		name        string
		remoteOK    bool
		storedMode  string
		storedFound bool
	}{
		{"remote over remote", true, "remote", true},
		{"remote over local", true, "local", true},
		{"local over local", false, "local", true},
		{"local over none", false, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeScorecardStore{storedMode: tc.storedMode, storedFound: tc.storedFound}
			var body string
			if tc.remoteOK {
				body = `case "$1" in --repo) printf '%s' '` + fakeScorecardJSON + `';; *) exit 3;; esac`
			} else {
				body = `case "$1" in --repo) exit 1;; --local) printf '%s' '` + fakeScorecardJSON + `';; esac`
			}
			installFakeScorecard(t, body)
			res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
				RepoURL:       "https://github.com/augurlabs/augur",
				LocalPath:     t.TempDir(),
				RemotePrimary: true,
				Timeout:       time.Minute,
				GithubToken:   "tok1",
			}, quietLogger())
			if err != nil {
				t.Fatalf("RunScorecard: %v", err)
			}
			if res.Discarded {
				t.Fatalf("%s: the run was discarded; only local-over-remote may be", tc.name)
			}
			calls := store.snapshot()
			rotated, inserted := 0, 0
			for _, c := range calls {
				if c == "rotate" {
					rotated++
				}
				if strings.HasPrefix(c, "insert:") {
					inserted++
				}
			}
			if rotated != 1 || inserted != 3 {
				t.Fatalf("%s: persist calls = %v, want one rotate + __overall__ + 2 checks", tc.name, calls)
			}
		})
	}
}

// TestScorecardModeProbeErrorWritesNothing (SR-5): a CurrentScorecardMode
// ERROR is not "no prior set". Rotating on bad information is exactly
// the deficit under a different cause, so the write is skipped and the
// error surfaces.
func TestScorecardModeProbeErrorWritesNothing(t *testing.T) {
	store := &fakeScorecardStore{modeErr: errors.New("pool closed")}
	res, err := runLocalFallback(t, store)
	if err == nil {
		t.Fatal("a mode-probe error must surface, not be read as 'no prior set'")
	}
	if res != nil {
		t.Errorf("result = %+v, want nil on a probe error", res)
	}
	for _, c := range store.snapshot() {
		if c == "rotate" || strings.HasPrefix(c, "insert:") {
			t.Fatalf("persist calls = %v — nothing may be rotated or inserted when the current mode is unknown", store.snapshot())
		}
	}
}

// TestScorecardLocalOnlyPlatformStillReplaces: GitLab/generic repos are
// local-only by construction, so their local runs must keep replacing
// their stored local sets — the gate keys on MODE, never on platform.
func TestScorecardLocalOnlyPlatformStillReplaces(t *testing.T) {
	store := &fakeScorecardStore{storedMode: "local", storedFound: true}
	installFakeScorecard(t, `printf '%s' '`+fakeScorecardJSON+`'`)
	res, err := RunScorecard(context.Background(), store, 7, ScorecardOptions{
		RepoURL:       "https://gitlab.com/petsc/petsc",
		LocalPath:     t.TempDir(),
		RemotePrimary: false,
		Timeout:       time.Minute,
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	if res.Discarded {
		t.Fatal("a local-only platform's local run over its own local set must be written")
	}
	calls := store.snapshot()
	if len(calls) < 2 || calls[len(calls)-1] != "insert:Code-Review:8:local" {
		t.Fatalf("persist calls = %v, want the local set written", calls)
	}
	if calls[1] != "rotate" {
		t.Fatalf("persist calls = %v, want the mode probe then the rotation", calls)
	}
	_ = db.ScorecardOverallName
}

// TestScorecardPersistFailureAfterRemoteSuccessDoesNotFallBack (L10
// pass on the PR #203 fixes): the fused persist transaction can now
// fail for any store reason, not just the mode read. A store failure
// after a SUCCESSFUL remote run must surface as that error — never be
// mistaken for a remote failure that triggers the local backstop (a
// second 15-minute subprocess run, and possibly an 11-check local set
// written over the complete result that was just thrown away).
func TestScorecardPersistFailureAfterRemoteSuccessDoesNotFallBack(t *testing.T) {
	store := &fakeScorecardStore{modeErr: errors.New("pool closed")}
	argsLog, _ := installFakeScorecard(t,
		`case "$1" in --repo) printf '%s' '`+fakeScorecardJSON+`';; --local) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(), // a backstop IS available — and must not be used
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "tok1",
	}, quietLogger())
	if err == nil || !errors.Is(err, errScorecardModeProbe) {
		t.Fatalf("err = %v, want the persist sentinel surfaced unchanged", err)
	}
	if res != nil {
		t.Errorf("result = %+v, want nil", res)
	}
	invocations := readLines(t, argsLog)
	if len(invocations) != 1 || !strings.Contains(invocations[0], "--repo") {
		t.Fatalf("scorecard invocations = %v, want exactly the one --repo run — a persist failure must not fall back to a local run", invocations)
	}
	for _, c := range store.snapshot() {
		if c == "rotate" || strings.HasPrefix(c, "insert:") {
			t.Fatalf("persist calls = %v — nothing may be written", store.snapshot())
		}
	}
}
