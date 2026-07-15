// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — behavioral tests for the remote-primary/local-backstop
// scorecard orchestration. A fake `scorecard` binary installed on PATH
// drives the real RunScorecard end-to-end (mode order, timeout
// fallback, multi-token env, args); a fake store records the persisted
// rows (mode marker, __overall__ contract, rotate-before-insert); an
// httptest server plays GitHub's /rate_limit for the instrumentation.

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

const fakeScorecardJSON = `{"score": 5.6, "checks": [` +
	`{"name":"Maintained","score":10,"reason":"ok"},` +
	`{"name":"Code-Review","score":8,"reason":"ok"}]}`

// installFakeScorecard writes an executable shell script named
// `scorecard` into a temp dir prepended to PATH. The script appends its
// argv to args.log and $GITHUB_TOKEN to env.log on every invocation,
// then executes the per-test body (which can branch on "$1" = --repo /
// --local).
func installFakeScorecard(t *testing.T, body string) (argsLog, envLog string) {
	t.Helper()
	dir := t.TempDir()
	argsLog = filepath.Join(dir, "args.log")
	envLog = filepath.Join(dir, "env.log")
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> " + argsLog + "\n" +
		"echo \"$GITHUB_TOKEN\" >> " + envLog + "\n" +
		body + "\n"
	if err := os.WriteFile(filepath.Join(dir, "scorecard"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return argsLog, envLog
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

// fakeScorecardStore records the persist half's calls in order.
type fakeScorecardStore struct {
	mu    sync.Mutex
	calls []string // "rotate" or "insert:<name>:<score>:<mode>"
}

func (f *fakeScorecardStore) RotateScorecardToHistory(ctx context.Context, repoID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "rotate")
	return nil
}

func (f *fakeScorecardStore) InsertScorecardResult(ctx context.Context, repoID int64, name, score string, detailsJSON []byte, mode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("insert:%s:%s:%s", name, score, mode))
	return nil
}

func (f *fakeScorecardStore) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestScorecardRemotePrimarySucceeds(t *testing.T) {
	argsLog, envLog := installFakeScorecard(t,
		`case "$1" in --repo) printf '%s' '`+fakeScorecardJSON+`';; *) exit 3;; esac`)
	store := &fakeScorecardStore{}

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(),
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "tok1,tok2",
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	if res == nil || res.Mode != "remote" {
		t.Fatalf("Mode = %v, want remote", res)
	}
	if res.OverallScore != 5.6 {
		t.Errorf("OverallScore = %v, want 5.6", res.OverallScore)
	}

	// Exactly ONE invocation, and it was --repo (remote FIRST — no
	// speculative local run when remote succeeds).
	args := readLines(t, argsLog)
	if len(args) != 1 || !strings.HasPrefix(args[0], "--repo ") {
		t.Errorf("invocations = %v, want exactly one --repo call", args)
	}

	// Multi-token GITHUB_TOKEN reaches the subprocess verbatim.
	env := readLines(t, envLog)
	if len(env) != 1 || env[0] != "tok1,tok2" {
		t.Errorf("GITHUB_TOKEN seen by scorecard = %v, want [tok1,tok2]", env)
	}

	// Persisted: rotate first, then __overall__ + both checks, all
	// carrying mode=remote.
	calls := store.snapshot()
	if len(calls) != 4 || calls[0] != "rotate" {
		t.Fatalf("persist calls = %v, want rotate + 3 inserts", calls)
	}
	if calls[1] != "insert:"+db.ScorecardOverallName+":5.6:remote" {
		t.Errorf("overall row = %q, want __overall__ 5.6 mode=remote", calls[1])
	}
	for _, c := range calls[1:] {
		if !strings.HasSuffix(c, ":remote") {
			t.Errorf("row %q missing mode=remote marker", c)
		}
	}
}

func TestScorecardRemoteFailureFallsBackToLocal(t *testing.T) {
	argsLog, _ := installFakeScorecard(t,
		`case "$1" in --repo) exit 1;; --local) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	store := &fakeScorecardStore{}

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(),
		RemotePrimary: true,
		Timeout:       time.Minute,
		GithubToken:   "tok1",
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v (11 checks beat none — local backstop must rescue)", err)
	}
	if res.Mode != "local" {
		t.Errorf("Mode = %q, want local (the backstop)", res.Mode)
	}

	// Attempt order: remote first, THEN local.
	args := readLines(t, argsLog)
	if len(args) != 2 ||
		!strings.HasPrefix(args[0], "--repo ") ||
		!strings.HasPrefix(args[1], "--local ") {
		t.Errorf("invocations = %v, want [--repo ..., --local ...]", args)
	}

	// Every persisted row carries mode=local.
	for _, c := range store.snapshot() {
		if strings.HasPrefix(c, "insert:") && !strings.HasSuffix(c, ":local") {
			t.Errorf("row %q missing mode=local marker", c)
		}
	}
}

func TestScorecardRemoteTimeoutFallsBackToLocal(t *testing.T) {
	argsLog, _ := installFakeScorecard(t,
		`case "$1" in --repo) sleep 30;; --local) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	store := &fakeScorecardStore{}

	start := time.Now()
	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     t.TempDir(),
		RemotePrimary: true,
		Timeout:       500 * time.Millisecond,
		GithubToken:   "tok1",
	}, quietLogger())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RunScorecard after remote timeout: %v", err)
	}
	if res.Mode != "local" {
		t.Errorf("Mode = %q, want local after remote wall-clock timeout", res.Mode)
	}
	// The per-attempt timeout must have killed the sleeping remote
	// attempt — nowhere near the 30s sleep.
	if elapsed > 10*time.Second {
		t.Errorf("run took %v — the 500ms per-attempt timeout did not fire", elapsed)
	}
	args := readLines(t, argsLog)
	if len(args) != 2 || !strings.HasPrefix(args[0], "--repo ") || !strings.HasPrefix(args[1], "--local ") {
		t.Errorf("invocations = %v, want remote attempt then local fallback", args)
	}
}

func TestScorecardLocalOnlyPlatformNeverRunsRemote(t *testing.T) {
	argsLog, _ := installFakeScorecard(t,
		`printf '%s' '`+fakeScorecardJSON+`'`)
	store := &fakeScorecardStore{}

	// A rate-limit server that must NEVER be hit: local mode makes no
	// instrumented calls, so the probe is skipped entirely.
	var rlHits atomic.Int64
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rlHits.Add(1)
	}))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:         "https://gitlab.com/group/project",
		LocalPath:       t.TempDir(),
		RemotePrimary:   false, // GitLab / generic git
		Timeout:         time.Minute,
		GithubToken:     "tok1",
		InstrumentToken: "tok1",
		RateLimitURL:    rl.URL,
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	if res.Mode != "local" {
		t.Errorf("Mode = %q, want local", res.Mode)
	}
	if res.APICalls != 0 {
		t.Errorf("APICalls = %d, want 0 for local mode", res.APICalls)
	}
	args := readLines(t, argsLog)
	if len(args) != 1 || !strings.HasPrefix(args[0], "--local ") {
		t.Errorf("invocations = %v, want exactly one --local call (never --repo on local-only platforms)", args)
	}
	if rlHits.Load() != 0 {
		t.Errorf("rate_limit probe fired %d times on a local-only run; must be 0", rlHits.Load())
	}
}

func TestScorecardLocalOnlyWithoutCloneSkips(t *testing.T) {
	argsLog, _ := installFakeScorecard(t, `exit 7`)
	store := &fakeScorecardStore{}

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://gitlab.com/group/project",
		LocalPath:     "",
		RemotePrimary: false,
		Timeout:       time.Minute,
	}, quietLogger())
	if err != nil || res != nil {
		t.Fatalf("local-only platform without a clone must skip cleanly, got res=%v err=%v", res, err)
	}
	if args := readLines(t, argsLog); len(args) != 0 {
		t.Errorf("scorecard was invoked %v times on a skip path", args)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Errorf("skip path persisted %v", calls)
	}
}

func TestScorecardRemoteOnlyNoCloneReturnsError(t *testing.T) {
	_, _ = installFakeScorecard(t, `case "$1" in --repo) exit 1;; esac`)
	store := &fakeScorecardStore{}

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:       "https://github.com/augurlabs/augur",
		LocalPath:     "", // e.g. the bulk run-scorecard pass — no clone
		RemotePrimary: true,
		Timeout:       time.Minute,
	}, quietLogger())
	if err == nil {
		t.Fatal("remote failure with no local backstop must surface the error")
	}
	if res != nil {
		t.Errorf("res = %v, want nil on failure", res)
	}
	if calls := store.snapshot(); len(calls) != 0 {
		t.Errorf("failed run persisted %v — nothing may be written on failure", calls)
	}
}

func TestScorecardInstrumentationCountsAPIDelta(t *testing.T) {
	_, _ = installFakeScorecard(t,
		`case "$1" in --repo) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	store := &fakeScorecardStore{}

	var hits atomic.Int64
	var authSeen atomic.Value
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		authSeen.Store(r.Header.Get("Authorization"))
		if n == 1 {
			fmt.Fprint(w, `{"resources":{"core":{"used":100},"graphql":{"used":10}}}`)
		} else {
			fmt.Fprint(w, `{"resources":{"core":{"used":132},"graphql":{"used":18}}}`)
		}
	}))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:         "https://github.com/augurlabs/augur",
		RemotePrimary:   true,
		Timeout:         time.Minute,
		GithubToken:     "tok1,tok2",
		InstrumentToken: "tok1",
		RateLimitURL:    rl.URL,
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	// (132-100) core + (18-10) graphql = 40.
	if res.APICalls != 40 {
		t.Errorf("APICalls = %d, want 40 (core delta 32 + graphql delta 8)", res.APICalls)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("rate_limit probed %d times, want exactly 2 (before + after)", got)
	}
	if got, _ := authSeen.Load().(string); got != "token tok1" {
		t.Errorf("probe Authorization = %q, want `token tok1` (the FIRST token)", got)
	}
}

func TestScorecardInstrumentationFailureIsNonFatal(t *testing.T) {
	_, _ = installFakeScorecard(t,
		`case "$1" in --repo) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	store := &fakeScorecardStore{}

	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:         "https://github.com/augurlabs/augur",
		RemotePrimary:   true,
		Timeout:         time.Minute,
		GithubToken:     "tok1",
		InstrumentToken: "tok1",
		RateLimitURL:    rl.URL,
	}, quietLogger())
	if err != nil {
		t.Fatalf("a failed rate_limit probe must never sink the run: %v", err)
	}
	if res.Mode != "remote" {
		t.Errorf("Mode = %q, want remote", res.Mode)
	}
	if res.APICalls != -1 {
		t.Errorf("APICalls = %d, want -1 (unknown — probe failed)", res.APICalls)
	}
	if len(store.snapshot()) == 0 {
		t.Error("results must still persist when instrumentation fails")
	}
}

// TestScorecardTokensHelper pins the pool → GITHUB_TOKEN construction:
// 0 = all tokens, N>0 = first N, nil pool safe.
func TestScorecardTokensHelper(t *testing.T) {
	logger := quietLogger()
	pool := platform.NewKeyPool([]string{"a", "b", "c"}, logger)

	if joined, first := ScorecardTokens(pool, 0); joined != "a,b,c" || first != "a" {
		t.Errorf("ScorecardTokens(pool, 0) = %q/%q, want a,b,c / a", joined, first)
	}
	if joined, first := ScorecardTokens(pool, 2); joined != "a,b" || first != "a" {
		t.Errorf("ScorecardTokens(pool, 2) = %q/%q, want a,b / a", joined, first)
	}
	if joined, first := ScorecardTokens(pool, 99); joined != "a,b,c" || first != "a" {
		t.Errorf("ScorecardTokens(pool, 99) = %q/%q, want all tokens when N exceeds pool", joined, first)
	}
	if joined, first := ScorecardTokens(nil, 0); joined != "" || first != "" {
		t.Errorf("ScorecardTokens(nil, 0) = %q/%q, want empty", joined, first)
	}
	empty := platform.NewKeyPool(nil, logger)
	if joined, first := ScorecardTokens(empty, 0); joined != "" || first != "" {
		t.Errorf("ScorecardTokens(empty, 0) = %q/%q, want empty", joined, first)
	}
}
