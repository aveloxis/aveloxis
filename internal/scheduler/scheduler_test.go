package scheduler

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/augurlabs/aveloxis/internal/collector"
	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
)

// TestSchedulerSourceHasImmediateFirstPoll verifies the scheduler doesn't wait
// for the full poll interval before starting work. On startup with 30 workers
// and 78 queued repos, waiting 10s for the first tick wastes time.
func TestSchedulerSourceHasImmediateFirstPoll(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The Run function should trigger an immediate dequeue attempt before
	// entering the ticker-based select loop. Look for evidence of immediate
	// slot filling (e.g., a dequeue call before the main select).
	runIdx := strings.Index(src, "func (s *Scheduler) Run(")
	if runIdx < 0 {
		t.Fatal("cannot find Run function")
	}
	runBody := src[runIdx:]

	// Find the position of the main select loop.
	selectIdx := strings.Index(runBody, "for {")
	if selectIdx < 0 {
		t.Fatal("cannot find main loop in Run")
	}

	// There should be a dequeue call or fillWorkerSlots call BEFORE the main loop.
	beforeLoop := runBody[:selectIdx]
	if !strings.Contains(beforeLoop, "fillWorkerSlots") && !strings.Contains(beforeLoop, "DequeueNext") {
		t.Error("scheduler Run should trigger immediate dequeue before waiting for first poll tick")
	}
}

// TestSchedulerUsesLocalScorecard verifies the scheduler runs scorecard
// locally against the retained analysis clone and marks the token as depleted
// afterward. No concurrency semaphore needed — local mode is mostly disk I/O
// and MarkDepleted handles token rotation for the small number of API calls.
func TestSchedulerUsesLocalScorecard(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "MarkDepleted") {
		t.Error("scheduler must mark token as depleted after scorecard (untracked API calls)")
	}
	if !strings.Contains(src, "RetainClone") {
		t.Error("scheduler must set RetainClone for local scorecard execution")
	}
}

// TestKeyPoolHasDepletedMarker verifies the key pool can mark a key as
// externally depleted (e.g., after scorecard uses it for hundreds of
// untracked API calls). This prevents the pool from handing the key to
// other workers who would get 403s.
func TestKeyPoolHasDepletedMarker(t *testing.T) {
	data, err := os.ReadFile("../platform/ratelimit.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "MarkDepleted") && !strings.Contains(src, "DepletedBy") {
		t.Error("KeyPool needs a method to mark a key as depleted after external use (scorecard)")
	}
}

func TestConfigDefaults(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	if s.cfg.Workers != 1 {
		t.Errorf("Workers = %d, want 1", s.cfg.Workers)
	}
	if s.cfg.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", s.cfg.PollInterval)
	}
	if s.cfg.RecollectAfter != 24*time.Hour {
		t.Errorf("RecollectAfter = %v, want 24h", s.cfg.RecollectAfter)
	}
	if s.cfg.StaleLockTimeout != 1*time.Hour {
		t.Errorf("StaleLockTimeout = %v, want 1h", s.cfg.StaleLockTimeout)
	}
	if s.cfg.OrgRefreshInterval != 4*time.Hour {
		t.Errorf("OrgRefreshInterval = %v, want 4h", s.cfg.OrgRefreshInterval)
	}
}

func TestConfigCustomValues(t *testing.T) {
	cfg := Config{
		Workers:            8,
		PollInterval:       30 * time.Second,
		RecollectAfter:     48 * time.Hour,
		StaleLockTimeout:   2 * time.Hour,
		OrgRefreshInterval: 12 * time.Hour,
	}
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), cfg)

	if s.cfg.Workers != 8 {
		t.Errorf("Workers = %d, want 8", s.cfg.Workers)
	}
	if s.cfg.PollInterval != 30*time.Second {
		t.Errorf("PollInterval = %v, want 30s", s.cfg.PollInterval)
	}
	if s.cfg.RecollectAfter != 48*time.Hour {
		t.Errorf("RecollectAfter = %v, want 48h", s.cfg.RecollectAfter)
	}
}

func TestWorkerIDIncludesHostname(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	hostname, _ := os.Hostname()
	if len(s.workerID) <= len(hostname) {
		t.Errorf("workerID %q too short, expected hostname prefix %q", s.workerID, hostname)
	}
	if s.workerID[:len(hostname)] != hostname {
		t.Errorf("workerID %q does not start with hostname %q", s.workerID, hostname)
	}
}

func TestSelectClient(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	// GitHub should return ghClient (nil in this test, but no error).
	client, err := s.selectClient(model.PlatformGitHub)
	if err != nil {
		t.Errorf("selectClient(GitHub) error = %v", err)
	}
	if client != nil {
		t.Errorf("selectClient(GitHub) = %v, want nil (ghClient)", client)
	}

	// GitLab should return glClient (nil in this test, but no error).
	client, err = s.selectClient(model.PlatformGitLab)
	if err != nil {
		t.Errorf("selectClient(GitLab) error = %v", err)
	}
	if client != nil {
		t.Errorf("selectClient(GitLab) = %v, want nil (glClient)", client)
	}

	// Unknown platform should return error.
	_, err = s.selectClient(model.Platform(99))
	if err == nil {
		t.Error("selectClient(99) should return error for unknown platform")
	}
}

func TestDetermineSince(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{
		RecollectAfter: 24 * time.Hour,
	})

	// First-time repo: LastCollected is nil -> zero time (full collection).
	job := &db.QueueJob{RepoID: 1}
	since := s.determineSince(job)
	if !since.IsZero() {
		t.Errorf("determineSince(nil LastCollected) = %v, want zero time", since)
	}

	// Previously collected repo: should return ~now minus recollect window.
	now := time.Now()
	job.LastCollected = &now
	since = s.determineSince(job)
	expected := time.Now().Add(-24 * time.Hour)
	// Allow 1 second of clock skew.
	if since.Sub(expected).Abs() > time.Second {
		t.Errorf("determineSince(collected) = %v, want ~%v", since, expected)
	}
}

func TestPlatformHostForModel(t *testing.T) {
	tests := []struct {
		platform model.Platform
		want     string
	}{
		{model.PlatformGitHub, "github.com"},
		{model.PlatformGitLab, "gitlab.com"},
		{model.Platform(99), "unknown"},
	}

	for _, tt := range tests {
		got := platformHostForModel(tt.platform)
		if got != tt.want {
			t.Errorf("platformHostForModel(%d) = %q, want %q", tt.platform, got, tt.want)
		}
	}
}

func TestBuildOutcome_Success(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	result := &collector.CollectResult{
		Issues:       10,
		PullRequests: 5,
		Messages:     20,
		Events:       30,
		Releases:     2,
		Contributors: 8,
	}
	facade := &collector.FacadeResult{Commits: 100}

	out := s.buildOutcome(result, facade, nil, nil)

	if !out.success {
		t.Error("expected success=true")
	}
	if out.errMsg != "" {
		t.Errorf("expected empty errMsg, got %q", out.errMsg)
	}
	if out.issues != 10 {
		t.Errorf("issues = %d, want 10", out.issues)
	}
	if out.prs != 5 {
		t.Errorf("prs = %d, want 5", out.prs)
	}
	if out.commits != 100 {
		t.Errorf("commits = %d, want 100", out.commits)
	}
	if out.contributors != 8 {
		t.Errorf("contributors = %d, want 8", out.contributors)
	}
}

func TestBuildOutcome_CollectionError(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	result := &collector.CollectResult{Issues: 5, PullRequests: 3}
	err := fmt.Errorf("rate limited")

	out := s.buildOutcome(result, nil, nil, err)

	if out.success {
		t.Error("expected success=false on collection error")
	}
	if out.errMsg != "rate limited" {
		t.Errorf("errMsg = %q, want %q", out.errMsg, "rate limited")
	}
}

func TestBuildOutcome_ResultErrors(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	result := &collector.CollectResult{
		Issues:       5,
		PullRequests: 3,
		Contributors: 2,
		Releases:     1,
		Errors:       []error{fmt.Errorf("partial failure")},
	}

	out := s.buildOutcome(result, nil, nil, nil)

	if out.success {
		t.Error("expected success=false when result has errors")
	}
	if out.errMsg != "partial failure" {
		t.Errorf("errMsg = %q, want %q", out.errMsg, "partial failure")
	}
}

func TestBuildOutcome_ZeroData(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	// Non-nil result with all zeros should be treated as failure.
	result := &collector.CollectResult{}

	out := s.buildOutcome(result, nil, nil, nil)

	if out.success {
		t.Error("expected success=false for zero-data result")
	}
	if out.errMsg == "" {
		t.Error("expected non-empty errMsg for zero-data result")
	}
}

func TestBuildOutcome_NilResult(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	// Nil result with no error should be success (can happen if collection
	// short-circuits but no error was set).
	out := s.buildOutcome(nil, nil, nil, nil)

	if !out.success {
		t.Error("expected success=true for nil result with no error")
	}
}

func TestBuildOutcome_FacadeOnlyCounts(t *testing.T) {
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})

	// Non-nil result with some data + facade commits.
	result := &collector.CollectResult{
		Issues:       1,
		Contributors: 1,
	}
	facade := &collector.FacadeResult{Commits: 500}

	out := s.buildOutcome(result, facade, nil, nil)

	if !out.success {
		t.Error("expected success=true")
	}
	if out.commits != 500 {
		t.Errorf("commits = %d, want 500", out.commits)
	}
}

// TestSchedulerRunJobHasHeartbeat verifies runJob starts a heartbeat goroutine
// that keeps locked_at fresh. Without this, RecoverStaleLocks steals jobs
// from workers that are still running on large repos (>1 hour collection).
func TestSchedulerRunJobHasHeartbeat(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find runJob function.
	idx := strings.Index(src, "func (s *Scheduler) runJob(")
	if idx < 0 {
		t.Fatal("cannot find runJob function")
	}
	// Look at the first ~1500 chars of runJob for heartbeat setup.
	fnBody := src[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	if !strings.Contains(fnBody, "heartbeat") || !strings.Contains(fnBody, "HeartbeatJob") {
		t.Error("runJob must start a heartbeat goroutine to keep locked_at fresh during collection")
	}
}

// TestSchedulerRecoverOtherLocksOnStartup verifies the startup sequence
// calls RecoverOtherWorkerLocks to immediately reclaim locks from dead
// processes. Without this, repos stuck in 'collecting' by a killed process
// won't be re-queued until the 1-hour stale lock timeout fires.
func TestSchedulerRecoverOtherLocksOnStartup(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The Run function's startup block should call RecoverOtherWorkerLocks.
	runIdx := strings.Index(src, "func (s *Scheduler) Run(")
	if runIdx < 0 {
		t.Fatal("cannot find Run function")
	}
	// Look in the first ~2000 chars of Run for the startup sequence.
	runBody := src[runIdx:]
	if len(runBody) > 2000 {
		runBody = runBody[:2000]
	}

	if !strings.Contains(runBody, "RecoverOtherWorkerLocks") {
		t.Error("scheduler Run must call RecoverOtherWorkerLocks on startup to reclaim dead workers' locks immediately")
	}
}
