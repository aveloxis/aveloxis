// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/spf13/cobra"
)

// v0.29.65 — heal-collection-gaps parks each repo it heals ('collecting',
// owner '<gap-heal-…>:drain'). Interrupted, it used to leave them parked,
// log a resume point past unvisited repos, exit 0 in single-repo mode and
// count cancellations as failures. These tests DRIVE the run (review
// round 3: the source pins that stood in for them let ten planted
// regressions through).

// fakeGapStore honours the boundary the real store does: a call on a
// cancelled context fails with context.Canceled, and a release records
// whether its context was live.
type fakeGapStore struct {
	mu         sync.Mutex
	candidates []db.GapHealCandidate // ascending repo_id
	events     []string
	locked     map[int64]bool
	releasedOn []bool // per ReleaseDrainLock: was its ctx live?
	lockTries  []int64
	// Interrupts that land INSIDE a store call, as a real Ctrl-C can:
	// cancel is called and the call fails with the cancelled context.
	cancel        context.CancelFunc
	cancelOnLock  int64 // repo whose lock is interrupted (0 = none)
	cancelOnQuery int   // 1-based candidate query that is interrupted (0 = none)
	queries       int
	refreshed     []int64
	// Real-store boundaries the round-4 review found unmodelled.
	notQueued  map[int64]bool // being collected by serve: never locks
	sawAll     bool           // GetGapHealCandidates' all flag
	refreshErr map[int64]error
	metaErr    error
	// When set, a per-repo release waits (bounded) for signal handling to
	// be restored, and records whether it was.
	restored      chan struct{}
	restoreWaited []bool
	interrupted   bool // set once a fill has seen the cancel
	// Round 5: failure arms and hooks the earlier fake never reached.
	lockErr       map[int64]error
	repoErr       error
	refreshCancel int64 // repo whose count refresh is interrupted
	releaseAllErr error
	onReleaseAll  func() // runs inside the exit release, before it returns
	restoreCalls  int32  // newGapRun's default restoreSignals counts here
	onMeta        func() // runs inside GetRepoMetaCounts
	// Owner honoured like the real store (review round 15): every lock,
	// release, release-all and heartbeat must name the run's worker ID; a
	// call under any other owner is recorded and does nothing.
	wrongOwner     []string
	cancelOnFillID int64 // runFromFlags' filler cancels on this repo
	// Queue rows for GetQueueStatus: a repo in notQueued is 'collecting'
	// unless queueStatus says otherwise; noQueueRow has no row at all.
	queueStatus    map[int64]string
	queueOwner     map[int64]string // locked_by for GetQueueStatus
	noQueueRow     map[int64]bool
	queueStatusErr error
	onQueueStatus  func() // runs inside GetQueueStatus
	statusCtxLive  bool   // the status read's ctx was still live after onQueueStatus
}

func (f *fakeGapStore) GetQueueStatus(ctx context.Context, id int64) (db.QueueRowStatus, bool, error) {
	if f.onQueueStatus != nil {
		f.onQueueStatus()
		f.statusCtxLive = ctx.Err() == nil
	}
	if err := ctx.Err(); err != nil {
		return db.QueueRowStatus{}, false, err
	}
	if f.queueStatusErr != nil {
		return db.QueueRowStatus{}, false, f.queueStatusErr
	}
	if f.noQueueRow[id] {
		return db.QueueRowStatus{}, false, nil
	}
	st := db.QueueRowStatus{Status: "queued", LockedBy: f.queueOwner[id]}
	if s, ok := f.queueStatus[id]; ok {
		st.Status = s
	} else if f.notQueued[id] {
		st.Status = "collecting"
	}
	return st, true, nil
}

func (f *fakeGapStore) log(e string) { f.mu.Lock(); f.events = append(f.events, e); f.mu.Unlock() }

// testWorkerID is the owner every test run uses.
const testWorkerID = "gap-heal-test"

func (f *fakeGapStore) LockReposForDrain(ctx context.Context, ids []int64, owner string) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if owner != testWorkerID {
		f.wrongOwner = append(f.wrongOwner, "lock:"+owner)
	}
	f.lockTries = append(f.lockTries, ids...)
	if len(ids) == 1 && ids[0] == f.cancelOnLock {
		f.cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 1 && f.lockErr[ids[0]] != nil {
		return nil, f.lockErr[ids[0]]
	}
	var got []int64
	for _, id := range ids {
		if f.notQueued[id] {
			continue // only 'queued' rows lock
		}
		f.locked[id] = true
		got = append(got, id)
	}
	return got, nil
}

func (f *fakeGapStore) ReleaseDrainLock(ctx context.Context, id int64, owner string) error {
	if owner != testWorkerID {
		f.mu.Lock()
		f.wrongOwner = append(f.wrongOwner, "release:"+owner)
		f.mu.Unlock()
		return nil // the real UPDATE matches no row
	}
	if f.restored != nil && f.interrupted {
		select {
		case <-f.restored:
			f.restoreWaited = append(f.restoreWaited, true)
		case <-time.After(5 * time.Second): // test-only bound: far above a goroutine handoff
			f.restoreWaited = append(f.restoreWaited, false)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releasedOn = append(f.releasedOn, ctx.Err() == nil)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	delete(f.locked, id)
	return nil
}

func (f *fakeGapStore) ReleaseDrainLocks(ctx context.Context, owner string) (int64, error) {
	f.log("release-all")
	if owner != testWorkerID {
		f.mu.Lock()
		f.wrongOwner = append(f.wrongOwner, "release-all:"+owner)
		f.mu.Unlock()
		return 0, nil // the real UPDATE matches no row
	}
	if f.onReleaseAll != nil {
		f.onReleaseAll()
	}
	if f.releaseAllErr != nil {
		return 0, f.releaseAllErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := int64(len(f.locked))
	f.locked = map[int64]bool{}
	return n, nil
}

func (f *fakeGapStore) RefreshQueueGatheredCounts(ctx context.Context, id int64) error {
	if id == f.refreshCancel && f.cancel != nil {
		f.cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.refreshErr[id]; err != nil {
		return err
	}
	f.mu.Lock()
	f.refreshed = append(f.refreshed, id)
	f.mu.Unlock()
	return nil
}

func (f *fakeGapStore) GetGapHealCandidates(ctx context.Context, after int64, limit int, all bool) ([]db.GapHealCandidate, error) {
	f.queries++
	f.sawAll = all
	if f.queries == f.cancelOnQuery {
		f.cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var page []db.GapHealCandidate
	for _, c := range f.candidates {
		if c.RepoID > after && len(page) < limit {
			page = append(page, c)
		}
	}
	return page, nil
}

func (f *fakeGapStore) GetRepoByID(ctx context.Context, id int64) (*model.Repo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.repoErr != nil {
		return nil, f.repoErr
	}
	return &model.Repo{ID: id, Owner: "single-owner", Name: fmt.Sprint(id), Platform: model.PlatformGitLab}, nil
}

func (f *fakeGapStore) GetRepoMetaCounts(ctx context.Context, _ int64) (int64, int64, error) {
	if f.onMeta != nil {
		f.onMeta()
	}
	if f.metaErr != nil {
		return 0, 0, f.metaErr
	}
	return 7, 9, ctx.Err()
}

func (f *fakeGapStore) StartDrainHeartbeat(_ context.Context, _ *slog.Logger, owner string) func() {
	if owner != testWorkerID {
		f.mu.Lock()
		f.wrongOwner = append(f.wrongOwner, "heartbeat:"+owner)
		f.mu.Unlock()
	}
	f.log("heartbeat-start")
	return func() { f.log("heartbeat-stop") }
}

func newGapRun(store *fakeGapStore, logBuf *bytes.Buffer, pageSize int) *gapHealRun {
	return &gapHealRun{
		store:    store,
		logger:   slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		out:      io.Discard,
		workerID: testWorkerID,
		workers:  1, // deterministic order: the cancel lands on a known repo
		pageSize: pageSize,
		canHeal:  func(db.GapHealCandidate) bool { return true },
		// Armed by default, as in production (round 5: with none armed a
		// restore called at the START of every run — which in production
		// cancels the run's own context — passed every test).
		restoreSignals: func() { atomic.AddInt32(&store.restoreCalls, 1) },
	}
}

// noRestore asserts an uninterrupted run never restored signal handling.
func noRestore(t *testing.T, store *fakeGapStore) {
	t.Helper()
	sameOwner(t, store)
	if n := atomic.LoadInt32(&store.restoreCalls); n != 0 {
		t.Errorf("an uninterrupted run restored signal handling %d time(s) — in production that cancels the run", n)
	}
}

func gapCandidates(ids ...int64) []db.GapHealCandidate {
	out := make([]db.GapHealCandidate, 0, len(ids))
	for _, id := range ids {
		out = append(out, db.GapHealCandidate{RepoID: id, Owner: "o", Name: fmt.Sprint(id), Platform: model.PlatformGitHub})
	}
	return out
}

// underLock wraps a fill so every heal asserts it runs while ITS repo is
// drain-locked and nothing else is (workers=1): the lock covers the
// heal, and each repo's lock is released before the next one's heal.
func underLock(t *testing.T, store *fakeGapStore, fill func(context.Context, db.GapHealCandidate) (int, error)) func(context.Context, db.GapHealCandidate) (int, error) {
	return func(ctx context.Context, c db.GapHealCandidate) (int, error) {
		store.mu.Lock()
		held := len(store.locked)
		mine := store.locked[c.RepoID]
		store.mu.Unlock()
		if !mine || held != 1 {
			t.Errorf("repo %d healed with lock=%v and %d row(s) parked; want its own lock only", c.RepoID, mine, held)
		}
		return fill(ctx, c)
	}
}

func healsOK(context.Context, db.GapHealCandidate) (int, error) { return 1, nil }

// cancelOnFill returns a fill that heals normally and cancels the run
// while healing the repo named (the interrupt lands mid-repo).
func cancelOnFill(cancel context.CancelFunc, at int64, visited *[]int64) func(context.Context, db.GapHealCandidate) (int, error) {
	return func(ctx context.Context, c db.GapHealCandidate) (int, error) {
		*visited = append(*visited, c.RepoID)
		if c.RepoID == at {
			cancel()
			return 0, ctx.Err()
		}
		return 1, nil
	}
}

// sameOwner asserts every lock, release and heartbeat named the run's
// worker ID (round 15: a release under another owner frees nothing).
func sameOwner(t *testing.T, store *fakeGapStore) {
	t.Helper()
	if len(store.wrongOwner) != 0 {
		t.Errorf("calls under another owner: %v", store.wrongOwner)
	}
}

func assertCleanInterrupt(t *testing.T, err error, logs string, run *gapHealRun, store *fakeGapStore) {
	t.Helper()
	sameOwner(t, store)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("an interrupted run returns context.Canceled (a nonzero exit), got %v", err)
	}
	if strings.Contains(logs, "gap heal complete") {
		t.Error("an interrupted run must not log completion")
	}
	for _, level := range []string{"level=WARN", "level=ERROR"} {
		if strings.Contains(logs, level) {
			t.Errorf("an interrupt logged at %s:\n%s", level, logs)
		}
	}
	if run.failed != 0 {
		t.Errorf("an interrupt counted %d failure(s)", run.failed)
	}
	if len(store.locked) != 0 {
		t.Errorf("rows still parked after the run: %v", store.locked)
	}
	for i, live := range store.releasedOn {
		if !live {
			t.Errorf("per-repo release %d ran on the cancelled context", i)
		}
	}
	// The exit release runs AFTER the heartbeat stops.
	ev := strings.Join(store.events, ",")
	if !strings.HasSuffix(ev, "heartbeat-stop,release-all") {
		t.Errorf("exit order: %s (want the heartbeat stopped, then release-all)", ev)
	}
}

// TestGapHealFleetInterruptResumesAtThePageStart — cancelled while
// healing the first repo of the SECOND page (page size 2, candidates
// 101..106): the run stops, names 102 (the keyset cursor the interrupted
// page was read from) as the resume point, never 104 (that page's last repo), and visits nothing
// after the interrupt.
func TestGapHealFleetInterruptResumesAtThePageStart(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103, 104, 105, 106), locked: map[int64]bool{}}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := newGapRun(store, &logs, 2)
	var visited []int64
	inner := cancelOnFill(cancel, 103, &visited)
	run.fill = underLock(t, store, func(ctx context.Context, c db.GapHealCandidate) (int, error) {
		n, err := inner(ctx, c)
		store.interrupted = ctx.Err() != nil
		return n, err
	})
	store.restored = make(chan struct{})
	var once sync.Once
	restored := false
	run.restoreSignals = func() { once.Do(func() { restored = true; close(store.restored) }) }

	err := run.run(ctx, 0, 100)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if !strings.Contains(logs.String(), "after_repo_id=102") {
		t.Errorf("the resume point must be the interrupted page's start (102):\n%s", logs.String())
	}
	if !strings.Contains(err.Error(), "--after-repo-id 102") {
		t.Errorf("the error names the resume point: %v", err)
	}
	if fmt.Sprint(visited) != "[101 102 103]" {
		t.Errorf("visited %v; nothing is visited after the interrupt", visited)
	}
	if fmt.Sprint(store.lockTries) != "[101 102 103]" {
		t.Errorf("lock attempts %v; nothing is even locked after the interrupt", store.lockTries)
	}
	if !restored {
		t.Error("the exit path restores default signal handling (a second Ctrl-C ends the process)")
	}
	// Restored on the FIRST interrupt, before the interrupted repo's own
	// release runs, so a release stalled on the database cannot swallow a
	// second Ctrl-C (review round 4).
	if len(store.restoreWaited) != 1 {
		t.Errorf("the interrupted repo's release waited %d time(s); want 1", len(store.restoreWaited))
	}
	for i, ok := range store.restoreWaited {
		if !ok {
			t.Errorf("per-repo release %d ran before signal handling was restored", i)
		}
	}
}

// TestGapHealSingleRepoInterruptIsAnError — --repo-id cancelled mid-heal
// exits nonzero (it exited 0 before).
func TestGapHealSingleRepoInterruptIsAnError(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := newGapRun(store, &logs, 2)
	var visited []int64
	run.fill = underLock(t, store, cancelOnFill(cancel, 49290, &visited))
	err := run.run(ctx, 49290, 0)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if err == nil || !strings.Contains(err.Error(), "repo 49290 interrupted") {
		t.Errorf("single-repo interrupt error: %v", err)
	}
}

// TestGapHealInterruptBeforeTheCandidateQuery — an interrupt that lands
// on the candidate query (or before anything starts) is an interrupt,
// not "candidate query: context canceled".
func TestGapHealInterruptBeforeTheCandidateQuery(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run := newGapRun(store, &logs, 2)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 0, 100)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if strings.Contains(err.Error(), "candidate query") {
		t.Errorf("an interrupt is not a failed query: %v", err)
	}
}

// TestGapHealFleetCompletes — the uninterrupted path: every candidate
// healed and released, completion logged, nil error.
func TestGapHealFleetCompletes(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 2)
	run.fill = underLock(t, store, func(context.Context, db.GapHealCandidate) (int, error) { return 2, nil })
	if err := run.run(context.Background(), 0, 100); err != nil {
		t.Fatalf("run: %v", err)
	}
	if run.visited != 3 || run.filled != 6 || run.failed != 0 {
		t.Errorf("visited=%d filled=%d failed=%d", run.visited, run.filled, run.failed)
	}
	// Each heal refreshes the queue's cached counts, so a healed repo
	// leaves the candidate set (round 23: rerun-until-0 converges).
	if fmt.Sprint(store.refreshed) != "[101 102 103]" {
		t.Errorf("refreshed %v; every healed repo's counts are refreshed", store.refreshed)
	}
	noRestore(t, store)
	if !strings.Contains(logs.String(), "gap heal complete") || len(store.locked) != 0 {
		t.Errorf("completion logged and nothing parked: locked=%v\n%s", store.locked, logs.String())
	}
}

// TestHealCollectionGapsWiring — the command function and the collector
// filler are pinned WHOLE, comment-stripped and whitespace-normalised
// (review rounds 16-17: substring needles and partial segments let an
// inserted statement through — `ctx = context.Background()`, a shadowing
// `flags := …`, a pointer alias, `cancel = func() {}`). Comments may change
// freely; any added, removed or altered statement fails here. Everything
// RunE calls after setup (runGapHeal) is driven at runtime by the TestGapHeal*
// tests.
func TestHealCollectionGapsWiring(t *testing.T) {
	norm := func(s string) string { return srctest.NormalizeWS(srctest.StripGoComments(s)) }
	runSrc := srctest.Read(t, "cmd/aveloxis/heal_collection_gaps_run.go")
	filler := norm(srctest.FuncBody(t, runSrc, "func collectorGapFiller("))
	wantFiller := norm(`func collectorGapFiller(store *db.PostgresStore, logger *slog.Logger, prChildMode string) gapFiller {
	return func(ctx context.Context, client platform.Client, c db.GapHealCandidate, threshold float64) (int, error) {
		gf := collector.NewGapFillerWithMode(store, client, logger, prChildMode)
		return gf.AssessAndFillGapsWithThreshold(ctx, c.RepoID, c.Owner, c.Name, c.MetaIssues, c.MetaPRs, threshold)
	}
}`)
	if filler != wantFiller {
		t.Errorf("collectorGapFiller changed:\n got %s\nwant %s", filler, wantFiller)
	}

	// The WHOLE command function (review round 18: pinning only RunE let
	// a PreRunE that forces dry-run, a replaced RunE, an Args validator or
	// a deferred flag write elsewhere in the function through). The Long
	// help text is elided so doc wording may change.
	longText := regexp.MustCompile("Long:\\s*`[^`]*`,")
	// Comments are stripped BEFORE the elision (review round 19: a crafted
	// comment containing "Long: `" could otherwise make the regex swallow
	// real code up to a later backtick).
	cmdSrc := norm(longText.ReplaceAllString(srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/heal_collection_gaps.go"), "func healCollectionGapsCmd(")), "Long: LONG,"))
	wantCmd := norm(`func healCollectionGapsCmd(cfgPath *string) *cobra.Command {
	var flags gapHealFlags

	cmd := &cobra.Command{
		Use:   "heal-collection-gaps",
		Short: "Heal issues/PRs lost to the pre-v0.27.139 blind-window bug (targeted, not a fleet rescan)",
		Long: LONG,
		RunE: func(cmd *cobra.Command, args []string) error {
			// v0.29.65: stop on SIGINT/SIGTERM so the exit path below releases
			// the rows this run parked (they read "collecting" otherwise).
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			if err := flags.validate(); err != nil {
				return err // before any connection: the error that applies
			}
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, logger)

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-heal-gaps"), logger)
			if err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("gap heal interrupted during setup: %w", ctx.Err())
				}
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
			if err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("gap heal interrupted during setup: %w", ctx.Err())
				}
				return fmt.Errorf("loading API keys: %w", err)
			}
			ghClient := github.New(cfg.GitHub.GitHubAPIBase(), ghKeys, logger)
			glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

			// Host, PID and start time in nanoseconds (v0.29.65 review rounds
			// 2 and 18): the release on exit frees every row under this owner,
			// so two runs must never share an ID — not across hosts, not two
			// containers that are both PID 1, not two shards started in the
			// same second.
			host, herr := os.Hostname()
			if herr != nil || host == "" {
				logger.Warn("hostname unavailable — the heal worker ID falls back to PID and start time", "error", herr)
				host = "unknown-host"
			}
			workerID := fmt.Sprintf("gap-heal-%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
			// v0.27.147 (round 26)/v0.27.150 (round 29): the run keeps ONE
			// set-wide heartbeat for every drain lock this worker holds —
			// a large repo's listing, fetch and processing can outlive a
			// running serve's RecoverStaleLocks timeout (1-hour default),
			// which would reclaim the park and let routine collection purge
			// the healer's staging mid-heal. The run releases every row it
			// still parks on exit, an interrupt included (v0.29.65).
			_, err = runGapHeal(ctx, cancel, flags, store, logger, os.Stdout, workerID, gapHealPageSize,
				gapHealClients{github: ghClient, gitlab: glClient},
				collectorGapFiller(store, logger, cfg.Collection.PRChildMode))
			return err
		},
	}

	bindGapHealFlags(cmd, &flags)
	return cmd
}`)
	if cmdSrc != wantCmd {
		t.Errorf("healCollectionGapsCmd changed; review it, then update this pin.\n got %s\nwant %s", cmdSrc, wantCmd)
	}
}

// TestRunGapHealRefusesInvalidFlags — the command's tail refuses a
// negative --repo-id (including -1, the boundary) or --limit with an
// error, before a heartbeat starts or a row is touched (review round 16:
// the refusal arm in the command was never made to fire, so exiting 0
// there passed).
func TestRunGapHealRefusesInvalidFlags(t *testing.T) {
	for _, f := range []gapHealFlags{{repoID: -1, workers: 1}, {repoID: -5, workers: 1}, {limit: -1, workers: 1}} {
		store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
		var logs bytes.Buffer
		_, err := runGapHeal(context.Background(), nil, f, store, slog.New(slog.NewTextHandler(&logs, nil)), io.Discard, testWorkerID, gapHealPageSize,
			gapHealClients{github: fakeForgeClient{name: "github"}},
			func(context.Context, platform.Client, db.GapHealCandidate, float64) (int, error) {
				t.Errorf("%+v healed", f)
				return 0, nil
			})
		if err == nil {
			t.Errorf("%+v must be refused", f)
		}
		if len(store.events) != 0 || len(store.lockTries) != 0 {
			t.Errorf("%+v touched the store: events=%v locks=%v", f, store.events, store.lockTries)
		}
	}
	if err := (gapHealFlags{repoID: 0, limit: 0}).validate(); err != nil {
		t.Errorf("zero means the fleet / all candidates: %v", err)
	}
}

// TestGapHealInterruptInsideTheLock — the Ctrl-C lands while repo 102's
// drain lock is being taken: not a failure, nothing parked, no WARN, and
// no later repo is tried.
func TestGapHealInterruptInsideTheLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}, cancel: cancel, cancelOnLock: 102}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 0, 100)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if fmt.Sprint(store.lockTries) != "[101 102]" {
		t.Errorf("lock attempts %v; 103 must not be tried after the interrupt", store.lockTries)
	}
}

// TestGapHealInterruptInsideTheCandidateQuery — the Ctrl-C lands during
// the second page's candidate query: an interrupt naming that page's
// cursor (102), not "candidate query: context canceled".
func TestGapHealInterruptInsideTheCandidateQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103, 104), locked: map[int64]bool{}, cancel: cancel, cancelOnQuery: 2}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 2)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 0, 100)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if strings.Contains(err.Error(), "candidate query") || !strings.Contains(err.Error(), "--after-repo-id 102") {
		t.Errorf("an interrupted query is an interrupt resuming at 102: %v", err)
	}
}

// TestGapHealDryRunHealsNothing — --dry-run lists the candidates and
// touches nothing: no lock, no fill, no refresh.
func TestGapHealDryRunHealsNothing(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
	var logs, out bytes.Buffer
	run := newGapRun(store, &logs, 2)
	run.dryRun = true
	run.out = &out
	run.fill = func(_ context.Context, c db.GapHealCandidate) (int, error) {
		t.Errorf("dry-run healed repo %d", c.RepoID)
		return 0, nil
	}
	if err := run.run(context.Background(), 0, 100); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(store.lockTries) != 0 || len(store.refreshed) != 0 {
		t.Errorf("dry-run locked %v / refreshed %v", store.lockTries, store.refreshed)
	}
	noRestore(t, store)
	for _, want := range []string{"repo 101  o/101", "repo 103  o/103", "3 candidate repo(s). Re-run without --dry-run to heal."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dry-run output lacks %q:\n%s", want, out.String())
		}
	}
}

// TestGapHealFailuresExitNonzero — a failed heal is counted, logged at
// WARN and makes the run exit nonzero; the other repos still heal and
// every lock is released (fleet and single-repo).
func TestGapHealFailuresExitNonzero(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, func(_ context.Context, c db.GapHealCandidate) (int, error) {
		if c.RepoID == 102 {
			return 0, errors.New("listing failed")
		}
		return 1, nil
	})
	err := run.run(context.Background(), 0, 100)
	if err == nil || !strings.Contains(err.Error(), "1 failure(s)") {
		t.Errorf("a failed heal must exit nonzero naming the count: %v", err)
	}
	if run.failed != 1 || run.visited != 3 || !strings.Contains(logs.String(), "level=WARN msg=\"gap heal error\"") {
		t.Errorf("failed=%d visited=%d\n%s", run.failed, run.visited, logs.String())
	}
	if len(store.locked) != 0 || fmt.Sprint(store.refreshed) != "[101 103]" {
		t.Errorf("locked=%v refreshed=%v", store.locked, store.refreshed)
	}
	noRestore(t, store)

	single := &fakeGapStore{locked: map[int64]bool{}}
	one := newGapRun(single, &logs, 5)
	one.fill = underLock(t, single, func(context.Context, db.GapHealCandidate) (int, error) { return 0, errors.New("boom") })
	if err := one.run(context.Background(), 7, 0); err == nil || !strings.Contains(err.Error(), "1 failure(s)") {
		t.Errorf("single-repo failure must exit nonzero: %v", err)
	}
	noRestore(t, single)
	single.metaErr = errors.New("meta lookup failed")
	if err := newGapRun(single, &logs, 5).run(context.Background(), 7, 0); err == nil || !strings.Contains(err.Error(), "meta counts for repo 7") {
		t.Errorf("a metadata lookup error is returned: %v", err)
	}
	noRestore(t, single)
}

// TestGapHealSkipsRepoUnderCollection — a repo serve is collecting (not
// 'queued') never locks, so it is skipped and counted, never healed.
func TestGapHealSkipsRepoUnderCollection(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}, notQueued: map[int64]bool{102: true}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	var healed []int64
	run.fill = underLock(t, store, func(_ context.Context, c db.GapHealCandidate) (int, error) {
		healed = append(healed, c.RepoID)
		return 1, nil
	})
	if err := run.run(context.Background(), 0, 100); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(healed) != "[101 103]" || run.skipped != 1 || run.failed != 0 {
		t.Errorf("healed=%v skipped=%d failed=%d", healed, run.skipped, run.failed)
	}
	// The summary names what the count is: rows not 'queued' (being
	// collected, or parked by a drain or heal), not only collections.
	if !strings.Contains(logs.String(), "skipped_not_queued=1") {
		t.Errorf("the summary reports the skip:\n%s", logs.String())
	}
	noRestore(t, store)
}

// TestGapHealLimitCanHealSweepAllAndRefreshFailure — --limit caps the
// candidates, a repo with no forge API (canHeal false) is neither locked
// nor healed, --all reaches the candidate query, and a failed count
// refresh warns without failing the heal.
func TestGapHealLimitCanHealSweepAllAndRefreshFailure(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103, 104), locked: map[int64]bool{},
		refreshErr: map[int64]error{101: errors.New("refresh failed")}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 2)
	run.limit = 3
	run.sweepAll = true
	run.canHeal = func(c db.GapHealCandidate) bool { return c.RepoID != 102 }
	run.fill = underLock(t, store, healsOK)
	if err := run.run(context.Background(), 0, 100); err != nil {
		t.Fatalf("a refresh failure is not a heal failure: %v", err)
	}
	if fmt.Sprint(store.lockTries) != "[101 103]" {
		t.Errorf("lock attempts %v; want 101 and 103 (102 has no API, 104 is past --limit 3)", store.lockTries)
	}
	if !store.sawAll {
		t.Error("--all must reach the candidate query")
	}
	if run.failed != 0 || !strings.Contains(logs.String(), "gathered-count refresh failed") {
		t.Errorf("failed=%d\n%s", run.failed, logs.String())
	}
	// A repo with no forge API is not "skipped (mid-collection)".
	if run.skipped != 0 {
		t.Errorf("skipped=%d; a no-API repo is not a mid-collection skip", run.skipped)
	}
	noRestore(t, store)
}

// TestGapHealRestoreStaysArmedThroughTheExitRelease — a Ctrl-C that lands
// while the exit release is running (a normal finish, the release slow on
// the database) still restores default signal handling, so a second
// Ctrl-C ends the process (round 5: moving the restore's disarm ahead of
// the exit release passed every test).
func TestGapHealRestoreStaysArmedThroughTheExitRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	restored := make(chan struct{})
	var once sync.Once
	run.restoreSignals = func() { once.Do(func() { close(restored) }) }
	sawRestore := false
	store.onReleaseAll = func() {
		cancel() // the Ctrl-C lands during the exit release
		select {
		case <-restored:
			sawRestore = true
		case <-time.After(5 * time.Second): // test-only bound: far above a goroutine handoff
		}
	}
	if err := run.run(ctx, 0, 100); err != nil {
		t.Fatalf("the run finished before the interrupt: %v", err)
	}
	if !sawRestore {
		t.Error("an interrupt during the exit release did not restore signal handling")
	}
}

// TestGapHealStoreFailuresAreFailures — a drain-lock error that is not an
// interrupt is counted and exits nonzero; a failed repo lookup in
// single-repo mode is returned (never a nil-pointer heal); and a failed
// exit release is logged.
func TestGapHealStoreFailuresAreFailures(t *testing.T) {
	store := &fakeGapStore{candidates: gapCandidates(101, 102), locked: map[int64]bool{},
		lockErr: map[int64]error{101: errors.New("lock failed")}, releaseAllErr: errors.New("db gone")}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	err := run.run(context.Background(), 0, 100)
	if err == nil || run.failed != 1 || !strings.Contains(logs.String(), "drain lock failed") {
		t.Errorf("a lock error is a counted failure with a nonzero exit: err=%v failed=%d\n%s", err, run.failed, logs.String())
	}
	if !strings.Contains(logs.String(), `level=WARN msg="releasing this run's parked rows failed`) {
		t.Errorf("a failed exit release is logged:\n%s", logs.String())
	}
	noRestore(t, store)

	single := &fakeGapStore{locked: map[int64]bool{}, repoErr: errors.New("no such repo")}
	one := newGapRun(single, &logs, 5)
	one.fill = func(context.Context, db.GapHealCandidate) (int, error) {
		t.Error("a failed repo lookup must not heal")
		return 0, nil
	}
	if err := one.run(context.Background(), 7, 0); err == nil || !strings.Contains(err.Error(), "repo 7: no such repo") {
		t.Errorf("the lookup error is returned: %v", err)
	}
	noRestore(t, single)
}

// TestGapHealSingleRepoHealsTheRepoAsStored — --repo-id heals the
// candidate built from the stored repo: its platform, owner, name and
// metadata counts reach the fill.
func TestGapHealSingleRepoHealsTheRepoAsStored(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	var got []db.GapHealCandidate
	run.fill = underLock(t, store, func(_ context.Context, c db.GapHealCandidate) (int, error) {
		got = append(got, c)
		return 1, nil
	})
	if err := run.run(context.Background(), 7, 0); err != nil {
		t.Fatal(err)
	}
	want := db.GapHealCandidate{RepoID: 7, Owner: "single-owner", Name: "7", Platform: model.PlatformGitLab, MetaIssues: 7, MetaPRs: 9}
	if len(got) != 1 || got[0] != want {
		t.Errorf("fill got %+v, want %+v", got, want)
	}
	if !strings.Contains(logs.String(), `msg="repo healed" repo_id=7`) {
		t.Errorf("a heal is reported:\n%s", logs.String())
	}
	noRestore(t, store)
}

// TestGapHealInterruptInsideTheRefresh — the Ctrl-C lands during a healed
// repo's count refresh: logged as an interrupt (INFO), not a failure.
func TestGapHealInterruptInsideTheRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeGapStore{candidates: gapCandidates(101, 102), locked: map[int64]bool{}, cancel: cancel, refreshCancel: 101}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 0, 100)
	assertCleanInterrupt(t, err, logs.String(), run, store)
	if !strings.Contains(logs.String(), "gathered-count refresh was interrupted") {
		t.Errorf("the interrupted refresh is reported:\n%s", logs.String())
	}
}

// TestGapHealRunsTheConfiguredWorkers — --workers N heals N repos at once.
func TestGapHealRunsTheConfiguredWorkers(t *testing.T) {
	const workers = 3
	store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.workers = workers
	var arrived sync.WaitGroup
	arrived.Add(workers)
	allIn := make(chan struct{})
	go func() { arrived.Wait(); close(allIn) }()
	concurrent := true
	var mu sync.Mutex
	run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
		arrived.Done()
		select {
		case <-allIn:
		case <-time.After(5 * time.Second): // test-only bound
			mu.Lock()
			concurrent = false
			mu.Unlock()
		}
		return 1, nil
	}
	if err := run.run(context.Background(), 0, 100); err != nil {
		t.Fatal(err)
	}
	if !concurrent {
		t.Errorf("--workers %d did not heal %d repos at once", workers, workers)
	}
	noRestore(t, store)
}

// TestGapHealSingleRepoInterruptedDuringLookup — a Ctrl-C during the
// single-repo lookups is reported as an interrupt, not as a failed lookup.
func TestGapHealSingleRepoInterruptedDuringLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 7, 0)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "repo 7 interrupted") {
		t.Errorf("an interrupted lookup is an interrupt: %v", err)
	}
	// The metadata lookup too: the repo lookup succeeds, then the cancel.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	store2 := &fakeGapStore{locked: map[int64]bool{}, metaErr: context.Canceled}
	run2 := newGapRun(store2, &logs, 5)
	run2.fill = underLock(t, store2, healsOK)
	store2.onMeta = cancel2
	if err := run2.run(ctx2, 7, 0); !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "repo 7 interrupted") {
		t.Errorf("an interrupted metadata lookup is an interrupt: %v", err)
	}
}

// TestGapHealSingleRepoWithNoForgeAPI — --repo-id on a generic git repo
// says there is nothing to list instead of exiting silently.
func TestGapHealSingleRepoWithNoForgeAPI(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.canHeal = func(db.GapHealCandidate) bool { return false }
	run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
		t.Error("a repo with no forge API must not be healed")
		return 0, nil
	}
	if err := run.run(context.Background(), 7, 0); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "no forge API to list") {
		t.Errorf("the no-API case is reported:\n%s", logs.String())
	}
	noRestore(t, store)
}

// TestGapHealSingleRepoUnderCollectionExitsNonzero — --repo-id on a repo
// serve is collecting heals nothing, so it must not exit 0: a script
// looping until success would read it as healed (review round 7).
func TestGapHealSingleRepoUnderCollectionExitsNonzero(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}, notQueued: map[int64]bool{7: true}}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
		t.Error("a repo under collection must not be healed")
		return 0, nil
	}
	err := run.run(context.Background(), 7, 0)
	if err == nil || !strings.Contains(err.Error(), "repo 7 is being collected") {
		t.Errorf("a skipped single repo exits nonzero: %v", err)
	}
	noRestore(t, store)
}

// TestGapHealSingleRepoRefusalReasons — --repo-id on a repo that cannot
// be drain-locked names the real reason, and every reason is an error
// (nothing was healed): being collected; parked by a staging drain or
// another heal run; not in the queue at all (prelim dequeued it: nothing
// will ever heal it); queued again since the lock attempt (a race: rerun
// now); or the status could not be read (review rounds 8-9: that arm
// must never read as "not in the queue").
func TestGapHealSingleRepoRefusalReasons(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeGapStore
		want  string
	}{
		{"being collected", &fakeGapStore{notQueued: map[int64]bool{7: true}, queueOwner: map[int64]string{7: "host-120000"}},
			"repo 7 is being collected — nothing healed; rerun when its collection finishes (a crashed owner's lock is reclaimed by stale-lock recovery or the next serve start)"},
		{"parked by a drain or heal", &fakeGapStore{notQueued: map[int64]bool{7: true}, queueOwner: map[int64]string{7: "gap-heal-h-1-120000:drain"}},
			"repo 7 is parked by a staging drain or another heal run — nothing healed; rerun when it is released (a crashed owner's park is reclaimed by stale-lock recovery or the next serve start)"},
		{"no queue row", &fakeGapStore{notQueued: map[int64]bool{7: true}, noQueueRow: map[int64]bool{7: true}},
			"repo 7 is not in the collection queue (gone or dequeued) — nothing to heal"},
		{"queued again since the lock attempt", &fakeGapStore{notQueued: map[int64]bool{7: true}, queueStatus: map[int64]string{7: "queued"}},
			"repo 7 became queued after the lock attempt — nothing healed; rerun now"},
		{"status unreadable", &fakeGapStore{notQueued: map[int64]bool{7: true}, queueStatusErr: errors.New("connection reset")},
			"repo 7 could not be locked for healing, and its queue status could not be read: connection reset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.store.locked = map[int64]bool{}
			var logs bytes.Buffer
			run := newGapRun(tc.store, &logs, 5)
			run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
				t.Error("an unlockable repo must not be healed")
				return 0, nil
			}
			err := run.run(context.Background(), 7, 0)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want %q", err, tc.want)
			}
			if strings.Contains(logs.String(), "mid-collection") || strings.Contains(logs.String(), "once it is queued again") {
				t.Errorf("the skip log must not predict the reason:\n%s", logs.String())
			}
			noRestore(t, tc.store)
		})
	}
}

// TestGapHealSingleRepoInterruptedDuringStatusRead — a Ctrl-C inside the
// queue-status read is an interrupt like every other lookup in runOne.
func TestGapHealSingleRepoInterruptedDuringStatusRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeGapStore{locked: map[int64]bool{}, notQueued: map[int64]bool{7: true}, onQueueStatus: cancel}
	var logs bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.fill = underLock(t, store, healsOK)
	err := run.run(ctx, 7, 0)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "repo 7 interrupted") {
		t.Errorf("an interrupted status read is an interrupt: %v", err)
	}
	if !strings.Contains(logs.String(), "gap heal interrupted") {
		t.Errorf("the interrupt is logged:\n%s", logs.String())
	}
	// The read runs on the run's context, so a Ctrl-C can cut a stalled
	// read short (a background context would wait it out).
	if store.statusCtxLive {
		t.Error("the queue-status read ignored the interrupt: it must run on the run's context")
	}
}

// TestGapHealSingleRepoDryRunHealsNothing — --repo-id with --dry-run
// lists the one candidate and touches nothing (review round 12: runOne
// ignored the flag and really healed the repo, a pre-existing bug the
// fleet-only dry-run test could not see).
func TestGapHealSingleRepoDryRunHealsNothing(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs, out bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.dryRun = true
	run.out = &out
	run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
		t.Error("dry-run healed the repo")
		return 0, nil
	}
	if err := run.run(context.Background(), 7, 0); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(store.lockTries) != 0 || len(store.refreshed) != 0 {
		t.Errorf("dry-run locked %v / refreshed %v", store.lockTries, store.refreshed)
	}
	for _, want := range []string{"repo 7  single-owner/7", "(meta issues=7 prs=9)", "Re-run without --dry-run to heal."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dry-run output lacks %q:\n%s", want, out.String())
		}
	}
	noRestore(t, store)
}

// fakeForgeClient stands in for a forge client; only its name is read.
type fakeForgeClient struct {
	platform.Client
	name string
}

// fillRecord records what the command's filler was handed per repo.
type fillRecord struct {
	mu         sync.Mutex
	thresholds []float64
	clients    map[int64]string
	liveCtx    []bool // was the fill's ctx still live on entry
	sawCancel  bool   // the fill's own ctx reported the interrupt
}

func (r *fillRecord) sortedThresholds() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := append([]float64(nil), r.thresholds...)
	sort.Float64s(c)
	return fmt.Sprint(c)
}

// runFromFlags parses real heal-collection-gaps flag strings through the
// command's own binder and returns an executor that calls runGapHeal — the
// exact function RunE ends in (review round 17: tests that built their own
// run left runGapHeal's body untested) — with named fake forge clients so
// which client healed which repo is observable (round 15), small pages,
// and a counting signal restore.
func runFromFlags(t *testing.T, store *fakeGapStore, logs, out *bytes.Buffer, args ...string) (func(context.Context) (*gapHealRun, error), gapHealFlags, *fillRecord) {
	t.Helper()
	cmd := &cobra.Command{Use: "heal-collection-gaps"}
	var f gapHealFlags
	bindGapHealFlags(cmd, &f)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	rec := &fillRecord{clients: map[int64]string{}}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	filler := func(ctx context.Context, client platform.Client, c db.GapHealCandidate, threshold float64) (int, error) {
		rec.mu.Lock()
		rec.thresholds = append(rec.thresholds, threshold)
		rec.clients[c.RepoID] = client.(fakeForgeClient).name
		rec.liveCtx = append(rec.liveCtx, ctx.Err() == nil)
		rec.mu.Unlock()
		if c.RepoID == store.cancelOnFillID && store.cancel != nil {
			store.cancel()
			rec.mu.Lock()
			rec.sawCancel = ctx.Err() != nil
			rec.mu.Unlock()
			return 0, ctx.Err()
		}
		return 1, nil
	}
	exec := func(ctx context.Context) (*gapHealRun, error) {
		return runGapHeal(ctx, func() { atomic.AddInt32(&store.restoreCalls, 1) }, f, store, logger, out, testWorkerID, 2,
			gapHealClients{github: fakeForgeClient{name: "github"}, gitlab: fakeForgeClient{name: "gitlab"}}, filler)
	}
	return exec, f, rec
}

// TestGapHealPageSize — the production page size is positive, and a page
// size below one is refused by the constructor (a zero page ends every
// fleet run at "0 candidates", the documented "done" signal).
func TestGapHealPageSize(t *testing.T) {
	if gapHealPageSize < 1 {
		t.Fatalf("gapHealPageSize %d must be at least 1", gapHealPageSize)
	}
	store := &fakeGapStore{locked: map[int64]bool{}}
	_, err := runGapHeal(context.Background(), nil, gapHealFlags{workers: 1}, store, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard,
		testWorkerID, 0, gapHealClients{}, nil)
	if err == nil || !strings.Contains(err.Error(), "page size 0") {
		t.Errorf("a zero page size must be refused: %v", err)
	}
}

// TestGapHealFlagsDriveTheRun — each flag, parsed as the command parses
// it, changes what runGapHeal does.
func TestGapHealFlagsDriveTheRun(t *testing.T) {
	bg := context.Background()
	t.Run("defaults", func(t *testing.T) {
		store := &fakeGapStore{locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, f, _ := runFromFlags(t, store, &logs, &out)
		run, err := exec(bg)
		if err != nil {
			t.Fatal(err)
		}
		if f.workers != 4 || run.workers != 4 || run.pageSize != 2 || f.limit != 0 || f.dryRun || f.sweepAll || f.repoID != 0 || f.afterRepoID != 0 {
			t.Errorf("defaults: flags=%+v run.workers=%d (commands.md documents --workers default 4)", f, run.workers)
		}
		noRestore(t, store)
	})
	t.Run("--dry-run heals nothing in the fleet", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101, 102), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out, "--dry-run")
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if len(rec.thresholds) != 0 || len(store.lockTries) != 0 || !strings.Contains(out.String(), "2 candidate repo(s)") {
			t.Errorf("dry-run healed %d / locked %v\n%s", len(rec.thresholds), store.lockTries, out.String())
		}
		noRestore(t, store)
	})
	t.Run("--repo-id with --dry-run heals nothing", func(t *testing.T) {
		store := &fakeGapStore{locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out, "--repo-id", "7", "--dry-run")
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if len(rec.thresholds) != 0 || len(store.lockTries) != 0 || !strings.Contains(out.String(), "repo 7 ") {
			t.Errorf("single dry-run healed %d / locked %v\n%s", len(rec.thresholds), store.lockTries, out.String())
		}
		noRestore(t, store)
	})
	t.Run("--all reaches the query and selects force-list", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out, "--all")
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if !store.sawAll || rec.sortedThresholds() != fmt.Sprint([]float64{collector.GapForceList}) {
			t.Errorf("sawAll=%v thresholds=%v", store.sawAll, rec.thresholds)
		}
		noRestore(t, store)
	})
	t.Run("routine fleet run uses threshold 0", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out)
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if store.sawAll || rec.sortedThresholds() != "[0]" {
			t.Errorf("sawAll=%v thresholds=%v", store.sawAll, rec.thresholds)
		}
		noRestore(t, store)
	})
	t.Run("--limit caps the candidates", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, _ := runFromFlags(t, store, &logs, &out, "--limit", "2")
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if got := sortedIDs(store.lockTries); got != "[101 102]" {
			t.Errorf("--limit 2 locked %v", got)
		}
		noRestore(t, store)
	})
	t.Run("--after-repo-id resumes past the cursor", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101, 102, 103), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, _ := runFromFlags(t, store, &logs, &out, "--after-repo-id", "101")
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if got := sortedIDs(store.lockTries); got != "[102 103]" {
			t.Errorf("--after-repo-id 101 locked %v", got)
		}
		noRestore(t, store)
	})
	t.Run("--repo-id heals only that repo, force-list", func(t *testing.T) {
		store := &fakeGapStore{candidates: gapCandidates(101, 102), locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out, "--repo-id", "7", "--workers", "0")
		run, err := exec(bg)
		if err != nil {
			t.Fatal(err)
		}
		if run.workers != 1 || fmt.Sprint(store.lockTries) != "[7]" || rec.sortedThresholds() != fmt.Sprint([]float64{collector.GapForceList}) {
			t.Errorf("workers=%d locked=%v thresholds=%v (only repo 7, force-list; --workers 0 clamps to 1)", run.workers, store.lockTries, rec.thresholds)
		}
		noRestore(t, store)
	})
	t.Run("negative --repo-id and --limit are refused", func(t *testing.T) {
		for _, args := range [][]string{{"--repo-id", "-1"}, {"--repo-id", "-5"}, {"--limit", "-1"}} {
			store := &fakeGapStore{candidates: gapCandidates(101), locked: map[int64]bool{}}
			var logs, out bytes.Buffer
			exec, _, rec := runFromFlags(t, store, &logs, &out, args...)
			run, err := exec(bg)
			if err == nil || run != nil || len(rec.thresholds) != 0 || len(store.events) != 0 {
				t.Errorf("%v must be refused before anything runs: run=%v err=%v events=%v", args, run, err, store.events)
			}
		}
	})
	t.Run("an interrupt reaches the filler and restores signals", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bg)
		defer cancel()
		store := &fakeGapStore{candidates: gapCandidates(101, 102), locked: map[int64]bool{}, cancel: cancel, cancelOnFillID: 101}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out, "--workers", "1")
		_, err := exec(ctx)
		if !errors.Is(err, context.Canceled) || strings.Contains(logs.String(), "level=WARN") {
			t.Errorf("an interrupt through runGapHeal is a clean interrupt: %v\n%s", err, logs.String())
		}
		if len(rec.clients) != 1 || !rec.sawCancel {
			t.Errorf("filled %v after the interrupt; the filler's context saw it: %v", rec.clients, rec.sawCancel)
		}
		// The restore passed to runGapHeal fires on the interrupt.
		deadline := time.Now().Add(5 * time.Second) // test-only bound
		for atomic.LoadInt32(&store.restoreCalls) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if atomic.LoadInt32(&store.restoreCalls) == 0 {
			t.Error("the interrupt did not restore default signal handling")
		}
		sameOwner(t, store)
	})
	t.Run("GitHub and GitLab candidates are filled with their own client", func(t *testing.T) {
		store := &fakeGapStore{candidates: []db.GapHealCandidate{{RepoID: 101, Platform: model.PlatformGitHub}, {RepoID: 102, Platform: model.PlatformGitLab}}, locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out)
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(rec.clients) != "map[101:github 102:gitlab]" {
			t.Errorf("clients by repo %v; want 101 github, 102 gitlab", rec.clients)
		}
		noRestore(t, store)
	})
	t.Run("a generic git candidate is never filled", func(t *testing.T) {
		store := &fakeGapStore{candidates: []db.GapHealCandidate{{RepoID: 101, Platform: model.PlatformGenericGit}, {RepoID: 102, Platform: model.PlatformGitLab}}, locked: map[int64]bool{}}
		var logs, out bytes.Buffer
		exec, _, rec := runFromFlags(t, store, &logs, &out)
		if _, err := exec(bg); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(store.lockTries) != "[102]" || len(rec.thresholds) != 1 {
			t.Errorf("locked %v, filled %d", store.lockTries, len(rec.thresholds))
		}
		noRestore(t, store)
	})
}

// TestGapHealSingleRepoDryRunPredictsTheRealRun — single-repo dry-run
// says what a real run would do: nothing for a generic git repo (no
// forge API), refuse a repo with no queue row or one not queued now, heal
// a queued one (review round 13: it said "re-run to heal" for all).
func TestGapHealSingleRepoDryRunPredictsTheRealRun(t *testing.T) {
	for _, tc := range []struct {
		name     string
		store    *fakeGapStore
		canHeal  bool
		want     string
		wantNone bool // prints no candidate line
	}{
		{"queued", &fakeGapStore{}, true, "Re-run without --dry-run to heal.", false},
		{"no queue row", &fakeGapStore{noQueueRow: map[int64]bool{7: true}}, true, "Not in the collection queue (gone or dequeued)", false},
		{"not queued now", &fakeGapStore{queueStatus: map[int64]string{7: "collecting"}}, true, "Not queued now", false},
		{"generic git", &fakeGapStore{}, false, "", true},
		// Round 14: a row parked by a drain or another heal is refused by a
		// real run, so dry-run must not predict a heal.
		{"parked by a drain or heal", &fakeGapStore{queueStatus: map[int64]string{7: "collecting"}, queueOwner: map[int64]string{7: "gap-heal-h-1-120000:drain"}}, true, "Not queued now", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.store.locked = map[int64]bool{}
			var logs, out bytes.Buffer
			run := newGapRun(tc.store, &logs, 5)
			run.dryRun = true
			run.out = &out
			run.canHeal = func(db.GapHealCandidate) bool { return tc.canHeal }
			run.fill = func(context.Context, db.GapHealCandidate) (int, error) {
				t.Error("dry-run healed")
				return 0, nil
			}
			if err := run.run(context.Background(), 7, 0); err != nil {
				t.Fatal(err)
			}
			if tc.wantNone {
				if out.Len() != 0 || !strings.Contains(logs.String(), "no forge API to list") {
					t.Errorf("a generic git repo is reported, not listed:\nout=%s\nlogs=%s", out.String(), logs.String())
				}
				return
			}
			if !strings.Contains(out.String(), tc.want) || len(tc.store.lockTries) != 0 {
				t.Errorf("want %q, locked %v:\n%s", tc.want, tc.store.lockTries, out.String())
			}
			noRestore(t, tc.store)
		})
	}
}

// sortedIDs renders ids in ascending order, for runs whose workers lock
// concurrently.
func sortedIDs(ids []int64) string {
	c := append([]int64(nil), ids...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return fmt.Sprint(c)
}

// TestGapHealSingleRepoDryRunStatusFailures — a failed queue-status read
// in dry-run is an error, never "not in the queue" (SR-5, the defect
// rounds 8-9 fixed in explainSkip); an interrupt inside it is an
// interrupt (review round 14).
func TestGapHealSingleRepoDryRunStatusFailures(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}, queueStatusErr: errors.New("connection reset")}
	var logs, out bytes.Buffer
	run := newGapRun(store, &logs, 5)
	run.dryRun = true
	run.out = &out
	err := run.run(context.Background(), 7, 0)
	if err == nil || !strings.Contains(err.Error(), "repo 7: queue status: connection reset") {
		t.Errorf("a failed status read is an error: %v", err)
	}
	if strings.Contains(out.String(), "Not in the collection queue") {
		t.Errorf("a failed read must not read as no row:\n%s", out.String())
	}
	noRestore(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store2 := &fakeGapStore{locked: map[int64]bool{}, onQueueStatus: cancel}
	run2 := newGapRun(store2, &logs, 5)
	run2.dryRun = true
	run2.out = &out
	if err := run2.run(ctx, 7, 0); !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "repo 7 interrupted") {
		t.Errorf("an interrupted status read is an interrupt: %v", err)
	}
}

// TestGapHealFillRefusesAPlatformWithNoClient — the layer enforces what
// canHeal screens (SR-18): a fill for a platform with no forge client is
// an error, never a nil-client call into the collector (review round 15:
// that was a panic in a worker goroutine that skipped the exit release).
func TestGapHealFillRefusesAPlatformWithNoClient(t *testing.T) {
	store := &fakeGapStore{locked: map[int64]bool{}}
	var logs bytes.Buffer
	run, err := newGapHealRun(gapHealFlags{workers: 1}, store, slog.New(slog.NewTextHandler(&logs, nil)), io.Discard, testWorkerID, gapHealPageSize,
		gapHealClients{github: fakeForgeClient{name: "github"}},
		func(_ context.Context, client platform.Client, c db.GapHealCandidate, _ float64) (int, error) {
			if client == nil {
				t.Errorf("repo %d reached the filler with a nil client", c.RepoID)
			}
			return 1, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []model.Platform{model.PlatformGitLab, model.PlatformGenericGit} {
		if run.canHeal(db.GapHealCandidate{Platform: p}) {
			t.Errorf("platform %d has no client here, so it cannot be healed", p)
		}
		if _, err := run.fill(context.Background(), db.GapHealCandidate{RepoID: 9, Platform: p}); err == nil || !strings.Contains(err.Error(), "no forge client") {
			t.Errorf("platform %d: fill must refuse, got %v", p, err)
		}
	}
	if n, err := run.fill(context.Background(), db.GapHealCandidate{RepoID: 9, Platform: model.PlatformGitHub}); err != nil || n != 1 {
		t.Errorf("GitHub fills: %d %v", n, err)
	}
}
