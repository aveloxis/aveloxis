// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// fakeScancodeStore records what the worker asked the database to do,
// so the DB-only paths can be OBSERVED instead of described (v0.28.19).
// Every counter is guarded: the worker calls these from runners, from
// the dispatcher and from best-effort Background-ctx goroutines.
type fakeScancodeStore struct {
	mu sync.Mutex

	claims      int
	claimJobs   []*db.ScancodeJob // handed out in order; nil once exhausted
	claimErr    error
	lockStates  int
	cleared     []int64
	completed   []int64
	skipped     []int64
	failures    []int64
	timeouts    []int64
	sidelines   int
	statuses    []string
	staleClears int
	snapshots   []int64
}

var _ ScancodeStore = (*fakeScancodeStore)(nil)

func (f *fakeScancodeStore) ClaimNextScancodeRepo(context.Context, time.Duration, time.Duration) (*db.ScancodeJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.claimJobs) == 0 {
		return nil, nil
	}
	job := f.claimJobs[0]
	f.claimJobs = f.claimJobs[1:]
	return job, nil
}

func (f *fakeScancodeStore) RecordScancodeLockState(_ context.Context, _ int64, _ int, _, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lockStates++
	return nil
}

func (f *fakeScancodeStore) ClearScancodeLock(_ context.Context, repoID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared = append(f.cleared, repoID)
	return nil
}

func (f *fakeScancodeStore) ClearStaleNullPidLocks(context.Context, time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staleClears++
	return 0, nil
}

func (f *fakeScancodeStore) ListLockedScancodeRows(context.Context) ([]db.ScancodeLockedRow, error) {
	return nil, nil
}

func (f *fakeScancodeStore) MarkScancodeComplete(_ context.Context, repoID int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed = append(f.completed, repoID)
	return nil
}

func (f *fakeScancodeStore) MarkScancodeSkipped(_ context.Context, repoID int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skipped = append(f.skipped, repoID)
	return nil
}

func (f *fakeScancodeStore) RecordScancodeFailure(_ context.Context, repoID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, repoID)
	return nil
}

func (f *fakeScancodeStore) RecordScancodeTimeout(_ context.Context, repoID int64, sideline bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.timeouts = append(f.timeouts, repoID)
	if sideline {
		f.sidelines++
	}
	return nil
}

func (f *fakeScancodeStore) SetAveloxisStatus(_ context.Context, _, status, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, status)
	return nil
}

func (f *fakeScancodeStore) ReplaceScancodeSnapshot(_ context.Context, repoID int64, _ db.ScancodeScanMeta, _ []*db.ScancodeFileRow) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots = append(f.snapshots, repoID)
	return 1, nil
}

// snapshot copies the observable counters under the lock.
func (f *fakeScancodeStore) snapshot() fakeScancodeStoreState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeScancodeStoreState{
		claims:      f.claims,
		cleared:     append([]int64(nil), f.cleared...),
		completed:   append([]int64(nil), f.completed...),
		skipped:     append([]int64(nil), f.skipped...),
		failures:    append([]int64(nil), f.failures...),
		timeouts:    append([]int64(nil), f.timeouts...),
		sidelines:   f.sidelines,
		statuses:    append([]string(nil), f.statuses...),
		staleClears: f.staleClears,
	}
}

type fakeScancodeStoreState struct {
	claims      int
	cleared     []int64
	completed   []int64
	skipped     []int64
	failures    []int64
	timeouts    []int64
	sidelines   int
	statuses    []string
	staleClears int
}
