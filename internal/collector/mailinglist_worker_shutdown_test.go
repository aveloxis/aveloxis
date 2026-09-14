// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
)

type releasingStore struct {
	fakeStageStore
	job      *db.ListJob
	released []int64
	lockedAt time.Time
	failures int
}

func (s *releasingStore) ClaimNextList(context.Context, string, time.Duration, int, string) (*db.ListJob, error) {
	return s.job, nil
}
func (s *releasingStore) ReleaseListLock(_ context.Context, id int64, lockedAt time.Time) error {
	s.released = append(s.released, id)
	s.lockedAt = lockedAt
	return nil
}
func (s *releasingStore) RecordListFailure(context.Context, int64) error {
	s.failures++
	return nil
}

// cancelBackend fails every fetch with the context's own error.
type cancelBackend struct {
	fakeBackend
	err error
}

func (b *cancelBackend) FetchMonth(ctx context.Context, _ string, _ string) ([]mailinglist.ArchiveMessage, time.Duration, error) {
	if b.err != nil {
		return nil, 0, b.err
	}
	return nil, 0, ctx.Err()
}

func newShutdownTestWorker(t *testing.T, store *releasingStore, backend mailinglist.ArchiveSource) *MailingListWorker {
	t.Helper()
	systems, err := mailinglist.LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	return NewMailingListWorker(store, systems["apache_ponymail"], backend,
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond),
		mailinglist.NewBreaker(10, time.Hour),
		90*24*time.Hour, 1, 4242, "boot-x", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Pass 37–39 (v0.28.18): a scan interrupted by shutdown releases ITS
// OWN lock — keyed on the claim's own lock stamp — instead of leaving
// the list unclaimable for the 2h stale window, and the cancellation is
// neither a failure (no RecordListFailure) nor a WARN.
func TestRunOnceReleasesTheListLockOnShutdown(t *testing.T) {
	claimedAt := time.Date(2026, 8, 28, 1, 2, 3, 456000, time.UTC)
	store := &releasingStore{job: &db.ListJob{RglsID: 7, RepoGroupID: 3, ListAddress: "dev@arrow.apache.org", System: "apache_ponymail", LockedAt: claimedAt}}
	w := newShutdownTestWorker(t, store, &cancelBackend{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	claimed, err := w.RunOnce(ctx)
	if !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce under a done ctx = (%v, %v), want (true, context.Canceled)", claimed, err)
	}
	if len(store.released) != 1 || store.released[0] != 7 {
		t.Fatalf("the interrupted list's lock must be released exactly once, got %v", store.released)
	}
	if !store.lockedAt.Equal(claimedAt) {
		t.Errorf("the release must be keyed on the claim's own lock stamp, got %v want %v", store.lockedAt, claimedAt)
	}
	if store.failures != 0 {
		t.Errorf("shutdown must not be recorded as a list failure (got %d)", store.failures)
	}
}

// Pass 39: the same contract against the REAL Pony Mail backend, whose
// transport failures are wrapped as ErrTransient (the cause was dropped
// before this pass) — a shutdown that lands mid-fetch, the phase a scan
// spends nearly all its time in, must still release and must not
// record a failure. The server never answers; the ctx is canceled
// 100ms into the fetch.
func TestRunOnceReleasesTheListLockWhenShutdownKillsARealFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	claimedAt := time.Date(2026, 8, 28, 1, 2, 3, 0, time.UTC)
	store := &releasingStore{job: &db.ListJob{RglsID: 9, RepoGroupID: 3, ListAddress: "dev@arrow.apache.org", System: "apache_ponymail", LockedAt: claimedAt}}
	w := newShutdownTestWorker(t, store, mailinglist.NewPonyMail(srv.URL, "aveloxis-test"))
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(100*time.Millisecond, cancel)
	claimed, err := w.RunOnce(ctx)
	if !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce with a real fetch killed by shutdown = (%v, %v), want (true, context.Canceled)", claimed, err)
	}
	if len(store.released) != 1 || store.released[0] != 9 || !store.lockedAt.Equal(claimedAt) {
		t.Fatalf("the lock must be released (keyed on the claim stamp), got released=%v lockedAt=%v", store.released, store.lockedAt)
	}
	if store.failures != 0 {
		t.Errorf("a shutdown-killed fetch must not be recorded as a list failure (got %d)", store.failures)
	}
}

// A real fetch failure keeps the pre-existing contract: RecordListFailure
// (which releases the lock itself) and no ReleaseListLock.
func TestRunOnceRecordsARealFetchFailureWithoutReleasing(t *testing.T) {
	store := &releasingStore{job: &db.ListJob{RglsID: 8, RepoGroupID: 3, ListAddress: "dev@arrow.apache.org", System: "apache_ponymail"}}
	w := newShutdownTestWorker(t, store, &cancelBackend{err: errors.New("upstream 500")})
	claimed, err := w.RunOnce(t.Context())
	if !claimed || err != nil {
		t.Fatalf("RunOnce on a real failure = (%v, %v), want (true, nil) — the failure is recorded, not returned", claimed, err)
	}
	if store.failures != 1 || len(store.released) != 0 {
		t.Errorf("a real failure must go through RecordListFailure (got %d) and never ReleaseListLock (got %v)", store.failures, store.released)
	}
}
