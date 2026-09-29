// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

type fakeSweepStore struct {
	issues []db.SweepIssue
	set    map[int64]string
	onSet  func() // e.g. a cancel arriving during the write
}

func (f *fakeSweepStore) IssuesNeedingClosedBySweep(context.Context, int64) ([]db.SweepIssue, error) {
	return f.issues, nil
}

func (f *fakeSweepStore) SetIssueClosedBy(ctx context.Context, issueID int64, cntrbID string) error {
	if f.onSet != nil {
		f.onSet()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.set == nil {
		f.set = map[int64]string{}
	}
	f.set[issueID] = cntrbID
	return nil
}

type fakeCloserClient struct{ calls []string }

func (f *fakeCloserClient) FetchIssueClosers(_ context.Context, owner, repo string, numbers []int) (map[int]model.UserRef, error) {
	f.calls = append(f.calls, owner+"/"+repo)
	out := map[int]model.UserRef{}
	for _, n := range numbers {
		out[n] = model.UserRef{Login: "closer"}
	}
	return out, nil
}

// fakeCloserResolver runs onResolve (e.g. a cancel) and returns err.
type fakeCloserResolver struct {
	onResolve func()
	err       error
}

func (f *fakeCloserResolver) Resolve(context.Context, int16, int64, string, string, string, string, string, string, string) (string, error) {
	if f.onResolve != nil {
		f.onResolve()
	}
	if f.err != nil {
		return "", f.err
	}
	return "00000000-0000-0000-0000-000000000001", nil
}

func twoRepoSweepIssues() []db.SweepIssue {
	return []db.SweepIssue{
		{IssueID: 1, RepoID: 10, Owner: "o", Repo: "a", Number: 1},
		{IssueID: 2, RepoID: 20, Owner: "o", Repo: "b", Number: 1},
	}
}

// TestClosedBySweepStopsWhenCancelledDuringResolve pins PR #218 review A6:
// cancellation was noticed only on FetchIssueClosers, so a stop during the
// resolve step was swallowed (the resolve error skipped silently), the next
// chunk was fetched, and the sweep logged "complete" and returned nil.
func TestClosedBySweepStopsWhenCancelledDuringResolve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	client := &fakeCloserClient{}
	s := &ClosedBySweep{
		store:    &fakeSweepStore{issues: twoRepoSweepIssues()},
		client:   client,
		resolver: &fakeCloserResolver{onResolve: cancel, err: context.Canceled},
		logger:   slog.New(slog.NewTextHandler(&logs, nil)),
		perQuery: 100,
	}
	_, err := s.Run(ctx, 0, false)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v; want the interruption (context.Canceled)", err)
	}
	if len(client.calls) != 1 {
		t.Errorf("FetchIssueClosers calls = %v; want only the first chunk's (stop noticed at the next chunk)", client.calls)
	}
	if strings.Contains(logs.String(), "closed_by sweep complete") {
		t.Errorf("an interrupted sweep logged complete:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("a stop during the resolve was logged as a failure:\n%s", logs.String())
	}
}

// TestClosedBySweepStopsWhenCancelledDuringTheWrite: a stop that the
// write sees (skipped quietly per issue) is noticed at the next chunk —
// not by fetching it — and, when it lands in the last chunk, before the
// "complete" line (PR #218 review A6).
func TestClosedBySweepStopsWhenCancelledDuringTheWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		issues    []db.SweepIssue
		wantCalls int
	}{
		{"mid-sweep", twoRepoSweepIssues(), 1},
		{"last chunk", twoRepoSweepIssues()[:1], 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var logs bytes.Buffer
			client := &fakeCloserClient{}
			s := &ClosedBySweep{
				store:    &fakeSweepStore{issues: tc.issues, onSet: cancel},
				client:   client,
				resolver: &fakeCloserResolver{},
				logger:   slog.New(slog.NewTextHandler(&logs, nil)),
				perQuery: 100,
			}
			_, err := s.Run(ctx, 0, false)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run = %v; want the interruption (context.Canceled)", err)
			}
			if len(client.calls) != tc.wantCalls {
				t.Errorf("FetchIssueClosers calls = %v; want %d", client.calls, tc.wantCalls)
			}
			if strings.Contains(logs.String(), "closed_by sweep complete") {
				t.Errorf("an interrupted sweep logged complete:\n%s", logs.String())
			}
		})
	}
}

// TestClosedBySweepLogsAResolveFailure pins the other half of A6: a
// resolve error that is not a stop was skipped without a log line.
func TestClosedBySweepLogsAResolveFailure(t *testing.T) {
	var logs bytes.Buffer
	s := &ClosedBySweep{
		store:    &fakeSweepStore{issues: twoRepoSweepIssues()[:1]},
		client:   &fakeCloserClient{},
		resolver: &fakeCloserResolver{err: errors.New("connection reset")},
		logger:   slog.New(slog.NewTextHandler(&logs, nil)),
		perQuery: 100,
	}
	if _, err := s.Run(context.Background(), 0, false); err != nil {
		t.Fatalf("Run = %v; one failed resolve must not fail the sweep", err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "connection reset") || !strings.Contains(logs.String(), "issue_id=1") {
		t.Errorf("the resolve failure is not logged with its issue:\n%s", logs.String())
	}
}
