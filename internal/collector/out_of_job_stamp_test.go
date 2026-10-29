// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// D7 (0.29.120, summary/63): the collector's runs outside a queued job — the
// mailing-list and Jira drains (refreshRepoActivity) and heal-messages —
// stamp every repository they wrote, so the API's cached answers, durable
// until the repository's state moves, follow them.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type fakeDrainRefresher struct {
	refreshErr error
	stampErr   error
	refreshed  []int64
	stamped    [][]int64
}

func (f *fakeDrainRefresher) RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error {
	f.refreshed = append(f.refreshed, repoID)
	return f.refreshErr
}

func (f *fakeDrainRefresher) StampReposChangedOutsideJob(ctx context.Context, repoIDs []int64) error {
	f.stamped = append(f.stamped, repoIDs)
	return f.stampErr
}

// The stamp is not best-effort the way the activity refresh is: the drain
// already landed its rows, so a canceled drain (a stop) and a failed refresh
// still stamp (the store makes the stamp itself outlast the cancel), and a
// failed stamp is logged.
func TestDrainRefreshStampsTheRepository(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, c := range map[string]struct {
		ctx        context.Context
		refreshErr error
	}{
		"live":           {context.Background(), nil},
		"canceled":       {canceled, nil},
		"refresh failed": {context.Background(), errors.New("refresh boom")},
	} {
		f := &fakeDrainRefresher{refreshErr: c.refreshErr}
		var logs bytes.Buffer
		refreshRepoActivity(c.ctx, f, slog.New(slog.NewTextHandler(&logs, nil)), 42, "test drain", true)
		if !reflect.DeepEqual(f.stamped, [][]int64{{42}}) {
			t.Errorf("%s: stamped %v, want [[42]]: a drain's rows must move the repository's cache state", name, f.stamped)
		}
	}
	f := &fakeDrainRefresher{stampErr: errors.New("stamp boom")}
	var logs bytes.Buffer
	refreshRepoActivity(context.Background(), f, slog.New(slog.NewTextHandler(&logs, nil)), 7, "test drain", true)
	if !strings.Contains(logs.String(), "stamp boom") || !strings.Contains(logs.String(), "repo_id=7") {
		t.Errorf("a failed stamp must be logged with its repository and error, got %q", logs.String())
	}
	if !reflect.DeepEqual(f.refreshed, []int64{7}) {
		t.Errorf("a failed stamp must not skip the activity refresh, refreshed %v", f.refreshed)
	}
	// Reached but nothing final written: the activity refresh, no stamp.
	f = &fakeDrainRefresher{}
	refreshRepoActivity(context.Background(), f, slog.New(slog.NewTextHandler(&logs, nil)), 8, "test drain", false)
	if len(f.stamped) != 0 || !reflect.DeepEqual(f.refreshed, []int64{8}) {
		t.Errorf("wrote=false: stamped %v (want none), refreshed %v (want [8])", f.stamped, f.refreshed)
	}
}

// heal-messages writes the conversations of every repository its batch names
// (the refetched parents and the stale links it deletes): that set, each
// once, in a stable order.
func TestHealBatchReposIsEveryRepositoryTheBatchNames(t *testing.T) {
	items := []db.MessageHealItem{
		{MsgID: 1, IssueRepoID: 5},
		{MsgID: 2, PRConvRepoID: 9, ReviewRepoID: 5},
		{MsgID: 3, ReviewRepoID: 3},
		{MsgID: 4}, // no parent anywhere: its cleanup touches no conversation
	}
	if got := healBatchRepos(items); !reflect.DeepEqual(got, []int64{3, 5, 9}) {
		t.Errorf("healBatchRepos = %v, want [3 5 9]", got)
	}
	if got := healBatchRepos(nil); len(got) != 0 {
		t.Errorf("an empty batch names no repository, got %v", got)
	}
}

// The wiring: the pass stamps that set after its last write (the refetch and
// the stale-link cleanup) and before the healed mark, whose failure returns
// an error after the rows were written; a dry run writes nothing and returns
// before it.
func TestMessageHealPassStampsAfterItsWrites(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func runMessageHealPass("))
	// Deferred, so every return after the first write reaches it — a stop
	// mid-pass included (round 1 finding 2: the per-repository loop returned
	// on ctx.Err() before a stamp placed after it).
	stamp := strings.Index(body, "defer stampHealBatch(ctx, store, logger, items)")
	if stamp < 0 {
		t.Fatal("runMessageHealPass must defer stampHealBatch(ctx, store, logger, items)")
	}
	dry := strings.Index(body, "if dryRun {")
	if dry < 0 || dry > stamp {
		t.Error("a dry run returns before the deferred stamp (it writes nothing)")
	}
	for _, write := range []string{"sw.Flush(ctx)", "proc.ProcessRepo(", "store.DeleteStaleConversationRefs("} {
		if i := strings.Index(body, write); i < 0 || i < stamp {
			t.Errorf("the stamp must be deferred before the pass's first write (%s)", write)
		}
	}
	fn := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func stampHealBatch("))
	if !strings.Contains(fn, "StampReposChangedOutsideJob(") || !strings.Contains(fn, "healBatchRepos(items)") {
		t.Error("stampHealBatch must stamp healBatchRepos(items)")
	}
}
