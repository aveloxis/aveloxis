// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

type offHostHistoryFetcher struct{ err error }

func (f offHostHistoryFetcher) FetchContributorHistoryMeta(context.Context, string) (time.Time, []int, error) {
	return time.Time{}, nil, f.err
}
func (f offHostHistoryFetcher) FetchContributorDailyHistory(context.Context, string, []github.HistoryWindow) ([]model.ContributorDayActivity, []model.ContributorDayTotal, error) {
	return nil, nil, f.err
}

// TestHistoryOffHostRefusalIsNotMarked pins worklist item 26 (activity
// history): processHistoryContributor stamped MarkHistoryBackfilled — "this
// contributor's history is done" — for ErrNotFound OR ANY ClassSkip error,
// and ClassSkip includes platform.ErrOffHostRefused, our own client refusing
// to send a key to another host, which is not the forge's answer about the
// contributor (platform.IsDefinitiveAnswer excludes it). Unreachable today
// (GraphQL never follows a redirect), pinned so it stays that way: the
// refusal is a failure (retried on the next claim), and the store is not
// touched — the Scheduler here has no store, so a stamp attempt would panic.
func TestHistoryOffHostRefusalIsNotMarked(t *testing.T) {
	s := &Scheduler{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	fetcher := offHostHistoryFetcher{err: errors.Join(errors.New("redirect to evil.example"), platform.ErrOffHostRefused)}
	got := s.processHistoryContributor(context.Background(), fetcher, db.ActivityCheckContributor{ID: "x", Login: "someone"}, 30)
	if got != historyFailed {
		t.Errorf("an off-host refusal = outcome %v; want historyFailed (not marked done)", got)
	}
}
