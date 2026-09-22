// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// TestPrelimSidelinesLegallyBlockedRepo — v0.29.58 (2026-09-22 log review,
// finding 3): a repository under a DMCA takedown answers 451 on the web
// page and on every API endpoint. Prelim only sidelined 404/410, so the
// job ran, every endpoint burned its retry budget, the job failed after
// eleven minutes and was re-queued — every cycle since November. A 451 is
// as definitive as a 404: sideline (archive + gone stamp), keep the data,
// and let the gone recheck notice if the block is lifted.
func TestPrelimSidelinesLegallyBlockedRepo(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
	}))
	t.Cleanup(srv.Close)

	repo := &model.Repo{GroupID: 1, Platform: model.PlatformGitHub, GitURL: srv.URL + "/hakyimlab/ptrs-ukb", Owner: "hakyimlab", Name: "ptrs-ukb"}
	id, err := store.UpsertRepo(ctx, repo)
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	repo.ID = id

	res, err := RunPrelim(ctx, store, repo, logger)
	if err != nil {
		t.Fatalf("RunPrelim: %v", err)
	}
	if !res.Skip {
		t.Fatal("a 451 must sideline the repository (Skip=true); it ran the full job every cycle before v0.29.58")
	}
	if !strings.Contains(res.SkipReason, "451") || !strings.Contains(res.SkipReason, "legal") {
		t.Errorf("SkipReason = %q, want it to name 451 and the legal block", res.SkipReason)
	}
	var archived, goneStamped bool
	if err := store.Pool().QueryRow(ctx, `SELECT COALESCE(repo_archived, FALSE), repo_gone_at IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&archived, &goneStamped); err != nil {
		t.Fatal(err)
	}
	if !archived {
		t.Error("the repository row must be archived (MarkRepoGone) so the queue drops it and the GUI says it is unavailable")
	}
	if !goneStamped {
		t.Error("repo_gone_at must be stamped so the gone recheck re-probes the block on its cadence")
	}
	if !strings.Contains(logBuf.String(), "sidelining permanently") || !strings.Contains(logBuf.String(), "status=451") {
		t.Errorf("expected the sideline WARN naming status 451, got:\n%s", logBuf.String())
	}
}
