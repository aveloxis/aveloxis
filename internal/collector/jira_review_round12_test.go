// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot round 12 on PR #193: every non-cancel failure arm that
// cannot record its outcome must still RELEASE the claim — a held
// jps_locked_at strands the project for the 2h stale window.

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// A transient DisableJiraProject failure on the dead-key arm must
// release the ownership-qualified claim: the pre-fix return left the
// project neither retryable nor disabled until the stale window
// expired.
func TestJiraDisableFailureReleasesClaim(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)                                                                                    // dead key → ClassSkip → disable arm
		_, _ = w.Write([]byte(`{"errorMessages":["The value 'X' does not exist for the field 'project'."]}`)) // round 23: project-not-found body
	}))
	defer srv.Close()
	store := &fakeJiraStore{
		job:        &db.JiraProjectJob{JpsID: 8, ProjectKey: "DEADKEY", BaseURL: srv.URL},
		disableErr: fmt.Errorf("connection reset by peer"),
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 100, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())
	if store.disabled || store.completed || store.failures != 0 {
		t.Fatalf("disabled=%v completed=%v failures=%d — a failed disable records nothing", store.disabled, store.completed, store.failures)
	}
	if len(store.released) != 1 {
		t.Fatalf("released = %v, want exactly one claim release so the next cadence can retry the disable", store.released)
	}
}

// A generic RecordJiraFailure write error on the sync-failure arm
// (suppressed round-12 finding): ErrJiraClaimLost is already excluded,
// so releasing with the claim's own stamp is safe — and without it the
// claim strands exactly like the pre-round-5 completion arm.
func TestJiraRecordFailureErrorReleasesClaim(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503) // transient → sync-failure arm → RecordJiraFailure
	}))
	defer srv.Close()
	store := &fakeJiraStore{
		job:       &db.JiraProjectJob{JpsID: 9, ProjectKey: "AVRF", BaseURL: srv.URL},
		recordErr: fmt.Errorf("connection reset by peer"),
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 100, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())
	if store.completed || store.failures != 0 {
		t.Fatalf("completed=%v failures=%d — the failed record must not fabricate an outcome", store.completed, store.failures)
	}
	if len(store.released) != 1 {
		t.Fatalf("released = %v, want exactly one claim release", store.released)
	}
}
