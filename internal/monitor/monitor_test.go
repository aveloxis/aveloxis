// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package monitor

// The monitor.Server takes a *db.PostgresStore directly (not an interface),
// so its HTTP handlers cannot be tested without a live PostgreSQL connection.
//
// Integration tests require setting AVELOXIS_TEST_DB to a valid connection
// string. Unit-testable helpers can be added here as the package evolves.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The :5555 monitor's `POST /api/repos` (v0.29.98, P-4 / WEB-17): it never
// added anything — it answered with the CLI command to run — and nothing
// called it; the route and its handler are gone. The monitor is loopback
// diagnostics and takes no writes.
func TestMonitorHasNoAddRepoRoute(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), `"POST /api/repos"`) || strings.Contains(string(src), "handleAddRepo") {
		t.Error("the monitor registers POST /api/repos again; repositories are added through the CLI, the web GUI or the API")
	}
}

func TestHandlePrioritizeInvalidID(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/prioritize/{repoID}", s.handlePrioritize)

	req := httptest.NewRequest("POST", "/api/prioritize/abc", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

// TestMonitorWritesOnlyThroughPrioritize: the :5555 monitor is loopback
// diagnostics; its one state-changing route is the boost. A second POST
// route (the deleted /api/repos under any name) fails here.
func TestMonitorWritesOnlyThroughPrioritize(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/monitor", "func newServer("))
	posts := regexp.MustCompile(`HandleFunc\("(POST [^"]*)"`).FindAllStringSubmatch(body, -1)
	if len(posts) != 1 || posts[0][1] != "POST /api/prioritize/{repoID}" {
		t.Errorf("the monitor registers POST routes %v; want exactly POST /api/prioritize/{repoID}", posts)
	}
}

// fakeMonitorStore answers the dashboard with an empty fleet so the meta
// refresh line can be read end to end (SR-10, stage review B5).
type fakeMonitorStore struct{}

func (fakeMonitorStore) QueueStats(context.Context) (map[string]int, error) {
	return map[string]int{"total": 0}, nil
}
func (fakeMonitorStore) ListQueuePage(context.Context, int, int, string, string, string) ([]db.QueueJob, int, error) {
	return nil, 0, nil
}
func (fakeMonitorStore) GetReposBatch(context.Context, []int64) (map[int64]*model.Repo, error) {
	return map[int64]*model.Repo{}, nil
}
func (fakeMonitorStore) GetRepoStatsBatch(context.Context, []int64) (map[int64]*db.RepoStats, error) {
	return map[int64]*db.RepoStats{}, nil
}
func (fakeMonitorStore) PrioritizeRepo(context.Context, int64) error { return nil }

// TestDashboardRefreshFollowsTheConfigEndToEnd (SR-10): monitor.refresh_seconds
// in aveloxis.json → config.MonitorConfig.MonitorRefreshSecondsOrDefault (the
// range clamp, 10–3600, what main.go feeds Options) → Options.RefreshSeconds →
// the meta-refresh the page carries. newServer only defaults a zero Option
// (v0.29.98 removed a third clamp in the handler).
func TestDashboardRefreshFollowsTheConfigEndToEnd(t *testing.T) {
	for _, tc := range []struct{ cfg, want int }{{0, DefaultDashboardRefreshSeconds}, {5, DefaultDashboardRefreshSeconds}, {30, 30}, {3600, 3600}, {3601, DefaultDashboardRefreshSeconds}, {-5, DefaultDashboardRefreshSeconds}} {
		opt := config.MonitorConfig{RefreshSeconds: tc.cfg}.MonitorRefreshSecondsOrDefault()
		s := newServer(fakeMonitorStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{RefreshSeconds: opt})
		w := httptest.NewRecorder()
		s.mux.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("refresh_seconds=%d: dashboard %d", tc.cfg, w.Code)
		}
		if want := fmt.Sprintf(`<meta http-equiv="refresh" content="%d">`, tc.want); !strings.Contains(w.Body.String(), want) {
			t.Errorf("refresh_seconds=%d: the page does not carry %s", tc.cfg, want)
		}
	}
}
