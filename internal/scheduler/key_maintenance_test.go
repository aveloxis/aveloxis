// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type fakeKeyMaintainer struct {
	reloadErr, reportErr error
	reloads, reports     atomic.Int32
}

func (f *fakeKeyMaintainer) Reload(context.Context) error { f.reloads.Add(1); return f.reloadErr }
func (f *fakeKeyMaintainer) Report(context.Context) error { f.reports.Add(1); return f.reportErr }

// v0.30.0 Phase C: one reload + report per tick. A failed reload is logged
// and the report is still saved (the pools it describes are intact); a
// failed report is a WARN; shutdown is neither.
func TestMaintainKeysOnce(t *testing.T) {
	cases := []struct {
		name                 string
		reloadErr, reportErr error
		wantReports          int32
		wantLog, notLog      string
	}{
		{"healthy", nil, nil, 1, "", "level=ERROR"},
		{"reload failed", errors.New("db down"), nil, 1, "API key reload failed", ""},
		{"report failed", nil, errors.New("db down"), 1, "API key report not saved", "level=ERROR"},
		{"shutdown during reload", context.Canceled, nil, 0, "", "API key"},
		{"shutdown during report", nil, context.Canceled, 1, "", "API key"},
	}
	for _, tc := range cases {
		logs := &bytes.Buffer{}
		f := &fakeKeyMaintainer{reloadErr: tc.reloadErr, reportErr: tc.reportErr}
		s := &Scheduler{logger: slog.New(slog.NewTextHandler(logs, nil)), keyMaint: f}
		s.maintainKeysOnce(context.Background())
		if f.reloads.Load() != 1 || f.reports.Load() != tc.wantReports {
			t.Errorf("%s: reloads=%d reports=%d, want 1 and %d", tc.name, f.reloads.Load(), f.reports.Load(), tc.wantReports)
		}
		if tc.wantLog != "" && !strings.Contains(logs.String(), tc.wantLog) {
			t.Errorf("%s: log lacks %q:\n%s", tc.name, tc.wantLog, logs)
		}
		if tc.notLog != "" && strings.Contains(logs.String(), tc.notLog) {
			t.Errorf("%s: log must not contain %q:\n%s", tc.name, tc.notLog, logs)
		}
	}
}

// The loop runs a tick at once (a freshly started serve reports within
// seconds), skips ticks while the database is marked unavailable, and
// returns on cancellation.
func TestRunKeyMaintenanceStartsAtOnceAndStops(t *testing.T) {
	f := &fakeKeyMaintainer{}
	s := &Scheduler{logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), keyMaint: f}
	s.dbHealthy.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.runKeyMaintenance(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for f.reports.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.reloads.Load() != 1 || f.reports.Load() != 1 {
		t.Fatalf("first tick: reloads=%d reports=%d, want an immediate reload and report", f.reloads.Load(), f.reports.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runKeyMaintenance did not return after cancellation")
	}

	g := &fakeKeyMaintainer{}
	down := &Scheduler{logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), keyMaint: g}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { down.runKeyMaintenance(ctx2); close(done2) }()
	time.Sleep(50 * time.Millisecond)
	cancel2()
	<-done2
	if g.reloads.Load() != 0 {
		t.Errorf("with the database marked unavailable the loop reloaded %d time(s), want 0", g.reloads.Load())
	}
}

// Wiring: serve injects the maintainer built from its own forge clients
// BEFORE the scheduler runs, and Run starts the loop as a tracked goroutine
// (so shutdown waits for an in-flight report before closing the store).
func TestServeWiresKeyMaintainer(t *testing.T) {
	body := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	set := strings.Index(body, "sched.SetKeyMaintainer(clients.keyMaintainer(cfg, store, logger))")
	run := strings.Index(body, "sched.Run(ctx)")
	if set < 0 || run < 0 || set > run {
		t.Errorf("serve must call sched.SetKeyMaintainer(clients.keyMaintainer(cfg, store, logger)) before sched.Run (set at %d, run at %d)", set, run)
	}
	runBody := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) Run("))
	if !strings.Contains(runBody, `s.goTracked("key-maintenance", func() { s.runKeyMaintenance(ctx) })`) {
		t.Error("Run must start runKeyMaintenance through goTracked")
	}
	web := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) scanOrgRepos("))
	reload, client := strings.Index(web, "s.reloadKeys(ctx)"), strings.Index(web, "platform.NewHTTPClient(")
	if reload < 0 || client < 0 || reload > client {
		t.Error("web's scanOrgRepos must reload its GitHub keys before building the API client")
	}
	// Which GitHub pool each process's maintainer reconciles: serve's own
	// clients.ghKeys, and web's own org-scan pool (the Reconcile-callers
	// tripwire allowlists the pairing code; this pins the pools passed).
	if !srctest.ContainsNormalized(body, "WithKeyReload(forgekeys.NewMaintainer(forgekeys.MaintainerConfig{ GitHubConfigTokens: cfg.GitHub.APIKeys, GitHub: ghKeys, Loader: keyLoader, Logger: logger, }).Reload)") {
		t.Error("the web command must wire a GitHub-only forgekeys.Maintainer over its own ghKeys into the web server")
	}
	fc := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/forge_clients.go"), "func (c *forgeClients) keyMaintainer("))
	if !strings.Contains(fc, "GitHub:             c.ghKeys,") || !strings.Contains(fc, "GitLab:             c.gl,") {
		t.Error("serve's keyMaintainer must reconcile the GitHub pool and GitLab router its forge clients were built with")
	}
}

// v0.30.0 Phase C made an empty GitHub pool reachable at runtime (every
// stored key removed on the API keys page). The GitHub sweeps must then
// skip, not run (review of Phase C, findings 3 and 4):
//   - enrichment must not switch to the GitLab client — thin logins carry
//     no platform, so GitHub logins would be looked up on gitlab.com and
//     same-name profiles written onto GitHub contributors (SR-6);
//   - breadth and search resolve mark every claimed contributor attempted
//     on any error, burning a multi-day cooldown without one API call.
//
// Each Scheduler below has a nil store: reaching the sweep body panics,
// so returning cleanly proves the entry gate.
func TestGitHubSweepsSkipAnEmptyGitHubPool(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	empty := platform.NewKeyPool(nil, logger)
	glPool := platform.NewKeyPool([]string{"glpat-main-0000000000"}, logger)
	glClient, err := gitlab.New(model.PlatformGitLab, "https://gitlab.com", "https://gitlab.com/api/v4", glPool, logger)
	if err != nil {
		t.Fatal(err)
	}
	router, err := gitlab.NewInstances([]*gitlab.InstanceSpec{{ID: model.PlatformGitLab, WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Primary: true, Client: glClient}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Scheduler{
		logger:        logger,
		ghClient:      github.New("https://api.github.com", empty, logger),
		ghKeys:        empty,
		gl:            router,
		breadthWorker: collector.NewBreadthWorker(nil, empty, logger),
		cfg:           Config{Collection: &config.CollectionConfig{}},
	}
	for name, run := range map[string]func(context.Context){
		"enrichment":       s.runEnrichment,
		"search resolve":   s.runSearchResolve,
		"breadth":          s.runBreadth,
		"activity history": s.runActivityHistory,
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s ran against an empty GitHub pool (reached the store: %v)", name, r)
				}
			}()
			run(context.Background())
		}()
	}
}

type noKeysHistoryFetcher struct{}

func (noKeysHistoryFetcher) FetchContributorHistoryMeta(context.Context, string) (time.Time, []int, error) {
	return time.Time{}, nil, fmt.Errorf("getting API key: %w", platform.ErrNoKeys)
}

func (noKeysHistoryFetcher) FetchContributorDailyHistory(context.Context, string, []github.HistoryWindow) ([]model.ContributorDayActivity, []model.ContributorDayTotal, error) {
	return nil, nil, fmt.Errorf("getting API key: %w", platform.ErrNoKeys)
}

// A history fetch that failed for want of a key stamps nothing — neither
// the 24-hour failure stamp nor a backfill stamp — and is not counted a
// failure (review of Phase C, pass 2, findings 2 and 3). The Scheduler has
// a nil store: a stamp would panic.
func TestActivityHistoryNoKeysStampsNothing(t *testing.T) {
	s := &Scheduler{logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	got := s.processHistoryContributor(context.Background(), noKeysHistoryFetcher{}, db.ActivityCheckContributor{ID: "c1", Login: "someone"}, 180)
	if got != historyNoKeys {
		t.Fatalf("outcome = %v, want historyNoKeys", got)
	}
	if historyNoKeys.stampsFailure() || !historyFailed.stampsFailure() || historyCanceled.stampsFailure() {
		t.Fatal("only historyFailed stamps the failure cooldown")
	}
}

func orgsBody(t *testing.T) string {
	t.Helper()
	return srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) refreshUserOrgs("))
}

// armBlock returns the body of the if-block that starts at the first
// occurrence of anchor in src (from its opening brace to the matching
// closing brace), or "" when anchor is absent.
func armBlock(src, anchor string) string {
	at := strings.Index(src, anchor)
	if at < 0 {
		return ""
	}
	open := strings.Index(src[at:], "{")
	if open < 0 {
		return ""
	}
	depth := 0
	for i := at + open; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[at+open+1 : i]
			}
		}
	}
	return ""
}

// Sweeps whose per-item error arm stamps an attempt must stop WITHOUT
// stamping when the error is ErrNoKeys — the entry gate alone is
// check-then-act, and a reload can empty the pool mid-cycle (review of
// Phase C, passes 2 and 3). Pinned per site: the ErrNoKeys arm comes before
// the first stamp, and its OWN block ends the item with the terminator
// (a token-only pin stayed green with the terminator deleted — pass 3,
// finding 4). Breadth, activity history and the distribution worker are
// pinned behaviourally in their own tests.
func TestNoKeysArmPrecedesAttemptStamps(t *testing.T) {
	for _, tc := range []struct{ file, fn, arm, terminator, stamp string }{
		{"internal/scheduler/scheduler.go", "func (s *Scheduler) runSearchResolve(", "if errors.Is(err, platform.ErrNoKeys) {", "return", "MarkContributorSearchAttempted"},
		{"internal/scheduler/mailinglist_wiring.go", "func (s *Scheduler) runMailingListSenderResolve(", "if errors.Is(rerr, platform.ErrNoKeys) {", "break candidates", "MarkSenderResolveAttempt(ctx, c.SenderEmail, false"},
		{"internal/collector/enrich.go", "func EnrichThinContributors(", "if errors.Is(err, platform.ErrNoKeys) {", "break", "MarkContributorEnriched"},
		{"internal/scheduler/scheduler.go", "func (s *Scheduler) refreshUserOrgs(", "if errors.Is(listErr, platform.ErrNoKeys) {", "continue", "MarkOrgRequestScanned"},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, tc.file), tc.fn))
		armAt, stampAt := strings.Index(body, tc.arm), strings.Index(body, tc.stamp)
		block := armBlock(body, tc.arm)
		lines := strings.Split(strings.TrimSpace(block), "\n")
		last := strings.TrimSpace(lines[len(lines)-1])
		if armAt < 0 || stampAt < 0 || armAt > stampAt || last != tc.terminator {
			t.Errorf("%s: the ErrNoKeys arm %q must precede the first %s and end with %q (arm at %d, stamp at %d, block ends %q)",
				tc.fn, tc.arm, tc.stamp, tc.terminator, armAt, stampAt, last)
		}
	}
	ml := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/mailinglist_wiring.go"), "func (s *Scheduler) runMailingListSenderResolve("))
	if tick := strings.Index(ml, "case <-t.C:"); tick < 0 || strings.TrimSpace(armBlock(ml[tick:], "if s.gitHubPoolEmpty() {")) != "continue" {
		t.Error("runMailingListSenderResolve must skip (continue) a tick while the GitHub pool is empty")
	}
	probe := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) maybeScanNewOrgs("))
	if strings.TrimSpace(armBlock(probe, "if s.gitHubPoolEmpty() {")) != "return" || strings.Index(probe, "s.gitHubPoolEmpty()") > strings.Index(probe, "HasNeverScannedOrgs") {
		t.Error("maybeScanNewOrgs must return before probing while the GitHub pool is empty — an unstamped no-keys registration would otherwise re-fire the scan every poll tick")
	}
	// The arm is only live if the listing CAPTURES ErrNoKeys: a capture
	// that skipped it would make the arm dead and stamp registrations again
	// (review of Phase C, pass 4, finding 4).
	if !srctest.ContainsNormalized(orgsBody(t), "if !errors.Is(err, platform.ErrNotFound) { listErr = err }") {
		t.Error("refreshUserOrgs must capture every non-not-found listing error (ErrNoKeys included) into listErr")
	}
	// Enrichment stopped on ErrNoKeys must not also log "complete"
	// (pass 4, finding 3); breadth's no-keys abort is INFO, not WARN
	// (finding 5).
	enrich := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/enrich.go"), "func EnrichThinContributors("))
	if !strings.Contains(enrich, "if len(enriched) > 0 && !stoppedNoKeys {") || !strings.Contains(armBlock(enrich, "if errors.Is(err, platform.ErrNoKeys) {"), "stoppedNoKeys = true") {
		t.Error("EnrichThinContributors must skip its \"complete\" summary after stopping on ErrNoKeys")
	}
	breadth := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) runBreadth("))
	nkBlock := strings.Split(strings.TrimSpace(armBlock(breadth, "if errors.Is(err, platform.ErrNoKeys) {")), "\n")
	if nk, warn := strings.Index(breadth, "platform.ErrNoKeys"), strings.Index(breadth, `"breadth worker failed"`); nk < 0 || warn < 0 || nk > warn || strings.TrimSpace(nkBlock[len(nkBlock)-1]) != "return" {
		t.Error("runBreadth must log the no-keys abort before, and instead of (its arm returns), the failure WARN")
	}
	hist := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/activity_history.go"), "func (s *Scheduler) runActivityHistory("))
	noKeysCase, deflt := strings.Index(hist, "case outcome == historyNoKeys:"), strings.Index(hist, "default:")
	if noKeysCase < 0 || deflt < 0 || noKeysCase > deflt || strings.Contains(hist[noKeysCase:deflt], "MarkHistoryFetchFailed") {
		t.Error("runActivityHistory's outcome switch must handle historyNoKeys (no stamp) before the failure-stamping default")
	}
}
