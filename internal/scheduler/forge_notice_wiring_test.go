// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestFirstNoticeErr(t *testing.T) {
	notice := &platform.NoticeError{Notice: platform.ForgeNotice{Message: "m"}, Err: platform.ErrForbidden}
	plain := errors.New("plain")
	if got := firstNoticeErr(fmt.Errorf("w: %w", notice), nil); got == nil {
		t.Error("the phase's own error carries the notice: want it")
	}
	if got := firstNoticeErr(plain, &collector.CollectResult{Errors: []error{plain, notice, notice}}); got != notice {
		t.Errorf("got %v, want the first per-endpoint error carrying a notice", got)
	}
	if got := firstNoticeErr(plain, &collector.CollectResult{Errors: []error{plain}}); got != nil {
		t.Errorf("got %v, want nil when nothing carries a notice", got)
	}
	if got := firstNoticeErr(nil, nil); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// TestForgeNoticeIsWiredIntoTheJob pins the three capture sites the
// helpers' runtime tests cannot reach without a forge: the 451 sideline
// fetches the block notice before the skip returns, the API phase's error
// is recorded, and the facade's clone outcome is noted before the
// "no clone" early return (so a refused clone still records its notice).
func TestForgeNoticeIsWiredIntoTheJob(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	run := srctest.FuncBody(t, src, "func (s *Scheduler) runJob(")
	for _, re := range []string{
		`prelim\.Skip \{[^}]*prelim\.Status == http\.StatusUnavailableForLegalReasons && repo\.Platform == model\.PlatformGitHub \{\s*if f, ok := s\.ghClient\.\(repoNoticeFetcher\); ok \{\s*s\.captureBlockNotice\(ctx, repo, f\)`,
		`result, err = s\.collectAndProcess\(ctx, job\.RepoID, repo, client, since\)\s*s\.recordForgeNotice\(ctx, repo, firstNoticeErr\(err, result\)\)`,
	} {
		if !regexp.MustCompile(re).MatchString(run) {
			t.Errorf("runJob lost a notice capture site: %s", re)
		}
	}
	fa := srctest.FuncBody(t, src, "func (s *Scheduler) runFacadeAndAnalysis(")
	if !regexp.MustCompile(`result, err := fc\.CollectRepo\(ctx, repoID, gitURL\)\s*if errors\.Is\(err, context\.Canceled\) \{\s*return nil, nil\s*\}\s*s\.noteCloneOutcome\(ctx, repo, result != nil && result\.CloneOK, err\)`).MatchString(fa) {
		t.Error("runFacadeAndAnalysis must note the clone outcome right after CollectRepo (after the shutdown return)")
	}
}

// TestGoneRecheckCapturesTheBlockNotice pins F1's wiring: the recheck's
// gone arm fetches the notice on a 451, after stamping the check.
func TestGoneRecheckCapturesTheBlockNotice(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/gone_recheck.go"))
	body := srctest.FuncBody(t, src, "func (s *Scheduler) runGoneRecheck(")
	re := regexp.MustCompile(`case platform\.IsRepoGoneStatus\(status\):[^\n]*\n\s*stillGone\+\+\s*stampChecked\(c\)\s*if status == http\.StatusUnavailableForLegalReasons \{\s*if f, ok := s\.ghClient\.\(repoNoticeFetcher\); ok \{\s*noticeStart := time\.Now\(\)\s*s\.recheckBlockNotice\(ctx, c\.RepoID, f\)`)
	if !re.MatchString(body) {
		t.Error("runGoneRecheck's gone arm must fetch the block notice on a 451 (review round 1 F1)")
	}
}

// TestSchedulerRecordsResetAgreementOnlyForTheGitHubPool — review round 1
// F2, at runtime: NewWithKeys opts in the pool its summary drains (GitHub)
// and not the GitLab pool, whose counts nothing would ever drain.
func TestSchedulerRecordsResetAgreementOnlyForTheGitHubPool(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gh := platform.NewKeyPool([]string{"g"}, logger)
	gl := platform.NewKeyPool([]string{"l"}, logger)
	cfg := config.DefaultConfig()
	_ = NewWithKeys(nil, nil, nil, gh, gl, logger, Config{Collection: &cfg.Collection})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "10")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	hit := func(kp *platform.KeyPool) int {
		t.Helper()
		if _, err := platform.NewHTTPClient(srv.URL, kp, logger, platform.AuthGitHub).Get(context.Background(), "/repos/o/r/issues"); err != nil {
			t.Fatal(err)
		}
		return len(kp.DrainResetAgreement())
	}
	if n := hit(gh); n == 0 {
		t.Error("the GitHub pool must record reset agreement (its summary drains it)")
	}
	if n := hit(gl); n != 0 {
		t.Errorf("the GitLab pool recorded %d keys; nothing drains it", n)
	}
}

// runJob and the gone recheck reach FetchRepoNotice through a silent type
// assertion on s.ghClient; if the method's signature drifted, the capture
// would go dead with every wiring pin still green (review round 1). This
// fails the build instead.
var _ repoNoticeFetcher = (*github.Client)(nil)

// TestGoneRecheckFillsTheCommitBounds pins review round 3 F3 / round 4 F2:
// every still-gone arm goes through stampChecked, which fills the stored
// commit bounds (a gone repository gets no facade run, whatever its probe
// answered), and the overrun WARN names the fills as a possible cause.
func TestGoneRecheckFillsTheCommitBounds(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/gone_recheck.go"))
	body := srctest.FuncBody(t, src, "func (s *Scheduler) runGoneRecheck(")
	if !regexp.MustCompile(`stampChecked := func\(c db\.GoneProbeCandidate\) \{[^}]*\}\s*fillStart := time\.Now\(\)\s*s\.fillCommitBounds\(ctx, c\.RepoID\)`).MatchString(body) {
		t.Error("stampChecked must fill the repository's stored commit bounds after stamping the check")
	}
	if !strings.Contains(body, `"fill_elapsed", fillElapsed.Round(time.Second)`) {
		t.Error("the overrun WARN must report the time spent filling commit bounds")
	}
	if strings.Count(body, `"notice_elapsed", noticeElapsed.Round(time.Second)`) != 2 {
		t.Error("the cycle line and the overrun WARN must both report the block-notice time (whole-branch review)")
	}
}
