// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Worklist item 83 (2026-10-06, found by the deployment runbook's clarity
// read): a clone or git-log failure on a GitHub/GitLab repository was one
// WARN and a job with no last_error and nothing on the monitor, while
// everything downstream of the clone (commits, contributor resolution,
// dependencies, libyear, licenses, SCC, scorecard, SBOM, vulnerabilities)
// stayed empty. The example configuration's clone directory
// (/data/aveloxis-repos) triggers it on every fresh host. Git-only
// repositories already failed the job here (v0.25.38); API repositories
// recorded nothing.
//
// The shape (after this change's own review): the facade's error is
// RECORDED — last_error and the job-complete line carry it — while the job
// stays a success on an API repository, because success is what anchors
// last_collected, keeps the listing ETags and clears force_full_collect;
// failing the outcome would re-walk the whole API history every cycle for
// as long as the clone kept failing. Guards: a refusal carrying the forge's
// own notice (a blocked, disabled or taken-down repository, item 82) is the
// forge's word, not an error to record; a git-only repository has no other
// collection, so its facade error still fails the job; a default branch
// the facade PROVED empty is no error at all (v0.29.56).

func newOutcomeScheduler() *Scheduler {
	return New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{})
}

func TestBuildOutcome_FacadeErrorIsRecordedWithoutUnanchoringTheAPIPhase(t *testing.T) {
	s := newOutcomeScheduler()
	apiData := &collector.CollectResult{Issues: 12, PullRequests: 3, Contributors: 4}
	cloneErr := errors.New("git clone: mkdir /data/aveloxis-repos: permission denied")

	out := s.buildOutcome(apiData, nil, nil, nil, nil, cloneErr)

	if !strings.HasPrefix(out.errMsg, "facade collection failed") || !strings.Contains(out.errMsg, "permission denied") {
		t.Fatalf("last_error must name the facade and carry the clone error, got %q", out.errMsg)
	}
	if !out.success {
		t.Fatal("the API phase completed: the job stays a success so last_collected anchors, the ETags stay and force_full clears (review HIGH)")
	}
	if out.issues != 12 || out.prs != 3 {
		t.Fatalf("the API counts that did arrive are still reported: issues=%d prs=%d", out.issues, out.prs)
	}
}

func TestBuildOutcome_ForgeRefusalWithNoticeRecordsNothing(t *testing.T) {
	s := newOutcomeScheduler()
	apiData := &collector.CollectResult{Issues: 1}
	refusal := &platform.NoticeError{
		Notice: platform.ForgeNotice{Message: "Repository access blocked", Reason: "dmca"},
		Err:    errors.New("git clone: remote: Repository access blocked"),
	}

	out := s.buildOutcome(apiData, nil, nil, nil, nil, refusal)

	if !out.success || out.errMsg != "" {
		t.Fatalf("a refusal carrying the forge's notice is the forge's word (stored on the repository), not an error to record: success=%v err=%q", out.success, out.errMsg)
	}
}

func TestBuildOutcome_GitOnlyFacadeErrorStillFailsTheJob(t *testing.T) {
	// v0.25.38 kept: a generic-git repository has no API collection, so a
	// facade error is its only signal and fails the job — a refusal whose
	// remote text parsed as a notice included (the notice is only stored
	// for GitHub and GitLab; recordForgeNotice).
	s := newOutcomeScheduler()
	for name, ferr := range map[string]error{
		"plain": errors.New("clone/fetch: exit status 128"),
		"notice": &platform.NoticeError{
			Notice: platform.ForgeNotice{Message: "access denied"},
			Err:    errors.New("git clone: remote: access denied"),
		},
	} {
		out := s.buildOutcome(nil, nil, nil, nil, nil, ferr)
		if out.success || !strings.HasPrefix(out.errMsg, "facade collection failed") {
			t.Fatalf("%s: git-only facade failure must fail the job: success=%v err=%q", name, out.success, out.errMsg)
		}
	}
}

func TestBuildOutcome_ProvenEmptyBranchIsNoFacadeError(t *testing.T) {
	s := newOutcomeScheduler()
	out := s.buildOutcome(&collector.CollectResult{}, &collector.FacadeResult{EmptyDefaultBranch: true}, nil, nil, nil, nil)
	if !out.success || out.errMsg != "" {
		t.Fatalf("a proven-empty default branch stays a success (v0.29.56): success=%v err=%q", out.success, out.errMsg)
	}
}

func TestBuildOutcome_CollectionErrorOutranksFacadeError(t *testing.T) {
	s := newOutcomeScheduler()
	out := s.buildOutcome(nil, nil, nil, errors.New("rate limited"), nil, errors.New("git clone: timeout"))
	if out.success || out.errMsg != "rate limited" {
		t.Fatalf("the collection error is the more informative one to record: success=%v err=%q", out.success, out.errMsg)
	}
}

// The producer, by behaviour: a clone directory that cannot be created (its
// parent is read-only — the fresh-host /data case in miniature; a missing
// clone stats as "not there", so the no-clone guard skips analysis and
// scorecard, v0.29.56) makes runFacadeAndAnalysis return the facade's error
// as its third value. No store is needed on this path: a notice-less error
// records nothing.
func TestRunFacadeAndAnalysis_ReturnsTheFacadeError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	parent := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })
	uncreatable := filepath.Join(parent, "clones")
	// The premise, asserted: if this process can create the directory
	// anyway (CAP_DAC_OVERRIDE, a mount that ignores mode bits) the facade
	// would clone, and the URL below is unroutable so that cannot reach a
	// forge or the nil store — but then the test proves nothing, so skip.
	if err := os.MkdirAll(uncreatable, 0o700); err == nil {
		t.Skip("this process can create directories under a read-only parent")
	}
	s := New(nil, nil, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)), Config{
		Collection: &config.CollectionConfig{RepoCloneDir: uncreatable},
	})
	repo := &model.Repo{ID: 1, Platform: model.PlatformGitHub, Owner: "chaoss", Name: "augur",
		GitURL: "https://forge.invalid/chaoss/augur.git"}

	facadeResult, analysisResult, facadeErr := s.runFacadeAndAnalysis(context.Background(), repo.ID, repo)

	if facadeErr == nil {
		t.Fatal("an unwritable clone directory must come back as the facade's error")
	}
	if facadeResult != nil || analysisResult != nil {
		t.Fatalf("no clone, no results: facade=%v analysis=%v", facadeResult, analysisResult)
	}
}

// assignmentsTo counts the assignment statements (any token, := included)
// in the named method of file whose left-hand side names ident.
func assignmentsTo(t *testing.T, file, method, ident string) int {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != method || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(node ast.Node) bool {
			as, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range as.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == ident {
					n++
				}
			}
			return true
		})
	}
	return n
}

// The consumer, by source (SR-18, house helper: srctest, comments
// stripped): runJob hands the value it received to buildOutcome. A refactor
// that captures the error and drops it on the way cannot pass.
func TestRunJob_HandsTheFacadeErrorToBuildOutcome(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	run := srctest.FuncBody(t, src, "func (s *Scheduler) runJob(")
	re := regexp.MustCompile(`facadeResult, analysisResult, facadeErr := s\.runFacadeAndAnalysis\(ctx, job\.RepoID, repo\)[\s\S]*?outcome := s\.buildOutcome\(result, facadeResult, analysisResult, err, gapFillErr, facadeErr\)`)
	if !re.MatchString(run) {
		t.Fatal("runJob must receive facadeErr from runFacadeAndAnalysis and pass that same value to buildOutcome")
	}
	if strings.Contains(run[strings.Index(run, "facadeErr :="):], "facadeErr = ") {
		t.Fatal("facadeErr must not be reassigned between the facade run and buildOutcome")
	}
}

// Every return path of the producer carries the facade's error (review
// round 2: a `return facadeResult, analysisResult, nil` on the
// clone-present, git-log-failed path — the last return, after analysis and
// scorecard — compiled and survived the no-clone behaviour test). The
// results are named and assigned once; the returns are bare, except the
// shutdown arm, which the forge-notice wiring pin keys on.
func TestRunFacadeAndAnalysis_EveryReturnCarriesTheFacadeError(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	body := srctest.FuncBody(t, src, "func (s *Scheduler) runFacadeAndAnalysis(")
	if !strings.Contains(src, "(facadeResult *collector.FacadeResult, analysisResult *collector.AnalysisResult, facadeErr error) {") {
		t.Fatal("runFacadeAndAnalysis must name its results so bare returns carry facadeErr")
	}
	// Exactly one statement assigns the named result, counted on the AST
	// (review round 3: a text count of "facadeErr = " missed `x, facadeErr
	// := f()` at the body's top level, which the spec lets redeclare a
	// named result, and `facadeErr, x = a, b`).
	if n := assignmentsTo(t, "scheduler.go", "runFacadeAndAnalysis", "facadeErr"); n != 1 {
		t.Fatalf("facadeErr is assigned exactly once (right after CollectRepo), found %d", n)
	}
	// ... and WHERE: the statement after the clone outcome, at the body's
	// top level, before any branch (review round 4: the one assignment moved
	// into the no-clone guard would pass the count and the bare returns and
	// leave the clone-present/git-log-failed path returning nil).
	if !regexp.MustCompile(`s\.noteCloneOutcome\(ctx, repo, result != nil && result\.CloneOK, err\)\s*facadeErr = err\s*if err != nil \{`).MatchString(body) {
		t.Fatal("facadeErr = err must be the top-level statement right after noteCloneOutcome, before the first branch on err")
	}
	returns := regexp.MustCompile(`(?m)^\s*return\b[^\n]*`).FindAllString(body, -1)
	if len(returns) < 4 {
		t.Fatalf("expected the facade function's return paths, found %d", len(returns))
	}
	shutdown := 0
	for _, r := range returns {
		switch strings.TrimSpace(r) {
		case "return":
		case "return nil, nil, nil":
			shutdown++
		default:
			t.Errorf("a return that names values can drop facadeErr: %q", strings.TrimSpace(r))
		}
	}
	if shutdown != 1 {
		t.Errorf("exactly one explicit return (the shutdown arm), found %d", shutdown)
	}
}
