// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// parseFuncDecl returns the declaration of the single function in the
// source srctest.PackageFunc returned for one signature.
func parseFuncDecl(t *testing.T, fset *token.FileSet, src string) *ast.FuncDecl {
	t.Helper()
	f, err := parser.ParseFile(fset, "src.go", "package collector\n"+src, 0)
	if err != nil {
		t.Fatalf("parse the function source: %v", err)
	}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok {
			return fn
		}
	}
	t.Fatal("the function source declares no function")
	return nil
}

// trafficFakeClient answers the two traffic reads of the collection
// step and counts them. Every other platform.Client method panics
// through the nil embedded interface: the traffic step must call
// nothing else. It returns its data even with an error, so a test can
// show that the step never uses what a failed read returned.
type trafficFakeClient struct {
	platform.Client
	plat model.Platform

	clones     []model.RepoClone
	cloneErr   error
	traffic    *model.RepoTraffic
	trafficErr error

	cloneCalls   int
	trafficCalls int
}

func (f *trafficFakeClient) Platform() model.Platform { return f.plat }

func (f *trafficFakeClient) FetchCloneStats(context.Context, string, string) ([]model.RepoClone, error) {
	f.cloneCalls++
	return f.clones, f.cloneErr
}

func (f *trafficFakeClient) FetchTraffic(context.Context, string, string) (*model.RepoTraffic, error) {
	f.trafficCalls++
	return f.traffic, f.trafficErr
}

// stagedTrafficEntity is one row the traffic step handed to the
// staging writer, with its payload as the JSON the staging table
// stores.
type stagedTrafficEntity struct {
	entityType string
	payload    json.RawMessage
}

// recordingStager keeps what the traffic step stages, encoded as
// db.StagingWriter.Stage encodes it. failTraffic, when set, is returned
// for the traffic row, as a staging flush that fails would be.
type recordingStager struct {
	t           *testing.T
	staged      []stagedTrafficEntity
	failTraffic error
}

func (r *recordingStager) Stage(_ context.Context, entityType string, payload any) error {
	if entityType == EntityTraffic && r.failTraffic != nil {
		return r.failTraffic
	}
	data, err := json.Marshal(payload)
	if err != nil {
		r.t.Fatalf("staged %s payload does not marshal: %v", entityType, err)
	}
	r.staged = append(r.staged, stagedTrafficEntity{entityType: entityType, payload: data})
	return nil
}

func (r *recordingStager) count(entityType string) int {
	n := 0
	for _, s := range r.staged {
		if s.entityType == entityType {
			n++
		}
	}
	return n
}

// trafficPayload decodes the one staged traffic row the way the
// processor reads it back.
func (r *recordingStager) trafficPayload(t *testing.T) model.RepoTraffic {
	t.Helper()
	var found []model.RepoTraffic
	for _, s := range r.staged {
		if s.entityType != EntityTraffic {
			continue
		}
		var tr model.RepoTraffic
		if err := json.Unmarshal(s.payload, &tr); err != nil {
			t.Fatalf("staged traffic payload does not decode as model.RepoTraffic: %v", err)
		}
		found = append(found, tr)
	}
	if len(found) != 1 {
		t.Fatalf("staged traffic rows = %d, want exactly 1", len(found))
	}
	return found[0]
}

// The errors below have the shapes the GitHub client returns: the HTTP
// client wraps a 403 that is not a rate limit as ErrForbidden and a
// 5xx that outlived its retries as ErrTransient, and FetchTraffic puts
// "traffic <endpoint>: " in front of the failing read's error.
var (
	errTrafficForbiddenClones = fmt.Errorf("%w: https://api.github.com/repos/o/r/traffic/clones (not a rate limit — may be a private repo or insufficient scope)", platform.ErrForbidden)
	errTrafficTransientClones = fmt.Errorf("exhausted 10 retries for https://api.github.com/repos/o/r/traffic/clones: %w", platform.ErrTransient)
	errTrafficForbiddenViews  = fmt.Errorf("traffic views?per=day: %w: https://api.github.com/repos/o/r/traffic/views?per=day (not a rate limit — may be a private repo or insufficient scope)", platform.ErrForbidden)
	errTrafficServerErrorWeek = fmt.Errorf("traffic views?per=week: exhausted 10 retries for https://api.github.com/repos/o/r/traffic/views?per=week: %w", platform.ErrTransient)
)

// trafficRead returns a traffic read with 14 daily views from first,
// weekly views and clones on the Mondays given, two paths and one
// referrer, as the GitHub client fills it (no snapshot date, no
// windows, no weeks).
func trafficRead(first time.Time, mondays []time.Time) *model.RepoTraffic {
	return &model.RepoTraffic{
		Views:        trafficViewDays(first, 14),
		ViewUniques:  21,
		CloneUniques: 6,
		WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, mondays, []int{11, 13}),
		WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, mondays, []int{4, 5}),
		Paths: []model.RepoTrafficPath{
			{Path: "/o/r", Title: "o/r", TotalViews: 40, UniqueViews: 12},
			{Path: "/o/r/issues", Title: "Issues", TotalViews: 9, UniqueViews: 4},
		},
		Referrers: []model.RepoTrafficReferrer{{Referrer: "github.com", TotalViews: 30, UniqueViews: 10}},
	}
}

func runCollectTraffic(ctx context.Context, t *testing.T, client *trafficFakeClient, now time.Time) (*CollectResult, *recordingStager, string) {
	t.Helper()
	return runCollectTrafficWith(ctx, t, client, now, &recordingStager{t: t})
}

func runCollectTrafficWith(ctx context.Context, t *testing.T, client *trafficFakeClient, now time.Time, sw *recordingStager) (*CollectResult, *recordingStager, string) {
	t.Helper()
	logger, logs := bufLogger()
	sc := &StagedCollector{client: client, logger: logger}
	result := &CollectResult{}
	sc.collectTraffic(ctx, sw, "o", "r", now, result)
	return result, sw, logs.String()
}

// failureLines returns the log lines at WARN or ERROR.
func failureLines(logs string) []string {
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") {
			out = append(out, line)
		}
	}
	return out
}

// skipLines returns the log lines of the traffic skip message.
func skipLines(logs string) []string {
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `msg="skipping traffic endpoints"`) {
			out = append(out, line)
		}
	}
	return out
}

// A key that can read traffic: the daily clones are staged as before,
// then one traffic row carrying the UTC collection date and the window
// and complete-week rows derived from the daily buckets. The collection
// runs at 22:30 in UTC-5 on 2026-10-19, which is already 2026-10-20 in
// UTC: the snapshot is keyed by the UTC date.
func TestCollectTrafficStagesClonesAndOneTrafficRow(t *testing.T) {
	clones := trafficCloneDays(trafficDay(5), 14)
	read := trafficRead(trafficDay(5), []time.Time{trafficDay(5), trafficDay(12)})
	client := &trafficFakeClient{plat: model.PlatformGitHub, clones: clones, traffic: read}
	now := time.Date(2026, time.October, 19, 22, 30, 0, 0, time.FixedZone("UTC-5", -5*60*60))

	result, sw, logs := runCollectTraffic(context.Background(), t, client, now)

	if len(result.Errors) != 0 {
		t.Errorf("result.Errors = %v, want none", result.Errors)
	}
	if got := sw.count(EntityCloneStats); got != 14 {
		t.Errorf("staged clone stats rows = %d, want 14 (one per daily bucket)", got)
	}
	if len(sw.staged) == 0 {
		t.Fatal("nothing staged, want the daily clones and one traffic row")
	}
	if last := sw.staged[len(sw.staged)-1].entityType; last != EntityTraffic {
		t.Errorf("last staged row is %q, want the traffic row after the daily clones", last)
	}
	got := sw.trafficPayload(t)
	if want := time.Date(2026, time.October, 20, 0, 0, 0, 0, time.UTC); !got.SnapshotDate.Equal(want) || got.SnapshotDate.Location() != time.UTC {
		t.Errorf("SnapshotDate = %v, want %v (the UTC date of the collection, at midnight UTC)", got.SnapshotDate, want)
	}
	wantWindows, wantWeeks := deriveTrafficRows(read, clones)
	if len(got.Windows) != 2 || len(got.Weeks) != 4 {
		t.Fatalf("staged %d windows and %d weeks, want 2 and 4 (one window per kind, two complete weeks per kind)", len(got.Windows), len(got.Weeks))
	}
	for i := range wantWindows {
		if !got.Windows[i].Start.Equal(wantWindows[i].Start) || !got.Windows[i].End.Equal(wantWindows[i].End) ||
			got.Windows[i].Kind != wantWindows[i].Kind || got.Windows[i].Uniques != wantWindows[i].Uniques {
			t.Errorf("window %d = %+v, want %+v", i, got.Windows[i], wantWindows[i])
		}
	}
	for i := range wantWeeks {
		if !got.Weeks[i].WeekStart.Equal(wantWeeks[i].WeekStart) || got.Weeks[i].Kind != wantWeeks[i].Kind || got.Weeks[i].Uniques != wantWeeks[i].Uniques {
			t.Errorf("week %d = %+v, want %+v", i, got.Weeks[i], wantWeeks[i])
		}
	}
	if len(got.Views) != 14 || len(got.Paths) != 2 || len(got.Referrers) != 1 {
		t.Errorf("staged %d views, %d paths, %d referrers, want the read's 14, 2 and 1", len(got.Views), len(got.Paths), len(got.Referrers))
	}
	if lines := append(skipLines(logs), failureLines(logs)...); len(lines) != 0 {
		t.Errorf("a successful read logged skip or failure lines:\n%s", strings.Join(lines, "\n"))
	}
}

// A 403 on the daily clones read means the key cannot read traffic: one
// Info line naming the repository and the reason, no further traffic
// request, nothing staged and nothing recorded against the collection.
func TestCollectTrafficForbiddenClonesSkipsCleanly(t *testing.T) {
	client := &trafficFakeClient{plat: model.PlatformGitHub, cloneErr: errTrafficForbiddenClones,
		traffic: trafficRead(trafficDay(5), nil)}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, trafficDay(19))

	if client.trafficCalls != 0 {
		t.Errorf("FetchTraffic called %d times after a 403 on the daily clones, want 0", client.trafficCalls)
	}
	if len(sw.staged) != 0 {
		t.Errorf("staged %d rows after a 403, want none", len(sw.staged))
	}
	if len(result.Errors) != 0 {
		t.Errorf("result.Errors = %v, want none: a 403 on traffic is a routine skip", result.Errors)
	}
	lines := skipLines(logs)
	if len(lines) != 1 {
		t.Fatalf("skip lines = %d, want exactly 1:\n%s", len(lines), logs)
	}
	for _, want := range []string{"level=INFO", "owner=o", "repo=r", "reason=", "forbidden"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("skip line %q lacks %q", lines[0], want)
		}
	}
	if f := failureLines(logs); len(f) != 0 {
		t.Errorf("a 403 logged failure lines:\n%s", strings.Join(f, "\n"))
	}
}

// A 403 on the first FetchTraffic read after the daily clones succeeded
// is the same routine skip: the clones are staged, the skip is logged
// once, and nothing is recorded.
func TestCollectTrafficForbiddenTrafficSkipsCleanly(t *testing.T) {
	client := &trafficFakeClient{plat: model.PlatformGitHub, clones: trafficCloneDays(trafficDay(5), 14),
		trafficErr: errTrafficForbiddenViews}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, trafficDay(19))

	if len(result.Errors) != 0 {
		t.Errorf("result.Errors = %v, want none: a 403 on traffic is a routine skip", result.Errors)
	}
	if got := sw.count(EntityCloneStats); got != 14 {
		t.Errorf("staged clone stats rows = %d, want 14", got)
	}
	if got := sw.count(EntityTraffic); got != 0 {
		t.Errorf("staged traffic rows = %d, want 0", got)
	}
	if lines := skipLines(logs); len(lines) != 1 || !strings.Contains(lines[0], "level=INFO") {
		t.Errorf("want exactly one INFO skip line, got:\n%s", logs)
	}
	if f := failureLines(logs); len(f) != 0 {
		t.Errorf("a 403 logged failure lines:\n%s", strings.Join(f, "\n"))
	}
}

// A daily clones read that fails with anything but a skip is recorded
// as a collection error (it used to be dropped), and the rest of the
// traffic is still read and staged. Without daily clones the staged
// read has the views window and no clones window.
func TestCollectTrafficClonesErrorIsRecordedAndTrafficStillRead(t *testing.T) {
	client := &trafficFakeClient{plat: model.PlatformGitHub,
		clones: trafficCloneDays(trafficDay(5), 3), cloneErr: errTrafficTransientClones,
		traffic: trafficRead(trafficDay(5), []time.Time{trafficDay(5), trafficDay(12)})}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, trafficDay(19))

	if len(result.Errors) != 1 {
		t.Fatalf("result.Errors = %v, want exactly the clone stats error", result.Errors)
	}
	if err := result.Errors[0]; !errors.Is(err, platform.ErrTransient) || !strings.HasPrefix(err.Error(), "clone stats: ") {
		t.Errorf("recorded %q, want it prefixed \"clone stats: \" and wrapping the cause", err)
	}
	if client.trafficCalls != 1 {
		t.Errorf("FetchTraffic called %d times, want 1: only a skip stops the traffic read", client.trafficCalls)
	}
	if got := sw.count(EntityCloneStats); got != 0 {
		t.Errorf("staged clone stats rows = %d, want 0: a failed read's data is not used", got)
	}
	got := sw.trafficPayload(t)
	if len(got.Windows) != 1 || got.Windows[0].Kind != model.TrafficKindViews {
		t.Errorf("windows = %+v, want only the views window: no daily clones, no clones window", got.Windows)
	}
	if lines := skipLines(logs); len(lines) != 0 {
		t.Errorf("a non-403 error logged a skip line:\n%s", strings.Join(lines, "\n"))
	}
}

// A server error on a FetchTraffic read is recorded once, with the
// endpoint the client named and the cause, not wrapped a second time;
// the daily clones read before it are still staged.
func TestCollectTrafficServerErrorNamesTheEndpoint(t *testing.T) {
	client := &trafficFakeClient{plat: model.PlatformGitHub, clones: trafficCloneDays(trafficDay(5), 14),
		traffic: trafficRead(trafficDay(5), nil), trafficErr: errTrafficServerErrorWeek}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, trafficDay(19))

	if len(result.Errors) != 1 {
		t.Fatalf("result.Errors = %v, want exactly the traffic error", result.Errors)
	}
	err := result.Errors[0]
	if !errors.Is(err, platform.ErrTransient) {
		t.Errorf("recorded %q does not wrap the cause", err)
	}
	if !strings.HasPrefix(err.Error(), "traffic views?per=week: ") || strings.Count(err.Error(), "traffic ") != 1 {
		t.Errorf("recorded %q, want it to start with the client's \"traffic views?per=week: \" exactly once", err)
	}
	if got := sw.count(EntityCloneStats); got != 14 {
		t.Errorf("staged clone stats rows = %d, want 14", got)
	}
	if got := sw.count(EntityTraffic); got != 0 {
		t.Errorf("staged traffic rows = %d, want 0 after a failed read", got)
	}
	if lines := skipLines(logs); len(lines) != 0 {
		t.Errorf("a 5xx logged a skip line:\n%s", strings.Join(lines, "\n"))
	}
}

// A shutdown during the traffic read is not a failure: nothing is
// recorded and no failure line is logged, whether the error carries
// context.Canceled or only the collection's context is done (the pool
// closed under the call reports neither).
func TestCollectTrafficShutdownIsNotRecorded(t *testing.T) {
	canceled := fmt.Errorf("GET https://api.github.com/repos/o/r/traffic/clones: %w", context.Canceled)
	cases := []struct {
		name       string
		ctx        context.Context
		cloneErr   error
		trafficErr error
	}{
		{"error carries context.Canceled", context.Background(), canceled,
			fmt.Errorf("traffic views?per=day: %w", context.Canceled)},
		{"context done, error without the cancellation", doneUnobservedCtx{context.Background()},
			errTrafficTransientClones, errTrafficServerErrorWeek},
		{"both", doneUnobservedCtx{context.Background()}, canceled,
			fmt.Errorf("traffic views?per=day: %w", context.Canceled)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &trafficFakeClient{plat: model.PlatformGitHub, cloneErr: tc.cloneErr, trafficErr: tc.trafficErr}

			result, sw, logs := runCollectTraffic(tc.ctx, t, client, trafficDay(19))

			if len(result.Errors) != 0 {
				t.Errorf("result.Errors = %v, want none under a shutdown", result.Errors)
			}
			if len(sw.staged) != 0 {
				t.Errorf("staged %d rows, want none", len(sw.staged))
			}
			if f := failureLines(logs); len(f) != 0 {
				t.Errorf("a shutdown logged failure lines:\n%s", strings.Join(f, "\n"))
			}
		})
	}
}

// A traffic row the staging writer refuses is recorded, except when the
// refusal is the shutdown's.
func TestCollectTrafficStagingFailure(t *testing.T) {
	flushErr := errors.New("flushing staging batch (500 rows): conn reset")
	cases := []struct {
		name  string
		ctx   context.Context
		err   error
		wantN int
	}{
		{"live collection", context.Background(), flushErr, 1},
		{"shutdown", doneUnobservedCtx{context.Background()}, flushErr, 0},
		{"cancelled flush", context.Background(), fmt.Errorf("flushing staging batch (500 rows): %w", context.Canceled), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &trafficFakeClient{plat: model.PlatformGitHub, clones: trafficCloneDays(trafficDay(5), 14),
				traffic: trafficRead(trafficDay(5), nil)}
			result, _, logs := runCollectTrafficWith(tc.ctx, t, client, trafficDay(19), &recordingStager{t: t, failTraffic: tc.err})
			if len(result.Errors) != tc.wantN {
				t.Fatalf("result.Errors = %v, want %d entries", result.Errors, tc.wantN)
			}
			if tc.wantN == 1 && (!errors.Is(result.Errors[0], flushErr) || !strings.HasPrefix(result.Errors[0].Error(), "stage traffic: ")) {
				t.Errorf("recorded %q, want it prefixed \"stage traffic: \" and wrapping the cause", result.Errors[0])
			}
			if f := failureLines(logs); len(f) != 0 {
				t.Errorf("staging failure logged failure lines:\n%s", strings.Join(f, "\n"))
			}
		})
	}
}

// GitLab has no traffic API: both reads return nothing without an
// error, and the step stages, logs and records nothing.
func TestCollectTrafficGitLabNilTrafficIsSilent(t *testing.T) {
	client := &trafficFakeClient{plat: model.PlatformGitLab}

	result, sw, logs := runCollectTraffic(context.Background(), t, client, trafficDay(19))

	if len(sw.staged) != 0 {
		t.Errorf("staged %d rows for GitLab, want none", len(sw.staged))
	}
	if len(result.Errors) != 0 {
		t.Errorf("result.Errors = %v, want none", result.Errors)
	}
	if logs != "" {
		t.Errorf("GitLab traffic logged:\n%s", logs)
	}
}

// TestTrafficStepWiring pins how the traffic step sits in the staged
// pipeline, from the parsed source of staged.go:
//   - CollectRepo runs collectTraffic as a plain statement of its body,
//     before the contributors phase, and collectTraffic returns nothing,
//     so no traffic outcome can stop the later phases;
//   - CollectRepo no longer reads FetchCloneStats itself;
//   - the metadata step's immediate processing list and ProcessRepo's
//     entity order name EntityTraffic right after EntityCloneStats;
//   - processOne has a case for EntityTraffic.
func TestTrafficStepWiring(t *testing.T) {
	fset := token.NewFileSet()
	// The subject is found through the package, not by naming staged.go:
	// a split moves a function to a new file without breaking the pin.
	funcs := map[string]*ast.FuncDecl{}
	for name, sig := range map[string]string{
		"CollectRepo":    "func (sc *StagedCollector) CollectRepo(",
		"collectTraffic": "func (sc *StagedCollector) collectTraffic(",
		"ProcessRepo":    "func (p *Processor) ProcessRepo(",
		"processOne":     "func (p *Processor) processOne(",
	} {
		funcs[name] = parseFuncDecl(t, fset, srctest.PackageFunc(t, "internal/collector", sig))
	}

	if res := funcs["collectTraffic"].Type.Results; res != nil && len(res.List) > 0 {
		t.Error("collectTraffic must return nothing: its errors go to result.Errors and the collection goes on")
	}

	// CollectRepo: the call is a top-level statement, before the
	// contributors phase, and the metadata list orders EntityTraffic
	// after EntityCloneStats.
	collect := funcs["CollectRepo"].Body.List
	callAt, contribAt, listsChecked := -1, -1, 0
	for i, stmt := range collect {
		if es, ok := stmt.(*ast.ExprStmt); ok && isMethodCall(es.X, "collectTraffic") {
			callAt = i
		}
		if contribAt < 0 && containsMethodCall(stmt, "ListContributors") {
			contribAt = i
		}
		if rs, ok := stmt.(*ast.RangeStmt); ok {
			if lit, ok := rs.X.(*ast.CompositeLit); ok && hasIdent(lit.Elts, "EntityRepoInfo") {
				checkTrafficAfterClones(t, "CollectRepo's metadata processing list", lit.Elts)
				listsChecked++
			}
		}
	}
	if callAt < 0 {
		t.Error("CollectRepo must call sc.collectTraffic(...) as a plain statement of its body")
	}
	if contribAt < 0 || callAt > contribAt {
		t.Errorf("collectTraffic (statement %d) must run before the contributors phase (statement %d)", callAt, contribAt)
	}
	if containsMethodCall(funcs["CollectRepo"].Body, "FetchCloneStats") {
		t.Error("CollectRepo must read the daily clones only through collectTraffic")
	}

	// ProcessRepo's entity order.
	ast.Inspect(funcs["ProcessRepo"].Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name == "entityTypes" {
			if lit, ok := as.Rhs[0].(*ast.CompositeLit); ok {
				checkTrafficAfterClones(t, "ProcessRepo's entity order", lit.Elts)
				listsChecked++
			}
		}
		return true
	})
	if listsChecked != 2 {
		t.Errorf("found %d entity lists, want 2 (the metadata step's and ProcessRepo's)", listsChecked)
	}

	// processOne dispatches EntityTraffic.
	dispatched := false
	ast.Inspect(funcs["processOne"].Body, func(n ast.Node) bool {
		if cc, ok := n.(*ast.CaseClause); ok && hasIdent(cc.List, "EntityTraffic") {
			dispatched = true
		}
		return true
	})
	if !dispatched {
		t.Error("processOne must have a case for EntityTraffic")
	}
}

func checkTrafficAfterClones(t *testing.T, where string, elts []ast.Expr) {
	t.Helper()
	clones, traffic := -1, -1
	for i, e := range elts {
		if id, ok := e.(*ast.Ident); ok {
			switch id.Name {
			case "EntityCloneStats":
				clones = i
			case "EntityTraffic":
				traffic = i
			}
		}
	}
	if clones < 0 || traffic != clones+1 {
		t.Errorf("%s: EntityTraffic at %d, EntityCloneStats at %d; want EntityTraffic right after EntityCloneStats", where, traffic, clones)
	}
}

func hasIdent(exprs []ast.Expr, name string) bool {
	for _, e := range exprs {
		if id, ok := e.(*ast.Ident); ok && id.Name == name {
			return true
		}
	}
	return false
}

func isMethodCall(e ast.Expr, method string) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == method
}

func containsMethodCall(n ast.Node, method string) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if e, ok := n.(ast.Expr); ok && isMethodCall(e, method) {
			found = true
		}
		return !found
	})
	return found
}
