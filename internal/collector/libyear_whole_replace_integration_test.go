// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The libyear phase walks, resolves and then replaces the inventory in one
// transaction (summary/70, 4.7). These tests drive the whole phase against a
// database and a fake npm registry.

type wholeReplaceFixture struct {
	store  *db.PostgresStore
	ac     *AnalysisCollector
	repoID int64
	dir    string
}

// npmAnswer is a registry body for any package: two versions, both dated.
const npmAnswer = `{"dist-tags":{"latest":"2.0.0"},"time":{"2.0.0":"2024-01-01T00:00:00.000Z","1.0.0":"2021-01-01T00:00:00.000Z"},"license":"MIT"}`

func newWholeReplaceFixture(t *testing.T, name string, registry http.HandlerFunc) *wholeReplaceFixture {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	repoName := fmt.Sprintf("%s%d", name, time.Now().UnixNano())
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/_avwholerep/" + repoName,
		Owner: "_avwholerep", Name: repoName,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			"DELETE FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})

	srv := httptest.NewServer(registry)
	t.Cleanup(srv.Close)
	// EVERY registry base points at the fixture's server, not only npm's:
	// a fixture with a pom or a composer.json would otherwise ask the real
	// registry over the network (two tests did until 0.29.179, when the
	// outbound-address rule sent them to a test address that times out).
	bases := []*string{&npmRegistryBase, &pypiRegistryBase, &goProxyBase, &cratesRegistryBase, &rubygemsRegistryBase,
		&mavenRepositoryBase, &packagistRegistryBase, &hexRegistryBase, &nugetRegistryBase, &pubDevRegistryBase, &hackageRegistryBase}
	saved := make([]string, len(bases))
	for i, b := range bases {
		saved[i], *b = *b, srv.URL
	}
	t.Cleanup(func() {
		for i, b := range bases {
			*b = saved[i]
		}
	})

	return &wholeReplaceFixture{store: store, ac: NewAnalysisCollector(store, logger, t.TempDir()), repoID: repoID, dir: t.TempDir()}
}

// manifest writes the clone's package.json with the given dependencies. Names
// are made unique to this test so the process-wide registry cache, which
// other tests share, never answers for them.
func (f *wholeReplaceFixture) manifest(t *testing.T, deps map[string]string) {
	t.Helper()
	var parts []string
	for name, version := range deps {
		parts = append(parts, fmt.Sprintf("%q:%q", name, version))
	}
	sort.Strings(parts)
	body := `{"dependencies":{` + strings.Join(parts, ",") + `}}`
	if err := os.WriteFile(filepath.Join(f.dir, "package.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *wholeReplaceFixture) inventory(t *testing.T) []string {
	t.Helper()
	rows, err := f.store.Pool().Query(context.Background(),
		`SELECT name || '@' || current_version FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 ORDER BY 1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (f *wholeReplaceFixture) historyRows(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.store.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`, f.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func uniq(t *testing.T, name string) string {
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
}

func TestLibyearPhaseReplacesTheInventoryWhole(t *testing.T) {
	f := newWholeReplaceFixture(t, "whole", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	a, b, c := uniq(t, "wr-a"), uniq(t, "wr-b"), uniq(t, "wr-c")

	f.manifest(t, map[string]string{a: "1.0.0", b: "1.0.0"})
	res := &AnalysisResult{}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, res); err != nil {
		t.Fatalf("first analysis: %v", err)
	}
	if got := f.inventory(t); len(got) != 2 || res.LibyearDeps != 2 {
		t.Fatalf("first analysis: inventory %v, counted %d; want 2 rows", got, res.LibyearDeps)
	}

	f.manifest(t, map[string]string{b: "1.0.0", c: "1.0.0"})
	res = &AnalysisResult{}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, res); err != nil {
		t.Fatalf("second analysis: %v", err)
	}
	if got, want := f.inventory(t), []string{b + "@1.0.0", c + "@1.0.0"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("second analysis: inventory %v, want %v", got, want)
	}
	if got := f.historyRows(t); got != 2 {
		t.Errorf("history rows = %d, want the first inventory's 2", got)
	}
}

// TestAnInterruptedLibyearPhaseKeepsThePreviousInventory is the red test for
// the order of work. The analysis is cancelled while it is resolving its
// second dependency. Before 0.29.157 the previous rows had already been
// rotated away and one new row inserted: the table held one row of three, and
// a vulnerability scan reading it would have closed the other two's findings.
func TestAnInterruptedLibyearPhaseKeepsThePreviousInventory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests atomic.Int32
	var interrupt atomic.Bool
	f := newWholeReplaceFixture(t, "interrupt", func(w http.ResponseWriter, _ *http.Request) {
		if interrupt.Load() && requests.Add(1) == 2 {
			cancel() // `stop serve` arrives during the second lookup
			return
		}
		_, _ = io.WriteString(w, npmAnswer)
	})
	a, b, c := uniq(t, "int-a"), uniq(t, "int-b"), uniq(t, "int-c")
	f.manifest(t, map[string]string{a: "1.0.0", b: "1.0.0", c: "1.0.0"})
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatalf("the analysis that builds the previous inventory: %v", err)
	}
	before := f.inventory(t)
	if len(before) != 3 {
		t.Fatalf("previous inventory = %v, want 3 rows", before)
	}

	// The next analysis sees three NEW names (so nothing is answered from
	// the cache) and is interrupted at its second lookup.
	x, y, z := uniq(t, "int-x"), uniq(t, "int-y"), uniq(t, "int-z")
	f.manifest(t, map[string]string{x: "1.0.0", y: "1.0.0", z: "1.0.0"})
	interrupt.Store(true)
	err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{})
	if err == nil {
		t.Fatal("an interrupted analysis must return an error")
	}
	if requests.Load() < 2 {
		t.Fatalf("the registry saw %d requests; the interruption never happened and the test proves nothing", requests.Load())
	}
	if after := f.inventory(t); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Errorf("after an interrupted analysis the inventory is %v; the previous inventory %v must stand whole", after, before)
	}
	if got := f.historyRows(t); got != 0 {
		t.Errorf("an interrupted analysis rotated %d rows to history; it must write nothing", got)
	}
}

// A walk that cannot read the clone's root visited nothing. Replacing the
// inventory with that nothing would close every finding as if its dependency
// were gone (v0.27.36 logged the failure and went on with zero dependencies).
func TestALibyearWalkThatFailsAtTheRootKeepsThePreviousInventory(t *testing.T) {
	f := newWholeReplaceFixture(t, "rootfail", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	a := uniq(t, "root-a")
	f.manifest(t, map[string]string{a: "1.0.0"})
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	before := f.inventory(t)

	gone := filepath.Join(f.dir, "no-such-clone")
	if err := f.ac.scanLibyear(context.Background(), f.repoID, gone, nil, &AnalysisResult{}); err == nil {
		t.Fatal("a walk that fails at the root must fail the phase")
	}
	if after := f.inventory(t); len(before) != 1 || strings.Join(after, ",") != strings.Join(before, ",") {
		t.Errorf("after a failed walk the inventory is %v, want the previous one %v", after, before)
	}
	if got := f.historyRows(t); got != 0 {
		t.Errorf("a failed walk rotated %d rows to history", got)
	}
}

// A clone with no manifest is a whole, empty inventory: the previous rows go
// to history. (This is what tells "declares nothing" from "could not look".)
func TestALibyearWalkThatFindsNothingEmptiesTheInventory(t *testing.T) {
	f := newWholeReplaceFixture(t, "nothing", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	f.manifest(t, map[string]string{uniq(t, "none-a"): "1.0.0"})
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(f.dir, "package.json")); err != nil {
		t.Fatal(err)
	}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatalf("an empty clone is not a failure: %v", err)
	}
	if got := f.inventory(t); len(got) != 0 {
		t.Errorf("inventory = %v, want none", got)
	}
	if got := f.historyRows(t); got != 1 {
		t.Errorf("history rows = %d, want 1", got)
	}
}

// A registry answer can hold a byte PostgreSQL refuses. One such row used to
// lose one row; under a whole replace it would refuse the whole inventory on
// every cycle. Rows are made storable before the transaction instead.
func TestARegistryAnswerWithANulDoesNotBlockTheReplace(t *testing.T) {
	bad := uniq(t, "nul-bad")
	f := newWholeReplaceFixture(t, "nul", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, bad) {
			_, _ = io.WriteString(w, `{"dist-tags":{"latest":"2.0.0\u0000"},"time":{"2.0.0\u0000":"2024-01-01T00:00:00.000Z","1.0.0":"2021-01-01T00:00:00.000Z"},"license":"MIT\u0000 OR Apache-2.0"}`)
			return
		}
		_, _ = io.WriteString(w, npmAnswer)
	})
	good := uniq(t, "nul-good")
	f.manifest(t, map[string]string{bad: "1.0.0", good: "1.0.0"})
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatalf("a NUL in a registry answer must not fail the replace: %v", err)
	}
	if got := f.inventory(t); len(got) != 2 {
		t.Fatalf("inventory = %v, want both rows", got)
	}
	var license, latest string
	if err := f.store.Pool().QueryRow(context.Background(),
		`SELECT license, latest_version FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = $2`,
		f.repoID, bad).Scan(&license, &latest); err != nil {
		t.Fatal(err)
	}
	if license != "MIT OR Apache-2.0" || latest != "2.0.0" {
		t.Errorf("stored licence %q and latest version %q; want them sanitized and kept", license, latest)
	}
}

func (f *wholeReplaceFixture) dependencyNames(t *testing.T) []string {
	t.Helper()
	rows, err := f.store.Pool().Query(context.Background(),
		`SELECT dep_language || '/' || dep_name FROM aveloxis_data.repo_dependencies WHERE repo_id = $1 ORDER BY 1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The name inventory (repo_dependencies) follows the same order of work
// (0.29.158, operator 2026-11-19): walk first, write last, in one
// transaction. A clone whose root cannot be read used to EMPTY the table: it
// was cleared before the walk, and the walk swallowed the root's own error.
func TestAnUnreadableCloneKeepsThePreviousDependencyNames(t *testing.T) {
	f := newWholeReplaceFixture(t, "depnames", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	t.Cleanup(func() {
		_, _ = f.store.Pool().Exec(context.Background(), "DELETE FROM aveloxis_data.repo_dependencies WHERE repo_id = $1", f.repoID)
	})
	f.manifest(t, map[string]string{"left-pad": "1.0.0", "express": "4.0.0"})
	res := &AnalysisResult{}
	if err := f.ac.scanDependencies(context.Background(), f.repoID, f.dir, res); err != nil {
		t.Fatal(err)
	}
	before := f.dependencyNames(t)
	if strings.Join(before, ",") != "JavaScript/express,JavaScript/left-pad" || res.Dependencies != 2 {
		t.Fatalf("first walk: names %v, counted %d", before, res.Dependencies)
	}

	if err := f.ac.scanDependencies(context.Background(), f.repoID, filepath.Join(f.dir, "no-such-clone"), &AnalysisResult{}); err == nil {
		t.Fatal("a walk that fails at the root must fail the step")
	}
	if after := f.dependencyNames(t); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Errorf("after a failed walk the names are %v; the previous ones %v must stand", after, before)
	}

	// A readable clone that now declares one dependency replaces the snapshot.
	f.manifest(t, map[string]string{"express": "4.0.0"})
	if err := f.ac.scanDependencies(context.Background(), f.repoID, f.dir, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if got := f.dependencyNames(t); strings.Join(got, ",") != "JavaScript/express" {
		t.Errorf("after the next walk: %v, want only express", got)
	}
}

func (f *wholeReplaceFixture) coverage(t *testing.T) (model.DependencyCoverage, bool) {
	t.Helper()
	cov, found, err := f.store.GetRepoDependencyCoverage(context.Background(), f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	return cov, found
}

// The analysis writes the coverage record's analysis half with the inventory
// it describes (0.29.159).
func TestLibyearPhaseWritesTheCoverageHalf(t *testing.T) {
	missing := uniq(t, "cov-missing")
	f := newWholeReplaceFixture(t, "coverage", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, missing) {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, npmAnswer)
	})
	if _, found := f.coverage(t); found {
		t.Fatal("a repository never analysed must have no coverage record")
	}

	// Two readable manifests (one of them declaring a dependency the
	// registry does not have and one from a local path), a third that is not
	// JSON, and a file that is not a manifest at all.
	a, b := uniq(t, "cov-a"), uniq(t, "cov-b")
	f.manifest(t, map[string]string{a: "1.0.0", missing: "1.0.0", "local-tool": "file:../tool"})
	for rel, body := range map[string]string{
		"svc/package.json":    fmt.Sprintf(`{"dependencies":{%q:"1.0.0"}}`, b),
		"broken/package.json": `{not json`,
		"README.md":           "# hello\n",
	} {
		p := filepath.Join(f.dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	cov, found := f.coverage(t)
	if !found || cov.AnalysedAt == nil {
		t.Fatalf("after an analysis: found %v, analysed_at %v", found, cov.AnalysedAt)
	}
	inv := cov.Inventory
	if inv.ManifestsFound != 3 || inv.ManifestsParsed != 2 {
		t.Errorf("manifests found %d parsed %d; want 3 and 2 (README.md is not a manifest)", inv.ManifestsFound, inv.ManifestsParsed)
	}
	if len(inv.ManifestsFailed) != 1 || inv.ManifestsFailed[0] != (model.ManifestFailure{Path: "broken/package.json", Kind: "package.json", Reason: model.ManifestFailedParse}) {
		t.Errorf("failed manifests = %+v; want broken/package.json, relative to the clone, as a parse failure", inv.ManifestsFailed)
	}
	if inv.DependenciesDeclared != 4 {
		t.Errorf("dependencies declared = %d, want 4", inv.DependenciesDeclared)
	}
	// (Since 0.29.162 the dependency the registry does not have keeps its
	// row, so only the local path is without one.)
	if got := inv.DependenciesWithoutRow; len(got) != 1 || got[model.NoRowNonRegistry] != 1 {
		t.Errorf("dependencies without a row = %v; want only the non_registry one", got)
	}
	// The numbers add up: every declared dependency has a row or a reason.
	rows := len(f.inventory(t))
	without := 0
	for _, n := range inv.DependenciesWithoutRow {
		without += n
	}
	if rows+without != inv.DependenciesDeclared {
		t.Errorf("%d rows + %d without a row != %d declared", rows, without, inv.DependenciesDeclared)
	}
	if cov.LastAttemptAt != nil {
		t.Errorf("a successful analysis recorded a failed attempt: %v %q", cov.LastAttemptAt, cov.LastAttemptResult)
	}
}

// A manifest that cannot be READ is a failure for every kind, not only the
// two JSON ones: it used to contribute nothing, silently.
func TestAnUnreadableManifestIsRecorded(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: a file cannot be made unreadable")
	}
	f := newWholeReplaceFixture(t, "unreadable", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	f.manifest(t, map[string]string{uniq(t, "unr-a"): "1.0.0"})
	locked := filepath.Join(f.dir, "requirements.txt")
	if err := os.WriteFile(locked, []byte("flask==2.0.0\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o600) })

	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatalf("one unreadable manifest must not fail the phase: %v", err)
	}
	cov, _ := f.coverage(t)
	inv := cov.Inventory
	if inv.ManifestsFound != 2 || inv.ManifestsParsed != 1 || len(inv.ManifestsFailed) != 1 ||
		inv.ManifestsFailed[0] != (model.ManifestFailure{Path: "requirements.txt", Kind: "requirements.txt", Reason: model.ManifestFailedRead}) {
		t.Errorf("coverage = %+v; want requirements.txt recorded as a read failure", inv)
	}
}

// An analysis that does not replace the inventory says so in the attempt
// columns, and leaves the half describing the inventory that is still there.
func TestAFailedWalkRecordsTheAttempt(t *testing.T) {
	f := newWholeReplaceFixture(t, "attempt", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	f.manifest(t, map[string]string{uniq(t, "att-a"): "1.0.0"})
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	first, _ := f.coverage(t)

	if err := f.ac.scanLibyear(context.Background(), f.repoID, filepath.Join(f.dir, "no-such-clone"), nil, &AnalysisResult{}); err == nil {
		t.Fatal("the walk must fail")
	}
	after, _ := f.coverage(t)
	if after.LastAttemptResult != model.InventoryAttemptRootUnreadable || after.LastAttemptAt == nil || !after.LastAttemptAt.After(*first.AnalysedAt) {
		t.Errorf("attempt = %q at %v; want root_unreadable, later than the analysis at %v", after.LastAttemptResult, after.LastAttemptAt, first.AnalysedAt)
	}
	if !after.AnalysedAt.Equal(*first.AnalysedAt) || after.Inventory.ManifestsFound != first.Inventory.ManifestsFound {
		t.Errorf("a failed attempt changed the half: %+v, was %+v", after, first)
	}
}

// A shutdown is not a failed attempt: nothing is recorded.
func TestAShutdownRecordsNoAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWholeReplaceFixture(t, "shutdown", func(http.ResponseWriter, *http.Request) { cancel() })
	f.manifest(t, map[string]string{uniq(t, "shut-a"): "1.0.0"})
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, nil, &AnalysisResult{}); err == nil {
		t.Fatal("a cancelled analysis must return an error")
	}
	if _, found := f.coverage(t); found {
		t.Error("a shutdown wrote a coverage record; it is not an attempt that failed")
	}
	// And when the failure that WOULD be recorded happens during a shutdown
	// (the walk fails at the root with the context already cancelled), it is
	// still not recorded.
	if err := f.ac.scanLibyear(ctx, f.repoID, filepath.Join(f.dir, "no-such-clone"), nil, &AnalysisResult{}); err == nil {
		t.Fatal("the walk must fail")
	}
	if _, found := f.coverage(t); found {
		t.Error("a failure during a shutdown was recorded as an attempt")
	}
}

// No clone to analyse: the analysis records that, in its one attempt writer.
func TestAnAnalysisWithNoCloneRecordsTheAttempt(t *testing.T) {
	f := newWholeReplaceFixture(t, "noclone", func(http.ResponseWriter, *http.Request) {})
	if _, err := f.ac.AnalyzeRepo(context.Background(), f.repoID); err == nil {
		t.Fatal("an analysis with no bare clone must fail")
	}
	cov, found := f.coverage(t)
	if !found || cov.LastAttemptResult != model.InventoryAttemptCloneUnavailable || cov.AnalysedAt != nil {
		t.Errorf("coverage = %+v (found %v); want clone_unavailable and no half", cov, found)
	}
}

// The scheduler's own no-clone guard returns before the analysis is entered;
// it records through the same writer, under the same rules.
func TestTheSchedulersNoCloneGuardRecordsTheAttempt(t *testing.T) {
	f := newWholeReplaceFixture(t, "schedguard", func(http.ResponseWriter, *http.Request) {})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	RecordAnalysisSkippedForNoClone(cancelled, f.store, logger, f.repoID)
	if _, found := f.coverage(t); found {
		t.Fatal("recorded during a shutdown")
	}

	RecordAnalysisSkippedForNoClone(context.Background(), f.store, logger, f.repoID)
	cov, found := f.coverage(t)
	if !found || cov.LastAttemptResult != model.InventoryAttemptCloneUnavailable || cov.AnalysedAt != nil {
		t.Errorf("coverage = %+v (found %v); want clone_unavailable and no half", cov, found)
	}
}

// Parse, then corrupt the manifest: its dependencies stay in the inventory,
// carried, for as long as the file exists and fails (summary/70, 4.7). Before
// 0.29.160 a manifest that stopped parsing contributed nothing, the
// inventory lost its dependencies, and the next vulnerability scan closed
// their findings as if they had been fixed.
func TestACorruptedManifestKeepsItsDependencies(t *testing.T) {
	f := newWholeReplaceFixture(t, "corrupt", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	a, b, c := uniq(t, "cor-a"), uniq(t, "cor-b"), uniq(t, "cor-c")
	f.manifest(t, map[string]string{a: "1.0.0", b: "1.0.0"})
	svc := filepath.Join(f.dir, "svc", "package.json")
	if err := os.MkdirAll(filepath.Dir(svc), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(svc, []byte(fmt.Sprintf(`{"dependencies":{%q:"1.0.0"}}`, c)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	before := f.inventory(t)
	if len(before) != 3 {
		t.Fatalf("inventory = %v, want 3 rows", before)
	}

	// The root manifest is corrupted; the other still parses.
	if err := os.WriteFile(filepath.Join(f.dir, "package.json"), []byte(`{"dependencies":{`), 0o600); err != nil {
		t.Fatal(err)
	}
	for cycle := 1; cycle <= 2; cycle++ {
		res := &AnalysisResult{}
		if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, res); err != nil {
			t.Fatalf("cycle %d: a manifest that fails must not fail the phase: %v", cycle, err)
		}
		if after := f.inventory(t); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("cycle %d: inventory %v; the corrupted manifest's dependencies must be carried: %v", cycle, after, before)
		}
		cov, _ := f.coverage(t)
		fails := cov.Inventory.ManifestsFailed
		if len(fails) != 1 || fails[0].Path != "package.json" || fails[0].Reason != model.ManifestFailedParse || fails[0].RowsCarried != 2 {
			t.Errorf("cycle %d: failed manifests = %+v; want package.json, a parse failure, 2 rows carried", cycle, fails)
		}
	}
	// Each cycle rotated a whole inventory: 3 rows, then 3 again.
	if got := f.historyRows(t); got != 6 {
		t.Errorf("history rows = %d, want 6", got)
	}

	// The file is deleted: it no longer exists and fails, so nothing is
	// carried and its dependencies are gone.
	if err := os.Remove(filepath.Join(f.dir, "package.json")); err != nil {
		t.Fatal(err)
	}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if got := f.inventory(t); len(got) != 1 || !strings.HasPrefix(got[0], c) {
		t.Errorf("after the manifest is deleted: %v; want only the other manifest's dependency", got)
	}
}

// Every stored row names the manifest that declared it.
func TestInventoryRowsNameTheirManifest(t *testing.T) {
	f := newWholeReplaceFixture(t, "paths", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	f.manifest(t, map[string]string{uniq(t, "path-a"): "1.0.0"})
	nested := filepath.Join(f.dir, "apps", "web", "package.json")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte(fmt.Sprintf(`{"dependencies":{%q:"1.0.0"}}`, uniq(t, "path-b"))), 0o600); err != nil {
		t.Fatal(err)
	}
	// The same names again, so the second analysis is answered from the
	// registry cache: a cached answer must carry THIS dependency's path.
	for range 2 {
		if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, &AnalysisResult{}); err != nil {
			t.Fatal(err)
		}
		rows, err := f.store.Pool().Query(context.Background(),
			`SELECT manifest_path FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 ORDER BY 1`, f.repoID)
		if err != nil {
			t.Fatal(err)
		}
		var paths []string
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, p)
		}
		rows.Close()
		if strings.Join(paths, ",") != "apps/web/package.json,package.json" {
			t.Errorf("manifest paths = %v; want apps/web/package.json and package.json, relative to the clone", paths)
		}
	}
}
