// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// The coverage state on the API (summary/70, 4.8): the list answer and both
// statistics answers send the same state and the same three facts for the
// same repository, over a set of repositories in different states. One
// function decides; this holds the three routes to it.
func TestCoverageStateIsTheSameOnTheListAndStatisticsAnswers(t *testing.T) {
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
	tag := fmt.Sprintf("_avcov%d", time.Now().UnixNano())
	var ids []int64
	mk := func(name string) int64 {
		var id int64
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`, "https://github.com/"+tag+"/"+name, name, tag).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		return id
	}
	t.Cleanup(func() {
		c := context.Background()
		for _, q := range []string{
			`DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = ANY($1)`,
			`DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = ANY($1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1)`,
		} {
			_, _ = store.Pool().Exec(c, q, ids)
		}
	})
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := store.Pool().Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	finding := func(repo int64, id string, closed bool) {
		t.Helper()
		if err := store.InsertVulnerabilityBatch(ctx, repo, []*model.VulnerabilityRow{{VulnID: id, PackageName: "p", PackagePurl: "pkg:npm/p@1",
			Source: "osv.dev", DependencyKind: "direct", VersionResolution: "locked"}}); err != nil {
			t.Fatal(err)
		}
		if closed {
			exec(`UPDATE aveloxis_data.repo_deps_vulnerabilities SET resolved_at = NOW() WHERE repo_id = $1 AND vuln_id = $2`, repo, id)
		}
	}
	want := map[int64]string{}

	never := mk("never")
	want[never] = vulnclass.CoverageNotScanned

	legacy := mk("legacy") // a stamp and findings, no coverage record
	exec(`UPDATE aveloxis_data.repos SET vuln_scan_last_run = NOW() WHERE repo_id = $1`, legacy)
	finding(legacy, "L-1", false)
	want[legacy] = vulnclass.CoverageScanned

	undated := mk("undated") // only a closed finding, no stamp
	finding(undated, "U-1", true)
	want[undated] = vulnclass.CoverageScanHistoryUndated

	partial := mk("partial")
	exec(`UPDATE aveloxis_data.repos SET vuln_scan_last_run = NOW() WHERE repo_id = $1`, partial)
	exec(`INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, analysed_at, last_scan_attempt_at, scan_outcome, scan_partial,
		manifests_failed, dependencies_without_row)
		VALUES ($1, NOW(), NOW(), 'sent', TRUE, '[{"path":"a/pom.xml","kind":"pom.xml","reason":"parse","rows_carried":2}]', '{"non_registry":3}')`, partial)
	want[partial] = vulnclass.CoverageScannedPartial

	unsendable := mk("unsendable")
	exec(`UPDATE aveloxis_data.repos SET vuln_scan_last_run = NOW() WHERE repo_id = $1`, unsendable)
	exec(`INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, analysed_at, last_scan_attempt_at, scan_outcome) VALUES ($1, NOW(), NOW(), 'unsendable')`, unsendable)
	want[unsendable] = vulnclass.CoverageNothingSendable

	notAnalysed := mk("not-analysed")
	exec(`INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, last_scan_attempt_at, scan_outcome, dependencies_unknown, last_attempt_at, last_attempt_result)
		VALUES ($1, NOW(), 'inventory_incomplete', TRUE, NOW(), 'clone_unavailable')`, notAnalysed)
	want[notAnalysed] = vulnclass.CoverageNotAnalysed

	s, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string, into any) {
		t.Helper()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "127.0.0.1:50000" // an exempt network: this test makes more requests than the anonymous allowance
		s.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s = %d %s", path, w.Code, w.Body.String())
		}
		if err := json.Unmarshal(w.Body.Bytes(), into); err != nil {
			t.Fatalf("GET %s: %v\n%s", path, err, w.Body.String())
		}
	}
	type answer struct {
		Coverage *vulnclass.CoverageView `json:"coverage"`
	}
	var idList []string
	for _, id := range ids {
		idList = append(idList, strconv.FormatInt(id, 10))
	}
	batch := map[string]answer{}
	get("/api/v1/repos/stats?ids="+strings.Join(idList, ","), &batch)

	for _, id := range ids {
		sid := strconv.FormatInt(id, 10)
		var list, single answer
		get("/api/v1/repos/"+sid+"/vulnerabilities", &list)
		get("/api/v1/repos/"+sid+"/stats", &single)
		b := batch[sid]
		if list.Coverage == nil || single.Coverage == nil || b.Coverage == nil {
			t.Fatalf("repository %d: a coverage object is missing (list %v, stats %v, batch %v)", id, list.Coverage != nil, single.Coverage != nil, b.Coverage != nil)
		}
		if list.Coverage.State != want[id] {
			t.Errorf("repository %d: state %q, want %q", id, list.Coverage.State, want[id])
		}
		for name, other := range map[string]*vulnclass.CoverageView{"stats": single.Coverage, "batch stats": b.Coverage} {
			if other.State != list.Coverage.State || other.CountBlank != list.Coverage.CountBlank || other.CleanResult != list.Coverage.CleanResult ||
				other.DependenciesUnknown != list.Coverage.DependenciesUnknown || other.SecondLine != list.Coverage.SecondLine || other.Partial != list.Coverage.Partial {
				t.Errorf("repository %d: the %s answer says %+v and the list answer %+v", id, name, *other, *list.Coverage)
			}
			// The paths are the list answer's alone.
			if len(other.ManifestPathsFailed) != 0 || len(other.ManifestPathsCarried) != 0 {
				t.Errorf("repository %d: the %s answer carries manifest paths", id, name)
			}
		}
	}

	// What the object carries, on the repository that has it all.
	var list answer
	get("/api/v1/repos/"+strconv.FormatInt(partial, 10)+"/vulnerabilities", &list)
	c := list.Coverage
	if !c.Partial || c.ManifestsFailed != 1 || c.ManifestsCarried != 1 || c.DependenciesWithoutRow["non_registry"] != 3 ||
		len(c.ManifestPathsFailed) != 1 || c.ManifestPathsFailed[0] != "a/pom.xml" || c.LastCompletedScanAt == nil || c.AnalysedAt == nil {
		t.Errorf("the partial repository's coverage = %+v", *c)
	}
	// A stamp from a scan that sent nothing proves nothing: blank, not 0.
	get("/api/v1/repos/"+strconv.FormatInt(unsendable, 10)+"/vulnerabilities", &list)
	if !list.Coverage.CountBlank || list.Coverage.CleanResult {
		t.Errorf("nothing sendable: %+v", *list.Coverage)
	}
	get("/api/v1/repos/"+strconv.FormatInt(notAnalysed, 10)+"/vulnerabilities", &list)
	if list.Coverage.SecondLine != vulnclass.SecondLineAttemptFailed || list.Coverage.LastAnalysisAttempt == nil || list.Coverage.LastAnalysisAttempt.Reason != "clone_unavailable" {
		t.Errorf("not analysed, with a failed attempt: %+v", *list.Coverage)
	}
}
