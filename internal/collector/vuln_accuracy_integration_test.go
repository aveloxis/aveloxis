// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.11 — end-to-end vulnerability-accuracy test (AVELOXIS_TEST_DB
// + an httptest OSV server serving the REAL two-phase shapes from the
// v0.27.4 harness):
//
//   - self-dependencies (name heuristic — no distribution rows needed)
//     produce NO purls, and a pre-existing self-dep finding gets
//     resolved_at stamped by the v0.27.4 lifecycle on the next
//     complete scan — the "no migration needed" healing story,
//     verified end to end;
//   - a lockfile-resolved dep is scanned at the LOCKED version (not
//     the manifest floor) with class 'locked';
//   - Go deps are 'locked' by construction;
//   - un-locked range deps stay at the floor and carry the raw
//     requirement + classification into the stored rows.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

func TestVulnScanAccuracyEndToEnd(t *testing.T) {
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

	suffix := time.Now().UnixNano()
	owner := "_avvulnacc"
	repoName := fmt.Sprintf("airflow%d", suffix)
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   fmt.Sprintf("https://github.com/%s/%s", owner, repoName),
		Owner:    owner,
		Name:     repoName,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			"DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})

	// Declared deps (repo_deps_libyear — what the scan reads):
	//   1. The repo's OWN package (name heuristic: dep name ==
	//      repo_name) declared as a range — the apache/airflow
	//      provider-manifest shape. Must produce NO purl.
	//   2. flask, range-floor declared, but LOCKED at 2.5.0 below.
	//   3. requests, == pinned → exact.
	//   4. a Go dep → locked by construction.
	selfPurl := fmt.Sprintf("pkg:pypi/%s@3.0.0", repoName)
	for _, lb := range []*db.LibyearRow{
		{Name: repoName, Requirement: repoName + ">=3.0.0", PackageManager: "pypi",
			CurrentVersion: "3.0.0", Purl: selfPurl},
		{Name: "flask", Requirement: "flask>=2.0.0", PackageManager: "pypi",
			CurrentVersion: "2.0.0", Purl: "pkg:pypi/flask@2.0.0"},
		{Name: "requests", Requirement: "requests==2.31.0", PackageManager: "pypi",
			CurrentVersion: "2.31.0", Purl: "pkg:pypi/requests@2.31.0"},
		{Name: "golang.org/x/text", Requirement: "golang.org/x/text v0.3.0", PackageManager: "go",
			CurrentVersion: "v0.3.0", Purl: "pkg:golang/golang.org/x/text@v0.3.0"},
	} {
		if err := store.InsertRepoLibyear(ctx, repoID, lb); err != nil {
			t.Fatal(err)
		}
	}

	// The committed lockfile resolved flask at 2.5.0.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID,
		[]*db.RepoLockfileInfo{{Ecosystem: "pypi", LockfilePath: "poetry.lock",
			LockfileKind: "poetry.lock", EntryCount: 12, DirectCount: 1}},
		[]*db.RepoLockfilePackage{{Ecosystem: "pypi", PackageName: "flask",
			ResolvedVersion: "2.5.0", LockfilePath: "poetry.lock"}}); err != nil {
		t.Fatal(err)
	}

	// A historical self-dep finding from a pre-v0.27.11 scan. No
	// migration exists for these ON PURPOSE: the next complete scan
	// omits the self-dep purl and the v0.27.4 lifecycle resolves it.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*db.VulnerabilityRow{{
		VulnID: "GHSA-selfdep", PackageName: repoName, PackagePurl: selfPurl,
		Severity: "CRITICAL", CVSSScore: 9.8, Source: "osv.dev",
	}}); err != nil {
		t.Fatal(err)
	}

	// httptest OSV with the REAL shapes: querybatch answers ID STUBS
	// keyed on purl content; /v1/vulns/{id} serves the full entries.
	var (
		mu            sync.Mutex
		receivedPurls []string
	)
	details := map[string]string{
		"GHSA-flask1": `{"id":"GHSA-flask1","summary":"flask bad","aliases":["CVE-2026-1"],
			"database_specific":{"severity":"HIGH"},
			"severity":[{"type":"CVSS_V3","score":"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N"}]}`,
		"GHSA-req1": `{"id":"GHSA-req1","summary":"requests bad",
			"database_specific":{"severity":"MODERATE"}}`,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/querybatch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Queries []struct {
				Package struct {
					Purl string `json:"purl"`
				} `json:"package"`
			} `json:"queries"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		results := make([]map[string]any, len(req.Queries))
		for i, q := range req.Queries {
			mu.Lock()
			receivedPurls = append(receivedPurls, q.Package.Purl)
			mu.Unlock()
			stubs := []map[string]string{}
			switch {
			case strings.Contains(q.Package.Purl, "flask"):
				stubs = append(stubs, map[string]string{"id": "GHSA-flask1", "modified": "2026-01-01T00:00:00Z"})
			case strings.Contains(q.Package.Purl, "requests"):
				stubs = append(stubs, map[string]string{"id": "GHSA-req1", "modified": "2026-01-01T00:00:00Z"})
			}
			results[i] = map[string]any{"vulns": stubs}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	})
	mux.HandleFunc("GET /v1/vulns/{id}", func(w http.ResponseWriter, r *http.Request) {
		body, ok := details[r.PathValue("id")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, body)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	oldBatch, oldVulns := osvBatchURL, osvVulnURLBase
	osvBatchURL = srv.URL + "/v1/querybatch"
	osvVulnURLBase = srv.URL + "/v1/vulns/"
	t.Cleanup(func() { osvBatchURL, osvVulnURLBase = oldBatch, oldVulns })

	result, err := ScanVulnerabilities(ctx, store, repoID, logger)
	if err != nil {
		t.Fatalf("ScanVulnerabilities: %v", err)
	}
	if result.SelfDepsExcluded != 1 {
		t.Errorf("self-dep exclusion count: got %d, want 1", result.SelfDepsExcluded)
	}

	// The purls that reached OSV.
	mu.Lock()
	purls := append([]string(nil), receivedPurls...)
	mu.Unlock()
	got := map[string]bool{}
	for _, p := range purls {
		got[p] = true
	}
	if got[selfPurl] {
		t.Errorf("the repo's own package must be EXCLUDED from purl generation; sent %v", purls)
	}
	if !got["pkg:pypi/flask@2.5.0"] {
		t.Errorf("flask must be scanned at the LOCKED version 2.5.0, sent %v", purls)
	}
	if got["pkg:pypi/flask@2.0.0"] {
		t.Errorf("the flask floor purl must be replaced by the locked version, sent %v", purls)
	}
	if !got["pkg:pypi/requests@2.31.0"] || !got["pkg:golang/golang.org/x/text@v0.3.0"] {
		t.Errorf("non-self, non-locked purls must still be scanned, sent %v", purls)
	}

	// Stored rows: classification columns + the self-dep lifecycle.
	rows, err := store.GetRepoVulnerabilities(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]*db.VulnerabilityRow{}
	for _, r := range rows {
		byID[r.VulnID] = r
	}
	if fl := byID["GHSA-flask1"]; fl == nil {
		t.Fatal("flask finding missing")
	} else {
		if fl.PackagePurl != "pkg:pypi/flask@2.5.0" || fl.VersionResolution != "locked" {
			t.Errorf("flask finding must be at the locked version with class 'locked': %+v", fl)
		}
		if fl.DeclaredRequirement != "flask>=2.0.0" {
			t.Errorf("raw requirement must be stored: %q", fl.DeclaredRequirement)
		}
		if fl.ResolvedAt != nil {
			t.Error("a finding the scan just reported must be current")
		}
	}
	if rq := byID["GHSA-req1"]; rq == nil {
		t.Fatal("requests finding missing")
	} else if rq.VersionResolution != "exact" || rq.DeclaredRequirement != "requests==2.31.0" {
		t.Errorf("== pins classify 'exact': %+v", rq)
	}
	if sd := byID["GHSA-selfdep"]; sd == nil {
		t.Fatal("historical self-dep row must be KEPT (historical record, never deleted)")
	} else if sd.ResolvedAt == nil {
		t.Error("the complete scan omitted the self-dep purl, so the v0.27.4 lifecycle " +
			"must stamp resolved_at — this is the no-migration healing path for " +
			"historical self-dep findings")
	}
}
