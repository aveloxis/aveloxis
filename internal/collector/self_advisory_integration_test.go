// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.29 — self-advisory scanning, pinned as the numpy/numpy shape
// (operator report 2026-07-21): a repo with ZERO dependencies showed
// zero vulnerabilities while OSV carried 16 advisories for the
// package it publishes, because the scan covered dependencies only
// and the v0.27.11 self-dep exclusion reinforced the blind spot for
// publisher repos. This test IS that scenario as a contract.

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

func TestSelfAdvisoryScanEndToEnd(t *testing.T) {
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
	owner := "_avselfadv"
	repoName := fmt.Sprintf("numpyish%d", suffix)
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
			"DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})

	// The numpy shape: ZERO dependency rows. Distribution evidence
	// carries the repo's own package PLUS deps.dev reverse-lookup
	// noise — packages that merely CLAIM this repo URL (numpy's real
	// rows include intel-numpy and mmwave). Only the exact-name match
	// may be scanned; attributing a claimant's advisories to this
	// repo would be misinformation.
	for _, row := range [][2]string{
		{"pypi", repoName},                       // the repo's own package
		{"pypi", "intel-" + repoName},            // lookalike claimant — MUST NOT scan
		{"go", "github.com/" + owner + "/other"}, // unrelated go module claimant
	} {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
			VALUES ($1, $2, $3, 'deps.dev')`, repoID, row[0], row[1]); err != nil {
			t.Fatal(err)
		}
	}

	// httptest OSV, real shapes: querybatch → id stubs; per-id details.
	var (
		mu            sync.Mutex
		receivedPurls []string
	)
	details := map[string]string{
		"GHSA-self1": `{"id":"GHSA-self1","summary":"buffer overflow in ` + repoName + `",
			"aliases":["CVE-2021-99001"],
			"database_specific":{"severity":"HIGH"},
			"affected":[{"package":{"name":"` + repoName + `"},"ranges":[{"events":[{"introduced":"0"},{"fixed":"1.22.0"}]}]}],
			"severity":[{"type":"CVSS_V3","score":"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H"}]}`,
		"GHSA-self2": `{"id":"GHSA-self2","summary":"older issue","database_specific":{"severity":"MODERATE"}}`,
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
			if q.Package.Purl == "pkg:pypi/"+strings.ToLower(repoName) {
				stubs = append(stubs,
					map[string]string{"id": "GHSA-self1", "modified": "2026-01-01T00:00:00Z"},
					map[string]string{"id": "GHSA-self2", "modified": "2026-01-01T00:00:00Z"})
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

	// ── Scan 1 ─────────────────────────────────────────────────
	result, err := ScanVulnerabilities(ctx, store, repoID, logger, nil, false)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.SelfAdvisoryPurls != 1 {
		t.Errorf("SelfAdvisoryPurls = %d, want 1 (the exact-name package only)", result.SelfAdvisoryPurls)
	}
	if result.VulnsFound != 2 {
		t.Errorf("VulnsFound = %d, want 2 self advisories", result.VulnsFound)
	}

	// Attribution precision: the lookalike claimant and the unrelated
	// go module must NEVER have been queried.
	mu.Lock()
	for _, p := range receivedPurls {
		if strings.Contains(p, "intel-") || strings.Contains(p, "/other") {
			t.Errorf("queried %q — deps.dev reverse-lookup claimants must not be attributed to this repo", p)
		}
	}
	mu.Unlock()

	// Stored shape: kind='self', versionless purl, detail fields real.
	var kind, purl, severity, fixed string
	if err := store.Pool().QueryRow(ctx, `
		SELECT COALESCE(dependency_kind,''), package_purl, severity, fixed_version
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND vuln_id = 'GHSA-self1'`, repoID).
		Scan(&kind, &purl, &severity, &fixed); err != nil {
		t.Fatalf("self finding not stored: %v", err)
	}
	if kind != "self" {
		t.Errorf("dependency_kind = %q, want self", kind)
	}
	if strings.Contains(purl, "@") {
		t.Errorf("self purl %q must be versionless", purl)
	}
	if severity != "HIGH" || fixed != "1.22.0" {
		t.Errorf("severity/fixed = %q/%q, want HIGH/1.22.0", severity, fixed)
	}

	// Headline exclusion: project advisories are NOT live dependency
	// exposure — the dashboard count for this dep-less repo stays 0.
	total, critical, err := store.CountRepoVulnerabilities(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || critical != 0 {
		t.Errorf("headline counts = %d/%d, want 0/0 — self advisories must not read as current exposure", total, critical)
	}

	// ── Scan 2: lifecycle stability ────────────────────────────
	// Self advisories are in every scan's seen-set; a re-scan must
	// not resolve them (they'd flap current↔resolved otherwise).
	if _, err := ScanVulnerabilities(ctx, store, repoID, logger, nil, false); err != nil {
		t.Fatalf("re-scan: %v", err)
	}
	var stillCurrent int
	_ = store.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND dependency_kind = 'self' AND resolved_at IS NULL`, repoID).
		Scan(&stillCurrent)
	if stillCurrent != 2 {
		t.Errorf("after re-scan %d self advisories current, want 2 (stable, not flapping)", stillCurrent)
	}
}
