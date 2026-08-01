// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.72 — end-to-end proof that the vuln scan heals the
// malformed-purl false positives WITHOUT waiting for analysis to
// rewrite the dep rows (the heal-vulnerabilities story):
//
//   1. Seed dep rows carrying the EXACT production garbage — pyyaml
//      at "6.0.3 \" with purl "pkg:pypi/pyyaml@6.0.3 \" (the zephyr
//      incident shape) and a cargo dep at "workspace = true".
//   2. Seed the false-positive finding those purls produced
//      (GHSA-fixed-in-5.4 against pyyaml "6.0.3").
//   3. Run ScanVulnerabilities against an httptest OSV that RECORDS
//      every purl it receives and answers advisories only for the
//      pre-fix malformed shapes.
//   4. Assert OSV received the CLEAN purl (pkg:pypi/pyyaml@6.0.3) and
//      the versionless cargo purl — never the garbage — and that the
//      v0.27.4 lifecycle stamped resolved_at on the stale finding.

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

func TestScanHealsMalformedPurlsEndToEnd(t *testing.T) {
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
	owner := "_avvulnheal"
	repoName := fmt.Sprintf("zephyr%d", suffix)
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
			"DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1",
			"DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1",
			"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		} {
			_, _ = store.Pool().Exec(context.Background(), stmt, repoID)
		}
	})

	// The exact pre-v0.27.71 production shapes, verbatim.
	for _, lb := range []*db.LibyearRow{
		{Name: "pyyaml", Requirement: `pyyaml==6.0.3 \`, PackageManager: "pypi",
			CurrentVersion: `6.0.3 \`, Purl: `pkg:pypi/pyyaml@6.0.3 \`},
		{Name: "serde", Requirement: "serde = { workspace = true }", PackageManager: "cargo",
			CurrentVersion: "workspace = true", Purl: "pkg:cargo/serde@workspace = true"},
	} {
		if err := store.InsertRepoLibyear(ctx, repoID, lb); err != nil {
			t.Fatal(err)
		}
	}

	// The false positive the malformed purl produced: an ancient
	// advisory (fixed in 5.4) recorded against a 6.x install.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*db.VulnerabilityRow{{
		VulnID: "GHSA-fixed-in-54", PackageName: "pyyaml",
		PackagePurl: `pkg:pypi/pyyaml@6.0.3 \`, Severity: "CRITICAL",
		CVSSScore: 9.8, FixedVersion: "5.4", Source: "osv.dev",
	}}); err != nil {
		t.Fatal(err)
	}

	var (
		mu            sync.Mutex
		receivedPurls []string
	)
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
			// A REAL OSV would only match GHSA-fixed-in-54 for a
			// version below 5.4 (or an unparseable one). The clean
			// 6.0.3 purl matches nothing.
			results[i] = map[string]any{"vulns": []map[string]string{}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	})
	mux.HandleFunc("GET /v1/vulns/{id}", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	oldBatch, oldVulns := osvBatchURL, osvVulnURLBase
	osvBatchURL = srv.URL + "/v1/querybatch"
	osvVulnURLBase = srv.URL + "/v1/vulns/"
	t.Cleanup(func() { osvBatchURL, osvVulnURLBase = oldBatch, oldVulns })

	if _, err := ScanVulnerabilities(ctx, store, repoID, logger, nil, false); err != nil {
		t.Fatalf("ScanVulnerabilities: %v", err)
	}

	// OSV must have received the CLEAN purls — never the garbage.
	mu.Lock()
	purls := append([]string(nil), receivedPurls...)
	mu.Unlock()
	sawCleanPyyaml, sawCleanSerde := false, false
	for _, p := range purls {
		if strings.Contains(p, `\`) || strings.Contains(p, "workspace") || strings.Contains(p, " ") {
			t.Errorf("malformed purl reached OSV: %q — the scan must normalize stored versions at read time", p)
		}
		if p == "pkg:pypi/pyyaml@6.0.3" {
			sawCleanPyyaml = true
		}
		if p == "pkg:cargo/serde" {
			sawCleanSerde = true
		}
	}
	if !sawCleanPyyaml {
		t.Errorf("clean pkg:pypi/pyyaml@6.0.3 never queried; received: %v", purls)
	}
	if !sawCleanSerde {
		t.Errorf("versionless pkg:cargo/serde never queried (workspace dep → unpinned); received: %v", purls)
	}

	// The stale false positive must be resolved by the complete scan
	// (v0.27.4 lifecycle) — the immediate-healing story for
	// heal-vulnerabilities, no analysis re-run required.
	var resolved *time.Time
	if err := store.Pool().QueryRow(ctx,
		`SELECT resolved_at FROM aveloxis_data.repo_deps_vulnerabilities
		 WHERE repo_id = $1 AND vuln_id = 'GHSA-fixed-in-54'`, repoID).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved == nil {
		t.Error("the pre-fix false positive (fixed-in-5.4 against 6.0.3) must get resolved_at stamped by the clean scan")
	}
}
