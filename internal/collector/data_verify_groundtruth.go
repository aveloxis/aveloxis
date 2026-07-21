// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Ground-truth spot checks (v0.27.43, summary/18 Phase 5b): sample N
// collected repos and compare STORED forge metadata against the
// forge's LIVE answer, re-fetched now. This closes the loop the
// mock-shaped-by-our-parser lesson demands — the database is
// periodically checked against the source of truth, not against
// itself. Requires GitHub API keys (one FetchRepoInfo per sampled
// repo); the drift line uses the same 5% semantic as the gap detector
// so "ground-truth drift" and "gap detected" mean the same thing.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"math"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// GroundTruthCheck re-fetches live metadata for n sampled collected
// GitHub repos and compares issue/PR counts against the stored latest
// repo_info snapshot.
func GroundTruthCheck(ctx context.Context, store *db.PostgresStore, client platform.Client, logger *slog.Logger, n int) []db.VerifyResult {
	if n <= 0 {
		n = 10
	}
	rows, err := store.Pool().Query(ctx, `
		WITH sample AS (
			SELECT q.repo_id FROM aveloxis_ops.collection_queue q
			JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
			WHERE q.last_collected IS NOT NULL AND r.platform_id = 1
			  AND COALESCE(r.repo_archived, FALSE) = FALSE
			ORDER BY random() LIMIT $1
		)
		SELECT s.repo_id, r.repo_owner, r.repo_name,
		       COALESCE(ri.issues_count, 0), COALESCE(ri.pr_count, 0)
		FROM sample s
		JOIN aveloxis_data.repos r ON r.repo_id = s.repo_id
		LEFT JOIN LATERAL (
			SELECT issues_count, pr_count FROM aveloxis_data.repo_info
			WHERE repo_id = s.repo_id ORDER BY data_collection_date DESC LIMIT 1
		) ri ON TRUE`, n)
	if err != nil {
		return []db.VerifyResult{{Check: "ground truth", Severity: "FAIL", Detail: fmt.Sprintf("sample query failed: %v", err)}}
	}
	defer rows.Close()

	type sampled struct {
		id          int64
		owner, name string
		issues, prs int
	}
	var reposToCheck []sampled
	for rows.Next() {
		var sr sampled
		if err := rows.Scan(&sr.id, &sr.owner, &sr.name, &sr.issues, &sr.prs); err != nil {
			return []db.VerifyResult{{Check: "ground truth", Severity: "FAIL", Detail: fmt.Sprintf("sample scan failed: %v", err)}}
		}
		reposToCheck = append(reposToCheck, sr)
	}
	if len(reposToCheck) == 0 {
		return []db.VerifyResult{{Check: "ground truth", Severity: "OK", Detail: "no collected GitHub repos to sample"}}
	}

	var out []db.VerifyResult
	drifted, checked := 0, 0
	for _, sr := range reposToCheck {
		if ctx.Err() != nil {
			break
		}
		live, err := client.FetchRepoInfo(ctx, sr.owner, sr.name)
		if err != nil {
			// A vanished/renamed repo is a reconcile-repos concern, not
			// a ground-truth failure — report and continue.
			out = append(out, db.VerifyResult{Check: "ground truth", Severity: "WARN",
				Detail: fmt.Sprintf("%s/%s: live fetch failed (%v) — if 404, the repo moved; see reconcile-repos", sr.owner, sr.name, err)})
			continue
		}
		checked++
		if pastGapThreshold(sr.issues, live.IssuesCount) || pastGapThreshold(sr.prs, live.PRCount) {
			drifted++
			out = append(out, db.VerifyResult{Check: "ground truth", Severity: "WARN",
				Detail: fmt.Sprintf("%s/%s: stored issues=%d prs=%d vs live issues=%d prs=%d — beyond the gap threshold; expected only when the forge moved faster than the recollect cadence",
					sr.owner, sr.name, sr.issues, sr.prs, live.IssuesCount, live.PRCount)})
		}
	}
	sev := "OK"
	if checked == 0 {
		sev = "WARN"
	}
	out = append(out, db.VerifyResult{Check: "ground truth", Severity: sev,
		Detail: fmt.Sprintf("%d repos checked live against GitHub, %d within the gap threshold, %d drifted", checked, checked-drifted, drifted)})
	return out
}

// pastGapThreshold mirrors gapExceedsThreshold's 5% semantic on a
// stored-vs-live pair (live is the denominator: the forge's answer is
// the truth being compared against).
func pastGapThreshold(stored, live int) bool {
	if live <= 0 {
		return false
	}
	return math.Abs(float64(stored-live))/float64(live) > 0.05
}
