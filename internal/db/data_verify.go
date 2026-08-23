// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Data-verification probe suite (v0.27.43, summary/18 Phase 5a).
//
// `aveloxis data-verify` runs this battery against a LIVE database —
// read-only, bounded, every query production-validated for cost during
// the 2026-07-21 audit — and reports invariant violations. This is
// what makes accuracy STAY absolute rather than audited-once: the DB
// is checked against its own invariants on demand (and against the
// forge's live answers via the --ground-truth flag).
//
// Severity semantics:
//   - FAIL: a code-enforced invariant is broken (cached counts diverge
//     from actual rows, batch and single endpoints disagree, NEW
//     cross-kind message corruption outside the known heal worklist,
//     duplicate Default groups, case-duplicate repos). Exit 1.
//   - WARN: known-and-healing state or operator-attention signal
//     (pending heal worklist, stranded repos awaiting reconcile-repos,
//     drift beyond the gap-detector threshold, fill-rate floors when
//     configured, schema behind the binary).
//   - OK: invariant verified.
//
// Every probe degrades gracefully: a probe error (including SQLSTATE
// 42P01 undefined_table on fleets whose schema predates the probe's
// tables) becomes a WARN finding naming the migration need — the run
// always completes and reports everything else.

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// VerifyResult is one probe outcome.
type VerifyResult struct {
	Check    string `json:"check"`
	Severity string `json:"severity"` // "FAIL" | "WARN" | "OK"
	Detail   string `json:"detail"`
}

// VerifyOptions bounds and configures the battery.
type VerifyOptions struct {
	// Sample bounds the drift and equality probes (repos sampled from
	// the recently-collected set). 0 → 200 (the audit's sample size).
	Sample int
	// MinIdentityFill / MinSeverityKnown are optional FLOORS in
	// percent (0–100). 0 = report-only (no magic-number defaults —
	// the operator sets floors deliberately; the audit measured
	// 99.1–99.6% identity fill and 77% severity-known on the fleet).
	MinIdentityFill  float64
	MinSeverityKnown float64
}

// driftThresholdPct mirrors the gap detector's 5% semantic
// (gapExceedsThreshold in internal/collector): drift beyond it is the
// same signal gap fill acts on, so data-verify WARNs at the same line.
const driftThresholdPct = 5.0

// RunDataVerification executes the probe battery. The returned slice
// is ordered FAIL → WARN → OK by the caller (sortVerifyResults).
func (s *PostgresStore) RunDataVerification(ctx context.Context, opts VerifyOptions) []VerifyResult {
	if opts.Sample <= 0 {
		opts.Sample = 200
	}
	var out []VerifyResult
	add := func(check, severity, detail string, args ...any) {
		out = append(out, VerifyResult{Check: check, Severity: severity, Detail: fmt.Sprintf(detail, args...)})
	}
	probeErr := func(check string, err error) {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			add(check, "WARN", "probe table missing (%s) — run `aveloxis migrate` to bring the schema up to this binary", pgErr.Message)
			return
		}
		add(check, "FAIL", "probe failed: %v", err)
	}

	// ── Structural invariants ────────────────────────────────────

	// Case-duplicate repo groups (v0.25.32 backstop): must be zero.
	var n int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT LOWER(repo_git) FROM aveloxis_data.repos WHERE platform_id IN (1,2)
			GROUP BY LOWER(repo_git) HAVING COUNT(*) > 1) g`).Scan(&n); err != nil {
		probeErr("case-duplicate repos", err)
	} else if n > 0 {
		add("case-duplicate repos", "FAIL", "%d case-variant duplicate groups — run `aveloxis dedup-repos` (analytics double-count until drained)", n)
	} else {
		add("case-duplicate repos", "OK", "0 duplicate groups")
	}

	// AT MOST one Default repo_group (v0.27.17 consolidation). Zero is
	// legitimate — the group is lazily created on first collection, so
	// a fresh database (CI!) has none; only DUPLICATES are the bug.
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repo_groups WHERE rg_name = 'Default'`).Scan(&n); err != nil {
		probeErr("default repo_group singleton", err)
	} else if n > 1 {
		add("default repo_group singleton", "FAIL", "%d 'Default' groups (want at most 1) — the v0.27.17 lazy-creation bug shape", n)
	} else {
		add("default repo_group singleton", "OK", "%d Default group(s)", n)
	}

	// Stale 'collecting' rows: locks older than 24h mean an orphaned
	// claim (worker died without release).
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.collection_queue
		WHERE status = 'collecting' AND locked_at < NOW() - INTERVAL '24 hours'`).Scan(&n); err != nil {
		probeErr("stale collecting locks", err)
	} else if n > 0 {
		add("stale collecting locks", "WARN", "%d rows locked >24h — orphaned claims; recoverStale heals them on the next serve start", n)
	} else {
		add("stale collecting locks", "OK", "no locks older than 24h")
	}

	// Stranded repos (v0.27.39 class): non-archived, no queue row.
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repos r
		WHERE COALESCE(r.repo_archived, FALSE) = FALSE
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = r.repo_id)`).Scan(&n); err != nil {
		probeErr("stranded repos", err)
	} else if n > 0 {
		add("stranded repos", "WARN", "%d non-archived repos have no queue row (invisible to the scheduler) — run `aveloxis reconcile-repos --dry-run`", n)
	} else {
		add("stranded repos", "OK", "every non-archived repo has a queue row")
	}

	// Cross-kind message corruption (v0.27.38 class). THREE states:
	//   1. msg_kind column absent → the kinded arbiter is not in force;
	//      collisions are the KNOWN pre-migration state (WARN with the
	//      migrate instruction, never a corruption FAIL). Caught live on
	//      production 2026-07-21: a partial v0.27.38 migrate had created
	//      the worklist table but not the column, and the first version
	//      of this probe misread that as impossible-state corruption.
	//   2. migrated, worklist pending → known-and-healing (WARN).
	//   3. migrated, intersection OUTSIDE the worklist → NEW corruption,
	//      which the kinded arbiter makes impossible — FAIL loudly.
	var kindColExists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'aveloxis_data' AND table_name = 'messages' AND column_name = 'msg_kind')`).Scan(&kindColExists); err != nil {
		probeErr("message kind schema", err)
		kindColExists = false
	}
	if !kindColExists {
		add("message kind schema", "WARN", "messages.msg_kind absent — the v0.27.38 kinded arbiter is NOT in force; cross-kind collisions are the known pre-migration state. Run `aveloxis migrate`, then `aveloxis heal-messages`")
		return append(out, s.postSchemaProbes(ctx, opts)...)
	}
	var pending int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.message_heal_worklist WHERE healed_at IS NULL`).Scan(&pending); err != nil {
		probeErr("message heal worklist", err)
	} else if pending > 0 {
		add("message heal worklist", "WARN", "%d cross-kind collision rows pending — run `aveloxis heal-messages`", pending)
	} else {
		add("message heal worklist", "OK", "no pending collision rows")
	}
	var newCollision bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM aveloxis_data.issue_message_ref imr
			JOIN aveloxis_data.review_comments rc ON rc.msg_id = imr.msg_id
			WHERE NOT EXISTS (
				SELECT 1 FROM aveloxis_ops.message_heal_worklist w WHERE w.msg_id = imr.msg_id))`).Scan(&newCollision); err != nil {
		probeErr("new cross-kind collisions", err)
	} else if newCollision {
		add("new cross-kind collisions", "FAIL", "a msg row is claimed by BOTH the issue bridge and review_comments OUTSIDE the known heal worklist — the kinded arbiter should make this impossible; investigate before trusting message text")
	} else {
		add("new cross-kind collisions", "OK", "bridge intersection ⊆ heal worklist")
	}

	out = append(out, s.postSchemaProbes(ctx, opts)...)
	return out
}

// postSchemaProbes are the probes independent of the msg_kind
// migration state — split out so the schema-behind early return above
// still runs the full remainder of the battery.
func (s *PostgresStore) postSchemaProbes(ctx context.Context, opts VerifyOptions) []VerifyResult {
	var out []VerifyResult
	add := func(check, severity, detail string, args ...any) {
		out = append(out, VerifyResult{Check: check, Severity: severity, Detail: fmt.Sprintf(detail, args...)})
	}
	probeErr := func(check string, err error) {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			add(check, "WARN", "probe table missing (%s) — run `aveloxis migrate` to bring the schema up to this binary", pgErr.Message)
			return
		}
		add(check, "FAIL", "probe failed: %v", err)
	}

	// ── Count-integrity probes (sampled) ─────────────────────────

	// Cached queue counts vs actual table counts. CompleteJob writes
	// these via COUNT subqueries (v0.21.2), so ANY mismatch is a FAIL.
	var mismatches, sampled int64
	if err := s.pool.QueryRow(ctx, `
		WITH sample AS (
			SELECT repo_id, last_issues, last_prs FROM aveloxis_ops.collection_queue
			WHERE last_collected IS NOT NULL AND status = 'queued'
			ORDER BY random() LIMIT LEAST($1, 30)
		)
		SELECT COUNT(*) FILTER (WHERE
			s.last_issues <> (SELECT COUNT(*) FROM aveloxis_data.issues i WHERE i.repo_id = s.repo_id)
			OR s.last_prs <> (SELECT COUNT(*) FROM aveloxis_data.pull_requests p WHERE p.repo_id = s.repo_id)),
			COUNT(*)
		FROM sample s`, opts.Sample).Scan(&mismatches, &sampled); err != nil {
		probeErr("cached counts vs actual", err)
	} else if mismatches > 0 {
		add("cached counts vs actual", "FAIL", "%d of %d sampled repos: collection_queue.last_issues/last_prs disagree with actual table counts — CompleteJob's cumulative-count contract is broken", mismatches, sampled)
	} else {
		add("cached counts vs actual", "OK", "%d sampled repos match exactly", sampled)
	}

	// Gathered vs forge-reported metadata drift (the gap detector's
	// 5%% semantic). Drift is expected on high-velocity repos between
	// cycles → WARN, not FAIL; gap fill is the healer.
	var drifted int64
	if err := s.pool.QueryRow(ctx, `
		WITH sample AS (
			SELECT q.repo_id, q.last_issues, q.last_prs
			FROM aveloxis_ops.collection_queue q
			WHERE q.last_collected > NOW() - INTERVAL '7 days'
			ORDER BY random() LIMIT $1
		), meta AS (
			SELECT DISTINCT ON (ri.repo_id) ri.repo_id, ri.issues_count, ri.pr_count
			FROM aveloxis_data.repo_info ri
			JOIN sample s ON s.repo_id = ri.repo_id
			ORDER BY ri.repo_id, ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		)
		SELECT COUNT(*) FILTER (WHERE
			(m.issues_count > 0 AND abs(s.last_issues - m.issues_count)::float / m.issues_count > $2 / 100.0)
			OR (m.pr_count > 0 AND abs(s.last_prs - m.pr_count)::float / m.pr_count > $2 / 100.0)),
			COUNT(*)
		FROM sample s JOIN meta m USING (repo_id)`,
		opts.Sample, driftThresholdPct).Scan(&drifted, &sampled); err != nil {
		probeErr("gathered vs metadata drift", err)
	} else if drifted > 0 {
		add("gathered vs metadata drift", "WARN", "%d of %d recently-collected repos drift >%.0f%% from forge metadata — gap fill heals these on cycle", drifted, sampled, driftThresholdPct)
	} else {
		add("gathered vs metadata drift", "OK", "0 of %d sampled repos beyond the %.0f%% gap threshold", sampled, driftThresholdPct)
	}

	// Batch vs single stats equality (the v0.27.36/1e contract): the
	// two vulnerability-count paths must agree row for row.
	out = append(out, s.verifyBatchSingleAgreement(ctx, opts.Sample))

	// ── Fill rates (report-only unless floors configured) ────────

	var nullIDs, total int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE reporter_id IS NULL), COUNT(*) FROM aveloxis_data.issues`).Scan(&nullIDs, &total); err != nil {
		probeErr("identity fill (issues)", err)
	} else if total > 0 {
		fill := 100 * float64(total-nullIDs) / float64(total)
		sev, note := "OK", ""
		if opts.MinIdentityFill > 0 && fill < opts.MinIdentityFill {
			sev, note = "WARN", fmt.Sprintf(" (below configured floor %.1f%%)", opts.MinIdentityFill)
		}
		add("identity fill (issues)", sev, "issues.reporter_id %.2f%% filled%s", fill, note)
	}
	var unknownSev, totalVulns int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE severity = 'UNKNOWN' OR severity = ''), COUNT(*)
		FROM aveloxis_data.repo_deps_vulnerabilities WHERE resolved_at IS NULL`).Scan(&unknownSev, &totalVulns); err != nil {
		probeErr("severity fill (vulnerabilities)", err)
	} else if totalVulns > 0 {
		known := 100 * float64(totalVulns-unknownSev) / float64(totalVulns)
		sev, note := "OK", " (UNKNOWN = OSV carries no severity data; honest per the no-guess policy)"
		if opts.MinSeverityKnown > 0 && known < opts.MinSeverityKnown {
			sev, note = "WARN", fmt.Sprintf(" (below configured floor %.1f%%)", opts.MinSeverityKnown)
		}
		add("severity fill (vulnerabilities)", sev, "%.2f%% of current findings carry a severity%s", known, note)
	}

	return out
}

// verifyBatchSingleAgreement compares GetRepoStatsBatch's vulnerability
// counts against CountRepoVulnerabilities for a bounded sample of
// repos that HAVE findings — the surface where the two paths disagreed
// before v0.27.36 (batch counted resolved + self rows).
func (s *PostgresStore) verifyBatchSingleAgreement(ctx context.Context, sample int) VerifyResult {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT repo_id FROM aveloxis_data.repo_deps_vulnerabilities
		ORDER BY repo_id LIMIT LEAST($1, 10)`, sample)
	if err != nil {
		return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("probe failed: %v", err)}
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("probe scan failed: %v", err)}
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return VerifyResult{Check: "batch vs single stats", Severity: "OK", Detail: "no repos with findings to compare"}
	}
	batch, err := s.GetRepoStatsBatch(ctx, ids)
	if err != nil {
		return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("batch stats failed: %v", err)}
	}
	for _, id := range ids {
		single, critical, err := s.CountRepoVulnerabilities(ctx, id)
		if err != nil {
			return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("single count failed for repo %d: %v", id, err)}
		}
		b, ok := batch[id]
		if !ok || b.Vulnerabilities != single || b.CriticalVulns != critical {
			got := "missing"
			if ok {
				got = fmt.Sprintf("%d/%d", b.Vulnerabilities, b.CriticalVulns)
			}
			return VerifyResult{Check: "batch vs single stats", Severity: "FAIL",
				Detail: fmt.Sprintf("repo %d: batch says %s, single says %d/%d — the two dashboard paths disagree (the pre-v0.27.36 bug shape)", id, got, single, critical)}
		}
	}
	return VerifyResult{Check: "batch vs single stats", Severity: "OK", Detail: fmt.Sprintf("%d repos with findings agree on both paths", len(ids))}
}

// SortVerifyResults orders FAIL → WARN → OK (stable within severity).
func SortVerifyResults(results []VerifyResult) []VerifyResult {
	rank := map[string]int{"FAIL": 0, "WARN": 1, "OK": 2}
	out := make([]VerifyResult, 0, len(results))
	for _, sev := range []string{"FAIL", "WARN", "OK"} {
		for _, r := range results {
			if rank[r.Severity] == rank[sev] && r.Severity == sev {
				out = append(out, r)
			}
		}
	}
	return out
}
