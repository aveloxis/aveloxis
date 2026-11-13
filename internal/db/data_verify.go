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
	"regexp"
	"strings"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/jackc/pgx/v5/pgconn"
)

// VerifyResult is one probe outcome.
type VerifyResult struct {
	Check    string `json:"check"`
	Severity string `json:"severity"` // "FAIL" | "WARN" | "OK"
	Detail   string `json:"detail"`
	// Sampled: the probe reads a random sample, so its detail differs run to
	// run on the same data (data-test's snapshot baseline compares such a
	// finding by check and severity only).
	Sampled bool `json:"sampled,omitempty"`
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
	// DeterministicSample draws the sampled probes' repositories by a hash of
	// the repo_id instead of at random, so two copies of one database draw the
	// same rows and their findings compare exactly (data-test's snapshot
	// mode). A production run keeps the random sample: over many runs it
	// covers the fleet, a fixed one never would.
	DeterministicSample bool
}

// sampleOrder is the sampled probes' ORDER BY over a column holding the
// repo_id.
func (o VerifyOptions) sampleOrder(repoID string) string {
	if o.DeterministicSample {
		return "hashtext(" + repoID + "::text), " + repoID
	}
	return "random()"
}

// markSampled flags the named check's findings as read from a random sample.
func markSampled(results []VerifyResult, check string) {
	for i := range results {
		if results[i].Check == check {
			results[i].Sampled = true
		}
	}
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

	// ── The gone history and the earlier era (0.29.140) ──────────
	// Each probe names up to five example repo_ids, the newest first (the
	// rows an incident just wrote), so an operator (and a test) can find
	// them. These run on a fleet after the deploy too.
	for _, inv := range []struct {
		check, severity, query, bad, ok string
	}{
		{
			check: "gone episodes match the stamps", severity: "FAIL",
			// Exactly one open episode per gone-stamped repository, none on
			// a reachable one (the gone marker opens it with the stamp, every
			// clearer closes it with the clear, in one transaction).
			query: `SELECT COUNT(*), COALESCE((array_agg(r.repo_id ORDER BY r.repo_id DESC))[1:5]::text, '{}')
				FROM aveloxis_data.repos r
				LEFT JOIN LATERAL (SELECT COUNT(*) AS open_n FROM aveloxis_data.repo_gone_history h
				                    WHERE h.repo_id = r.repo_id AND h.restored_at IS NULL) o ON TRUE
				WHERE (r.repo_gone_at IS NOT NULL AND o.open_n <> 1) OR (r.repo_gone_at IS NULL AND o.open_n > 0)`,
			bad: "%d repositories whose open gone episodes disagree with the stamp (a stamped one needs exactly one, a reachable one none) — e.g. repo_id %s",
			ok:  "every gone-stamped repository has one open episode, no reachable one has any",
		},
		{
			check: "gone and queued", severity: "WARN",
			// A pending re-probe: a user's re-add of a gone repository, which
			// prelim's next probe clears or sidelines. Many of them, or old
			// ones, mean the probe is not reaching them.
			query: `SELECT COUNT(*), COALESCE((array_agg(r.repo_id ORDER BY r.repo_id DESC))[1:5]::text, '{}')
				FROM aveloxis_data.repos r
				WHERE r.repo_gone_at IS NOT NULL
				  AND EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = r.repo_id)`,
			bad: "%d gone-stamped repositories are queued — pending re-probes (a re-add); prelim's next probe settles each — e.g. repo_id %s",
			ok:  "no gone-stamped repository is queued",
		},
		{
			check: "repositories without owner or name", severity: "WARN",
			// A row that is not a repository's (0.29.146). Two paths wrote
			// them before that release: the add path stored a URL that did
			// not parse as one (an organization's page) as a git-only row
			// with no owner and no name, never cloned; and a redirect to a
			// URL that did not parse blanked the owner and name of a row
			// that may hold collected data. Nothing repairs either: the
			// operator looks at each (the upgrade guide has the query).
			query: `SELECT COUNT(*), COALESCE((array_agg(r.repo_id ORDER BY r.repo_id DESC))[1:5]::text, '{}')
				FROM aveloxis_data.repos r
				WHERE COALESCE(r.repo_owner, '') = '' OR COALESCE(r.repo_name, '') = ''`,
			bad: "%d repository rows have no owner or no name (written before 0.29.146: an add of a URL that is not a repository's, such as an organization page, or a redirect to one) — look at each row's repo_git and whether it holds collected data before removing anything; the upgrade guide's 0.29.146 entry has the steps — e.g. repo_id %s",
			ok:  "every repository row has an owner and a name",
		},
		{
			check: "repositories at a forge account page", severity: "WARN",
			// A GitHub or GitLab row whose URL is under the forge's own
			// account space (platform.ForgeAccountSegments): before
			// 0.29.146 a redirect to the sign-in page was stored as the
			// repository's new URL (gitlab.com sends a project that went
			// private to /users/sign_in), and every later repository
			// redirected there was taken for that row's duplicate. A real
			// namespace can carry one of the names on GitLab, so this is a
			// WARN to look at, not a verdict.
			query: `SELECT COUNT(*), COALESCE((array_agg(r.repo_id ORDER BY r.repo_id DESC))[1:5]::text, '{}')
				FROM aveloxis_data.repos r
				WHERE r.platform_id IN (1, 2) AND r.repo_git ~* '` + forgeAccountPageRegex() + `'`,
			bad: "%d GitHub or GitLab repository rows have a URL under the forge's own account pages (a sign-in page stored as a redirect target before 0.29.146, unless the namespace is real) — look at each row's repo_git; the upgrade guide's 0.29.146 entry has the steps — e.g. repo_id %s",
			ok:  "no GitHub or GitLab repository row is stored under the forge's account pages",
		},
		{
			check: "earlier-era daily buckets", severity: "FAIL",
			// An adopted forge-ID change keeps the earlier repository's
			// commits in the daily table: no bucket before the era boundary
			// may sit below its fold from the append-only commits table (a
			// lowered bucket is the shape the 0.29.140 repair raises). Only
			// repositories whose daily table is complete; each one read
			// through a LATERAL scan of its own rows (a subquery with GROUP
			// BY is never flattened into a whole-table join).
			query: `WITH eras AS (
				SELECT c.repo_id, MAX((COALESCE(c.forge_created_at, c.first_observed_at) AT TIME ZONE 'UTC')::date) AS boundary
				FROM aveloxis_data.repo_forge_id_changes c
				JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id AND r.commit_daily_complete_at IS NOT NULL
				WHERE c.adopted_at IS NOT NULL
				GROUP BY c.repo_id)
				SELECT COUNT(*), COALESCE((array_agg(DISTINCT f.repo_id ORDER BY f.repo_id DESC))[1:5]::text, '{}')
				FROM eras e
				CROSS JOIN LATERAL (
					SELECT co.repo_id, (co.cmt_author_timestamp AT TIME ZONE 'UTC')::date AS day, co.cmt_author_email AS email,
					       COUNT(DISTINCT co.cmt_commit_hash) AS n
					FROM aveloxis_data.commits co
					WHERE co.repo_id = e.repo_id AND co.cmt_author_timestamp IS NOT NULL
					  AND (co.cmt_author_timestamp AT TIME ZONE 'UTC')::date < e.boundary
					GROUP BY 1, 2, 3) f
				LEFT JOIN aveloxis_data.repo_commit_daily d
				       ON d.repo_id = f.repo_id AND d.day = f.day AND d.author_email = f.email
				WHERE COALESCE(d.commits, 0) < f.n`,
			bad: "%d earlier-era daily buckets sit below their fold from commits (the charts undercount the earlier repository) — e.g. repo_id %s",
			ok:  "every earlier-era bucket of an adopted change holds its commits",
		},
	} {
		var examples string
		if err := s.pool.QueryRow(ctx, inv.query).Scan(&n, &examples); err != nil {
			probeErr(inv.check, err)
		} else if n > 0 {
			add(inv.check, inv.severity, inv.bad, n, examples)
		} else {
			add(inv.check, "OK", "%s", inv.ok)
		}
	}

	// INVALID indexes (0.29.142; the advice and the in-progress split
	// 0.29.144). See invalidIndexFindings for what each count means. The
	// progress rows are matched by this database (index OIDs are per
	// database; the view is cluster-wide). A build by another role is
	// visible only to a role with pg_read_all_stats: without it the row keeps
	// pid and datid and its index_relid is NULL, so such a build is counted
	// as hidden and its index reads INVALID.
	var building, hidden int64
	var invalidNames, buildingNames string
	if err := s.pool.QueryRow(ctx, `
		WITH progress AS (
			SELECT p.index_relid FROM pg_stat_progress_create_index p
			WHERE p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())),
		v AS (
			SELECT n.nspname || '.' || i.relname AS name,
			       EXISTS (SELECT 1 FROM progress p WHERE p.index_relid = i.oid) AS building
			FROM pg_index x
			JOIN pg_class i ON i.oid = x.indexrelid
			JOIN pg_namespace n ON n.oid = i.relnamespace
			WHERE NOT x.indisvalid AND n.nspname IN ('aveloxis_data', 'aveloxis_ops', 'aveloxis_scan'))
		SELECT (SELECT COUNT(*) FROM v WHERE NOT building),
		       COALESCE((SELECT (array_agg(name ORDER BY name))[1:5]::text FROM v WHERE NOT building), '{}'),
		       (SELECT COUNT(*) FROM v WHERE building),
		       COALESCE((SELECT (array_agg(name ORDER BY name))[1:5]::text FROM v WHERE building), '{}'),
		       (SELECT COUNT(*) FROM progress WHERE index_relid IS NULL)`).Scan(&n, &invalidNames, &building, &buildingNames, &hidden); err != nil {
		probeErr("invalid indexes", err)
		probeErr("index builds in progress", err)
	} else {
		out = append(out, invalidIndexFindings(n, invalidNames, building, buildingNames, hidden)...)
	}

	// The case-insensitive e-mail indexes (0.29.141): built by the migrate,
	// CONCURRENTLY; missing means the migrate has not run (or a build failed
	// and was logged).
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM unnest(ARRAY['idx_contributors_email_lower', 'idx_contributors_aliases_email_lower',
		                                  'idx_contributor_identities_email_lower']) AS want(name)
		WHERE NOT EXISTS (SELECT 1 FROM pg_class i JOIN pg_namespace n ON n.oid = i.relnamespace
		                  JOIN pg_index x ON x.indexrelid = i.oid
		                  WHERE n.nspname = 'aveloxis_data' AND i.relname = want.name AND x.indisvalid)`).Scan(&n); err != nil {
		probeErr("lower(email) indexes", err)
	} else if n > 0 {
		add("lower(email) indexes", "WARN", "%d of the three lower(email) indexes are missing or invalid — run `aveloxis migrate`", n)
	} else {
		add("lower(email) indexes", "OK", "the three lower(email) indexes are valid")
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
			ORDER BY `+opts.sampleOrder("repo_id")+` LIMIT LEAST($1, 30)
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
	if !opts.DeterministicSample {
		markSampled(out, "cached counts vs actual")
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
			ORDER BY `+opts.sampleOrder("q.repo_id")+` LIMIT $1
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
	if !opts.DeterministicSample {
		markSampled(out, "gathered vs metadata drift")
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
// counts against CountRepoVulnerabilityClasses (every class, the
// unknown-version count included — v0.29.70) for a bounded sample of
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
	if err := rows.Err(); err != nil {
		return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("probe read failed: %v", err)}
	}
	if len(ids) == 0 {
		return VerifyResult{Check: "batch vs single stats", Severity: "OK", Detail: "no repos with findings to compare"}
	}
	batch, err := s.GetRepoStatsBatch(ctx, ids)
	if err != nil {
		return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("batch stats failed: %v", err)}
	}
	for _, id := range ids {
		single, err := s.CountRepoVulnerabilityClasses(ctx, id)
		if err != nil {
			return VerifyResult{Check: "batch vs single stats", Severity: "FAIL", Detail: fmt.Sprintf("single count failed for repo %d: %v", id, err)}
		}
		b := batch[id]
		if !statsAgree(b, single) {
			got := "missing"
			if b != nil {
				got = fmt.Sprintf("%d/%d/%d", b.Vulnerabilities, b.CriticalVulns, b.VulnsVersionUnknown)
			}
			return VerifyResult{Check: "batch vs single stats", Severity: "FAIL",
				Detail: fmt.Sprintf("repo %d: batch says %s, single says %d/%d/%d (exposure/critical/version unknown) — the two dashboard paths disagree (the pre-v0.27.36 bug shape)",
					id, got, single.Exposure, single.Critical, single.UnknownVersion)}
		}
	}
	return VerifyResult{Check: "batch vs single stats", Severity: "OK", Detail: fmt.Sprintf("%d repos with findings agree on both paths", len(ids))}
}

// statsAgree reports whether a batch stats row carries the single path's
// counts — every class, the unknown-version count included (v0.29.70 review
// round 7). A missing row disagrees.
func statsAgree(b *RepoStats, c VulnClassCounts) bool {
	return b != nil && b.Vulnerabilities == c.Exposure && b.CriticalVulns == c.Critical && b.VulnsVersionUnknown == c.UnknownVersion
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

// invalidIndexFindings words the INVALID-index results (0.29.142; 0.29.144).
// invalid counts INVALID indexes in the Aveloxis schemas with no visible
// build in progress, building those with one, and hidden the builds in this
// database whose index this role cannot see (another role's, without
// pg_read_all_stats).
//
// An interrupted concurrent build leaves an INVALID index that serves no
// query and still slows every write. The migrate drops and rebuilds only the
// indexes it builds itself, so the advice covers the other kind too (kate's
// idx_contributor_repo_repo_git, an aborted hand-run build no migrate
// touches): drop it concurrently. A build still running is INVALID until it
// finishes — an interrupted migrate leaves its build running on the server —
// so it is a WARN, never an index to drop.
func invalidIndexFindings(invalid int64, invalidNames string, building int64, buildingNames string, hidden int64) []VerifyResult {
	var out []VerifyResult
	if invalid > 0 {
		detail := fmt.Sprintf("%d INVALID indexes (an interrupted concurrent build: it serves no query and slows every write) — e.g. %s. "+
			"First confirm no build of it is running: SELECT pid, phase FROM pg_stat_progress_create_index WHERE index_relid = '<schema.index>'::regclass "+
			"AND datid = (SELECT oid FROM pg_database WHERE datname = current_database()) returns no row, run as a role with pg_read_all_stats. "+
			"Then drop it with DROP INDEX CONCURRENTLY <schema.index>; the next `aveloxis migrate` rebuilds it if it is one the migrate builds, and any other (a hand-run build) stays dropped",
			invalid, invalidNames)
		if hidden > 0 {
			detail += fmt.Sprintf(". Caution: %d index builds of any kind are running in this database under a role this one cannot see (it lacks pg_read_all_stats), and an INVALID index above may be one of them", hidden)
		}
		out = append(out, VerifyResult{Check: "invalid indexes", Severity: "FAIL", Detail: detail})
	} else {
		out = append(out, VerifyResult{Check: "invalid indexes", Severity: "OK", Detail: "no INVALID index"})
	}
	var running []string
	if building > 0 {
		running = append(running, fmt.Sprintf("%d concurrent builds of INVALID indexes are running (INVALID until they finish; an interrupted migrate leaves its build running on the server) — e.g. %s", building, buildingNames))
	}
	if hidden > 0 {
		running = append(running, fmt.Sprintf("%d index builds of any kind are running under a role this one cannot see (it lacks pg_read_all_stats)", hidden))
	}
	if len(running) > 0 {
		out = append(out, VerifyResult{Check: "index builds in progress", Severity: "WARN", Detail: strings.Join(running, "; ") + "; rerun data-verify when they finish"})
	} else {
		out = append(out, VerifyResult{Check: "index builds in progress", Severity: "OK", Detail: "no concurrent index build is running"})
	}
	return out
}

// forgeAccountPageRegex is the POSIX pattern (matched with ~*, any case)
// of a URL whose first path segment is one of
// platform.ForgeAccountSegments and which has a second segment: the SQL
// form of platform.IsForgeAccountURL's path rule, built from the one list.
// The segments are constants of this program, quoted for the pattern.
func forgeAccountPageRegex() string {
	quoted := make([]string, 0, len(platform.ForgeAccountSegments))
	for _, seg := range platform.ForgeAccountSegments {
		quoted = append(quoted, regexp.QuoteMeta(seg))
	}
	return `^https?://[^/]+/(` + strings.Join(quoted, "|") + `)/[^/]`
}
