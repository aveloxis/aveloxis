// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLoadSeedShape(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	s, err := loadSeedShape(write("ok.json", `{"fleet_repos": 10, "gone_stamped": 2, "adopted": 1, "contributors": 5, "source": "test"}`))
	if err != nil || s.AdoptedDepthFactor != 1 || s.FleetRepos != 10 {
		t.Fatalf("a shape loads, depth factor defaulting to 1 (the source's depth): %+v %v", s, err)
	}
	for name, body := range map[string]string{
		"negative.json": `{"fleet_repos": -1, "source": "test"}`,
		"unknown.json":  `{"fleet_repo": 10, "source": "test"}`,
		"nosource.json": `{"fleet_repos": 10}`,
		"broken.json":   `{"fleet_repos": `,
	} {
		if _, err := loadSeedShape(write(name, body)); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	if script := seedScript(s); !strings.HasPrefix(script, "SET LOCAL dtseed.adopted = '1';") || !strings.Contains(script, "forge.invalid") {
		t.Errorf("the script sets every dtseed value first, then the seed:\n%.300s", script)
	}
}

// The seed, in a transaction rolled back at the end, on a fixture with one
// deep repository whose daily buckets are complete: every effect the seed
// promises is checked, above all that it leaves the 0.29.140 repair real work
// (earlier-era buckets missing or below their fold, boundary-day buckets of
// the earlier authors holding only the new history's commits).
func TestTheSeedShapesTheFleetAndLeavesTheRepairItsWork(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var repoID int64
	if err := tx.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, commit_daily_complete_at)
		VALUES ('https://github.com/_avseed_fixture/deep', '_avseed_fixture', 'deep', 1, NOW()) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	// 40 days × 4 authors × 2 commits, each commit touching two files: the
	// deepest repository by far, with every day busy before and after noon.
	start := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		SELECT $1, md5(d || '-' || a || '-' || c), f, 'n', 'a' || a || '@x', to_char($2::timestamptz + d * INTERVAL '1 day', 'YYYY-MM-DD'),
		       $2::timestamptz + d * INTERVAL '1 day' + (6 + a * 3 + c * 2) * INTERVAL '1 hour'
		FROM generate_series(0, 39) d, generate_series(0, 3) a, generate_series(0, 1) c, unnest(ARRAY['f1.go', 'f2.go']) f`, repoID, start); err != nil {
		t.Fatal(err)
	}
	// An author with a commit every hour of every day straddles any era
	// start: on the boundary day the seed lowers that bucket to the new
	// history's commits (the mutation proof: without that step every other
	// assertion still held).
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		SELECT $1, md5('m-' || d || '-' || h), 'f1.go', 'n', 'm@x', to_char($2::timestamptz + d * INTERVAL '1 day', 'YYYY-MM-DD'),
		       $2::timestamptz + d * INTERVAL '1 day' + h * INTERVAL '1 hour' + INTERVAL '30 minutes'
		FROM generate_series(0, 39) d, generate_series(0, 23) h`, repoID, start); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits)
		SELECT repo_id, (cmt_author_timestamp AT TIME ZONE 'UTC')::date, cmt_author_email, COUNT(DISTINCT cmt_commit_hash)
		FROM aveloxis_data.commits WHERE repo_id = $1 GROUP BY 1, 2, 3`, repoID); err != nil {
		t.Fatal(err)
	}
	// A deeper repository whose daily table is not complete: the seed must
	// not adopt it (data-verify's earlier-era check would judge nothing).
	var incomplete int64
	if err := tx.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/_avseed_fixture/incomplete', '_avseed_fixture', 'incomplete', 1) RETURNING repo_id`).Scan(&incomplete); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		SELECT $1, md5('i-' || g), 'f.go', 'n', 'i@x', '2025-01-01', '2025-01-01 12:00:00+00'::timestamptz + g * INTERVAL '1 minute'
		FROM generate_series(1, 5000) g`, incomplete); err != nil {
		t.Fatal(err)
	}
	var repos, gone, contributors, aliases, identities int64
	if err := tx.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM aveloxis_data.repos), (SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_gone_at IS NOT NULL),
		(SELECT COUNT(*) FROM aveloxis_data.contributors), (SELECT COUNT(*) FROM aveloxis_data.contributors_aliases),
		(SELECT COUNT(*) FROM aveloxis_data.contributor_identities)`).Scan(&repos, &gone, &contributors, &aliases, &identities); err != nil {
		t.Fatal(err)
	}
	shape := seedShape{FleetRepos: repos + 30, GoneStamped: gone + 5, Adopted: 1, AdoptedDepthFactor: 2,
		Contributors: contributors + 20, ContributorsAliases: aliases + 7, ContributorIdentities: identities + 9, Source: "test"}
	if _, err := tx.Exec(ctx, seedScript(shape), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("seed: %v", err)
	}
	counts, err := readSeedCounts(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Repos != shape.FleetRepos || counts.Synthetic < 30 || counts.Gone != shape.GoneStamped ||
		counts.Contributors != shape.Contributors || counts.Aliases != shape.ContributorsAliases || counts.Identities != shape.ContributorIdentities {
		t.Errorf("the seed tops every count up to the shape: %+v, shape %+v", counts, shape)
	}
	var realHost, goneQueued int64
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_owner = 'dtseed' AND repo_git NOT LIKE 'https://forge.invalid/%'),
		(SELECT COUNT(*) FROM aveloxis_data.repos r JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		  WHERE r.repo_owner = 'dtseed' AND r.repo_gone_at IS NOT NULL)`).Scan(&realHost, &goneQueued); err != nil {
		t.Fatal(err)
	}
	if realHost != 0 || goneQueued != 0 {
		t.Errorf("synthetic repositories live on forge.invalid only (%d elsewhere) and the gone ones are queueless (%d queued)", realHost, goneQueued)
	}
	var adoptedRepo int64
	var created time.Time
	if err := tx.QueryRow(ctx, `SELECT repo_id, forge_created_at FROM aveloxis_data.repo_forge_id_changes WHERE adopted_by = 'dtseed'`).Scan(&adoptedRepo, &created); err != nil {
		t.Fatalf("one adopted change: %v", err)
	}
	if adoptedRepo != repoID {
		t.Fatalf("the adoption lands on the deepest repository %d, not %d", repoID, adoptedRepo)
	}
	var commitRows int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&commitRows); err != nil {
		t.Fatal(err)
	}
	if want := int64(2 * (40*4*2*2 + 40*24)); commitRows != want {
		t.Errorf("depth factor 2 doubles the commit rows: %d, want %d", commitRows, want)
	}
	var straddler, after int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE((SELECT d.commits FROM aveloxis_data.repo_commit_daily d
		                  WHERE d.repo_id = $1 AND d.author_email = 'm@x' AND d.day = ($2 AT TIME ZONE 'UTC')::date), 0),
		       (SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits
		         WHERE repo_id = $1 AND cmt_author_email = 'm@x' AND cmt_author_timestamp >= $2
		           AND (cmt_author_timestamp AT TIME ZONE 'UTC')::date = ($2 AT TIME ZONE 'UTC')::date)`,
		repoID, created).Scan(&straddler, &after); err != nil {
		t.Fatal(err)
	}
	if after == 0 || straddler != after {
		t.Errorf("the straddling author's boundary-day bucket holds only the new history's commits: %d, want %d", straddler, after)
	}
	// The repair's work: buckets below their fold before the boundary, and
	// on the boundary day for the authors with a commit before the era start.
	var earlierBelow, boundaryBelow int64
	if err := tx.QueryRow(ctx, `
		WITH fold AS (
			SELECT (cmt_author_timestamp AT TIME ZONE 'UTC')::date AS day, cmt_author_email AS email, COUNT(DISTINCT cmt_commit_hash) AS n,
			       bool_or(cmt_author_timestamp < $2) AS before
			FROM aveloxis_data.commits WHERE repo_id = $1 GROUP BY 1, 2)
		SELECT COUNT(*) FILTER (WHERE f.day < ($2 AT TIME ZONE 'UTC')::date AND COALESCE(d.commits, 0) < f.n),
		       COUNT(*) FILTER (WHERE f.day = ($2 AT TIME ZONE 'UTC')::date AND f.before AND COALESCE(d.commits, 0) < f.n)
		FROM fold f LEFT JOIN aveloxis_data.repo_commit_daily d ON d.repo_id = $1 AND d.day = f.day AND d.author_email = f.email`,
		repoID, created).Scan(&earlierBelow, &boundaryBelow); err != nil {
		t.Fatal(err)
	}
	if earlierBelow == 0 || boundaryBelow == 0 {
		t.Errorf("the seed leaves the repair work: %d earlier-era and %d boundary-day buckets below their fold (both must be > 0)", earlierBelow, boundaryBelow)
	}
}

// The seed refuses a source it already shaped (the L10 pass: a second seed
// multiplied the adopted repositories' commits again).
func TestTheSeedRefusesASeededSource(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://forge.invalid/dtseed/r0', 'dtseed', 'r0', 1)`); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, seedScript(seedShape{Source: "test", AdoptedDepthFactor: 1}), pgx.QueryExecModeSimpleProtocol)
	if err == nil || !strings.Contains(err.Error(), "already seeded") {
		t.Fatalf("a seeded source is refused: %v", err)
	}
}

// A shape asking for more adoptions than the source can supply is refused,
// saying why (the fourth L10 pass: a short adoption left the release's
// earlier-era rules unmet with no pointer to the seed).
func TestTheSeedRefusesAShortAdoption(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, seedScript(seedShape{Source: "test", Adopted: 1000000, AdoptedDepthFactor: 1}), pgx.QueryExecModeSimpleProtocol)
	if err == nil || !strings.Contains(err.Error(), "asks for 1000000 adopted changes") {
		t.Fatalf("a short adoption is refused, saying why: %v", err)
	}
}

// A base that already ran a newer release's one-shot step is refused: the
// local migrate would skip it, and its work would never be compared.
func TestCheckNoNewerLedgerSteps(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	if err := checkNoNewerLedgerSteps(ctx, dsn); err != nil {
		t.Fatalf("a database migrated by this binary has no newer step: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	const label = "v9999.0.0 a step from the future (TestCheckNoNewerLedgerSteps)"
	// No recorded binary (tool_version ''): the label's version decides.
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.migration_ledger (step_label, tool_version) VALUES ($1, '')`, label); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	})
	err = checkNoNewerLedgerSteps(ctx, dsn)
	if err == nil || !strings.Contains(err.Error(), label) {
		t.Fatalf("a newer release's step is refused, naming it: %v", err)
	}
}

// The binary a ledger row records outranks its label: an old step ledgered by
// a newer release (labels keep their introducing version) is that release's.
func TestCheckNoNewerLedgerStepsReadsTheRecordedVersion(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	const label = "v0.1.0 an old step a future release ledgered (TestCheckNoNewerLedgerStepsReadsTheRecordedVersion)"
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.migration_ledger (step_label, tool_version) VALUES ($1, '9999.0.0')`, label); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	})
	if err := checkNoNewerLedgerSteps(ctx, dsn); err == nil || !strings.Contains(err.Error(), "9999.0.0") {
		t.Fatalf("the recorded newer binary is refused: %v", err)
	}
}

// The shipped 0.29.141 rewind, in a transaction rolled back (PostgreSQL DDL is
// transactional): on a database this binary migrated it leaves the 0.29.139
// schema — no gone history, no observed_private, no lower(email) indexes, no
// ledger row of a newer release, the stamp 0.29.139.
func TestTheShippedRewindLeavesTheReleasedSchema(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	raw, err := os.ReadFile("testdata/rewind-0.29.141.sql")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// An older release's ledger row must survive (the rewind removes only
	// what 0.29.140+ wrote; the L10 pass: a delete-everything mutant passed).
	const older = "v0.29.9 an older release's step (TestTheShippedRewindLeavesTheReleasedSchema)"
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_ops.migration_ledger (step_label, tool_version) VALUES ($1, '0.29.9')`, older); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(raw), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("rewind: %v", err)
	}
	var kept bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM aveloxis_ops.migration_ledger WHERE step_label = $1)`, older).Scan(&kept); err != nil || !kept {
		t.Fatalf("an older release's ledger row survives the rewind: %v %v", kept, err)
	}
	// Newer than 0.29.139 by version order (a text comparison puts 0.29.9
	// above 0.29.140).
	rows, err := tx.Query(ctx, `SELECT step_label, tool_version FROM aveloxis_ops.migration_ledger`)
	if err != nil {
		t.Fatal(err)
	}
	var newerLedger int64
	for rows.Next() {
		var label, v string
		if err := rows.Scan(&label, &v); err != nil {
			t.Fatal(err)
		}
		if lv, ok := ledgerLabelVersion(label); ok && v == "" {
			v = lv
		}
		if v != "" && v != "0.29.139" && db.SchemaVersionAtLeast(v, "0.29.139") {
			newerLedger++
		}
	}
	rows.Close()
	var history, column, indexes int64
	var stamp string
	if err := tx.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'aveloxis_data' AND tablename = 'repo_gone_history'),
		(SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'aveloxis_data' AND table_name = 'repo_forge_id_changes' AND column_name = 'observed_private'),
		(SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'aveloxis_data'
		   AND indexname IN ('idx_contributors_email_lower', 'idx_contributors_aliases_email_lower', 'idx_contributor_identities_email_lower')),
		(SELECT schema_version FROM aveloxis_ops.schema_meta)`).Scan(&history, &column, &indexes, &stamp); err != nil {
		t.Fatal(err)
	}
	if history != 0 || column != 0 || indexes != 0 || newerLedger != 0 || stamp != "0.29.139" {
		t.Errorf("the rewind leaves the 0.29.139 schema: gone history %d, observed_private %d, lower(email) indexes %d, newer ledger rows %d, stamp %s",
			history, column, indexes, newerLedger, stamp)
	}
}
