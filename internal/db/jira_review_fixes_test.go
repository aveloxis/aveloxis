// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Adversarial-round fixes on the v0.29.0 Jira subsystem (findings 1, 2,
// 5, 9, 10, 15 of the 2026-08-30 fresh-context pass, plus the L10
// round's additions). Behavioral tests were RED against the pre-fix
// code except where noted (the sequential mint test pins idempotency
// only — the race half is source-pinned).

// TestUpsertJiraIssueSurfacesNonUniqueErrors is finding #1a (HIGH): the
// native-LINK fallback must fire ONLY on a genuine 23505 — any other
// insert error (transient, encode, constraint) must surface, or the
// caller marks the staging row processed while the API's state/title
// write was silently dropped and the incremental JQL never re-fetches it.
func TestUpsertJiraIssueSurfacesNonUniqueErrors(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	// Seed the mail-minted synthetic the projection would create, so the
	// buggy fallback's key probe has a row to "succeed" onto.
	key := "AVJR-701"
	pid := syntheticIssueID(key)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title,
			issue_state, external_key, data_source)
		VALUES ($1, $2, 701, 't', 'open', $3, 'JIRA')
		ON CONFLICT DO NOTHING`, jsRepoID, pid, key)

	// An invalid reporter UUID makes the INSERT fail with 22P02 — a
	// NON-unique error. Pre-fix: the fallback probed the synthetic,
	// enriched jira_issue_id only, and returned success.
	_, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key, JiraIssueID: 9701,
		Title: "real title", Status: "Resolved",
		ResolutionDate: time.Now(), Updated: time.Now(),
		ReporterCntrb: "not-a-uuid",
	})
	if err == nil {
		t.Fatal("non-23505 insert error must surface, not be swallowed by the native-LINK fallback")
	}
	var state string
	if qerr := store.pool.QueryRow(ctx,
		`SELECT issue_state FROM aveloxis_data.issues WHERE repo_id=$1 AND external_key=$2`,
		jsRepoID, key).Scan(&state); qerr != nil {
		t.Fatal(qerr)
	}
	if state != "open" {
		t.Fatalf("synthetic must be untouched by the failed write, state=%q", state)
	}
}

// TestUpsertJiraIssueNonUniqueErrorNeverLinksNative — REWRITTEN by
// Copilot round 12 on PR #193. The L10 original drove a 22P02 (bad
// reporter UUID) beside a native key holder and required it to
// SURFACE, because pre-round-12 the flow reached the INSERT first and
// a deleted 23505 gate would have mis-LINKed on the failure. With
// LINK-first probes the INSERT never runs for a native holder: the
// key probe LINKs directly, and the columns that could 22P02
// (reporter/state) are never sent — they are forge-owned on native
// rows (rank 1) and were never applied by the LINK tail anyway. The
// contract this pins now: a native holder absorbs ANY envelope —
// malformed reporter included — as a clean LINK that enriches
// jira_issue_id and touches nothing forge-owned (the envelope says
// Resolved; the native row must stay open). The 23505 gate itself
// remains in the source as the probe-to-insert race backstop; the
// synthetic-seed sibling above still proves non-unique errors surface
// wherever the INSERT actually runs.
func TestUpsertJiraIssueNonUniqueErrorNeverLinksNative(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	key := "AVJR-704"
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title,
			issue_state, external_key, data_source)
		VALUES ($1, 704001, 704, 'native', 'open', $2, 'GitHub API')
		ON CONFLICT DO NOTHING`, jsRepoID, key)

	id, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key, JiraIssueID: 9704,
		Title: "t", Status: "Resolved", ResolutionDate: time.Now(),
		Updated: time.Now(), ReporterCntrb: "not-a-uuid",
	})
	if err != nil {
		t.Fatalf("a native key holder must absorb the envelope as a LINK (reporter/state are never written to native rows): %v", err)
	}
	var jid *int64
	var state string
	var pid int64
	if qerr := store.pool.QueryRow(ctx,
		`SELECT issue_id, platform_issue_id, issue_state, jira_issue_id
		 FROM aveloxis_data.issues WHERE repo_id=$1 AND external_key=$2`,
		jsRepoID, key).Scan(&id, &pid, &state, &jid); qerr != nil {
		t.Fatal(qerr)
	}
	if pid != 704001 {
		t.Fatalf("platform_issue_id = %d — the LINK must land on the native row, never mint a synthetic beside it", pid)
	}
	if jid == nil || *jid != 9704 {
		t.Fatalf("jira_issue_id = %v, want the enriched 9704", jid)
	}
	if state != "open" {
		t.Fatalf("issue_state = %q — the envelope's Resolved must not close a forge-owned row (rank 1)", state)
	}
}

// TestUpsertJiraIssueRefusesSyntheticLinkTarget is finding #1b: when the
// 23505 comes from the (repo_id, external_key) unique but the row holding
// the key is NOT native (platform_issue_id < 0 under a foreign id — an
// anomaly, e.g. a hand-inserted row), the "Rank 1 protection" must be
// real: error out rather than enrich-and-report-success.
func TestUpsertJiraIssueRefusesSyntheticLinkTarget(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	key := "AVJR-702"
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title,
			issue_state, external_key, data_source)
		VALUES ($1, -999702, 702, 't', 'open', $2, 'JIRA')
		ON CONFLICT DO NOTHING`, jsRepoID, key)

	_, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key, JiraIssueID: 9702,
		Title: "t", Status: "Open", Updated: time.Now(),
	})
	if err == nil {
		t.Fatal("a key-collision row that is not native (pid < 0, foreign id) must error, never LINK")
	}
}

// TestJiraStagingBatchReadsLiveRegistrationRepo is finding #2 (MEDIUM):
// rows staged before the project's repo mapping existed must heal when
// the operator fixes the registration — the drain reads the repo through
// jira_project_serve, not the frozen staged copy.
func TestJiraStagingBatchReadsLiveRegistrationRepo(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", nil); err != nil {
		t.Fatal(err)
	}
	var jpsID int64
	if err := store.pool.QueryRow(ctx,
		`SELECT jps_id FROM aveloxis_ops.jira_project_serve WHERE project_key='AVJR'`).Scan(&jpsID); err != nil {
		t.Fatal(err)
	}
	if err := store.StageJiraIssue(ctx, jpsID, "AVJR", "AVJR-801", time.Now(), nil, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	// Operator heals the registration.
	rid := int64(jsRepoID)
	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", &rid); err != nil {
		t.Fatal(err)
	}
	batch, err := store.GetJiraStagingBatch(ctx, jpsID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 staged row, got %d", len(batch))
	}
	if batch[0].RepoID == nil || *batch[0].RepoID != jsRepoID {
		t.Fatalf("staged row must read the registration's healed repo_id, got %v", batch[0].RepoID)
	}

	// L10 #4: the registration WINS over a wrong staged snapshot too —
	// the staged copy is only jira_project_serve's value at stage time,
	// so preferring it would leave wrong-repo rows draining wrong after
	// the operator's fix.
	wrong := int64(999_999_999)
	if err := store.StageJiraIssue(ctx, jpsID, "AVJR", "AVJR-802", time.Now(), &wrong, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	batch, err = store.GetJiraStagingBatch(ctx, jpsID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range batch {
		if row.IssueKey == "AVJR-802" {
			if row.RepoID == nil || *row.RepoID != jsRepoID {
				t.Fatalf("registration must win over the stale staged snapshot, got %v", row.RepoID)
			}
		}
	}
}

// TestJiraStateClosedFromResolution is finding #5 (MEDIUM): ASF Jira
// workflows carry per-project custom terminal status names — a set
// resolution (or resolutiondate) means CLOSED regardless of the status
// vocabulary; keying only on Resolved/Closed/Done writes rank-2 "open"
// over a mail-derived close.
func TestJiraStateClosedFromResolution(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	id, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: "AVJR-703", JiraIssueID: 9703,
		Title: "custom workflow", Status: "Delivered", Resolution: "Fixed",
		ResolutionDate: time.Now().Add(-time.Hour), Updated: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var state string
	var closedAt *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT issue_state, closed_at FROM aveloxis_data.issues WHERE issue_id=$1`,
		id).Scan(&state, &closedAt); err != nil {
		t.Fatal(err)
	}
	if state != "closed" || closedAt == nil {
		t.Fatalf("resolution-bearing issue must be closed with closed_at, got state=%q closed_at=%v", state, closedAt)
	}

	// L10 #2: each OR-term must close ALONE. (a) resolution NAME set,
	// date cleared — the legacy/bulk-import shape; closed_at stays NULL.
	idA, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: "AVJR-705", JiraIssueID: 9705,
		Title: "t", Status: "Custom Terminal", Resolution: "Fixed", Updated: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var stateA string
	var closedA *time.Time
	if err := store.pool.QueryRow(ctx,
		`SELECT issue_state, closed_at FROM aveloxis_data.issues WHERE issue_id=$1`,
		idA).Scan(&stateA, &closedA); err != nil {
		t.Fatal(err)
	}
	if stateA != "closed" || closedA != nil {
		t.Fatalf("resolution-name-only must close with NULL closed_at, got %q %v", stateA, closedA)
	}
	// (b) resolutiondate set, resolution name absent.
	idB, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: "AVJR-706", JiraIssueID: 9706,
		Title: "t", Status: "Whatever", ResolutionDate: time.Now().Add(-time.Hour), Updated: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var stateB string
	if err := store.pool.QueryRow(ctx,
		`SELECT issue_state FROM aveloxis_data.issues WHERE issue_id=$1`, idB).Scan(&stateB); err != nil {
		t.Fatal(err)
	}
	if stateB != "closed" {
		t.Fatalf("resolutiondate-only must close, got %q", stateB)
	}
}

// TestRegisterJiraProjectRefusesSecondInstance is finding #10 (LOW):
// jira_identities and the comment arbiter are instance-blind (usernames
// and comment ids are unique only PER Jira instance), so a second
// distinct base_url must be refused until instance scoping exists.
func TestRegisterJiraProjectRefusesSecondInstance(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", nil); err != nil {
		t.Fatal(err)
	}
	err := store.RegisterJiraProject(ctx, "AVJR2", "https://other-jira.example.org", nil)
	if err == nil {
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key='AVJR2'`)
		})
		t.Fatal("a second distinct base_url must be refused — identity/comment keys are instance-blind")
	}
	if !strings.Contains(err.Error(), "instance") {
		t.Fatalf("refusal should name the instance-scoping constraint, got: %v", err)
	}
	// Same base_url on another project is fine.
	if err := store.RegisterJiraProject(ctx, "AVJR2", "https://jira.example.org", nil); err != nil {
		t.Fatalf("same-instance second project must register: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key='AVJR2'`)
	})
}

// TestMintJiraContributorReturnsWinningCntrb is finding #9 (LOW): the
// race-window half is pinned at the source (the identity upsert must be
// the single source of the returned id, via RETURNING), and the
// sequential half behaviorally: two mints of one name yield one id.
func TestMintJiraContributorReturnsWinningCntrb(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	a, err := store.MintJiraContributor(ctx, "_avjr_mint", "_avjr Mint Person")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.MintJiraContributor(ctx, "_avjr_mint", "_avjr Mint Person")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("repeat mint must return the same contributor, got %s then %s", a, b)
	}
}

// TestMintJiraContributorUpsertReturnsWinner pins the race-window
// mechanism at the source: the identity upsert must RETURN cntrb_id and
// the function must return THAT value — under a concurrent double-mint
// the ON CONFLICT keeps the winner's link and the loser must not hand
// its orphan row id to its batch's issues/comments.
func TestMintJiraContributorUpsertReturnsWinner(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/jira_store.go"),
		"func (s *PostgresStore) MintJiraContributor("))
	// Anchor on the IDENTITY statement, not the whole body — the
	// contributor INSERT also says RETURNING cntrb_id, so a body-wide
	// needle is always-true (the L10 round's escapable-pin finding).
	i := strings.Index(body, "jira_identities")
	if i < 0 {
		t.Fatal("MintJiraContributor must write jira_identities")
	}
	ident := body[i:]
	if !srctest.ContainsNormalized(ident, "RETURNING cntrb_id") {
		t.Fatal("the identity upsert must RETURNING cntrb_id — the winner under ON CONFLICT, not the local mint")
	}
	if !strings.Contains(body, "return winner, nil") || strings.Contains(body, "return cntrb, nil") {
		t.Fatal("the function must return the SCANNED winner, never the locally minted cntrb")
	}
}

// TestJiraIdentityProbesAreIndexServable is finding #15: each identity
// probe arm carries the LITERAL non-empty guard (v0.27.125 — a generic
// plan cannot prove the parameter is non-empty, so the partial index
// is unusable without the guard in the query) and the matching lower() expression indexes
// are migration-owned (SR-2; the contributors table is fleet-scale).
func TestJiraIdentityProbesAreIndexServable(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/jira_store.go"),
		"func (s *PostgresStore) ResolveJiraIdentity("))
	for _, guard := range []string{"gh_login <> ''", "cntrb_login <> ''", "cntrb_full_name <> ''"} {
		if !srctest.ContainsNormalized(body, guard) {
			t.Errorf("identity probe must carry the literal guard %q so the partial index is provable", guard)
		}
	}
	migrate := srctest.Read(t, "internal/db/migrate.go")
	schema := srctest.Read(t, "internal/db/schema.sql")
	for _, ix := range []string{"idx_contributors_cntrb_login_lower", "idx_contributors_full_name_lower"} {
		if strings.Contains(schema, ix) {
			t.Errorf("%s must be migration-only (SR-2), not in schema.sql", ix)
		}
		if !srctest.ContainsNormalized(migrate, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+ix) {
			t.Errorf("migrate.go must build %s CONCURRENTLY for the Jira identity probes", ix)
		}
	}
}
