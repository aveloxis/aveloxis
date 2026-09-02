// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// nodeIDIndexes are the v0.28.20 probe indexes for mirror-link resolution.
var nodeIDIndexes = []struct{ index, table string }{
	{"idx_pull_requests_node_id", "aveloxis_data.pull_requests (node_id)"},
	{"idx_issues_node_id", "aveloxis_data.issues (node_id)"},
}

// TestNodeIDIndexesAreMigrationOnlyAndConcurrent enforces SR-2 for the two
// indexes ResolveMirrorLinkByNodeID and scripts/heal_mirror_links.sh probe.
//
// Declaring them in schema.sql would defeat the rollout: the base DDL runs
// before any migration step, so an upgrading fleet would block-build them on
// 23.0M pull_requests / 9.6M issues (aveloxis_large) during startup migrate,
// and the CONCURRENTLY step would then no-op.
func TestNodeIDIndexesAreMigrationOnlyAndConcurrent(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	migrate := srctest.Read(t, "internal/db/migrate.go")

	for _, ix := range nodeIDIndexes {
		if strings.Contains(schema, ix.index) {
			t.Errorf("schema.sql declares %s — SR-2: fleet-scale indexes are migration-owned "+
				"via execCreateIndexConcurrently so the base DDL cannot block-build them.", ix.index)
		}
		if !strings.Contains(migrate, ix.index) {
			t.Fatalf("migrate.go must create %s", ix.index)
		}
		want := "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + ix.index
		if !srctest.ContainsNormalized(migrate, want) {
			t.Errorf("migrate.go must build %s with %q (a blocking build stalls writers "+
				"on a fleet-scale table)", ix.index, want)
		}
		if !srctest.ContainsNormalized(migrate, "ON "+ix.table) {
			t.Errorf("%s must be ON %s", ix.index, ix.table)
		}
	}
}

// TestNodeIDIndexesAreNotPartial pins the v0.27.54 lesson. The heal joins
// email_message against these columns, so the probe value is a JOIN VARIABLE
// the planner cannot prove a partial predicate for. A partial variant
// (restricted to non-empty node_id) is silently ignored and the query falls
// back to a sequential scan.
// The heal's resolvable-count join was measured not to finish in 5 minutes
// over 396,809 mirror rows unindexed (~26s indexed), so a silently-unusable
// partial index is the same outage.
func TestNodeIDIndexesAreNotPartial(t *testing.T) {
	migrate := srctest.Read(t, "internal/db/migrate.go")
	for _, ix := range nodeIDIndexes {
		i := strings.Index(migrate, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+ix.index)
		if i < 0 {
			t.Fatalf("%s not found in migrate.go", ix.index)
		}
		// The statement ends at the closing backtick of the raw literal.
		end := strings.Index(migrate[i:], "`")
		if end < 0 {
			t.Fatalf("could not bound the %s statement", ix.index)
		}
		if strings.Contains(strings.ToUpper(migrate[i:i+end]), "WHERE") {
			t.Errorf("%s carries a WHERE clause — it must be NON-partial (v0.27.54): "+
				"the heal probes it with a join variable, which a partial index cannot serve.", ix.index)
		}
	}
}

// TestNodeIDIndexNamesNotDropTargets enforces SR-4: DROP steps run on EVERY
// migrate, so reusing a dropped name would rebuild the index on every startup
// forever. Verified clean at introduction; this keeps it that way.
func TestNodeIDIndexNamesNotDropTargets(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	dropStmt := regexp.MustCompile(`(?i)DROP INDEX[^;` + "`" + `"]*`)
	for _, stmt := range dropStmt.FindAllString(src, -1) {
		for _, ix := range nodeIDIndexes {
			if strings.Contains(stmt, ix.index) {
				t.Errorf("%s appears in a DROP INDEX statement (%q) — SR-4: dropped names are "+
					"never recreated, because the drop step reruns on every migrate", ix.index, stmt)
			}
		}
	}
}

// TestResolveMirrorLinkByNodeIDRoutesByPrefix is the behavioral cover for the
// v0.28.20 resolver. It exists because a mutation review found the method had
// NONE: swapping the PR_ branch to query the issues table AND deleting both
// error arms left the entire suite green.
//
// The routing is load-bearing. A PR_ node resolved against the issues table
// would silently link a mirror message to an unrelated entity — worse than
// the NULL it replaced, because it looks like data.
func TestResolveMirrorLinkByNodeIDRoutesByPrefix(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/apache/_avnodeid",
		Owner: "apache", Name: "_avnodeid",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Distinct node IDs so a wrong-table lookup cannot accidentally succeed.
	const prNode, issueNode = "PR_avTESTnode111", "I_avTESTnode222"
	var wantPR, wantIssue int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, node_id)
		VALUES ($1, 99000001, 990001, $2) RETURNING pull_request_id`, repoID, prNode).Scan(&wantPR); err != nil {
		t.Fatalf("seed pr: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, node_id)
		VALUES ($1, 99000002, 990002, $2) RETURNING issue_id`, repoID, issueNode).Scan(&wantIssue); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	t.Run("PR_ resolves to the pull request", func(t *testing.T) {
		gotIssue, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, prNode, repoID, nil)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if gotIssue != nil {
			t.Errorf("issue = %v, want nil — a PR_ node must never resolve against issues", *gotIssue)
		}
		if gotPR == nil || *gotPR != wantPR {
			t.Errorf("pr = %v, want %d", gotPR, wantPR)
		}
	})
	t.Run("I_ resolves to the issue", func(t *testing.T) {
		gotIssue, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, issueNode, repoID, nil)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if gotPR != nil {
			t.Errorf("pr = %v, want nil — an I_ node must never resolve against pull_requests", *gotPR)
		}
		if gotIssue == nil || *gotIssue != wantIssue {
			t.Errorf("issue = %v, want %d", gotIssue, wantIssue)
		}
	})
	// Clean misses and non-node input are honest absences, not errors.
	for _, tc := range []struct{ name, node string }{
		{"uncollected entity", "PR_avTESTnodeMISSING"},
		{"legacy node id", "MDExOlB1bGxSZXF1ZXN0MQ=="},
		{"empty", ""},
	} {
		t.Run(tc.name+" is a clean miss", func(t *testing.T) {
			gotIssue, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, tc.node, repoID, nil)
			if err != nil || gotIssue != nil || gotPR != nil {
				t.Errorf("got (%v,%v,%v), want (nil,nil,nil)", gotIssue, gotPR, err)
			}
		})
	}
}

// TestResolveMirrorLinkByNodeIDPropagatesQueryErrors pins SR-5 at the store
// layer: a query FAILURE must not be reported as "not found". Driven by
// closing the pool so the query cannot succeed.
func TestResolveMirrorLinkByNodeIDPropagatesQueryErrors(t *testing.T) {
	store, ctx := emConnect(t)
	store.Close() // every subsequent query fails

	for _, node := range []string{"PR_avTESTclosed", "I_avTESTclosed"} {
		gotIssue, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, node, 1, nil)
		if err == nil {
			t.Errorf("%s: err = nil on a closed pool — SR-5: a lookup error is not 'no link'", node)
		}
		if gotIssue != nil || gotPR != nil {
			t.Errorf("%s: returned a link alongside an error", node)
		}
	}
}

// TestUpsertEmailMessagePreservesKindWithLinks pins the v0.28.20 SR-18 fix.
//
// The three linked_* columns are preserve-on-conflict but projected_kind used
// to be a blind EXCLUDED overwrite. A re-processed message whose resolver
// failed (transient DB error → both pointers nil → kind "mailing_list_only")
// would then assert absence over a link the row already held. That state is
// UNREPAIRABLE by scripts/heal_mirror_links.sh, whose candidate filter
// requires both link columns NULL.
func TestUpsertEmailMessagePreservesKindWithLinks(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/apache/_avkind",
		Owner: "apache", Name: "_avkind",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	var prID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, node_id)
		VALUES ($1, 99000003, 990003, 'PR_avKINDnode') RETURNING pull_request_id`, repoID).Scan(&prID); err != nil {
		t.Fatalf("seed pr: %v", err)
	}
	const hdr = "PR_avKINDnode@gitbox.apache.org"
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.email_message WHERE message_id_header = $1`, hdr)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	base := func() *model.EmailMessage {
		return &model.EmailMessage{
			RepoID: &repoID, PlatformID: MailingListPlatformID, MLSystem: "apache_ponymail",
			MessageIDHeader: hdr, ListAddress: "dev@_avkind.apache.org",
			MsgClass: "github_mirror", IsMirror: true,
		}
	}
	// 1. Resolved write (what the forward path or the heal produces).
	linked := base()
	linked.LinkedPullRequestID = &prID
	linked.ProjectedKind = "pr"
	if _, err := store.UpsertEmailMessage(ctx, linked); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// 2. Re-process where the resolver FAILED: no link, kind mailing_list_only.
	unresolved := base()
	unresolved.ProjectedKind = "mailing_list_only"
	if _, err := store.UpsertEmailMessage(ctx, unresolved); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	readBack := func() (*int64, string) {
		t.Helper()
		var pr *int64
		var kind string
		if err := store.pool.QueryRow(ctx, `
			SELECT linked_pull_request_id, projected_kind
			FROM aveloxis_data.email_message WHERE message_id_header = $1`, hdr).Scan(&pr, &kind); err != nil {
			t.Fatalf("read back: %v", err)
		}
		return pr, kind
	}
	gotPR, gotKind := readBack()
	if gotPR == nil || *gotPR != prID {
		t.Errorf("linked_pull_request_id = %v, want %d (preserve-on-conflict)", gotPR, prID)
	}
	if gotKind != "pr" {
		t.Errorf("projected_kind = %q, want \"pr\" — a failed re-resolve desynchronised kind from the "+
			"surviving link, and the heal can never repair that row", gotKind)
	}

	// 3. PRECEDENCE. With BOTH a pull request and an issue linked, 'pr' must
	// win — the same order projectedKind() uses in
	// internal/collector/mailinglist_processor.go. Without this case the CASE
	// arms can be reordered (SQL disagreeing with Go) and stay green.
	var issueID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, node_id)
		VALUES ($1, 99000004, 990004, 'I_avKINDnode') RETURNING issue_id`, repoID).Scan(&issueID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	both := base()
	both.LinkedIssueID = &issueID
	both.ProjectedKind = "issue" // caller asserts issue; the store must still say pr
	if _, err := store.UpsertEmailMessage(ctx, both); err != nil {
		t.Fatalf("third upsert: %v", err)
	}
	if _, kind := readBack(); kind != "pr" {
		t.Errorf("projected_kind = %q with BOTH links set, want \"pr\" — the SQL CASE must use the "+
			"same precedence as projectedKind() (pr > review > issue)", kind)
	}
}
