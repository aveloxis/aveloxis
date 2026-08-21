// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMailingListPipelineEndToEnd runs a few hundred staged messages ALL THE
// WAY THROUGH the real pipeline: stage (via the real store) → drain (via the
// real MailingListProcessor against the real DB) → assert the email_message /
// messages / email_message_ref rows landed and staging is fully processed.
//
// This is the 1.5b end-to-end guard the operator asked for: it exercises the
// staging→batch boundary, the per-list single-threaded drain, the metadata-only
// mirror handling (no body re-copy), and the body+ref write for discussion
// classes — on a live Postgres, not a fake.
//
// Gated on AVELOXIS_TEST_DB (a scratch DB, e.g. aveloxis_cascade_test).
func TestMailingListPipelineEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const (
		listAddr  = "dev@_av_mlpipe.apache.org"
		grpName   = "_av_mlpipe_grp"
		total     = 300
		mirrors   = 100 // metadata-only: NO body
		bodies    = total - mirrors
		repoGit   = "https://github.com/_av_mlpipe/repo"
		repoOwner = "_av_mlpipe"
	)

	cleanup := func() {
		// FK-safe order. The clean_fit policy projects issue_event rows onto
		// issues (+ issue_message_ref bridges), and email_message.linked_issue_id
		// references issues, so: bridges → email_message_ref → email_message
		// (clears linked_issue_id) → issues → messages → staging → list → repo → group.
		repoSub := `(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id IN `+repoSub, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message_ref WHERE email_message_id IN
			(SELECT email_message_id FROM aveloxis_data.email_message WHERE data_source=$1)`, listAddr)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE data_source=$1`, listAddr)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id IN `+repoSub, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE data_source=$1`, listAddr)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE rgls_id IN
			(SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email=$1)`, listAddr)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email=$1`, listAddr)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name=$1`, grpName)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Seed a repo_group + a repo in it so GetPrimaryRepoForGroup resolves.
	var repoGroupID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO aveloxis_data.repo_groups (rg_name) VALUES ($1) RETURNING repo_group_id`, grpName).
		Scan(&repoGroupID); err != nil {
		t.Fatalf("seed repo_group: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name, repo_group_id)
		 VALUES (1, $1, $2, 'repo', $3)`, repoGit, repoOwner, repoGroupID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Register the list (email_message.rgls_id has an FK to repo_groups_list_serve).
	if err := store.RegisterMailingList(ctx, repoGroupID, listAddr, "apache_ponymail"); err != nil {
		t.Fatalf("register list: %v", err)
	}
	var rgls int64
	if err := pool.QueryRow(ctx,
		`SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email=$1`, listAddr).Scan(&rgls); err != nil {
		t.Fatalf("lookup rgls_id: %v", err)
	}

	// Stage `total` messages via the real store: every 3rd is a github_mirror
	// (metadata-only), the rest alternate discuss / issue_event.
	for i := range total {
		msg := model.MailingListStagedMessage{
			MessageID:   fmt.Sprintf("<mlpipe-%d@example>", i),
			ListAddress: listAddr,
			Subject:     fmt.Sprintf("msg %d", i),
			SenderEmail: fmt.Sprintf("user%d@example.org", i%25), // 25 distinct senders → cache reuse
			SentAt:      time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute),
			Body:        fmt.Sprintf("body of message %d", i),
		}
		if i < mirrors {
			msg.MsgClass = mailinglist.ClassGitHubMirror
			msg.IsMirror = true
			msg.SignaledRepoURL = "https://github.com/apache/repo"
			msg.MirrorOwner, msg.MirrorRepo, msg.MirrorKind, msg.MirrorNumber = "apache", "repo", "pull", i+1
		} else if i%2 == 0 {
			msg.MsgClass = mailinglist.ClassDiscuss
		} else {
			msg.MsgClass = mailinglist.ClassIssueEvent
			msg.ExternalKey = fmt.Sprintf("AV-%d", i)
		}
		rgid := repoGroupID
		if err := store.StageMailingListMessage(ctx, rgls, &rgid, nil, msg); err != nil {
			t.Fatalf("stage message %d: %v", i, err)
		}
	}

	// Drain through the real processor (metadata_only mirror handling).
	proc := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, lg)
	n, err := proc.DrainList(ctx, rgls)
	if err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if n != total {
		t.Fatalf("DrainList processed %d, want %d", n, total)
	}

	// All staging rows processed.
	var unprocessed int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id=$1 AND NOT processed`, rgls).Scan(&unprocessed)
	if unprocessed != 0 {
		t.Errorf("expected 0 unprocessed staging rows, got %d", unprocessed)
	}

	// One email_message per message.
	var emCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.email_message WHERE rgls_id=$1`, rgls).Scan(&emCount)
	if emCount != total {
		t.Errorf("email_message rows = %d, want %d", emCount, total)
	}

	// Bodies + refs only for the non-mirror messages (mirrors are metadata-only).
	var bodyCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.messages WHERE data_source=$1`, listAddr).Scan(&bodyCount)
	if bodyCount != bodies {
		t.Errorf("messages (bodies) = %d, want %d (mirrors are metadata-only)", bodyCount, bodies)
	}
	var refCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.email_message_ref r
		JOIN aveloxis_data.email_message e ON e.email_message_id=r.email_message_id
		WHERE e.rgls_id=$1`, rgls).Scan(&refCount)
	if refCount != bodies {
		t.Errorf("email_message_ref rows = %d, want %d", refCount, bodies)
	}

	// Mirror messages carry IsMirror; non-mirror don't.
	var mirrorCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.email_message WHERE rgls_id=$1 AND is_mirror`, rgls).Scan(&mirrorCount)
	if mirrorCount != mirrors {
		t.Errorf("is_mirror email_message rows = %d, want %d", mirrorCount, mirrors)
	}

	// Phase A projection: the issue_event cohort (i in [mirrors,total) with i odd)
	// each became a synthetic issue under the seeded repo, provenance data_source='JIRA'.
	wantIssues := 0
	for i := mirrors; i < total; i++ {
		if i%2 == 1 {
			wantIssues++
		}
	}
	var issueCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.issues
		WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)
		  AND data_source='JIRA'`, repoGit).Scan(&issueCount)
	if issueCount != wantIssues {
		t.Errorf("projected issues = %d, want %d (one per issue_event message)", issueCount, wantIssues)
	}

	// Re-draining is a no-op (everything already processed).
	n2, err := proc.DrainList(ctx, rgls)
	if err != nil {
		t.Fatalf("DrainList re-run: %v", err)
	}
	if n2 != 0 {
		t.Errorf("re-drain processed %d, want 0", n2)
	}
}
