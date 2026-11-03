// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// A message is one row for the whole fleet (its platform id), owned by the
// repository that wrote it first; the retention series counts it under the
// OWNER once per conversation link it has, from any repository. So a link
// written by repository B to a message repository A owns moves A's cached
// answer, and A's own stamp comes only with its next collection. 0.29.130
// closed the delete side (the case-variant merge); this is the insert side,
// decided as a class (operator, 2026-11-03, after L10 round 1 of 0.29.130
// and Copilot's tenth review of PR #246 on the projection backfill): every
// conversation-link INSERT stamps the message's owner in the same statement,
// for rows it actually inserted (a re-collection's ON CONFLICT DO NOTHING
// stamps nobody: a lasting no-op must not move a repository's state), and
// only when the owner is another repository (the link's own repository is
// stamped by its job, drain or backfill as before).

type linkedOwnerFixture struct {
	owner, linker   int64 // the message's owner; the repository writing the link
	linkerIssue     int64
	linkerPR        int64
	ownerPlatformID int64 // the owner's message, platform_msg_id (staged, platform 1, comment kind)
}

func seedLinkedOwner(ctx context.Context, t *testing.T, store *PostgresStore, slug string) linkedOwnerFixture {
	t.Helper()
	cleanupDedupRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })
	gid := defaultRepoGroup(ctx, t, store)
	repo := func(name string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
			VALUES ($1, 1, $2, $3, $4) RETURNING repo_id`, gid, "https://github.com/"+slug+"/"+name, name, slug).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	fx := linkedOwnerFixture{owner: repo("owner"), linker: repo("linker"), ownerPlatformID: 909091001}
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number)
		VALUES ($1, 424243001, 1) RETURNING issue_id`, fx.linker).Scan(&fx.linkerIssue); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
		VALUES ($1, 565657001, 2) RETURNING pull_request_id`, fx.linker).Scan(&fx.linkerPR); err != nil {
		t.Fatal(err)
	}
	// The owner's message: a staged comment (platform 1, kind comment), the
	// row a second collector of the same comment lands on.
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, $3, 'shared')`, fx.owner, fx.ownerPlatformID, MsgKindComment); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id IN ($1, $2)`, fx.owner, fx.linker); err != nil {
		t.Fatal(err)
	}
	return fx
}

func (fx linkedOwnerFixture) batch(issue, pr bool) []platform.MessageWithRef {
	msg := model.Message{RepoID: fx.linker, PlatformMsgID: fx.ownerPlatformID, PlatformID: model.PlatformGitHub, Text: "shared", Timestamp: time.Now()}
	var out []platform.MessageWithRef
	if issue {
		out = append(out, platform.MessageWithRef{Message: msg, IssueRef: &model.IssueMessageRef{IssueID: fx.linkerIssue, RepoID: fx.linker}})
	}
	if pr {
		out = append(out, platform.MessageWithRef{Message: msg, PRRef: &model.PullRequestMessageRef{PRID: fx.linkerPR, RepoID: fx.linker}})
	}
	return out
}

// The staged processor: a second collector of a comment lands on the
// owner's messages row and links it from its own issue or pull request.
func TestUpsertMessageBatchStampsTheOwnerOfASharedMessage(t *testing.T) {
	ctx, store := caseConnect(t)
	for _, c := range []struct {
		name      string
		issue, pr bool
	}{{"issue link", true, false}, {"pull request link", false, true}} {
		t.Run(c.name, func(t *testing.T) {
			fx := seedLinkedOwner(ctx, t, store, "_avlinked_"+strings.ReplaceAll(c.name, " ", "_"))
			if err := store.UpsertMessageBatch(ctx, fx.batch(c.issue, c.pr)); err != nil {
				t.Fatal(err)
			}
			if dataChangedAt(ctx, t, store, fx.owner) == nil {
				t.Error("the owner of the linked message must be stamped: its retention inputs gained a link")
			}
			if dataChangedAt(ctx, t, store, fx.linker) != nil {
				t.Error("the linking repository is not stamped by the batch: its job stamps it")
			}
			if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, fx.owner); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertMessageBatch(ctx, fx.batch(c.issue, c.pr)); err != nil {
				t.Fatal(err)
			}
			if dataChangedAt(ctx, t, store, fx.owner) != nil {
				t.Error("a re-collection inserts no link (ON CONFLICT DO NOTHING) and must not move the owner's state again")
			}
		})
	}
}

// A message the linking repository owns itself stamps nobody: the link's
// own repository is covered by its job.
func TestUpsertMessageBatchStampsNobodyForItsOwnMessage(t *testing.T) {
	ctx, store := caseConnect(t)
	fx := seedLinkedOwner(ctx, t, store, "_avlinked_own")
	own := fx.batch(true, true)
	for i := range own {
		own[i].Message.PlatformMsgID = fx.ownerPlatformID + 1 // a new message, the linker's own
	}
	if err := store.UpsertMessageBatch(ctx, own); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{fx.owner, fx.linker} {
		if dataChangedAt(ctx, t, store, id) != nil {
			t.Errorf("repository %d must not be stamped by a link to the linker's own message", id)
		}
	}
}

// The mailing-list bridge (the drain, the projection backfill, and the Jira
// comment bridge through the same statement): a cross-posted email is one
// messages row (platform 6, its Message-ID) owned by the list that wrote it
// first; another list's repository bridging it onto its issue stamps the owner.
func TestBridgeEmailToIssueStampsTheOwnerOfACrossPostedMessage(t *testing.T) {
	ctx, store := caseConnect(t)
	fx := seedLinkedOwner(ctx, t, store, "_avlinked_mail")
	var msgID int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, node_id, msg_text, data_source)
		VALUES ($1, -909091002, 6, '<cross@posted.example>', 'x', '_avlinked_mail') RETURNING msg_id`, fx.owner).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	if err := store.BridgeEmailToIssue(ctx, fx.linkerIssue, fx.linker, msgID); err != nil {
		t.Fatal(err)
	}
	if dataChangedAt(ctx, t, store, fx.owner) == nil {
		t.Error("the owner of the bridged message must be stamped")
	}
	if dataChangedAt(ctx, t, store, fx.linker) != nil {
		t.Error("the bridging repository is stamped by its drain or backfill, not the bridge")
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, fx.owner); err != nil {
		t.Fatal(err)
	}
	if err := store.BridgeEmailToIssue(ctx, fx.linkerIssue, fx.linker, msgID); err != nil {
		t.Fatal(err)
	}
	if dataChangedAt(ctx, t, store, fx.owner) != nil {
		t.Error("a re-bridge inserts nothing and must not move the owner's state again")
	}
	// The linker's own email: nobody stamped.
	var ownMsg int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, node_id, msg_text, data_source)
		VALUES ($1, -909091003, 6, '<own@posted.example>', 'y', '_avlinked_mail') RETURNING msg_id`, fx.linker).Scan(&ownMsg); err != nil {
		t.Fatal(err)
	}
	if err := store.BridgeEmailToIssue(ctx, fx.linkerIssue, fx.linker, ownMsg); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{fx.owner, fx.linker} {
		if dataChangedAt(ctx, t, store, id) != nil {
			t.Errorf("repository %d must not be stamped by a bridge to the linker's own message", id)
		}
	}
}

// The wiring pin: every INSERT into a conversation-link table in the store
// is sent through stampLinkedOwnerSQL — the call that sends the statement is
// the wrapper, so a new link writer cannot skip the owner's stamp.
func TestEveryConversationLinkInsertStampsTheOwner(t *testing.T) {
	dir := filepath.Join(srctest.Root(t), "internal", "db")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	senders := []string{"stampLinkedOwnerSQL(", ".Exec(", ".QueryRow(", ".Query(", ".Queue(", "Sprintf(", "const ", "= `"}
	// Any spelling of the INSERT: case, whitespace, the schema optional.
	insertRE := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(?:aveloxis_data\.)?(issue|pull_request)_message_ref\b`)
	sites := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		src := srctest.StripGoComments(string(b))
		for _, m := range insertRE.FindAllStringIndex(src, -1) {
			sites++
			before := src[:m[0]]
			// The nearest sender before the INSERT is what sends it.
			sender, where := "", -1
			for _, s := range senders {
				if i := strings.LastIndex(before, s); i > where {
					sender, where = s, i
				}
			}
			if sender != "stampLinkedOwnerSQL(" {
				t.Errorf("%s: %q is sent by %q, not through stampLinkedOwnerSQL — the message's owner would not be stamped", name, src[m[0]:m[1]], sender)
			}
		}
	}
	srctest.MinCount(t, "conversation-link INSERT sites", sites, 3)
}
