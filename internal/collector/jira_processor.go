// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_processor.go — the Jira collector's resolve/write half (C3).
// Drains aveloxis_ops.jira_staging: each envelope (one jira.Issue,
// staged raw by the worker) becomes an issues row through the C3a
// provider-precedence writer and native comment messages, with
// per-person identity resolution:
//
//   - unambiguous match (login, then display) links to the existing
//     contributor;
//   - a pure Jira-only identity (zero candidates) MINTS a contributor
//     — the construct is the connection, and a person who reported
//     and discussed tickets is a network node;
//   - AMBIGUOUS stays raw with cntrb NULL (SR-6 — minting beside two
//     candidates would fabricate a third person).
//
// Rows whose project has no repo mapping stay staged (the
// mailing-list stuck-list pattern): an operator fixing the
// registration row heals them on the next drain.

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform/jira"
)

// jiraProcStore is the processor's narrow store surface.
type jiraProcStore interface {
	JiraProjectsWithStaging(ctx context.Context, limit int) ([]int64, error)
	GetJiraStagingBatch(ctx context.Context, jpsID int64, limit int) ([]db.JiraStagingRow, error)
	ResolveJiraIdentity(ctx context.Context, jiraName, jiraUserKey, displayName string) (string, string, bool, error)
	MintJiraContributor(ctx context.Context, jiraName, displayName string) (string, error)
	UpsertJiraIssueFromAPI(ctx context.Context, in db.JiraAPIIssue) (int64, error)
	UpsertJiraComment(ctx context.Context, in db.JiraAPIComment) (int64, error)
	MarkJiraStagingProcessed(ctx context.Context, jsIDs []int64) error
}

const (
	jiraDrainProjectLimit = 200
	jiraDrainBatchSize    = 500
)

// JiraProcessor drains staged Jira envelopes.
type JiraProcessor struct {
	store  jiraProcStore
	logger *slog.Logger
}

// NewJiraProcessor builds a processor.
func NewJiraProcessor(store jiraProcStore, logger *slog.Logger) *JiraProcessor {
	return &JiraProcessor{store: store, logger: logger}
}

// DrainOnce drains every project with staged rows once. Returns rows
// processed.
func (p *JiraProcessor) DrainOnce(ctx context.Context) (int, error) {
	projects, err := p.store.JiraProjectsWithStaging(ctx, jiraDrainProjectLimit)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, jpsID := range projects {
		for {
			if ctx.Err() != nil {
				return total, ctx.Err()
			}
			batch, err := p.store.GetJiraStagingBatch(ctx, jpsID, jiraDrainBatchSize)
			if err != nil {
				return total, err
			}
			if len(batch) == 0 {
				break
			}
			done := make([]int64, 0, len(batch))
			skipped := 0
			for _, row := range batch {
				if ctx.Err() != nil {
					// Rows drained so far still mark below via done.
					break
				}
				if row.RepoID == nil {
					skipped++ // no repo mapping — stays staged (operator heals the registration)
					continue
				}
				if err := p.processEnvelope(ctx, *row.RepoID, row.Envelope); err != nil {
					if !errors.Is(err, context.Canceled) {
						p.logger.Warn("jira: envelope drain failed", "issue_key", row.IssueKey, "error", err)
						continue // stays staged for the next drain
					}
					break // shutdown: rows drained so far still mark below
				}
				done = append(done, row.JsID)
			}
			if len(done) > 0 {
				if err := p.store.MarkJiraStagingProcessed(ctx, done); err != nil {
					return total, err
				}
				total += len(done)
			}
			if len(done) == 0 {
				// Nothing in this batch could drain (all skipped/failed)
				// — move on rather than spinning on the same rows.
				break
			}
		}
	}
	return total, nil
}

// resolveIdentity links or mints per the SR-6 matrix; "" = stays
// unattributed (ambiguous or nameless — those are ANSWERS). A
// resolve/mint ERROR fails the envelope (Copilot round on PR #193, C1;
// SR-5): the pre-fix swallow let the issue/comment write succeed with
// empty attribution and the staging row be marked processed — a
// transient DB error permanently lost the attribution AND the raw Jira
// identity the mint banks. Failing keeps the row staged for the next
// drain.
func (p *JiraProcessor) resolveIdentity(ctx context.Context, u *jira.User) (string, error) {
	if u == nil || u.Name == "" {
		return "", nil
	}
	cntrb, _, ambiguous, err := p.store.ResolveJiraIdentity(ctx, u.Name, u.Key, u.DisplayName)
	if err != nil {
		return "", fmt.Errorf("resolve jira identity %q: %w", u.Name, err)
	}
	if cntrb != "" || ambiguous {
		return cntrb, nil
	}
	minted, err := p.store.MintJiraContributor(ctx, u.Name, u.DisplayName)
	if err != nil {
		return "", fmt.Errorf("mint jira contributor %q: %w", u.Name, err)
	}
	return minted, nil
}

func (p *JiraProcessor) processEnvelope(ctx context.Context, repoID int64, envelope []byte) error {
	var is jira.Issue
	if err := json.Unmarshal(envelope, &is); err != nil {
		return err
	}
	var jiraID int64
	if is.ID != "" {
		// Jira serves the numeric id as a string on the wire.
		for _, ch := range is.ID {
			if ch < '0' || ch > '9' {
				jiraID = 0
				break
			}
			jiraID = jiraID*10 + int64(ch-'0')
		}
	}
	reporter, err := p.resolveIdentity(ctx, is.Fields.Reporter)
	if err != nil {
		return err // stays staged — a transient identity failure must retry (C1)
	}
	in := db.JiraAPIIssue{
		RepoID:        repoID,
		ExternalKey:   is.Key,
		JiraIssueID:   jiraID,
		Title:         is.Fields.Summary,
		ReporterCntrb: reporter,
	}
	if is.Fields.Status != nil {
		in.Status = is.Fields.Status.Name
	}
	if is.Fields.Resolution != nil {
		// Review 2026-08-30 #5: the resolution NAME drives closed-state
		// derivation for per-project custom terminal statuses the three
		// canonical names miss.
		in.Resolution = is.Fields.Resolution.Name
	}
	in.Created = jiraTime(is.Fields.Created)
	in.Updated = jiraTime(is.Fields.Updated)
	in.ResolutionDate = jiraTime(is.Fields.ResolutionDate)

	issueID, err := p.store.UpsertJiraIssueFromAPI(ctx, in)
	if err != nil {
		return err
	}
	if is.Fields.Comment == nil {
		return nil
	}
	// Review 2026-08-30 #6: the pilot measured zero inline truncation
	// (718/718), but if Jira ever returns fewer comments than the
	// block's total the tail is never collected — make it visible.
	if is.Fields.Comment.Total > len(is.Fields.Comment.Comments) {
		p.logger.Warn("jira: inline comment block truncated — tail comments not collected",
			"issue", is.Key,
			"total", is.Fields.Comment.Total,
			"returned", len(is.Fields.Comment.Comments))
	}
	for _, cm := range is.Fields.Comment.Comments {
		author, aerr := p.resolveIdentity(ctx, cm.Author)
		if aerr != nil {
			return aerr // stays staged — retry the whole envelope (C1)
		}
		var commentID int64
		for _, ch := range cm.ID {
			if ch < '0' || ch > '9' {
				commentID = 0
				break
			}
			commentID = commentID*10 + int64(ch-'0')
		}
		if commentID == 0 {
			p.logger.Warn("jira: comment without a numeric id skipped", "issue", is.Key, "raw_id", cm.ID)
			continue
		}
		_, err := p.store.UpsertJiraComment(ctx, db.JiraAPIComment{
			RepoID:        repoID,
			IssueID:       issueID,
			ExternalKey:   is.Key,
			CommentID:     commentID,
			Body:          cm.Body,
			AuthorCntrbID: author,
			Created:       jiraTime(cm.Created),
			Updated:       jiraTime(cm.Updated),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// jiraTime parses a Jira Server timestamp; zero on absence or parse
// failure (NullTime maps zero to NULL downstream).
func jiraTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(jiraTimeLayout, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}
