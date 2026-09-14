// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// issueCloserClient is the slice of the GitHub client the sweep needs
// (role interface per the v0.25.38 pattern).
type issueCloserClient interface {
	FetchIssueClosers(ctx context.Context, owner, repo string, numbers []int) (map[int]model.UserRef, error)
}

// ClosedBySweep is Phase 3 of the identity backfill: for closed GitHub
// issues whose closer is unreachable via the (history-capped)
// repo-wide events feed, fetch it from the per-issue timeline in
// batched GraphQL queries and resolve it through the standard
// contributor-resolution contract.
type ClosedBySweep struct {
	store    *db.PostgresStore
	client   issueCloserClient
	resolver *db.ContributorResolver
	logger   *slog.Logger
	perQuery int // issues per batched GraphQL query
}

func NewClosedBySweep(store *db.PostgresStore, client issueCloserClient, logger *slog.Logger, perQuery int) *ClosedBySweep {
	if perQuery <= 0 || perQuery > 100 {
		perQuery = 100
	}
	return &ClosedBySweep{
		store:    store,
		client:   client,
		resolver: db.NewContributorResolver(store),
		logger:   logger,
		perQuery: perQuery,
	}
}

// Run sweeps up to limit candidate issues (0 = all). dryRun reports
// the candidate count without any API call or write. Returns the
// number of issues whose closed_by_id was filled.
func (s *ClosedBySweep) Run(ctx context.Context, limit int64, dryRun bool) (int64, error) {
	issues, err := s.store.IssuesNeedingClosedBySweep(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("sweep candidates: %w", err)
	}
	if dryRun {
		s.logger.Info("closed_by sweep dry-run", "candidate_issues", len(issues))
		return int64(len(issues)), nil
	}

	var filled int64
	// Group by repo, chunk by perQuery.
	for start := 0; start < len(issues); {
		end := start + 1
		for end < len(issues) && issues[end].RepoID == issues[start].RepoID && end-start < s.perQuery {
			end++
		}
		chunk := issues[start:end]
		start = end

		numbers := make([]int, len(chunk))
		byNumber := make(map[int]db.SweepIssue, len(chunk))
		for i, si := range chunk {
			numbers[i] = si.Number
			byNumber[si.Number] = si
		}
		closers, err := s.client.FetchIssueClosers(ctx, chunk[0].Owner, chunk[0].Repo, numbers)
		if err != nil {
			// One bad repo (deleted, renamed, access lost) must not sink
			// the sweep — log and continue with the next chunk.
			s.logger.Warn("closed_by sweep batch failed",
				"owner", chunk[0].Owner, "repo", chunk[0].Repo, "issues", len(chunk), "error", err)
			continue
		}
		for num, ref := range closers {
			si := byNumber[num]
			cid, err := s.resolver.Resolve(ctx, int16(model.PlatformGitHub), ref.PlatformID,
				ref.Login, ref.Name, ref.Email, ref.AvatarURL, ref.URL, ref.NodeID, ref.Type)
			if err != nil || cid == "" {
				continue
			}
			if err := s.store.SetIssueClosedBy(ctx, si.IssueID, cid); err != nil {
				// Round-8 burn-down: a cancelled context is a `stop serve`, not a
				// defect. Only the log is suppressed — surrounding behaviour is
				// unchanged and the work is retried on the next cycle.
				if !errors.Is(err, context.Canceled) {
					s.logger.Warn("closed_by sweep write failed", "issue_id", si.IssueID, "error", err)
				}
				continue
			}
			filled++
		}
		if filled > 0 && filled%1000 == 0 {
			s.logger.Info("closed_by sweep progress", "filled", filled)
		}
	}
	s.logger.Info("closed_by sweep complete", "candidates", len(issues), "filled", filled)
	return filled, nil
}
