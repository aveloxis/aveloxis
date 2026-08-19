// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.27.97 batch write path for the facade (summary/21 F2): the per-row
// UpsertCommit loop cost 296.8 h / 293.5M round trips in the 2026-08-18
// production snapshot; commit_messages another 39.5 h / 58.8M. These
// methods collapse the round trips ~commitBatchChunk×. Semantics match the
// single-row methods exactly: same column list, same conflict arbiters,
// same withRetry wrapper, same UTF-8 tracer coverage (the v0.23.5 boundary
// scrub operates on pgx args regardless of arity).

// commitBatchChunk bounds rows per INSERT statement. 22 params/row against
// the 65,535-param wire limit allows ~2,900; 500 keeps statements a
// comfortable size for the planner and the retry loop.
const commitBatchChunk = 500

// UpsertCommitBatch inserts commit-file rows in multi-row chunks.
// ON CONFLICT (repo_id, cmt_commit_hash, cmt_filename) DO NOTHING is
// intra-statement-duplicate safe, so callers need not dedup.
func (s *PostgresStore) UpsertCommitBatch(ctx context.Context, commits []*model.Commit) error {
	for start := 0; start < len(commits); start += commitBatchChunk {
		end := start + commitBatchChunk
		if end > len(commits) {
			end = len(commits)
		}
		chunk := commits[start:end]

		var sb strings.Builder
		args := make([]any, 0, len(chunk)*22)
		now := time.Now()
		for i, c := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			base := i * 22
			sb.WriteByte('(')
			for p := 1; p <= 22; p++ {
				if p > 1 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, "$%d", base+p)
			}
			sb.WriteByte(')')
			args = append(args,
				c.RepoID, c.Hash, c.AuthorName, c.AuthorRawEmail,
				c.AuthorEmail, c.AuthorDate, c.AuthorAffiliation,
				c.CommitterName, c.CommitterRawEmail, c.CommitterEmail,
				c.CommitterDate, c.CommitterAffiliation,
				c.LinesAdded, c.LinesRemoved, c.LinesWhitespace, c.Filename,
				now, c.CommitterTimestamp, c.AuthorTimestamp,
				c.AuthorPlatformLogin,
				c.Origin.ToolSource, c.Origin.DataSource,
			)
		}

		sql := `
			INSERT INTO aveloxis_data.commits
				(repo_id, cmt_commit_hash, cmt_author_name, cmt_author_raw_email,
				 cmt_author_email, cmt_author_date, cmt_author_affiliation,
				 cmt_committer_name, cmt_committer_raw_email, cmt_committer_email,
				 cmt_committer_date, cmt_committer_affiliation,
				 cmt_added, cmt_removed, cmt_whitespace, cmt_filename,
				 cmt_date_attempted, cmt_committer_timestamp, cmt_author_timestamp,
				 cmt_author_platform_username,
				 tool_source, data_source)
			VALUES ` + sb.String() + `
			ON CONFLICT (repo_id, cmt_commit_hash, cmt_filename) DO NOTHING`

		if err := s.withRetry(ctx, func(ctx context.Context) error {
			_, err := s.pool.Exec(ctx, sql, args...)
			return err
		}); err != nil {
			return fmt.Errorf("commit batch chunk [%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// UpsertCommitMessageBatch inserts commit messages in multi-row chunks.
// The conflict action is DO UPDATE, and one statement may not affect the
// same (repo_id, cmt_hash) twice — so the batch dedups by hash first,
// last-write-wins (matching the sequential per-row behavior where the
// later call overwrote the earlier).
func (s *PostgresStore) UpsertCommitMessageBatch(ctx context.Context, msgs []*model.CommitMessage) error {
	type key struct {
		repoID int64
		hash   string
	}
	seen := make(map[key]int, len(msgs))
	deduped := make([]*model.CommitMessage, 0, len(msgs))
	for _, m := range msgs {
		k := key{m.RepoID, m.Hash}
		if idx, ok := seen[k]; ok {
			deduped[idx] = m // last write wins
			continue
		}
		seen[k] = len(deduped)
		deduped = append(deduped, m)
	}

	for start := 0; start < len(deduped); start += commitBatchChunk {
		end := start + commitBatchChunk
		if end > len(deduped) {
			end = len(deduped)
		}
		chunk := deduped[start:end]

		var sb strings.Builder
		args := make([]any, 0, len(chunk)*3)
		for i, m := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "($%d,$%d,$%d,'aveloxis-facade','git')", i*3+1, i*3+2, i*3+3)
			args = append(args, m.RepoID, SanitizeText(m.Message), m.Hash)
		}

		sql := `
			INSERT INTO aveloxis_data.commit_messages
				(repo_id, cmt_msg, cmt_hash, tool_source, data_source)
			VALUES ` + sb.String() + `
			ON CONFLICT (repo_id, cmt_hash) DO UPDATE SET
				cmt_msg = EXCLUDED.cmt_msg,
				tool_version = EXCLUDED.tool_version,
				data_collection_date = NOW()`

		if err := s.withRetry(ctx, func(ctx context.Context) error {
			_, err := s.pool.Exec(ctx, sql, args...)
			return err
		}); err != nil {
			return fmt.Errorf("commit message batch chunk [%d:%d]: %w", start, end, err)
		}
	}
	return nil
}
