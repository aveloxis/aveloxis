package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// nullEscape is the 6-byte JSON NUL-escape sequence (backslash, u,
// 0, 0, 0, 0). Built from byte literals so this source file never
// contains the sequence directly — otherwise an over-eager editor or
// JSON-aware tool can decode it to an actual NUL byte and corrupt
// the source.
//
// PostgreSQL JSONB rejects this escape with SQLSTATE 22P05
// ("unsupported Unicode escape sequence"). A single occurrence in
// any row of a batched insert fails the whole flush.
//
// JSON (RFC 8259) only defines the lowercase form; there is no
// uppercase \U escape in JSON. Go's encoding/json always emits the
// lowercase form, so a single needle suffices.
var nullEscape = []byte{'\\', 'u', '0', '0', '0', '0'}

// sanitizeJSONForJSONB strips JSON NUL-escape sequences from
// marshaled JSON before it hits PostgreSQL JSONB. GitHub/GitLab API
// responses occasionally carry NUL bytes in text fields (bot-
// generated comments, binary content echoed back in diffs), and
// PostgreSQL refuses the escape. We drop it rather than replace with
// a placeholder — the character has no legitimate semantic value in
// the fields we stage.
//
// The sanitizer is BACKSLASH-RUN AWARE. A candidate NUL-escape at
// position j is only a real JSON escape when the run of consecutive
// backslashes ending at j-1 has even length (including zero). If
// the run is odd, the backslash at j is itself the tail of a "\\"
// literal-backslash pair, and the "u0000" that follows is ordinary
// content — the string represents those six characters literally
// (e.g. a user pasted that text into an issue body or PR comment).
// In that case we MUST preserve the sequence verbatim.
//
// Why this matters: a context-free bytes.ReplaceAll, as used before
// v0.18.13, would match the escape needle starting at the SECOND
// backslash of an escaped-backslash-plus-u0000 pair and strip six
// bytes — leaving a lone backslash before the next JSON token. That
// produced SQLSTATE 22P02 "invalid input syntax for type json" and
// killed the whole 500-row staging batch: the v0.18.12 production
// swarm this routine was patched to prevent.
func sanitizeJSONForJSONB(data []byte) []byte {
	if !bytes.Contains(data, nullEscape) {
		return data
	}
	out := make([]byte, 0, len(data))
	i := 0
	for i < len(data) {
		rel := bytes.Index(data[i:], nullEscape)
		if rel < 0 {
			out = append(out, data[i:]...)
			break
		}
		j := i + rel
		// Count consecutive backslashes immediately preceding j in
		// the ORIGINAL input (not the output buffer). Even count =
		// the backslash at j introduces a real escape. Odd count =
		// it's the tail of a "\\" literal-backslash pair, and the
		// u0000 is content.
		precedingBackslashes := 0
		for k := j - 1; k >= 0 && data[k] == '\\'; k-- {
			precedingBackslashes++
		}
		if precedingBackslashes%2 == 0 {
			// Real NUL escape — keep bytes up to j, drop the 6
			// escape bytes.
			out = append(out, data[i:j]...)
			i = j + len(nullEscape)
		} else {
			// Literal content — keep everything through the
			// candidate inclusive.
			out = append(out, data[i:j+len(nullEscape)]...)
			i = j + len(nullEscape)
		}
	}
	return out
}

// StagingWriter appends raw API responses to the staging table.
// This is the fast path: no FK lookups, no contributor resolution, just JSONB inserts.
type StagingWriter struct {
	store  *PostgresStore
	repoID int64
	platID int16
	batch  *pgx.Batch
	count  int
	logger *slog.Logger
}

const stagingFlushSize = 500

// NewStagingWriter creates a writer that buffers staging inserts.
func NewStagingWriter(store *PostgresStore, repoID int64, platformID int16, logger *slog.Logger) *StagingWriter {
	return &StagingWriter{
		store:  store,
		repoID: repoID,
		platID: platformID,
		batch:  &pgx.Batch{},
		logger: logger,
	}
}

// Stage buffers a single entity for insert. Call Flush() when done.
func (w *StagingWriter) Stage(ctx context.Context, entityType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling %s: %w", entityType, err)
	}
	// Strip JSON NUL-escape sequences. PostgreSQL JSONB rejects them
	// and a single poisoned row would fail the whole 500-row batch
	// flush. See sanitizeJSONForJSONB for the backslash-run rule.
	data = sanitizeJSONForJSONB(data)
	w.batch.Queue(`
		INSERT INTO aveloxis_ops.staging (repo_id, platform_id, entity_type, payload)
		VALUES ($1, $2, $3, $4)`,
		w.repoID, w.platID, entityType, data,
	)
	w.count++

	if w.count >= stagingFlushSize {
		return w.Flush(ctx)
	}
	return nil
}

// Flush writes all buffered staging rows to the database.
//
// Goes through PostgresStore.sendBatchWithRetry so a stale
// prepared-statement cache entry on one pooled connection (SQLSTATE
// 26000 under heavy LAN load — see prepared_stmt_retry.go) becomes
// a single transparent retry instead of a 500-row data loss.
func (w *StagingWriter) Flush(ctx context.Context) error {
	if w.count == 0 {
		return nil
	}
	err := w.store.sendBatchWithRetry(ctx, w.batch)
	if err != nil {
		return fmt.Errorf("flushing staging batch (%d rows): %w", w.count, err)
	}
	w.logger.Debug("flushed staging rows", "count", w.count, "repo_id", w.repoID)
	w.batch = &pgx.Batch{}
	w.count = 0
	return nil
}

// Count returns number of unflushed rows.
func (w *StagingWriter) Count() int { return w.count }

// ProcessStaged reads unprocessed staging rows for a repo and entity type,
// calls the handler for each batch, then marks them processed.
// batchSize controls how many rows are read per iteration.
func (s *PostgresStore) ProcessStaged(ctx context.Context, repoID int64, entityType string, batchSize int, handler func(rows []StagedRow) error) error {
	for {
		rows, err := s.readStagedBatch(ctx, repoID, entityType, batchSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil // all done
		}

		if err := handler(rows); err != nil {
			return fmt.Errorf("processing %s batch: %w", entityType, err)
		}

		// Mark processed.
		ids := make([]int64, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		if err := s.markStagedProcessed(ctx, ids); err != nil {
			return err
		}
	}
}

// StagedRow is a single row from the staging table.
type StagedRow struct {
	ID      int64
	RepoID  int64
	Payload json.RawMessage
}

func (s *PostgresStore) readStagedBatch(ctx context.Context, repoID int64, entityType string, limit int) ([]StagedRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT staging_id, repo_id, payload
		FROM aveloxis_ops.staging
		WHERE repo_id = $1 AND entity_type = $2 AND NOT processed
		ORDER BY staging_id
		LIMIT $3`,
		repoID, entityType, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []StagedRow
	for rows.Next() {
		var r StagedRow
		if err := rows.Scan(&r.ID, &r.RepoID, &r.Payload); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *PostgresStore) markStagedProcessed(ctx context.Context, ids []int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.staging SET processed = TRUE
		WHERE staging_id = ANY($1)`, ids)
	return err
}

// PurgeStagedForRepo removes all unprocessed staging rows for a specific repo.
// Called before re-staging to prevent stale child entities from causing FK errors.
func (s *PostgresStore) PurgeStagedForRepo(ctx context.Context, repoID int64) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.staging WHERE repo_id = $1 AND NOT processed`, repoID)
	if err != nil {
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		s.logger.Info("purged stale staging rows", "repo_id", repoID, "rows", n)
	}
}

// PurgeStagedProcessed removes old processed rows to prevent table bloat.
func (s *PostgresStore) PurgeStagedProcessed(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.staging
		WHERE processed AND created_at < NOW() - INTERVAL '7 days'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
