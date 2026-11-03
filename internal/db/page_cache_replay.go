// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
)

// PageCacheReplayKey names one request on the API's re-warm list: a cached
// route's request URI of one repository.
type PageCacheReplayKey struct {
	RepoID int64
	URI    string
}

// PageCacheReplayRow is one entry of the re-warm list and its last
// computation cost in milliseconds (the re-warm replays the costliest first).
type PageCacheReplayRow struct {
	Key    PageCacheReplayKey
	CostMS int64
}

// PageCacheReplayAdvisoryLockID is the saved re-warm list's owner lock
// ("AVLXRPLY" packed into 64 bits).
const PageCacheReplayAdvisoryLockID int64 = 0x41564C5852504C59

// ErrPageCacheReplayHeld is ClaimPageCacheReplay's answer when another
// session owns the list.
var ErrPageCacheReplayHeld = errors.New("the saved re-warm list is held by another session (advisory lock held)")

// ErrPageCacheReplayUnbounded is a load's or save's answer to a context with
// no deadline: they run on the lock's own connection, detached from the
// caller's cancel, so an unbounded statement would hold every later pass
// and the stop (Copilot's review of PR #246 on 0.29.124, M1; SR-18 — the
// layer that writes enforces the bound, and the API gives each call one).
var ErrPageCacheReplayUnbounded = errors.New("the saved re-warm list needs a context with a deadline")

// btreeMaxIndexTupleBytes is PostgreSQL's limit on one btree index entry
// with the default 8 kB page ("index row size … exceeds btree version 4
// maximum 2704"): a page holds three entries, and the entry is MAXALIGNed.
const btreeMaxIndexTupleBytes = 2704

// MaxPageCacheReplayURIBytes is the longest request URI a saved row can
// hold (Copilot's review of PR #246, M2: one over-long row — a crafted
// license filter — failed the whole pass's save). The primary key's entry
// is the 8-byte index tuple header, the 8-byte repo_id and the URI with its
// 4-byte varlena header; a URI of this many bytes fits whatever its bytes
// (an incompressible one too), pinned by TestPageCacheReplayURIBoundFitsTheIndex.
// The API caches no request whose canonical URI is longer, so nothing the
// list cannot hold is ever on it.
const MaxPageCacheReplayURIBytes = btreeMaxIndexTupleBytes - 8 - 8 - 4

// PageCacheReplayList is the saved re-warm list (0.29.122, summary/63 §8)
// while this process owns it.
type PageCacheReplayList struct {
	lock *heldAdvisoryLock
}

// ClaimPageCacheReplay takes ownership of the saved re-warm list for one
// API process, or answers ErrPageCacheReplayHeld when another holds it. The
// list has one owner per database (whole-branch review of
// 0.29.119–0.29.123, A1, operator decision 2026-10-29): each owner saves the
// difference from what it last saved itself, so a second writer deleted
// rows the first still held. The owner holds an advisory lock until
// Release, and its loads and saves run on the lock's own session: once
// that session is gone, so is the lock — another process may then claim
// the list — and the old owner's saves fail instead of writing unowned
// (the api then gives it up and claims it again at its next pass). A
// session-level lock needs a direct or session-mode connection: behind a
// transaction pooler two processes' statements share server sessions, and
// an advisory lock is reentrant within one. Two stated limits (ASVS pass of 0.29.124):
// any role that can connect may take the lock first and keep every api off
// the list — they then re-warm from memory and say so, nothing is written
// unowned; and the held connection is exempt from the pool's
// MaxConnLifetime recycling, which costs nothing here (a session keeps its
// credentials across a rotation, and a failover ends it — the re-claim).
func (s *PostgresStore) ClaimPageCacheReplay(ctx context.Context) (*PageCacheReplayList, error) {
	lock, held, err := s.tryHoldAdvisoryLock(ctx, PageCacheReplayAdvisoryLockID)
	if err != nil {
		return nil, fmt.Errorf("claim page cache replay list: %w", err)
	}
	if !held {
		return nil, ErrPageCacheReplayHeld
	}
	return &PageCacheReplayList{lock: lock}, nil
}

// Release gives the list up (the next process to claim it owns it).
func (l *PageCacheReplayList) Release() { l.lock.release() }

// Held reports whether the list's lock is still held: false once the lock's
// database session ended (a database restart, a dropped connection), after
// which its loads and saves fail and another process may claim it.
func (l *PageCacheReplayList) Held() bool { return !l.lock.conn.Conn().IsClosed() }

// LoadPageCacheReplay reads the saved re-warm list. The rows are a hint
// written by the API; it validates each before it replays one.
func (l *PageCacheReplayList) LoadPageCacheReplay(ctx context.Context) ([]PageCacheReplayRow, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		return nil, ErrPageCacheReplayUnbounded
	}
	rows, err := l.lock.conn.Query(ctx, `SELECT repo_id, uri, cost_ms FROM aveloxis_ops.page_cache_replay`)
	if err != nil {
		return nil, fmt.Errorf("load page cache replay list: %w", err)
	}
	defer rows.Close()
	var out []PageCacheReplayRow
	for rows.Next() {
		var r PageCacheReplayRow
		if err := rows.Scan(&r.Key.RepoID, &r.Key.URI, &r.CostMS); err != nil {
			return nil, fmt.Errorf("load page cache replay list: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load page cache replay list: %w", err)
	}
	return out, nil
}

// SavePageCacheReplay applies one difference to the saved re-warm list in one
// transaction: each upsert stores its row (a key already saved takes the new
// cost), each delete removes its key. An empty difference writes nothing.
func (l *PageCacheReplayList) SavePageCacheReplay(ctx context.Context, upserts []PageCacheReplayRow, deletes []PageCacheReplayKey) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return ErrPageCacheReplayUnbounded
	}
	tx, err := l.lock.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("save page cache replay list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if len(deletes) > 0 {
		var ids []int64
		var uris []string
		for _, k := range deletes {
			ids, uris = append(ids, k.RepoID), append(uris, k.URI)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM aveloxis_ops.page_cache_replay p
			USING unnest($1::bigint[], $2::text[]) AS d(repo_id, uri)
			WHERE p.repo_id = d.repo_id AND p.uri = d.uri`, ids, uris); err != nil {
			return fmt.Errorf("save page cache replay list (delete): %w", err)
		}
	}
	if len(upserts) > 0 {
		var ids, costs []int64
		var uris []string
		for _, r := range upserts {
			ids, uris, costs = append(ids, r.Key.RepoID), append(uris, r.Key.URI), append(costs, r.CostMS)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.page_cache_replay (repo_id, uri, cost_ms)
			SELECT repo_id, uri, GREATEST(cost_ms, 0) FROM unnest($1::bigint[], $2::text[], $3::bigint[]) AS u(repo_id, uri, cost_ms)
			ON CONFLICT (repo_id, uri) DO UPDATE SET cost_ms = EXCLUDED.cost_ms, recorded_at = NOW()`, ids, uris, costs); err != nil {
			return fmt.Errorf("save page cache replay list (upsert): %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("save page cache replay list: %w", err)
	}
	return nil
}
