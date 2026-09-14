// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// MessageHealer (v0.27.38, summary/18 Phase 1a) — consumes the
// aveloxis_ops.message_heal_worklist captured by the msg_kind
// migration. Each worklist row is a messages row that TWO different
// entity kinds claimed under the old two-column arbiter, so its
// text/author belong to at most one side (the later writer).
//
// Healing is parent-deduplicated: many colliding messages share the
// same issue/PR, and one per-item refetch of a parent re-delivers ALL
// of its comments, so the API cost is per-PARENT, not per-collision.
// Refetched comments flow through the normal staging → Processor path
// and land under the new kinded arbiter as their OWN rows with correct
// text. Afterwards the stale conversation-side bridge links on the
// collision rows (which pointed the issue/PR at the other kind's
// text) are deleted, and the rows are stamped healed.
//
// The refetches ride the v0.27.37 (1g) page-read retry — without it,
// a healing pass over ~200K collisions would itself die on mid-body
// stream CANCELs.
//
// Resumable and idempotent: a parent whose refetch fails leaves its
// messages unhealed for the next run; re-running re-upserts the same
// rows (kinded ON CONFLICT DO UPDATE).

package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// MessageHealResult summarizes one healing pass.
type MessageHealResult struct {
	Pending        int64 // worklist size before the pass
	Healed         int   // rows stamped healed this pass
	ParentsFetched int   // distinct parent refetches performed
	ParentErrors   int   // parents whose refetch failed (their rows stay pending)
	// v0.28.8 cursor plumbing: Batch is the number of worklist rows
	// this pass claimed; MaxMsgID is the highest msg_id among them —
	// the next pass resumes ABOVE it, so a fully-failing batch can
	// never starve the rows behind it.
	Batch    int
	MaxMsgID int64
}

type healParentKey struct {
	repoID int64
	number int64
	side   string // "issue-conv" | "pr-conv" | "pr-review-side"
}

// healPassSize bounds one internal pass — the unit that gets fetched,
// processed, and STAMPED before the next batch starts (v0.28.1 A8).
// Pre-A8, healed_at stamped once at the very end of the whole run:
// the 2026-08 full-batch run was killed ~20% done after 6 days having
// stamped NOTHING, and the operator's recovery was hand-chunked
// --limit 25000 passes. This constant makes that chunking automatic
// and load-bearing: an interrupted run loses at most one pass of
// attribution. 25,000 is the operator-proven chunk from that recovery
// (~4s batch SELECT on the production worklist post-v0.27.67 index).
const healPassSize = 25000

// HealMessages CONSIDERS up to limit worklist rows (0 = all pending)
// — the cap bounds the WORK (rows claimed + parents refetched), not
// just successes: a failure-heavy canary run must stay a canary
// (v0.28.8, Copilot round 5 — decrementing by healed-only let a
// bounded run traverse the whole worklist when failures dominated,
// exactly the cohort a canary probes). It loops
// looping in healPassSize internal passes so progress persists
// incrementally (SR-3: each pass stamps only rows proven processed —
// and stamps them as soon as they ARE proven, not at run end).
//
// v0.28.8 (Copilot round 4): passes walk the worklist by a strictly
// increasing msg_id CURSOR. The old "zero-progress pass terminates"
// rule looked safe but starved: failed rows stay pending, each pass
// reselected the same lowest 25K ids, so once failures filled the
// first batch the run exited "successfully" without ever visiting a
// higher-id row. Now a fully-failing batch just advances the cursor
// — the run ends when no rows remain ABOVE the cursor, failed rows
// stay pending (the worklist IS the resume state, SR-19), and a
// fresh run restarts at 0 to retry them. Callers see ParentErrors
// and must report incomplete work (the CLI exits nonzero).
// dryRun reports the first pass's plan without mutating.
func HealMessages(ctx context.Context, store *db.PostgresStore, client platform.Client, logger *slog.Logger, limit int, dryRun bool) (*MessageHealResult, error) {
	total := &MessageHealResult{}
	remaining := limit // 0 = unbounded
	var cursor int64
	for pass := 1; ; pass++ {
		passLimit := healPassSize
		if remaining > 0 && remaining < passLimit {
			passLimit = remaining
		}
		res, err := runMessageHealPass(ctx, store, client, logger, cursor, passLimit, dryRun)
		if res != nil {
			if pass == 1 {
				total.Pending = res.Pending
			}
			total.Healed += res.Healed
			total.ParentsFetched += res.ParentsFetched
			total.ParentErrors += res.ParentErrors
			total.Batch += res.Batch
		}
		if err != nil {
			return total, err
		}
		if dryRun {
			return total, nil
		}
		if res.Batch == 0 {
			// Nothing pending above the cursor — the walk is done.
			// Rows that failed this run stay pending for the next run.
			return total, nil
		}
		cursor = res.MaxMsgID // strictly increases: the loop terminates
		if remaining > 0 {
			remaining -= res.Batch // rows CONSIDERED, not just healed
			if remaining <= 0 {
				return total, nil
			}
		}
	}
}

// runMessageHealPass is one bounded healing pass. limit caps the
// number of worklist rows considered (0 = all pending). The pass
// stamps its own healed rows before returning.
func runMessageHealPass(ctx context.Context, store *db.PostgresStore, client platform.Client, logger *slog.Logger, afterMsgID int64, limit int, dryRun bool) (*MessageHealResult, error) {
	res := &MessageHealResult{}
	var err error
	res.Pending, err = store.CountMessageHealPending(ctx)
	if err != nil {
		return nil, fmt.Errorf("counting pending heal rows: %w", err)
	}
	if res.Pending == 0 {
		logger.Info("message heal: nothing pending")
		return res, nil
	}
	if limit <= 0 {
		limit = int(res.Pending)
	}

	items, err := store.GetMessageHealBatch(ctx, afterMsgID, limit)
	if err != nil {
		return nil, fmt.Errorf("loading heal batch: %w", err)
	}
	res.Batch = len(items)
	for _, it := range items {
		if it.MsgID > res.MaxMsgID {
			res.MaxMsgID = it.MsgID
		}
	}

	// Parent-dedup: one refetch covers every collision under it.
	parents := map[healParentKey][]int64{} // parent → msg_ids depending on it
	deps := map[int64]int{}                // msg_id → number of parents it waits on
	for _, it := range items {
		add := func(k healParentKey) {
			parents[k] = append(parents[k], it.MsgID)
			deps[it.MsgID]++
		}
		if it.IssueRepoID != 0 {
			add(healParentKey{it.IssueRepoID, it.IssueNumber, "issue-conv"})
		}
		if it.PRConvRepoID != 0 {
			add(healParentKey{it.PRConvRepoID, it.PRConvNumber, "pr-conv"})
		}
		if it.ReviewRepoID != 0 {
			add(healParentKey{it.ReviewRepoID, it.ReviewPRNum, "pr-review-side"})
		}
		if deps[it.MsgID] == 0 {
			// No resolvable parent anywhere (both sides deleted
			// upstream/locally): nothing to refetch — the row heals by
			// stale-link cleanup alone.
			deps[it.MsgID] = 0
		}
	}

	logger.Info("message heal plan",
		"pending", res.Pending, "batch", len(items),
		"distinct_parents", len(parents), "dry_run", dryRun)
	if dryRun {
		return res, nil
	}

	repos := map[int64]struct{ owner, name string }{}
	repoOf := func(id int64) (struct{ owner, name string }, error) {
		if r, ok := repos[id]; ok {
			return r, nil
		}
		r, err := store.GetRepoByID(ctx, id)
		if err != nil {
			return struct{ owner, name string }{}, err
		}
		v := struct{ owner, name string }{r.Owner, r.Name}
		repos[id] = v
		return v, nil
	}

	failed := map[int64]bool{} // msg_id → some parent refetch failed
	// Group parents by repo so each repo gets ONE staging flush +
	// ProcessRepo pass (the Processor resolves parents and writes
	// through the kinded upserts).
	byRepo := map[int64][]healParentKey{}
	for k := range parents {
		byRepo[k.repoID] = append(byRepo[k.repoID], k)
	}

	for repoID, keys := range byRepo {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		rinfo, err := repoOf(repoID)
		if err != nil {
			logger.Warn("message heal: repo lookup failed — its parents stay pending", "repo_id", repoID, "error", err)
			for _, k := range keys {
				for _, m := range parents[k] {
					failed[m] = true
				}
			}
			res.ParentErrors += len(keys)
			continue
		}
		sw := db.NewStagingWriter(store, repoID, int16(1), logger)
		staged := 0
		for _, k := range keys {
			fetchErr := healFetchParent(ctx, client, sw, rinfo.owner, rinfo.name, k, &staged)
			res.ParentsFetched++
			if fetchErr != nil {
				if isOptionalEndpointSkip(fetchErr) {
					// Parent gone upstream (404/410) — nothing to
					// refetch; the stale-link cleanup below is still
					// the right outcome for its messages.
					logger.Info("message heal: parent gone upstream — healing by cleanup only",
						"repo_id", k.repoID, "number", k.number, "side", k.side)
					continue
				}
				// Round-8 burn-down: a cancelled context is a `stop serve`, not a
				// defect. Only the log is suppressed — surrounding behaviour is
				// unchanged and the work is retried on the next cycle.
				if !errors.Is(fetchErr, context.Canceled) {
					logger.Warn("message heal: parent refetch failed — its messages stay pending",
						"repo_id", k.repoID, "number", k.number, "side", k.side, "error", fetchErr)
				}
				res.ParentErrors++
				for _, m := range parents[k] {
					failed[m] = true
				}
			}
		}
		if err := sw.Flush(ctx); err != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(err, context.Canceled) {
				logger.Warn("message heal: staging flush failed — repo's messages stay pending", "repo_id", repoID, "error", err)
			}
			for _, k := range keys {
				for _, m := range parents[k] {
					failed[m] = true
				}
			}
			continue
		}
		if staged > 0 {
			proc := NewProcessor(store, logger)
			if err := proc.ProcessRepo(ctx, repoID, 1); err != nil {
				// Round-8 burn-down: a cancelled context is a `stop serve`, not a
				// defect. Only the log is suppressed — surrounding behaviour is
				// unchanged and the work is retried on the next cycle.
				if !errors.Is(err, context.Canceled) {
					logger.Warn("message heal: processing failed — repo's messages stay pending", "repo_id", repoID, "error", err)
				}
				for _, k := range keys {
					for _, m := range parents[k] {
						failed[m] = true
					}
				}
				continue
			}
		}
	}

	// Cleanup + stamp for every item whose parents ALL succeeded.
	var healedIDs []int64
	for _, it := range items {
		if failed[it.MsgID] {
			continue
		}
		// Stale cross-kind links: collision rows were interim-assigned
		// kind 2 or 3 by the backfill, so the conversation-side links
		// on them are the stale half (the refetch just recreated the
		// correct links against new kind-1 rows).
		if it.MsgKind == db.MsgKindReviewComment || it.MsgKind == db.MsgKindReviewBody {
			if err := store.DeleteStaleConversationRefs(ctx, it.MsgID); err != nil {
				// Round-8 burn-down: a cancelled context is a `stop serve`, not a
				// defect. Only the log is suppressed — surrounding behaviour is
				// unchanged and the work is retried on the next cycle.
				if !errors.Is(err, context.Canceled) {
					logger.Warn("message heal: stale-link cleanup failed — row stays pending", "msg_id", it.MsgID, "error", err)
				}
				continue
			}
		}
		healedIDs = append(healedIDs, it.MsgID)
	}
	if err := store.MarkMessagesHealed(ctx, healedIDs, time.Now()); err != nil {
		return res, fmt.Errorf("stamping healed rows: %w", err)
	}
	res.Healed = len(healedIDs)
	logger.Info("message heal pass complete",
		"healed", res.Healed, "parents_fetched", res.ParentsFetched,
		"parent_errors", res.ParentErrors, "remaining", res.Pending-int64(res.Healed))
	return res, nil
}

// healFetchParent refetches one parent's comments/reviews and stages
// them through the normal entity envelopes.
func healFetchParent(ctx context.Context, client platform.Client, sw *db.StagingWriter, owner, repo string, k healParentKey, staged *int) error {
	switch k.side {
	case "issue-conv":
		for ref, err := range client.ListCommentsForIssue(ctx, owner, repo, int(k.number)) {
			if err != nil {
				return err
			}
			if err := sw.Stage(ctx, EntityMessage, ref); err != nil {
				return err
			}
			*staged++
		}
	case "pr-conv":
		for ref, err := range client.ListCommentsForPR(ctx, owner, repo, int(k.number)) {
			if err != nil {
				return err
			}
			if err := sw.Stage(ctx, EntityMessage, ref); err != nil {
				return err
			}
			*staged++
		}
	case "pr-review-side":
		// Inline comments AND review bodies both hang off the PR: the
		// per-PR review-comments endpoint re-delivers the inline rows,
		// and ListPRReviews re-delivers the body rows. Fetch both — a
		// collision row's stored text may belong to either.
		for rc, err := range client.ListReviewCommentsForPR(ctx, owner, repo, int(k.number)) {
			if err != nil {
				return err
			}
			if err := sw.Stage(ctx, EntityReviewComment, rc); err != nil {
				return err
			}
			*staged++
		}
		for range client.ListPRReviews(ctx, owner, repo, int(k.number)) {
			// Reviews are not a staged entity type; body refresh rides
			// the next PR-batch cycle. Draining the iterator here would
			// be wasted work — reviews are re-upserted by force-full
			// collection, and the review-body collision class is the
			// small remainder (conversation ∩ inline dominates). Break
			// immediately; the stale-link cleanup is what matters.
			break
		}
	}
	return nil
}
