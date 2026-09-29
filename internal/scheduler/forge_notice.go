// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// recordForgeNotice stores the forge's own message for a blocked or
// disabled repository when err carries one (worklist item 82): GitHub's
// block object from a REST 451/403, or the `remote:` text of a refused
// clone. GitHub and GitLab only — a generic git host can print any remote
// text, and the page presents this as the forge's word. An error without
// a notice changes nothing: only a success clears a stored notice.
func (s *Scheduler) recordForgeNotice(ctx context.Context, repo *model.Repo, err error) {
	if err == nil || (repo.Platform != model.PlatformGitHub && repo.Platform != model.PlatformGitLab) {
		return
	}
	n, ok := platform.NoticeOf(err)
	if !ok {
		return
	}
	if serr := s.store.SetRepoUnavailable(ctx, repo.ID, n.Text(), n.URL); serr != nil {
		if errors.Is(serr, context.Canceled) {
			return // a stop, not a failure
		}
		s.logger.Warn("could not store the forge's notice for the repository",
			"repo_id", repo.ID, "notice", n.Text(), "error", serr)
		return
	}
	s.logger.Info("forge notice recorded",
		"repo_id", repo.ID, "notice", n.Text(), "notice_url", platform.RedactURLUserinfo(n.URL))
}

// repoNoticeFetcher is the one REST request that fetches a repository's
// block object (github.Client.FetchRepoNotice).
type repoNoticeFetcher interface {
	FetchRepoNotice(ctx context.Context, owner, repo string) (platform.ForgeNotice, bool, error)
}

// captureBlockNotice fetches and stores the block notice for a repository
// prelim just sidelined on a 451: prelim's probe is a web HEAD, so the
// status arrives without GitHub's block object. One keyed request per
// sideline (a sidelined repository leaves the queue). A request that gets
// no answer is logged and stores nothing (SR-16).
func (s *Scheduler) captureBlockNotice(ctx context.Context, repo *model.Repo, f repoNoticeFetcher) {
	n, ok, err := f.FetchRepoNotice(ctx, repo.Owner, repo.Name)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // a stop, not a failure
		}
		s.logger.Warn("could not fetch the forge's block notice for a legally blocked repository",
			"repo_id", repo.ID, "error", err)
		return
	}
	if ok {
		s.recordForgeNotice(ctx, repo, &platform.NoticeError{Notice: n, Err: platform.ErrLegallyBlocked})
	}
}

// recheckBlockNotice fetches the block notice for a gone-stamped
// repository whose recheck probe got a 451 (review round 1 F1): a
// repository prelim sidelined never runs the API phase or the facade again,
// so the recheck is the only place its notice can be captured — the fleet's
// existing DMCA repositories included. Refreshed every cadence (one keyed
// request per blocked repository), so a changed notice is picked up. GitHub
// rows only: the REST notice is GitHub's.
func (s *Scheduler) recheckBlockNotice(ctx context.Context, repoID int64, f repoNoticeFetcher) {
	repo, err := s.store.GetRepoByID(ctx, repoID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // a stop, not a failure
		}
		s.logger.Warn("gone recheck: could not load the repository to fetch its block notice",
			"repo_id", repoID, "error", err)
		return
	}
	if repo.Platform != model.PlatformGitHub {
		return
	}
	s.captureBlockNotice(ctx, repo, f)
}

// fillCommitBounds fills a gone repository's stored commit bounds (O11
// option 2, review round 3 F3): a gone repository has no queue row, so no
// facade run will fill them, and its page would scan its commit rows on
// every view. Called from every still-gone arm of the recheck (round 4 F2).
// A filled row is not rewritten (the store's guard).
func (s *Scheduler) fillCommitBounds(ctx context.Context, repoID int64) {
	if err := s.store.RecordCommitBounds(ctx, repoID, time.Time{}, time.Time{}); err != nil {
		if errors.Is(err, context.Canceled) {
			return // a stop, not a failure
		}
		s.logger.Warn("gone recheck: could not fill the repository's commit bounds — its page reads them live until a later recheck",
			"repo_id", repoID, "error", err)
	}
}

// noteCloneOutcome keeps the stored notice in step with the clone. A
// clone or fetch that succeeded (cloned) proves the forge answers, so the
// notice is cleared even when a later facade phase failed (review round 1
// F4: keying on the whole facade's error left a lifted block's banner up
// while git log failed); a refused clone records the forge's remote text.
func (s *Scheduler) noteCloneOutcome(ctx context.Context, repo *model.Repo, cloned bool, err error) {
	if !cloned {
		s.recordForgeNotice(ctx, repo, err)
		return
	}
	if cerr := s.store.ClearRepoUnavailable(ctx, repo.ID); cerr != nil {
		if errors.Is(cerr, context.Canceled) {
			return // a stop, not a failure
		}
		s.logger.Warn("could not clear the forge's notice after a successful clone",
			"repo_id", repo.ID, "error", cerr)
	}
}

// firstNoticeErr returns the first of the API phase's errors — its own,
// then the per-endpoint ones it collected — that carries a forge notice,
// so one job writes the notice once however many endpoints were refused.
func firstNoticeErr(err error, result *collector.CollectResult) error {
	if _, ok := platform.NoticeOf(err); ok {
		return err
	}
	if result != nil {
		for _, e := range result.Errors {
			if _, ok := platform.NoticeOf(e); ok {
				return e
			}
		}
	}
	return nil
}
