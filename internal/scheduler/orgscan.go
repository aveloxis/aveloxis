// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"net/url"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// The one way a listed repository enters the catalog (stage 1 of
// summary/59, eval-3 target 3): the legacy GitHub org refresh, the legacy
// GitLab group refresh and the user-org pass all list through the forge
// clients (platform/github ListOwnerRepos, platform/gitlab
// ListGroupProjects) and record what they list through ingestDiscovered.
// Before stage 1 the three passes, the web GUI and the CLI each carried
// their own copy of the sequence, with four error policies and three
// silent failure arms between them.

// ingestTargets names where a discovered repository goes: the legacy
// repo_groups row a NEW repository is recorded under (0: none — the
// user-org pass records none) and the user groups it is linked into.
type ingestTargets struct {
	legacyGroupID int64
	userGroupIDs  []int64
}

// ingestOutcome is what ingestDiscovered did: the repository's id, whether
// this call recorded and queued it, and the user groups whose link row this
// call inserted (a repository already linked is not "new" for that group —
// the v0.27.91 rule: 9.3M bogus "new" repositories in the Aug 7–16 2026
// run came from counting the no-op re-link).
type ingestOutcome struct {
	repoID  int64
	created bool
	linked  map[int64]bool
}

// ingestDiscovered records one listed repository in one sequence:
// FindRepoByURL → an existing row gets its forge id backfilled
// (SetPlatformRepoIDIfEmptySeen, fill-empty-only: the org-tracked cohort
// is the population at risk of rename re-discovery, v0.27.102) | a new row
// is recorded (UpsertRepo with the forge id, so the rename-heal inside it
// applies) and queued (EnqueueRepo; a failed enqueue is logged with the
// operator's repair — the next pass takes the existing-row branch, which
// re-queues ONE case: a gone-stamped row the forge lists publicly under the
// forge ID it stores (ResurrectIfListed, 0.29.140); a renamed repository
// is not revived by it — and
// the repository is still linked) and a waiting mailing-list signal is
// resolved onto it (ResolveSignaledRepoForURL, §5c) → the repository is
// linked into every target user group (AddOrgRepoToGroupByID; the
// account's repository quota skips a group, logged once a day by the
// store). Every failure arm is logged; a cancellation is returned
// unlogged, and a lookup failure skips the item (SR-5: a failed
// FindRepoByURL is not "not tracked" — recording would mint a duplicate).
// Not one transaction: that would change lock scope on hot tables for no
// correctness gain.
func (s *Scheduler) ingestDiscovered(ctx context.Context, item platform.OwnerRepo, plat model.Platform, t ingestTargets) (ingestOutcome, error) {
	out := ingestOutcome{linked: map[int64]bool{}}
	repoID, err := s.store.FindRepoByURL(ctx, item.URL)
	if errors.Is(err, context.Canceled) {
		return out, err
	}
	if err != nil {
		s.logger.Warn("org scan: repository lookup failed — not recorded this pass", "url", platform.RedactURLUserinfo(item.URL), "error", err)
		return out, err
	}
	if repoID > 0 {
		out.repoID = repoID
		if item.ForgeID != "" {
			goneUnderListedID, idErr := s.store.SetPlatformRepoIDIfEmptySeen(ctx, repoID, item.ForgeID, item.CreatedAt, item.Private)
			if errors.Is(idErr, context.Canceled) {
				return out, idErr
			}
			if idErr != nil {
				s.logger.Warn("org scan: forge-ID backfill failed — the gone state is unknown, so a public listing still asks for the resurrection", "repo_id", repoID, "error", idErr)
			}
			// 0.29.140: the forge lists this repository PUBLICLY — a
			// gone-stamped row that stores the listed forge ID is reachable
			// again (the stamp was a transient 404, or the organization is
			// public again), so it is cleared and re-enqueued
			// (ResurrectIfListed; nothing for a row that is not gone, or
			// whose stored ID differs — that is the pending forge-ID change
			// above, observation-only; and once per gone stretch — the store
			// refuses after an org-scan resurrection prelim reversed, i.e.
			// one no collection followed (no metadata snapshot after it).
			// bobsql (2026-11-05) sat gone-stamped and dequeued for a day
			// after the scan had listed it. A PRIVATE listing (a member
			// token lists them too) is no sign of life: prelim's probe is
			// unauthenticated and answers 404 for it (round 1 F1). Asked only
			// for a row the id backfill saw gone-stamped under the listed id
			// (Copilot PR #252, ninth review: one more statement per public
			// repository of every scan, almost all of them not gone); the
			// store re-checks every condition in the statement itself. A
			// backfill ERROR is not "not gone" (SR-5): the listing then asks
			// as before, and the statement decides.
			if !item.Private && (goneUnderListedID || idErr != nil) {
				back, rerr := s.store.ResurrectIfListed(ctx, repoID, item.ForgeID, 100)
				if errors.Is(rerr, context.Canceled) {
					return out, rerr
				}
				if rerr != nil {
					s.logger.Warn("org scan: could not resurrect a listed repository — it stays gone until the next scan or recheck", "repo_id", repoID, "url", platform.RedactURLUserinfo(item.URL), "error", rerr)
				} else if back {
					s.logger.Info("org scan: repository listed by its forge again — gone state cleared and re-enqueued",
						"repo_id", repoID, "url", platform.RedactURLUserinfo(item.URL), "forge_id", item.ForgeID)
				}
			}
		}
	} else {
		repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
			Platform:   plat,
			GitURL:     item.URL,
			Name:       item.Name,
			Owner:      item.Owner,
			GroupID:    t.legacyGroupID,
			PlatformID: item.ForgeID,
		})
		if errors.Is(err, context.Canceled) {
			return out, err
		}
		if err != nil {
			s.logger.Warn("org scan: could not record discovered repository", "url", platform.RedactURLUserinfo(item.URL), "error", err)
			return out, err
		}
		out.repoID, out.created = repoID, true
		if enqErr := s.store.EnqueueRepo(ctx, repoID, 100); errors.Is(enqErr, context.Canceled) {
			return out, enqErr
		} else if enqErr != nil {
			s.logger.Warn("org scan: could not queue discovered repository — it will not be collected until `aveloxis reconcile-repos` queues it", "repo_id", repoID, "url", platform.RedactURLUserinfo(item.URL), "error", enqErr)
		}
		s.logger.Info("new repo discovered", "repo", platform.RedactURLUserinfo(item.URL), "repo_id", repoID)
		if n, rerr := s.store.ResolveSignaledRepoForURL(ctx, repoID, item.URL); errors.Is(rerr, context.Canceled) {
			return out, rerr
		} else if rerr != nil {
			s.logger.Warn("org scan: signaled-repository resolution failed", "repo_id", repoID, "error", rerr)
		} else if n > 0 {
			s.logger.Info("resolved signaled_repo for new repo", "repo", platform.RedactURLUserinfo(item.URL), "messages", n)
		}
	}
	for _, gid := range t.userGroupIDs {
		inserted, lerr := s.store.AddOrgRepoToGroupByID(ctx, gid, repoID)
		if errors.Is(lerr, context.Canceled) {
			return out, lerr
		}
		if _, capped := capacity.AsExceeded(lerr); capped {
			continue // the account's repository quota (the store logs it once a day)
		}
		if lerr != nil {
			s.logger.Warn("failed to link discovered repo into user_repos", "repo_id", repoID, "group_id", gid, "error", lerr)
			continue
		}
		if inserted {
			out.linked[gid] = true
		}
	}
	return out, nil
}

// githubOrgClient is the client the org passes list through: the
// deployment's GitHub host and key pool, WITHOUT the collection client's
// rename hook (an org listing that answers 301 must not move a
// repository). nil when the scheduler has no GitHub keys.
func (s *Scheduler) githubOrgClient() *github.Client {
	if s.ghKeys == nil {
		return nil
	}
	s.ghOrgClientOnce.Do(func() { s.ghOrgClient = github.New(s.ghAPIBase, s.ghKeys, s.logger) })
	return s.ghOrgClient
}

// gitlabAPIBase is gitlab.base_url as configured ("" when the block is
// absent); gitlabWebBase its web base (platform.GitLabWebBaseForAPI), the
// one GitLab host this deployment enumerates groups on and sends keys to.
func (s *Scheduler) gitlabAPIBase() string {
	if s.cfg.GitLab == nil {
		return ""
	}
	return s.cfg.GitLab.BaseURL
}

func (s *Scheduler) gitlabWebBase() string {
	base, _ := platform.GitLabWebBaseForAPI(s.gitlabAPIBase())
	return base
}

// gitlabClientForWebURL returns a GitLab client for the instance a web URL
// (a group's website, an org registration) lives on, or ok=false with the
// reason: no GitLab keys, no usable host, or a host other than the
// configured instance's — GitLab keys are only ever sent to that instance
// (platform.GitLabAPIBaseForURL, v0.29.11). On the v0.30 rebase this body
// becomes the instances registry's lookup.
func (s *Scheduler) gitlabClientForWebURL(webURL string) (*gitlab.Client, bool, string) {
	if s.glKeys == nil || s.glKeys.IsEmpty() {
		return nil, false, "no GitLab API keys configured"
	}
	// A website with no usable host is never assumed to be the instance
	// (v0.29.11 review: a schemeless legacy website fell back to gitlab.com).
	if u, err := url.Parse(webURL); err != nil || u.Host == "" {
		return nil, false, "the URL has no usable host"
	}
	// The gate's own host rule (platform.OnGitLabInstance through
	// GitLabAPIBaseForURL): a URL the gate admitted is never refused here
	// (round-2 review of 0.29.100: www. and a default port were).
	apiBase, ok := platform.GitLabAPIBaseForURL(s.gitlabAPIBase(), webURL)
	if !ok {
		return nil, false, "the URL is not on the configured GitLab instance (gitlab.base_url), and GitLab API keys are only sent to that instance"
	}
	return gitlab.New(apiBase, s.glKeys, s.logger), true, ""
}
