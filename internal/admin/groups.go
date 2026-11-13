// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

const (
	// MaxGroupNameLength is the longest group name, in characters (the
	// API-token label's limit).
	MaxGroupNameLength = 200
	// MaxPasteURLs is how many URLs one add may carry: a paste of a large
	// project's repositories fits; a larger set is added as its
	// organization. Policy number (2026-10-10), stated in api.md.
	MaxPasteURLs = 1000
)

// CreateGroup makes a group owned by the actor's account, or returns the
// id of the actor's group already called that (the store's upsert). The
// name is trimmed; empty or over MaxGroupNameLength is InvalidInput.
func (s *Service) CreateGroup(ctx context.Context, a Actor, name string) (int64, error) {
	const op = "create group"
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, refuseInput("a group needs a name")
	}
	if utf8.RuneCountInString(name) > MaxGroupNameLength {
		return 0, refuseInput(fmt.Sprintf("a group name is at most %d characters", MaxGroupNameLength))
	}
	id, err := s.st.CreateUserGroup(ctx, a.UserID, name)
	if err != nil {
		return 0, fmt.Errorf("%s by %s: %w", op, a, err)
	}
	s.audit(op, a, "group_id", id)
	return id, nil
}

// Access is what the actor wants to do with a group.
type Access int

const (
	Read Access = iota + 1
	Write
)

func (ac Access) String() string {
	if ac == Write {
		return "write"
	}
	return "read"
}

// GroupRef is a group the actor was granted access to.
type GroupRef struct {
	ID      int64
	Name    string
	OwnerID int
}

// GroupAccess is the one group access rule (eval-2 item 5; DBS-1, WEB-15,
// API-12): the owner reads and writes; an administrator — a Session or the
// Operator, never an API token — reads another account's group, logged at
// INFO; everyone else is refused with db.ErrGroupNotOwned, and a group that
// does not exist with db.ErrGroupNotFound. Adapters answer both the same
// way (N11), so a stranger learns nothing about a group's existence. An
// administrator never gains Write on another account's group here: the
// owner-only writes stay owner-only (the store enforces that too, SR-18).
func (s *Service) GroupAccess(ctx context.Context, a Actor, groupID int64, want Access) (GroupRef, error) {
	owner, name, err := s.st.GroupOwner(ctx, groupID)
	if errors.Is(err, db.ErrGroupNotFound) {
		return GroupRef{}, err
	}
	if err != nil {
		return GroupRef{}, fmt.Errorf("group access for %s: %w", a, err)
	}
	ref := GroupRef{ID: groupID, Name: name, OwnerID: owner}
	if owner == a.UserID {
		// The owner, whatever the credential: a token writes to its
		// owner's groups, and the Operator acting for an account is that
		// account here.
		return ref, nil
	}
	if want == Read {
		admin, err := s.IsAdmin(ctx, a)
		if err != nil {
			return GroupRef{}, err
		}
		if admin {
			s.logger.Info("admin: read another account's group", "actor", a.String(), "group_id", groupID, "owner_user_id", owner)
			return ref, nil
		}
	}
	return GroupRef{}, fmt.Errorf("group %d (%s access by %s): %w", groupID, want, a, db.ErrGroupNotOwned)
}

// AuthorizeGroupRepo is GroupAccess(Read) plus membership: the repository
// must be linked into that group (ErrRepoNotInGroup, answered as
// NotFound). The web GUI's repository routes name both ids.
func (s *Service) AuthorizeGroupRepo(ctx context.Context, a Actor, groupID, repoID int64) (GroupRef, error) {
	ref, err := s.GroupAccess(ctx, a, groupID, Read)
	if err != nil {
		return GroupRef{}, err
	}
	in, err := s.st.RepoInGroup(ctx, groupID, repoID)
	if err != nil {
		return GroupRef{}, fmt.Errorf("authorize repo for %s: %w", a, err)
	}
	if !in {
		return GroupRef{}, fmt.Errorf("repo %d in group %d: %w", repoID, groupID, ErrRepoNotInGroup)
	}
	return ref, nil
}

// AddOutcome is what AddToGroup did: the store's counts, plus the lines
// of the paste that were not repository URLs (each "url: why", with any
// credential redacted) — they were skipped, the rest went through.
type AddOutcome struct {
	db.AddOutcome
	Invalid []string
}

// AddToGroup adds a paste of repository URLs to the actor's group under
// the per-add approval rule (web-api rules: tracked repositories link at
// once, an administrator's new ones are queued, a non-admin's wait for
// approval unless the auto-approve limit covers them). Every line is
// parsed as a repository URL first — a missing scheme is supplied, a
// GitHub, GitLab or generic git host is accepted, anything else is
// reported in Invalid and skipped; a paste with no usable line is
// InvalidInput; more than MaxPasteURLs lines is TooMany (a credentialed
// line is reported in Invalid and skipped, like any other the parser
// refuses). The store refuses the whole paste for a URL over its byte
// limit, for a group the actor does not own or an administrator rejected, and
// for an account whose allocation the paste would exceed (capacity). The
// actor's ScopeChanged hook runs when anything was linked or queued, even
// when some items failed; a pending request notifies the operator.
func (s *Service) AddToGroup(ctx context.Context, a Actor, groupID int64, paste []string) (AddOutcome, error) {
	const op = "add repositories"
	var out AddOutcome
	// Blank lines are not URLs: a textarea paste of exactly MaxPasteURLs
	// lines ends in a newline (round-2 review of 0.29.100).
	lines := make([]string, 0, len(paste))
	for _, line := range paste {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) > MaxPasteURLs {
		return out, refuseTooMany(fmt.Sprintf("at most %d URLs per add: paste in parts, or add the organization", MaxPasteURLs))
	}
	urls := make([]string, 0, len(lines))
	for _, line := range lines {
		if platform.SCPCloneAddress(line) {
			// git@host:owner/repo is a git address, not a web URL; said so,
			// rather than "credentials" after the scheme is supplied
			// (review round 1 F11). The platform's one predicate decides,
			// with its boundary against a credential-shaped paste
			// (0.29.106 whole-branch review A1: a second spelling here
			// disagreed on `git@host:1234/repo`).
			out.Invalid = append(out.Invalid, logURL(line)+": a git SSH address, not a web URL (expected https://host/owner/repo)")
			continue
		}
		ru, err := platform.ParseAnyRepoURLWithHints(platform.WithDefaultScheme(line), s.gitlabHints())
		if err != nil {
			reason := "not a repository URL (expected https://host/owner/repo)"
			switch {
			case errors.Is(err, platform.ErrURLUserinfo):
				reason = "URL must not contain credentials"
			case platform.IsForgeOwnerPageURL(line):
				// What people paste when they mean all of it (0.29.146).
				reason = "an organization, group or user page, not a repository (add it as an organization or group instead, or give https://host/owner/repo)"
			}
			out.Invalid = append(out.Invalid, logURL(line)+": "+reason)
			continue
		}
		urls = append(urls, ru.Raw)
	}
	if len(urls) == 0 {
		if len(out.Invalid) > 0 {
			// The one sentence for a refused URL (the adapters answer the
			// error, not the outcome); the refused lines, redacted, are
			// logged here, since they are what an operator asks about.
			err := refuseInput(urlInputDetail)
			s.refused(op, a, err, "group_id", groupID, "invalid", out.Invalid)
			return out, err
		}
		return out, refuseInput("nothing to add: give at least one repository URL")
	}
	res, err := s.st.AddReposToGroup(ctx, a.UserID, groupID, urls, s.opts.AutoApproveAddLimit, s.glWebBase)
	out.AddOutcome = res
	if res.Linked+res.Enqueued > 0 {
		// The actor's scope changed, whatever the error: an add that fails
		// for some URLs (ErrAddItemsFailed) has still linked the others
		// (PR #218 review C1). Only the actor's cache (0.29.82).
		s.hooks.ScopeChanged(a.UserID)
	}
	if err != nil {
		if errors.Is(err, db.ErrAddItemsFailed) {
			// Not the caller's input: items the pass could not add (the
			// others were). The operator should see it; the message holds
			// counts only, no database text.
			s.logger.Warn("admin: add repositories: some could not be added", "actor", a.String(), "group_id", groupID, "error", err)
			return out, err
		}
		if _, refused := Classify(err); refused {
			s.refused(op, a, err, "group_id", groupID, "urls", len(urls))
			return out, err
		}
		return out, fmt.Errorf("%s to group %d by %s: %w", op, groupID, a, err)
	}
	s.audit(op, a, "group_id", groupID, "linked", res.Linked, "enqueued", res.Enqueued, "pending_approval", res.Pending, "invalid", len(out.Invalid))
	if res.Pending > 0 {
		s.NotifyAddRequestSubmitted(res.RequestID)
	}
	return out, nil
}

// AddOrgToGroup registers an organisation for tracking in the actor's
// group: an administrator's registration is approved at once, a
// non-admin's new organisation waits for approval (an already-registered
// one auto-approves, v0.27.84). Nothing is listed here (summary/59 C2: the
// web and API processes hold no forge keys): serve's org refresh finds the
// registration on its next pass. No scope hook: registering links no
// repository; the links the refresh makes later reach the API's caches
// through their TTLs (PR #218 review C3, recorded).
func (s *Service) AddOrgToGroup(ctx context.Context, a Actor, groupID int64, orgURL string) (db.OrgAddOutcome, error) {
	const op = "add organization"
	orgURL = strings.TrimSpace(orgURL)
	if orgURL == "" {
		return db.OrgAddOutcome{}, refuseInput("nothing to add: give the organization's URL")
	}
	out, err := s.st.AddOrgToGroup(ctx, a.UserID, groupID, orgURL, s.opts.GitHubAPIBase, s.glWebBase)
	if err != nil {
		switch {
		case errors.Is(err, db.ErrOrgOffGitHubHost):
			err = &offHostError{cause: db.ErrOrgOffGitHubHost, host: platform.GitHubWebHost(s.opts.GitHubAPIBase)}
		case errors.Is(err, db.ErrOrgOffGitLabInstance):
			err = &offHostError{cause: db.ErrOrgOffGitLabInstance, host: db.GitLabInstanceLabel(s.glWebBase)}
		}
		if _, refused := Classify(err); refused {
			s.refused(op, a, err, "group_id", groupID, "org_url", logURL(orgURL))
			return out, err
		}
		return out, fmt.Errorf("%s to group %d by %s: %w", op, groupID, a, err)
	}
	s.audit(op, a, "group_id", groupID, "registered", out.Registered, "request_id", out.RequestID)
	if !out.Registered {
		s.NotifyAddRequestSubmitted(out.RequestID)
	}
	return out, nil
}

// RemoveFromGroup unlinks a repository from the actor's own group
// (db.ErrGroupNotOwned otherwise). removed=false: it was not in the group
// (another tab, a stale page); nothing changed. The actor's ScopeChanged
// hook runs after a removal.
func (s *Service) RemoveFromGroup(ctx context.Context, a Actor, groupID, repoID int64) (bool, error) {
	const op = "remove repository"
	removed, err := s.st.RemoveRepoFromGroup(ctx, a.UserID, groupID, repoID)
	if err != nil {
		if _, refused := Classify(err); refused {
			s.refused(op, a, err, "group_id", groupID, "repo_id", repoID)
			return false, err
		}
		return false, fmt.Errorf("%s %d from group %d by %s: %w", op, repoID, groupID, a, err)
	}
	if removed {
		s.hooks.ScopeChanged(a.UserID)
		s.audit(op, a, "group_id", groupID, "repo_id", repoID)
	}
	return removed, nil
}
