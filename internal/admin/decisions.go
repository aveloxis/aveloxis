// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// DecideAddRequest approves or rejects a pending add request. changed is
// whether THIS call decided it: a second click finds it decided and
// notifies nobody. An approved repos request's items are processed in the
// background — a 50K-item approval must not hold the admin's response —
// and re-approving an approved repos request resumes that pass (items are
// stamped as they go, so a finished pass does nothing). An org approval
// registers the organisation in the store's transaction and lists nothing
// here (summary/59 C2): serve's refresh enumerates it. The requester is
// mailed the decision; their ScopeChanged hook runs after the decision
// and again when the pass ends (a request served while it ran cached the
// old scope).
//
// Refusals: db.ErrAddRequestNotFound; db.ErrGroupRejected (the group takes
// no approval); *OrgUnregistrableError when the organisation could never
// be enumerated (off this deployment's GitHub host or GitLab instance,
// credentialed or over-long URL — reject it instead).
func (s *Service) DecideAddRequest(ctx context.Context, a Actor, requestID int64, approve bool) (changed bool, err error) {
	const op = "decide add request"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return false, err
	}
	req, changed, err := s.st.DecideAddRequest(ctx, requestID, a.UserID, approve, s.opts.GitHubAPIBase, s.glWebBase)
	if err != nil {
		if errors.Is(err, db.ErrOrgOffGitHubHost) || errors.Is(err, db.ErrOrgOffGitLabInstance) || errors.Is(err, platform.ErrURLUserinfo) || errors.Is(err, db.ErrURLTooLong) {
			// A pending org that nothing could ever enumerate cannot be
			// approved (v0.29.57 round 3; review 5267193512: the message
			// names no URL).
			err = &OrgUnregistrableError{Cause: err, Advice: db.OrgApprovalRefusalAdvice(err, s.opts.GitHubAPIBase, s.glWebBase)}
		}
		if _, refused := Classify(err); refused {
			s.refused(op, a, err, "request_id", requestID, "approve", approve)
			return false, err
		}
		return false, fmt.Errorf("%s %d (approve=%t) by %s: %w", op, requestID, approve, a, err)
	}
	resume := !changed && approve && req.Status == "approved" && req.Kind != "org"
	if !changed && !resume {
		s.logger.Info("admin: add request already decided", "actor", a.String(), "request_id", requestID, "status", req.Status)
		return false, nil
	}
	if approve && req.Kind != "org" {
		s.runApprovedPass(req)
	}
	if !changed {
		return false, nil
	}
	s.audit(op, a, "request_id", requestID, "approve", approve, "kind", req.Kind, "group_id", req.GroupID, "requester_user_id", req.UserID, "items", req.ItemCount)
	// Approval changes the requester's repository scope (an org
	// registration changes it later, through serve).
	s.hooks.ScopeChanged(req.UserID)
	if req.UserEmail != "" {
		s.send("add-request decision email", []any{"request_id", requestID}, func(m Mailer) error {
			return m.SendAddRequestDecided(req.UserEmail, req.UserLogin, req.GroupName, req.Kind, approve, req.ItemCount)
		})
	}
	return true, nil
}

// runApprovedPass processes an approved repos request's items in the
// background, detached from the admin's request, and drops the
// requester's cached scope when it ends.
func (s *Service) runApprovedPass(req db.AddRequest) {
	safego.Go(s.logger, "approved-add-request", func() {
		// The pass links repos into the requester's scope, so drop their
		// cache again when it ends — a request resolved while it ran cached
		// the old scope (Copilot review of PR #207).
		defer s.hooks.ScopeChanged(req.UserID)
		n, failed, err := s.st.ProcessApprovedAddRequest(context.Background(), req.RequestID, s.glWebBase)
		switch {
		case errors.Is(err, db.ErrAddRequestInProgress):
			s.logger.Info("add-request is already being processed", "request_id", req.RequestID)
		case errors.Is(err, db.ErrGroupRejected):
			// v0.29.71: the pass refuses a rejected group's items;
			// re-approving the request cannot change that.
			s.logger.Info("approved add-request not processed — its group was rejected", "request_id", req.RequestID, "group_id", req.GroupID)
		case err != nil:
			s.logger.Warn("processing approved add-request failed — re-approving resumes it",
				"request_id", req.RequestID, "processed", n, "failed", failed, "error", err)
		case failed > 0:
			s.logger.Warn("approved add-request processed with items that could not be added", "request_id", req.RequestID, "repos", n, "failed", failed)
		default:
			s.logger.Info("approved add-request processed", "request_id", req.RequestID, "repos", n)
		}
	})
}

// NotifyAddRequestSubmitted mails the operator about a new pending add
// request (v0.27.20): best-effort, in the background; nothing without a
// mailer or an operator address. One row is read (db.GetAddRequest).
func (s *Service) NotifyAddRequestSubmitted(requestID int64) {
	m := s.opts.Mailer
	if m == nil || m.OperatorEmail() == "" || requestID == 0 {
		return
	}
	safego.Go(s.logger, "add-request-submitted-email", func() {
		req, err := s.st.GetAddRequest(context.Background(), requestID)
		if err != nil {
			s.logger.Warn("add-request email: request could not be read", "request_id", requestID, "error", err)
			return
		}
		sample := req.SampleURLs
		if req.Kind == "org" {
			sample = []string{req.OrgURL}
		}
		if err := m.SendAddRequestSubmitted(m.OperatorEmail(), req.UserLogin, req.GroupName, req.Kind, req.ItemCount, sample, req.RequestID); err != nil && !mailer.IsSkip(err) {
			s.logger.Warn("failed to send add-request email", "request_id", requestID, "error", err)
		}
	})
}

// RejectGroup flips a group to rejected: it refuses every later addition
// and its tracked organisations stop being listed; its repositories stay
// (the owner might appeal) and remain in everyone's scope — visibility was
// never gated — so no scope hook runs. Any non-rejected group.
func (s *Service) RejectGroup(ctx context.Context, a Actor, groupID int64) error {
	const op = "reject group"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return err
	}
	// The store's flip touches no row for a group that does not exist AND
	// for one already rejected (idempotent); only the first is a refusal
	// (review round 1 F7: the audit line named a group that did not exist).
	if _, _, err := s.st.GroupOwner(ctx, groupID); err != nil {
		if errors.Is(err, db.ErrGroupNotFound) {
			s.refused(op, a, err, "group_id", groupID)
			return err
		}
		return fmt.Errorf("%s %d by %s: %w", op, groupID, a, err)
	}
	changed, err := s.st.RejectGroup(ctx, groupID, a.UserID)
	if err != nil {
		return fmt.Errorf("%s %d by %s: %w", op, groupID, a, err)
	}
	if !changed {
		// Already rejected (or gone since the owner read): idempotent, and
		// an audit line records a change (whole-branch review of PR #238, A4).
		s.logger.Info("admin: group already rejected — no change, not audited", "actor", a.String(), "group_id", groupID)
		return nil
	}
	s.audit(op, a, "group_id", groupID)
	return nil
}

// AdoptNoteMaxLength bounds the note recorded with a forge-ID adoption, in
// characters — the unit a form's maxlength counts, and the capacity
// notes' unit (whole-branch review of 0.29.109: the bound was bytes, so a
// 400-character note of 3-byte characters passed the form and was refused).
const AdoptNoteMaxLength = 1000

// AdoptForgeID adopts one recorded forge-ID change — the pair the
// administrator chose, never "the newest" (review of v0.29.63) — so the
// repository is treated as a continuation of the new upstream. Refusals:
// InvalidInput (a missing id, an over-long note); db.ErrNoPendingForgeIDChange
// (NotFound: adopted already, or never observed); *supersededError when
// the row no longer stores oldID (another adoption won; nothing written).
// The CLI's `aveloxis adopt-forge-id` is a different operation: it asks
// the forge for the current id first (cmd/aveloxis/adopt_forge_id.go).
func (s *Service) AdoptForgeID(ctx context.Context, a Actor, repoID int64, oldID, newID, note string) error {
	const op = "adopt forge-ID change"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return err
	}
	if oldID == "" || newID == "" {
		return refuseInput("old_forge_id and new_forge_id are required: they name the change to adopt")
	}
	// A forge ID is a positive integer (model.ForgeIDString); anything else
	// names no recorded change and is not logged (round-2 review of
	// 0.29.100: the ids reached the audit attrs unbounded).
	if _, ok := model.ParseForgeID(oldID); !ok {
		return refuseInput("old_forge_id is not a forge ID (a positive integer)")
	}
	if _, ok := model.ParseForgeID(newID); !ok {
		return refuseInput("new_forge_id is not a forge ID (a positive integer)")
	}
	if utf8.RuneCountInString(note) > AdoptNoteMaxLength {
		return refuseInput("note too long")
	}
	adoptedBy := s.actorLabel(ctx, a)
	err := s.st.AdoptPendingForgeIDChange(ctx, repoID, oldID, newID, adoptedBy, note)
	switch {
	case errors.Is(err, db.ErrNoPendingForgeIDChange):
		s.refused(op, a, err, "repo_id", repoID, "old", oldID, "new", newID)
		return err
	case errors.Is(err, db.ErrSchemaNotMigrated):
		// A deployment fault, not the administrator's request: ERROR, like
		// the readers' own schema-behind lines, so a grep of api.log or
		// web.log finds the click (the startup ERROR may be days old). The
		// adapters answer 503 with the deploy steps; nothing was written.
		httpserver.LogFailure(ctx, s.logger, slog.LevelError, err, "admin: adopt forge-ID change refused — the database schema is behind this binary; run the deploy steps",
			"actor", a.String(), "repo_id", repoID, "old", oldID, "new", newID, "error", err)
		return err
	case errors.Is(err, db.ErrForgeIDNotAsExpected):
		// The stored ID is no longer this change's old ID — usually another
		// change was adopted (the CLI, a sibling row, or a second admin's
		// click on this same change). Nothing was written.
		sup := &supersededError{oldID: oldID}
		s.refused(op, a, sup, "repo_id", repoID, "old", oldID, "new", newID)
		return sup
	case err != nil:
		return fmt.Errorf("%s for repo %d by %s: %w", op, repoID, a, err)
	}
	s.audit(op, a, "repo_id", repoID, "old", oldID, "new", newID, "adopted_by", adoptedBy)
	return nil
}

// actorLabel is the name recorded as the adopter: the account's label, or
// "operator" for the CLI. A failed label lookup records the user id (logged;
// the adoption is not refused for it).
func (s *Service) actorLabel(ctx context.Context, a Actor) string {
	if a.Kind == Operator {
		return "operator"
	}
	label, err := s.st.UserLabel(ctx, a.UserID)
	if err != nil {
		httpserver.LogFailure(ctx, s.logger, slog.LevelWarn, err, "admin: label lookup failed — recording the user id", "user_id", a.UserID, "error", err)
		return "user " + strconv.Itoa(a.UserID)
	}
	return label
}
