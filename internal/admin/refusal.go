// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"errors"
	"fmt"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Kind is why an operation refused. The Service returns typed errors;
// Classify names the kind, and each adapter keeps ONE table from kind to
// its answer (the API's status and JSON body, the web GUI's status or
// query flag) — the words stay presentation, the decision is here
// (eval-2 item 4: the same sentinel was 400, 403 and 404 across seven
// sites).
type Kind int

const (
	// Forbidden: not an administrator (ErrNotAdmin).
	Forbidden Kind = iota + 1
	// NotFound: no such group, repository-in-group, or forge-ID change.
	NotFound
	// NotOwned: the group belongs to someone else. Adapters answer it as
	// NotFound (N11): a group's existence is not disclosed to a stranger.
	NotOwned
	// GroupRejected: an administrator rejected the group; it takes no
	// additions and its requests no approval.
	GroupRejected
	// InvalidInput: the caller's input, fixable by the caller (an empty
	// name, a URL that is not a repository, carries credentials or is too
	// long, a value the database refused).
	InvalidInput
	// TooMany: more items than one request may carry.
	TooMany
	// OffHost: an organisation on another GitHub host than this
	// deployment's, or a GitLab group not on its configured instance
	// (Detail names the host or instance).
	OffHost
	// OrgUnregistrable: an approval that would register an organisation
	// nothing can enumerate (off-host, credentialed or over-long URL);
	// Detail is the advice (reject it instead).
	OrgUnregistrable
	// Capacity: the account's allocation would be exceeded; Exceeded
	// carries the refusal the API renders (nothing was written).
	Capacity
	// Partial: some repositories could not be added (Detail has the
	// counts); the others were.
	Partial
	// LastAdmin: demoting the only administrator.
	LastAdmin
	// SelfDemotion: an administrator demoting their own account.
	SelfDemotion
	// RequestNotFound: no such add request.
	RequestNotFound
	// UserNotFound: no such user.
	UserNotFound
	// Superseded: the forge-ID change was overtaken by another adoption;
	// nothing was written (Detail says what the row holds now).
	Superseded
	// Conflict: the request is well formed but the current state refuses
	// it — a name already taken, a quota aveloxis.json decides (Detail
	// names what to change instead). Adapters answer 409 (stage 3b).
	Conflict
	// ReauthenticationRequired: a sensitive change to the actor's own
	// account (ending every session of it) asked by a sign-in older than the
	// inactivity bound, or by a credential with no sign-in that counts
	// (ASVS V7.5.2; Copilot's seventh review of PR #246, 0.29.129: the rule
	// is the Service's, so every route reaching the operation takes it).
	// Detail says what needs the sign-in and the bound; adapters answer
	// 403 reauthentication_required.
	ReauthenticationRequired
	// NotMigrated: the database schema is behind this binary (the api and
	// web processes do not migrate); nothing was written. Detail names the
	// deploy steps. Adapters answer 503: a temporary condition of the
	// service, fixed by the migrate (Copilot PR #252, fourteenth review).
	NotMigrated
)

func (k Kind) String() string {
	switch k {
	case Forbidden:
		return "forbidden"
	case NotFound:
		return "not found"
	case NotOwned:
		return "not owned"
	case GroupRejected:
		return "group rejected"
	case InvalidInput:
		return "invalid input"
	case TooMany:
		return "too many"
	case OffHost:
		return "off host"
	case OrgUnregistrable:
		return "org unregistrable"
	case Capacity:
		return "capacity"
	case Partial:
		return "partial"
	case LastAdmin:
		return "last admin"
	case SelfDemotion:
		return "self demotion"
	case RequestNotFound:
		return "request not found"
	case UserNotFound:
		return "user not found"
	case Superseded:
		return "superseded"
	case Conflict:
		return "conflict"
	case ReauthenticationRequired:
		return "reauthentication required"
	case NotMigrated:
		return "not migrated"
	}
	return fmt.Sprintf("kind %d", int(k))
}

// Refusal is a classified refusal: the kind, the detail an adapter may
// show (empty when the kind's table text is the whole answer), and the
// capacity refusal when Kind is Capacity.
type Refusal struct {
	Kind     Kind
	Detail   string
	Exceeded *capacity.Exceeded
}

// ErrInvalidInput is the sentinel every InvalidInput refusal matches
// (errors.Is); the detail rides on the value.
var ErrInvalidInput = errors.New("invalid input")

// ErrTooMany is the sentinel every TooMany refusal matches.
var ErrTooMany = errors.New("too many items")

// ErrSelfDemotion is the refusal when an administrator asks to demote
// their own account.
// Refused ahead of the store: the last-admin rule would let the SECOND
// administrator do it, leaving them unable to undo.
var ErrSelfDemotion = errors.New("refusing self-demotion — use another admin account")

// ErrRepoNotInGroup is the refusal when the repository is not linked into
// that group.
var ErrRepoNotInGroup = errors.New("that repository is not in that group")

// ErrRecentSignInRequired is the sentinel every ReauthenticationRequired
// refusal matches (errors.Is); the detail rides on the value.
var ErrRecentSignInRequired = errors.New("a recent sign-in is required")

// reauthError is a ReauthenticationRequired refusal with its own words.
type reauthError struct{ detail string }

func (e *reauthError) Error() string        { return e.detail }
func (e *reauthError) Is(target error) bool { return target == ErrRecentSignInRequired }

// refuseReauthentication is the refusal of what (a verb phrase, "ending
// your own sessions") by a sign-in older than bound — one wording for the
// API's JSON message and the web GUI's page.
func refuseReauthentication(what string, bound time.Duration) error {
	return &reauthError{detail: fmt.Sprintf("%s needs a sign-in within the last %d minutes; sign in again and retry", what, int(bound.Minutes()))}
}

// inputError is an InvalidInput or TooMany refusal with its own words.
type inputError struct {
	sentinel error
	detail   string
}

func (e *inputError) Error() string        { return e.detail }
func (e *inputError) Is(target error) bool { return target == e.sentinel }

func refuseInput(detail string) error { return &inputError{sentinel: ErrInvalidInput, detail: detail} }

// kindError is a refusal of a kind with its own words, decided inside the
// service before or instead of a store sentinel (stage 3b): a NotFound
// for a quota name, a Conflict for a value aveloxis.json decides.
type kindError struct {
	kind   Kind
	detail string
}

func (e *kindError) Error() string { return e.detail }

func refuseNotFound(detail string) error { return &kindError{kind: NotFound, detail: detail} }
func refuseConflict(detail string) error { return &kindError{kind: Conflict, detail: detail} }
func refuseTooMany(detail string) error {
	return &inputError{sentinel: ErrTooMany, detail: detail}
}

// OrgUnregistrableError is an approval refused because the organisation
// could never be enumerated. Cause is the store's refusal; Advice is the
// one sentence both admin surfaces show (db.OrgApprovalRefusalAdvice).
type OrgUnregistrableError struct {
	Cause  error
	Advice string
}

func (e *OrgUnregistrableError) Error() string { return e.Advice }
func (e *OrgUnregistrableError) Unwrap() error { return e.Cause }

// offHostError: an organisation add refused for its host (the GitHub host
// or the GitLab instance; cause is the store's sentinel); the message
// names the host this deployment serves.
type offHostError struct {
	cause error
	host  string
}

func (e *offHostError) Error() string {
	return e.cause.Error() + " (" + e.host + ")"
}
func (e *offHostError) Unwrap() error { return e.cause }

// supersededError: a forge-ID adoption overtaken by another; the message
// says what the row holds now and that nothing was written.
type supersededError struct {
	oldID string
}

func (e *supersededError) Error() string {
	return "superseded: the repository no longer stores " + e.oldID + " — nothing adopted; reload the list"
}
func (e *supersededError) Unwrap() error { return db.ErrForgeIDNotAsExpected }

// urlInputDetail is the one sentence for a URL the store refused: too
// long, carrying credentials (the message must not echo it; review
// 5261384568), or a value the database rejected.
const urlInputDetail = "a URL in the request is invalid, carries credentials or is too long"

// rejectedValueDetail is the sentence for a value the database refused
// (SQLSTATE class 22: invalid text such as a NUL byte, or a number out of
// its column's range) on an operation that takes no URL.
const rejectedValueDetail = "a value in the request was refused by the database (invalid text, or out of range)"

// Classify names the refusal an operation returned. ok=false means the
// error is not a refusal: a failure the adapter logs and answers 500
// (SR-5: an unknown error is never read as "no").
func Classify(err error) (Refusal, bool) {
	if err == nil {
		return Refusal{}, false
	}
	if ex, ok := capacity.AsExceeded(err); ok {
		return Refusal{Kind: Capacity, Exceeded: ex}, true
	}
	var in *inputError
	if errors.As(err, &in) {
		if in.sentinel == ErrTooMany {
			return Refusal{Kind: TooMany, Detail: in.detail}, true
		}
		return Refusal{Kind: InvalidInput, Detail: in.detail}, true
	}
	var unreg *OrgUnregistrableError
	if errors.As(err, &unreg) {
		return Refusal{Kind: OrgUnregistrable, Detail: unreg.Advice}, true
	}
	var off *offHostError
	if errors.As(err, &off) {
		return Refusal{Kind: OffHost, Detail: off.Error()}, true
	}
	var sup *supersededError
	if errors.As(err, &sup) {
		return Refusal{Kind: Superseded, Detail: sup.Error()}, true
	}
	var ke *kindError
	if errors.As(err, &ke) {
		return Refusal{Kind: ke.kind, Detail: ke.detail}, true
	}
	switch {
	case errors.Is(err, ErrNotAdmin):
		return Refusal{Kind: Forbidden}, true
	case errors.Is(err, db.ErrSchemaNotMigrated):
		return Refusal{Kind: NotMigrated, Detail: err.Error()}, true
	case errors.Is(err, ErrRecentSignInRequired):
		return Refusal{Kind: ReauthenticationRequired, Detail: err.Error()}, true
	case errors.Is(err, ErrSelfDemotion):
		return Refusal{Kind: SelfDemotion, Detail: ErrSelfDemotion.Error()}, true
	case errors.Is(err, db.ErrAddRequestNotFound):
		return Refusal{Kind: RequestNotFound}, true
	case errors.Is(err, db.ErrGroupNotFound):
		return Refusal{Kind: NotFound}, true
	case errors.Is(err, db.ErrGroupNotOwned), errors.Is(err, db.ErrNotGroupOwner):
		return Refusal{Kind: NotOwned}, true
	case errors.Is(err, ErrRepoNotInGroup):
		return Refusal{Kind: NotFound, Detail: ErrRepoNotInGroup.Error()}, true
	case errors.Is(err, db.ErrGroupRejected):
		return Refusal{Kind: GroupRejected, Detail: db.ErrGroupRejected.Error()}, true
	case errors.Is(err, db.ErrOrgOffGitHubHost):
		return Refusal{Kind: OffHost, Detail: db.ErrOrgOffGitHubHost.Error()}, true
	case errors.Is(err, db.ErrOrgOffGitLabInstance):
		return Refusal{Kind: OffHost, Detail: db.ErrOrgOffGitLabInstance.Error()}, true
	case errors.Is(err, db.ErrURLTooLong), errors.Is(err, db.ErrInvalidOrgURL), errors.Is(err, platform.ErrURLUserinfo):
		return Refusal{Kind: InvalidInput, Detail: urlInputDetail}, true
	case db.IsRejectedValue(err):
		// SQLSTATE class 22 from any operation's store call (stage 3b routes
		// thirteen non-URL operations through here): a NUL byte in a token
		// label or a collection name is 22021. The words name the class, not
		// a URL (whole-branch review of 0.29.109).
		return Refusal{Kind: InvalidInput, Detail: rejectedValueDetail}, true
	case errors.Is(err, db.ErrAddItemsFailed):
		return Refusal{Kind: Partial, Detail: err.Error()}, true
	case errors.Is(err, db.ErrLastAdmin):
		return Refusal{Kind: LastAdmin, Detail: db.ErrLastAdmin.Error()}, true
	case errors.Is(err, db.ErrUserNotFound):
		return Refusal{Kind: UserNotFound, Detail: db.ErrUserNotFound.Error()}, true
	case errors.Is(err, db.ErrNoPendingForgeIDChange):
		return Refusal{Kind: NotFound, Detail: "no pending forge-ID change with that pair for this repository (already adopted, or never observed)"}, true
	case errors.Is(err, db.ErrForgeIDNotAsExpected):
		return Refusal{Kind: Superseded, Detail: "superseded: another change was adopted first — nothing adopted; reload the list"}, true
	// Stage 3b: the token, capacity and collection sentinels. The
	// operations wrap the capacity-settings and CIDR refusals with their own
	// words first; these rows are the fallback for a path that does not.
	case errors.Is(err, db.ErrAPITokenNotFound):
		return Refusal{Kind: NotFound, Detail: "no API token with that id"}, true
	case errors.Is(err, db.ErrInvalidCIDR), errors.Is(err, db.ErrInvalidCapacitySettings):
		return Refusal{Kind: InvalidInput, Detail: err.Error()}, true
	case errors.Is(err, db.ErrCollectionNotFound):
		return Refusal{Kind: NotFound, Detail: "no collection with that id"}, true
	case errors.Is(err, db.ErrCollectionNameTaken):
		return Refusal{Kind: Conflict, Detail: db.ErrCollectionNameTaken.Error()}, true
	case errors.Is(err, db.ErrGroupNotAdminOwned):
		return Refusal{Kind: InvalidInput, Detail: db.ErrGroupNotAdminOwned.Error()}, true
	}
	return Refusal{}, false
}
