// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestClassifyNamesEveryRefusalTheServiceReturns is the classifier's table
// AND its denominator: every sentinel the Service can hand an adapter is
// listed with its kind, wrapped once (the Service wraps refusals in
// context) and bare. An error outside the table is not a refusal.
func TestClassifyNamesEveryRefusalTheServiceReturns(t *testing.T) {
	ex := &capacity.Exceeded{Quota: "repos"}
	cases := []struct {
		err    error
		kind   Kind
		detail string // "" = no check
	}{
		{ErrNotAdmin, Forbidden, ""},
		{ErrSelfDemotion, SelfDemotion, ErrSelfDemotion.Error()},
		{refuseInput("a group needs a name"), InvalidInput, "a group needs a name"},
		{refuseTooMany("at most 1000"), TooMany, "at most 1000"},
		{ErrRepoNotInGroup, NotFound, ErrRepoNotInGroup.Error()},
		{&OrgUnregistrableError{Cause: db.ErrOrgOffGitHubHost, Advice: "reject it"}, OrgUnregistrable, "reject it"},
		{&offHostError{cause: db.ErrOrgOffGitHubHost, host: "github.example"}, OffHost, "(github.example)"},
		{&offHostError{cause: db.ErrOrgOffGitLabInstance, host: "https://code.example"}, OffHost, "GitLab instance (https://code.example)"},
		{db.ErrOrgOffGitLabInstance, OffHost, db.ErrOrgOffGitLabInstance.Error()},
		{&supersededError{oldID: "42"}, Superseded, "no longer stores 42"},
		{db.ErrAddRequestNotFound, RequestNotFound, ""},
		{db.ErrGroupNotFound, NotFound, ""},
		{db.ErrGroupNotOwned, NotOwned, ""},
		{db.ErrNotGroupOwner, NotOwned, ""},
		{db.ErrGroupRejected, GroupRejected, db.ErrGroupRejected.Error()},
		{db.ErrOrgOffGitHubHost, OffHost, db.ErrOrgOffGitHubHost.Error()},
		{db.ErrURLTooLong, InvalidInput, urlInputDetail},
		{db.ErrInvalidOrgURL, InvalidInput, urlInputDetail},
		{platform.ErrURLUserinfo, InvalidInput, urlInputDetail},
		{db.ErrAddItemsFailed, Partial, db.ErrAddItemsFailed.Error()},
		{db.ErrLastAdmin, LastAdmin, db.ErrLastAdmin.Error()},
		{db.ErrUserNotFound, UserNotFound, db.ErrUserNotFound.Error()},
		{db.ErrNoPendingForgeIDChange, NotFound, "no pending forge-ID change"},
		{db.ErrForgeIDNotAsExpected, Superseded, "superseded"},
		{refuseNotFound(`no quota named "x"`), NotFound, `no quota named "x"`},
		{refuseConflict("x is set in aveloxis.json"), Conflict, "x is set in aveloxis.json"},
		{refuseReauthentication("ending your own sessions", 10*time.Minute), ReauthenticationRequired, "ending your own sessions needs a sign-in within the last 10 minutes; sign in again and retry"},
		{ErrRecentSignInRequired, ReauthenticationRequired, ErrRecentSignInRequired.Error()},
		{db.ErrAPITokenNotFound, NotFound, "no API token with that id"},
		{db.ErrInvalidCIDR, InvalidInput, db.ErrInvalidCIDR.Error()},
		{db.ErrInvalidCapacitySettings, InvalidInput, db.ErrInvalidCapacitySettings.Error()},
		{db.ErrCollectionNotFound, NotFound, "no collection with that id"},
		{db.ErrCollectionNameTaken, Conflict, db.ErrCollectionNameTaken.Error()},
		{db.ErrGroupNotAdminOwned, InvalidInput, db.ErrGroupNotAdminOwned.Error()},
		{db.ErrSchemaNotMigrated, NotMigrated, db.ErrSchemaNotMigrated.Error()},
		{ex, Capacity, ""},
	}
	for _, c := range cases {
		for _, err := range []error{c.err, fmt.Errorf("op by user 1: %w", c.err)} {
			ref, ok := Classify(err)
			if !ok || ref.Kind != c.kind {
				t.Errorf("Classify(%v) = %+v, %v; want kind %s", err, ref, ok, c.kind)
				continue
			}
			if c.detail != "" && !strings.Contains(ref.Detail, c.detail) {
				t.Errorf("Classify(%v).Detail = %q, want it to contain %q", err, ref.Detail, c.detail)
			}
			if c.kind == Capacity && ref.Exceeded != ex {
				t.Errorf("Classify(capacity).Exceeded = %p, want the refusal itself", ref.Exceeded)
			}
		}
	}
	for _, err := range []error{nil, errors.New("closed pool"), fmt.Errorf("read: %w", errors.New("timeout"))} {
		if ref, ok := Classify(err); ok {
			t.Errorf("Classify(%v) = %+v, true; a failure is not a refusal (SR-5)", err, ref)
		}
	}
	// The denominator: every db.Err* sentinel named in this package's
	// non-test source is in the table above, so a new refusal the Service
	// starts returning cannot slip past the classifier unnamed.
	src := packageSourceStripped(t)
	named := map[string]bool{}
	for _, tok := range strings.FieldsFunc(src, func(r rune) bool {
		return !(r == '.' || r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if strings.HasPrefix(tok, "db.Err") {
			named[strings.TrimSuffix(tok, ".Error")] = true
		}
	}
	srctest.MinCount(t, "db.Err* sentinels named in internal/admin", len(named), 8)
	classified := map[string]bool{}
	for _, sentinel := range []struct {
		name string
		err  error
	}{
		{"db.ErrAddRequestNotFound", db.ErrAddRequestNotFound}, {"db.ErrGroupNotFound", db.ErrGroupNotFound},
		{"db.ErrGroupNotOwned", db.ErrGroupNotOwned}, {"db.ErrNotGroupOwner", db.ErrNotGroupOwner},
		{"db.ErrGroupRejected", db.ErrGroupRejected}, {"db.ErrOrgOffGitHubHost", db.ErrOrgOffGitHubHost},
		{"db.ErrOrgOffGitLabInstance", db.ErrOrgOffGitLabInstance},
		{"db.ErrURLTooLong", db.ErrURLTooLong}, {"db.ErrInvalidOrgURL", db.ErrInvalidOrgURL}, {"db.ErrAddItemsFailed", db.ErrAddItemsFailed},
		{"db.ErrLastAdmin", db.ErrLastAdmin}, {"db.ErrUserNotFound", db.ErrUserNotFound},
		{"db.ErrNoPendingForgeIDChange", db.ErrNoPendingForgeIDChange}, {"db.ErrForgeIDNotAsExpected", db.ErrForgeIDNotAsExpected},
		{"db.ErrAPITokenNotFound", db.ErrAPITokenNotFound}, {"db.ErrInvalidCIDR", db.ErrInvalidCIDR},
		{"db.ErrInvalidCapacitySettings", db.ErrInvalidCapacitySettings}, {"db.ErrCollectionNotFound", db.ErrCollectionNotFound},
		{"db.ErrCollectionNameTaken", db.ErrCollectionNameTaken}, {"db.ErrGroupNotAdminOwned", db.ErrGroupNotAdminOwned},
		{"db.ErrSchemaNotMigrated", db.ErrSchemaNotMigrated},
		// Named in the package but never returned to an adapter: the
		// background pass logs the first; ConfirmAccountEmail turns the
		// second into a ConfirmOutcome.
		{"db.ErrAddRequestInProgress", nil},
		{"db.ErrConfirmationTokenInvalid", nil},
	} {
		classified[sentinel.name] = true
	}
	for n := range named {
		if !classified[n] {
			t.Errorf("%s is named in internal/admin but not in the classifier's table — add it with its Kind, or record here that no adapter sees it", n)
		}
	}
}

func TestKindString(t *testing.T) {
	for k := Forbidden; k <= ReauthenticationRequired; k++ {
		if s := k.String(); s == "" || strings.HasPrefix(s, "kind ") {
			t.Errorf("Kind(%d).String() = %q", int(k), s)
		}
	}
	if s := Kind(99).String(); s != "kind 99" {
		t.Errorf("Kind(99).String() = %q", s)
	}
}

// A value the database refused on a non-URL operation (a NUL byte in a
// collection name is 22021) is InvalidInput in the class's own words, not
// the URL sentence the group operations use (whole-branch review of
// 0.29.109); the URL sentinels keep theirs.
func TestClassifyRejectedValueIsNotTheURLSentence(t *testing.T) {
	err := fmt.Errorf("CreateCollection: %w", &pgconn.PgError{Code: "22021", Message: "invalid byte sequence for encoding \"UTF8\": 0x00"})
	ref, ok := Classify(err)
	if !ok || ref.Kind != InvalidInput || ref.Detail != rejectedValueDetail {
		t.Errorf("Classify(22021) = %+v, %v; want InvalidInput %q", ref, ok, rejectedValueDetail)
	}
	if ref, ok := Classify(db.ErrURLTooLong); !ok || ref.Detail != urlInputDetail {
		t.Errorf("the URL sentinel must keep the URL sentence: %+v", ref)
	}
}
