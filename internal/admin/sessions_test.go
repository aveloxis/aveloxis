// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// 0.29.113 (ASVS V7.5.2, V7.4.5): an account lists and ends its own
// sessions; an administrator ends any account's; an API token does neither.
func TestSessionsAreTheAccountsOrAnAdministrators(t *testing.T) {
	ctx := context.Background()
	var ended []int
	st := &fakeStore{
		isUserAdmin:        func(_ context.Context, id int) (bool, error) { return id == 1, nil },
		listUserSessions:   func(_ context.Context, id int) ([]db.UserSession, error) { return []db.UserSession{{Kind: "web"}}, nil },
		deleteUserSessions: func(_ context.Context, id int) (int64, error) { ended = append(ended, id); return 2, nil },
	}
	svc, _, logs := testService(t, st, Options{})
	me := Actor{UserID: 2, Kind: Session}
	if list, err := svc.ListSessions(ctx, me, 2); err != nil || len(list) != 1 {
		t.Errorf("own list = %v, %v", list, err)
	}
	if n, err := svc.RevokeSessions(ctx, me, 2); err != nil || n != 2 {
		t.Errorf("own revoke = %d, %v", n, err)
	}
	if _, err := svc.ListSessions(ctx, me, 3); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("another account's list by a non-admin = %v; want ErrNotAdmin", err)
	}
	if _, err := svc.RevokeSessions(ctx, me, 3); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("another account's revoke by a non-admin = %v; want ErrNotAdmin", err)
	}
	if n, err := svc.RevokeSessions(ctx, adminActor(), 3); err != nil || n != 2 {
		t.Errorf("an administrator's revoke = %d, %v", n, err)
	}
	if !strings.Contains(logs.String(), "admin: end sessions") || !strings.Contains(logs.String(), "target_user_id=3") || !strings.Contains(logs.String(), "sessions=2") {
		t.Errorf("the audit line names the target and the count:\n%s", logs.String())
	}
	// An API token is not a session and never its owner's keeper: refused
	// like every administrative call, even for its own account.
	tok := Actor{UserID: 2, Kind: APIToken}
	if _, err := svc.RevokeSessions(ctx, tok, 2); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a token's revoke of its owner's sessions = %v; want ErrNotAdmin", err)
	}
	if _, err := svc.ListSessions(ctx, tok, 2); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a token's list = %v; want ErrNotAdmin", err)
	}
	if _, err := svc.RevokeSessions(ctx, adminActor(), 0); !isInput(err, "user id must be a positive integer") {
		t.Errorf("revoke of user 0 = %v", err)
	}
	if len(ended) != 2 || ended[0] != 2 || ended[1] != 3 {
		t.Errorf("the store ended %v; want [2 3]", ended)
	}
}

// Copilot's seventh review of PR #246 (0.29.129): ending the account's OWN
// sessions is the sensitive change the re-authentication rule guards (ASVS
// V7.5.2), and the rule is the Service's — so an older administrator
// session cannot reach around it through the administrative route by naming
// its own id. An actor whose sign-in the adapter did not record is never
// recent (SR-18: a caller that forgets the field cannot pass). Another
// account's sessions are the administrator's call at any sign-in age (a
// lost device is ended by whoever is signed in). With no idle bound
// configured, the rule passes.
func TestEndingOwnSessionsNeedsARecentSignIn(t *testing.T) {
	ctx := context.Background()
	var ended []int
	st := &fakeStore{
		isUserAdmin:        func(_ context.Context, id int) (bool, error) { return id == 1, nil },
		deleteUserSessions: func(_ context.Context, id int) (int64, error) { ended = append(ended, id); return 1, nil },
	}
	svc, _, logs := testService(t, st, Options{SessionIdle: 10 * time.Minute})
	old := Actor{UserID: 1, Kind: Session, SignedInAt: time.Now().Add(-time.Hour)}
	_, err := svc.RevokeSessions(ctx, old, 1)
	if !errors.Is(err, ErrRecentSignInRequired) {
		t.Fatalf("an hour-old sign-in ending its own sessions = %v; want ErrRecentSignInRequired", err)
	}
	if out := logs.String(); !strings.Contains(out, "admin: end sessions refused") || !strings.Contains(out, "target_user_id=1") {
		t.Errorf("the refusal is the record of a hijacked older session trying; not logged:\n%s", out)
	}
	if ref, ok := Classify(err); !ok || ref.Kind != ReauthenticationRequired || !strings.Contains(ref.Detail, "10 minutes") {
		t.Errorf("Classify = %+v, %v; want ReauthenticationRequired naming the bound", ref, ok)
	}
	if _, err := svc.RevokeSessions(ctx, Actor{UserID: 1, Kind: Session}, 1); !errors.Is(err, ErrRecentSignInRequired) {
		t.Errorf("a sign-in the adapter did not record = %v; want ErrRecentSignInRequired (never recent)", err)
	}
	if _, err := svc.RevokeSessions(ctx, Actor{UserID: 2, Kind: Session, SignedInAt: time.Now().Add(-time.Hour)}, 2); !errors.Is(err, ErrRecentSignInRequired) {
		t.Errorf("a non-administrator's old sign-in ending its own = %v; want ErrRecentSignInRequired", err)
	}
	if n, err := svc.RevokeSessions(ctx, Actor{UserID: 1, Kind: Session, SignedInAt: time.Now()}, 1); err != nil || n != 1 {
		t.Errorf("a fresh sign-in ending its own = %d, %v; want 1, nil", n, err)
	}
	if n, err := svc.RevokeSessions(ctx, old, 2); err != nil || n != 1 {
		t.Errorf("an old administrator sign-in ending ANOTHER account's = %d, %v; want 1, nil (the lost device is ended by whoever is signed in)", n, err)
	}
	if n, err := svc.RevokeSessions(ctx, Actor{UserID: 1, Kind: Operator}, 1); err != nil || n != 1 {
		t.Errorf("the operator's shell = %d, %v; want 1, nil", n, err)
	}
	unbounded, _, _ := testService(t, st, Options{})
	if n, err := unbounded.RevokeSessions(ctx, old, 1); err != nil || n != 1 {
		t.Errorf("with no idle bound the old sign-in ends its own = %d, %v; want 1, nil", n, err)
	}
	if len(ended) != 4 {
		t.Errorf("the store ended %v; want four revocations", ended)
	}
}
