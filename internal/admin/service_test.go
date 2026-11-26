// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

// TestIsAdminDecidesFromTheCredential (eval-2, the SR-18 crux): the
// Operator is always an administrator, an API token never — even when its
// owner's users row says admin — and a Session is what the row says, read
// now. A lookup error is returned, never "not an administrator" (SR-5).
func TestIsAdminDecidesFromTheCredential(t *testing.T) {
	for _, tc := range []struct {
		name    string
		a       Actor
		row     bool
		rowErr  error
		want    bool
		wantErr bool
		reads   bool
	}{
		{name: "operator", a: Actor{Kind: Operator}, want: true},
		{name: "operator for a user whose row says no", a: Actor{UserID: 3, Kind: Operator}, row: false, want: true},
		{name: "api token of an administrator", a: Actor{UserID: 1, Kind: APIToken}, row: true, want: false},
		{name: "session of an administrator", a: Actor{UserID: 1, Kind: Session}, row: true, want: true, reads: true},
		{name: "session of a regular user", a: Actor{UserID: 2, Kind: Session}, row: false, want: false, reads: true},
		{name: "session whose lookup fails", a: Actor{UserID: 2, Kind: Session}, rowErr: errors.New("closed pool"), wantErr: true, reads: true},
		{name: "unknown credential kind", a: Actor{UserID: 2}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			st := &fakeStore{isUserAdmin: func(_ context.Context, id int) (bool, error) {
				reads++
				if id != tc.a.UserID {
					t.Errorf("IsUserAdmin asked for user %d, want the actor's %d", id, tc.a.UserID)
				}
				return tc.row, tc.rowErr
			}}
			svc, _, _ := testService(t, st, Options{})
			got, err := svc.IsAdmin(context.Background(), tc.a)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("IsAdmin(%+v) = %v, %v; want %v, err=%v", tc.a, got, err, tc.want, tc.wantErr)
			}
			if (reads > 0) != tc.reads {
				t.Errorf("IsAdmin(%+v) read the users row %d times; want reads=%v", tc.a, reads, tc.reads)
			}
		})
	}
}

// TestAdministrativeOperationsRefuseAnAdministratorsAPIToken is the
// runtime pin for the token hazard: every administrative operation, asked
// by an API token whose OWNER is an administrator (the store says so),
// returns ErrNotAdmin and touches no store write. The context is the
// request's own; nothing here depends on db.WithoutAdminPrivilege.
func TestAdministrativeOperationsRefuseAnAdministratorsAPIToken(t *testing.T) {
	token := Actor{UserID: 1, Kind: APIToken}
	for name, op := range adminOperations() {
		t.Run(name, func(t *testing.T) {
			st := &fakeStore{isUserAdmin: adminIs(true)}
			svc, hooks, logs := testService(t, st, Options{})
			err := op(svc, context.Background(), token)
			if !errors.Is(err, ErrNotAdmin) {
				t.Fatalf("%s by an administrator's API token = %v; want ErrNotAdmin", name, err)
			}
			if ref, ok := Classify(err); !ok || ref.Kind != Forbidden {
				t.Errorf("Classify = %+v, %v; want Forbidden", ref, ok)
			}
			if got := hooks.got(); len(got) != 0 {
				t.Errorf("hooks ran: %v", got)
			}
			if !strings.Contains(logs.String(), "not an administrator") {
				t.Errorf("the refusal must be logged:\n%s", logs.String())
			}
		})
	}
}

// TestAdministrativeOperationsNeedAnAdministratorSession: the same
// operations by a regular Session are refused the same way, and by an
// administrator's Session or the Operator they reach the store.
func TestAdministrativeOperationsNeedAnAdministratorSession(t *testing.T) {
	for name, op := range adminOperations() {
		t.Run(name+"/regular session", func(t *testing.T) {
			st := &fakeStore{isUserAdmin: adminIs(false)}
			svc, _, _ := testService(t, st, Options{})
			if err := op(svc, context.Background(), Actor{UserID: 2, Kind: Session}); !errors.Is(err, ErrNotAdmin) {
				t.Fatalf("%s by a regular session = %v; want ErrNotAdmin", name, err)
			}
		})
		t.Run(name+"/lookup error", func(t *testing.T) {
			st := &fakeStore{isUserAdmin: func(context.Context, int) (bool, error) { return false, errors.New("closed pool") }}
			svc, _, _ := testService(t, st, Options{})
			err := op(svc, context.Background(), Actor{UserID: 2, Kind: Session})
			if err == nil || errors.Is(err, ErrNotAdmin) {
				t.Fatalf("%s on a failed admin lookup = %v; want the lookup's error, not a refusal (SR-5)", name, err)
			}
			if _, refused := Classify(err); refused {
				t.Errorf("a lookup failure classified as a refusal: %v", err)
			}
		})
	}
}

// adminOperations enumerates every operation gated on requireAdmin, each
// as a call that would reach the store if admitted. Keep it equal to the
// requireAdmin call sites (TestEveryRequireAdminSiteIsEnumerated).
func adminOperations() map[string]func(*Service, context.Context, Actor) error {
	return map[string]func(*Service, context.Context, Actor) error{
		"ListSessions": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.ListSessions(ctx, a, 77) // another account's
			return err
		},
		"RevokeSessions": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.RevokeSessions(ctx, a, 77)
			return err
		},
		"DecideAddRequest": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.DecideAddRequest(ctx, a, 1, true)
			return err
		},
		"RejectGroup": func(s *Service, ctx context.Context, a Actor) error { return s.RejectGroup(ctx, a, 1) },
		"SetUserAdmin": func(s *Service, ctx context.Context, a Actor) error {
			return s.SetUserAdmin(ctx, a, 2, true)
		},
		"AdoptForgeID": func(s *Service, ctx context.Context, a Actor) error {
			return s.AdoptForgeID(ctx, a, 1, "1", "2", "")
		},
		// Stage 3b (0.29.109): tokens, capacity, collections.
		"GrantAPIToken": func(s *Service, ctx context.Context, a Actor) error {
			_, _, err := s.GrantAPIToken(ctx, a, TokenGrant{UserID: 2, Label: "x"})
			return err
		},
		"RevokeAPIToken": func(s *Service, ctx context.Context, a Actor) error { return s.RevokeAPIToken(ctx, a, 1) },
		"SetAPITokenDefaults": func(s *Service, ctx context.Context, a Actor) error {
			hour, days := int64(10), int64(30)
			_, err := s.SetAPITokenDefaults(ctx, a, TokenDefaults{RateLimitPerHour: &hour, LifetimeDays: &days})
			return err
		},
		"SetCapacityQuota": func(s *Service, ctx context.Context, a Actor) error {
			allowed := int64(5)
			_, err := s.SetCapacityQuota(ctx, a, capacity.QuotaNames()[0], QuotaChange{Allowed: &allowed, Mode: "enforce"})
			return err
		},
		"SetCapacityContact": func(s *Service, ctx context.Context, a Actor) error { return s.SetCapacityContact(ctx, a, "") },
		"SetCapacityOverride": func(s *Service, ctx context.Context, a Actor) error {
			return s.SetCapacityOverride(ctx, a, db.CapacityOverride{UserID: 2})
		},
		"AddSignupAllowlist": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.AddSignupAllowlist(ctx, a, "192.0.2.0/24", "")
			return err
		},
		"RemoveSignupAllowlist": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.RemoveSignupAllowlist(ctx, a, "192.0.2.0/24")
			return err
		},
		"CreateCollection": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.CreateCollection(ctx, a, CollectionFields{Name: "x"})
			return err
		},
		"UpdateCollection": func(s *Service, ctx context.Context, a Actor) error {
			return s.UpdateCollection(ctx, a, 1, CollectionFields{Name: "x"})
		},
		"DeleteCollection":          func(s *Service, ctx context.Context, a Actor) error { return s.DeleteCollection(ctx, a, 1) },
		"AddGroupToCollection":      func(s *Service, ctx context.Context, a Actor) error { return s.AddGroupToCollection(ctx, a, 1, 2) },
		"RemoveGroupFromCollection": func(s *Service, ctx context.Context, a Actor) error { return s.RemoveGroupFromCollection(ctx, a, 1, 2) },
		// 0.29.187: the package repository hosts.
		"ListRepositoryHosts": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.ListRepositoryHosts(ctx, a, nil)
			return err
		},
		"PreviewRepositoryHostDistrust": func(s *Service, ctx context.Context, a Actor) error {
			_, _, err := s.PreviewRepositoryHostDistrust(ctx, a, "repo.example")
			return err
		},
		"DistrustRepositoryHost": func(s *Service, ctx context.Context, a Actor) error {
			_, _, err := s.DistrustRepositoryHost(ctx, a, "repo.example", "")
			return err
		},
		"RestoreRepositoryHost": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.RestoreRepositoryHost(ctx, a, "repo.example")
			return err
		},
		"DismissRepositoryHostSignals": func(s *Service, ctx context.Context, a Actor) error {
			_, err := s.DismissRepositoryHostSignals(ctx, a, "repo.example")
			return err
		},
	}
}

// TestEveryRequireAdminSiteIsEnumerated guards adminOperations' denominator:
// the number of requireAdmin call sites in the package equals the number
// of operations the two tests above drive.
func TestEveryRequireAdminSiteIsEnumerated(t *testing.T) {
	// Every call of the gate, whatever its arguments are named (review
	// round 1 F8: a literal spelling let a differently-named call escape).
	src := packageSourceStripped(t)
	sites := strings.Count(src, "s.requireAdmin(")
	if sites != len(adminOperations()) {
		t.Errorf("%d requireAdmin sites in internal/admin, %d operations enumerated in adminOperations — add the new operation to the table", sites, len(adminOperations()))
	}
}

// TestNewNormalisesTheGitHubBase: an empty base is public GitHub, so the
// host gate never compares against "".
func TestNewNormalisesTheGitHubBase(t *testing.T) {
	svc, _, _ := testService(t, &fakeStore{}, Options{})
	if got := svc.Options().GitHubAPIBase; got == "" || !strings.HasPrefix(got, "https://") {
		t.Errorf("GitHubAPIBase after New = %q, want the public base", got)
	}
}

// TestNilHooksAreANoOp: the CLI passes nil.
func TestNilHooksAreANoOp(t *testing.T) {
	st := &fakeStore{t: t, removeRepoFromGroup: func(context.Context, int, int64, int64) (bool, error) { return true, nil }}
	svc := New(st, Options{}, nil, discardLogger())
	if removed, err := svc.RemoveFromGroup(context.Background(), Actor{UserID: 1, Kind: Operator}, 1, 1); err != nil || !removed {
		t.Fatalf("RemoveFromGroup with nil hooks = %v, %v", removed, err)
	}
}

// TestNewRequiresALogger: every failure, audit line and store refusal is logged, so a
// Service without a logger is a construction error, not a silent one.
func TestNewRequiresALogger(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New(nil logger) did not panic")
		}
	}()
	New(&fakeStore{}, Options{}, nil, nil)
}

// compile-time: the service refuses with the store's sentinels where they
// apply (keeps the import honest for the tests above).
var _ = db.ErrGroupNotOwned
