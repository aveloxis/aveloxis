// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncLog) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncLog) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// TestDetachedAddRequestPassFailureIsLoggedHere (AVELOXIS_TEST_DB) — NET-6
// review r8 F1: an auto-approved add request is committed as approved and
// processed on context.WithoutCancel, so its pass's failure can never be
// the caller's request ending — but the only line naming it was the
// handler's LogFailure(r.Context(), …), Debug once nginx or the bound had
// ended the request, leaving an approved request with unprocessed items
// that no admin sees. The owning layer (SR-18) logs the pass's failure at
// WARN itself.
func TestDetachedAddRequestPassFailureIsLoggedHere(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logs := &syncLog{}
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const login = "_avdetachedpass_probe"
	const trigger = "_avtest_detached_pass_pending"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_ops.collection_add_requests`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_ops.`+trigger+`()`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "detached pass probe")
	if err != nil {
		t.Fatal(err)
	}
	// The pass refuses a request that is not approved: a trigger turns this
	// user's approved insert into a pending one, so the detached pass fails
	// at its own first check (not per item).
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_ops.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.user_id = `+strconv.Itoa(uid)+` THEN NEW.status := 'pending'; END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_ops.collection_add_requests FOR EACH ROW EXECUTE FUNCTION aveloxis_ops.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}
	out, err := store.AddReposToGroup(ctx, uid, gid, []string{"https://github.com/_avdetachedpass-owner/_avdetachedpass-repo"}, 5)
	if err == nil {
		t.Fatal("the probe's pass must fail (the trigger made the request pending)")
	}
	l := logs.String()
	if !strings.Contains(l, "level=WARN") || !strings.Contains(l, "auto-approved add request") || !strings.Contains(l, "request_id="+strconv.FormatInt(out.RequestID, 10)) {
		t.Errorf("the detached pass's failure must be a WARN from the store naming the request:\n%s", l)
	}
}
