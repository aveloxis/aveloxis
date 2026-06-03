// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestEmailMessageStoreMethodsExist — source-contract: the three Phase-1
// store methods must exist by name so the worker (Phase 2) can rely on them.
func TestEmailMessageStoreMethodsExist(t *testing.T) {
	data, err := os.ReadFile("email_message_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, sig := range []string{
		"func (s *PostgresStore) UpsertEmailMessage(",
		"func (s *PostgresStore) InsertEmailMessageRef(",
		"func (s *PostgresStore) ResolveSignaledRepoForURL(",
	} {
		if !contains(src, sig) {
			t.Errorf("email_message_store.go must declare %s", sig)
		}
	}
	// The dedup key is the Message-ID; preserve-on-conflict for a resolved FK.
	if !contains(src, "ON CONFLICT (message_id_header)") {
		t.Error("UpsertEmailMessage must upsert on message_id_header (idempotent re-collection)")
	}
	if !contains(src, "COALESCE(aveloxis_data.email_message.signaled_repo_id") {
		t.Error("UpsertEmailMessage must preserve an already-resolved signaled_repo_id across re-collection")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0)
}
func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

func emConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return store, ctx
}

func TestUpsertEmailMessageIsIdempotent(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	const mid = "emtest-idem-<aaa@dev.kafka.apache.org>"
	em := &model.EmailMessage{
		PlatformID:      model.Platform(6),
		MLSystem:        "apache_ponymail",
		MessageIDHeader: mid,
		ListAddress:     "dev@kafka.apache.org",
		Subject:         "[DISCUSS] KIP-1",
		SenderEmail:     "a@example.org",
		MsgClass:        "discuss",
	}
	id1, err := store.UpsertEmailMessage(ctx, em)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	id2, err := store.UpsertEmailMessage(ctx, em)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if id1 != id2 {
		t.Errorf("re-collecting the same Message-ID must return the same row: %d vs %d", id1, id2)
	}
	var n int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.email_message WHERE message_id_header = $1`, mid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 row for the Message-ID, got %d", n)
	}
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE message_id_header = $1`, mid)
}

func TestResolveSignaledRepoForURL(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	const url = "https://github.com/emtest-owner/emtest-resolve-repo"
	const mid = "emtest-resolve-<bbb@github.arrow.apache.org>"

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: url, Owner: "emtest-owner", Name: "emtest-resolve-repo",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}

	em := &model.EmailMessage{
		PlatformID:      model.Platform(6),
		MLSystem:        "apache_ponymail",
		MessageIDHeader: mid,
		ListAddress:     "dev@arrow.apache.org",
		MsgClass:        "github_mirror",
		SignaledRepoURL: url, // not yet resolved
	}
	emID, err := store.UpsertEmailMessage(ctx, em)
	if err != nil {
		t.Fatalf("upsert email_message: %v", err)
	}

	n, err := store.ResolveSignaledRepoForURL(ctx, repoID, url)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected at least 1 row resolved, got %d", n)
	}
	var got *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT signaled_repo_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != repoID {
		t.Errorf("signaled_repo_id should be backfilled to %d, got %v", repoID, got)
	}

	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE message_id_header = $1`, mid)
}

func TestInsertEmailMessageRefIdempotent(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/emtest-owner/emtest-ref-repo",
		Owner: "emtest-owner", Name: "emtest-ref-repo",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	msgID, err := store.UpsertMessage(ctx, &model.Message{
		RepoID: repoID, PlatformMsgID: 0, PlatformID: model.Platform(6),
		NodeID: "emtest-ref-<ccc@dev.arrow.apache.org>", Text: "body",
	})
	if err != nil {
		t.Fatalf("seed message: %v", err)
	}
	emID, err := store.UpsertEmailMessage(ctx, &model.EmailMessage{
		PlatformID: model.Platform(6), MLSystem: "apache_ponymail",
		MessageIDHeader: "emtest-ref-<ccc@dev.arrow.apache.org>", ListAddress: "dev@arrow.apache.org",
		MsgClass: "discuss",
	})
	if err != nil {
		t.Fatalf("upsert email_message: %v", err)
	}

	if err := store.InsertEmailMessageRef(ctx, emID, msgID, nil); err != nil {
		t.Fatalf("first ref: %v", err)
	}
	if err := store.InsertEmailMessageRef(ctx, emID, msgID, nil); err != nil {
		t.Fatalf("second ref (must be idempotent): %v", err)
	}
	var n int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM aveloxis_data.email_message_ref WHERE email_message_id = $1 AND msg_id = $2`,
		emID, msgID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 bridge row, got %d", n)
	}
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE email_message_id = $1`, emID)
}
