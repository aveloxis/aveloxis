// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.14 — the implicit per-user "Comparisons" group: out-of-scope
// compare selections auto-add here (same shape as v0.27.4's Starred
// group; both ride one shared find-or-create helper so the v0.19.0
// status rules can never drift between them).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// TestComparisonsGroupSharesStarredHelper pins the shared-helper
// contract: both named-group wrappers delegate to ONE implementation.
func TestComparisonsGroupSharesStarredHelper(t *testing.T) {
	b, err := os.ReadFile("home_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	if !strings.Contains(src, `ComparisonsGroupName = "Comparisons"`) {
		t.Error("home_store.go must declare ComparisonsGroupName = \"Comparisons\"")
	}
	for _, needle := range []string{
		"func (s *PostgresStore) FindOrCreateComparisonsGroup(",
		"func (s *PostgresStore) FindOrCreateStarredGroup(",
		"findOrCreateNamedGroup(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("home_store.go must contain %q — Starred and Comparisons share one find-or-create helper", needle)
		}
	}
	// Both wrappers must actually delegate (2 wrapper call sites + 1
	// definition = at least 3 occurrences).
	if strings.Count(src, "findOrCreateNamedGroup(") < 3 {
		t.Error("both FindOrCreateStarredGroup and FindOrCreateComparisonsGroup must delegate to findOrCreateNamedGroup")
	}
}

func TestFindOrCreateComparisonsGroup(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email)
		VALUES ($1, 'github', '') RETURNING user_id`,
		fmt.Sprintf("_avcmpgrp_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	// First call creates; second call reuses — never a duplicate group.
	g1, err := store.FindOrCreateComparisonsGroup(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := store.FindOrCreateComparisonsGroup(ctx, userID)
	if err != nil || g1 != g2 {
		t.Fatalf("Comparisons group must be reused: %d vs %d (err=%v)", g1, g2, err)
	}

	// v0.19.0 status rules apply verbatim: a non-admin's implicit group
	// starts pending (approval gates future COLLECTION enqueues, never
	// visibility — see GetUserRepoScope).
	var name, status string
	if err := store.pool.QueryRow(ctx, `
		SELECT name, COALESCE(status, 'approved') FROM aveloxis_ops.user_groups WHERE group_id = $1`,
		g1).Scan(&name, &status); err != nil {
		t.Fatal(err)
	}
	if name != ComparisonsGroupName {
		t.Errorf("group name = %q, want %q", name, ComparisonsGroupName)
	}
	if status != "pending" {
		t.Errorf("non-admin Comparisons group must follow the normal v0.19.0 status rules (pending), got %q", status)
	}

	// It must not collide with the user's Starred group.
	sg, err := store.FindOrCreateStarredGroup(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if sg == g1 {
		t.Error("Starred and Comparisons must be distinct groups")
	}
}
