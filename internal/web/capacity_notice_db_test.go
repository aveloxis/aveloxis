// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// Round 4 R4-4: after a paste the daily-additions quota refused, the group
// page shows that quota's message with the account's real numbers (today's
// additions, the daily value) — not the allocation's "remove repositories".
func TestGroupPageNamesTheDailyAdditionsRefusal(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const login = "_avweb_links_notice"
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	key := capacity.CountKey(capacity.Subject{Kind: capacity.KindAccount, ID: strconv.Itoa(uid)}, capacity.QuotaRepoLinksPerDay)
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.request_counts WHERE subject = $1`, key)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, uid)
	})
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.request_counts (subject, day, requests) VALUES ($1, $2::date, 37)
		ON CONFLICT (subject, day) DO UPDATE SET requests = 37`, key, time.Now().UTC().Format(time.DateOnly)); err != nil {
		t.Fatal(err)
	}
	s := New(store, config.WebConfig{}, nil, "", logger)
	got := s.capacityNotice(httptest.NewRequest("GET", "/groups/1?add_error=capacity_links", nil), uid)
	if !strings.Contains(got, "has added 37 repositories") || strings.Contains(got, "remove repositories") {
		t.Fatalf("daily-additions notice = %q; want today's 37 additions, not the allocation's advice", got)
	}
	if allocation := s.capacityNotice(httptest.NewRequest("GET", "/groups/1?add_error=capacity", nil), uid); !strings.Contains(allocation, "Your groups can hold up to") {
		t.Errorf("allocation notice = %q", allocation)
	}
}
