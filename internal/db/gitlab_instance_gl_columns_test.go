// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): the gl_* denormalized columns mirror a
// GitLab identity of ANY instance, so a self-hosted user's username, URL and
// state reach the contributor row the display and bot filters read (they
// fall back to gl_username). gl_id is written only for the historical
// instance (platform_id 2): a numeric user id is meaningful only together
// with its instance, and the column has no instance beside it.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestGitLabInstanceIdentityFillsGlColumnsButGlIDOnlyForInstance2(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const instanceID = 198 // inside [GitLabInstanceIDMin, GitLabInstanceIDMax]
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '_avglc_%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '_avglc_%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avglc_%'`,
			`DELETE FROM aveloxis_data.platforms WHERE platform_id = 198 AND platform_name = '_avglc GitLab instance'`,
		} {
			cleanupExecRetry(ctx, store, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
		VALUES (198, '_avglc GitLab instance') ON CONFLICT DO NOTHING`)

	cases := []struct {
		login    string
		platform model.Platform
		userID   int64
		wantGlID bool
	}{
		{"_avglc_legacy", model.PlatformGitLab, 940001, true},
		{"_avglc_selfhosted", instanceID, 940002, false},
	}
	for _, tc := range cases {
		err := store.UpsertContributorBatch(ctx, []model.Contributor{{
			Login: tc.login,
			Identities: []model.ContributorIdentity{{
				Platform: tc.platform, UserID: tc.userID, Login: tc.login,
				URL: "https://example.invalid/" + tc.login, State: "active",
			}},
		}})
		if err != nil {
			t.Fatalf("%s: UpsertContributorBatch: %v", tc.login, err)
		}
		var glID *int64
		var glUsername, glState string
		if err := store.pool.QueryRow(ctx, `
			SELECT gl_id, COALESCE(gl_username, ''), COALESCE(gl_state, '')
			FROM aveloxis_data.contributors WHERE cntrb_login = $1`, tc.login,
		).Scan(&glID, &glUsername, &glState); err != nil {
			t.Fatalf("%s: %v", tc.login, err)
		}
		if glUsername != tc.login || glState != "active" {
			t.Errorf("platform %d: gl_username=%q gl_state=%q, want the identity's username and state for every GitLab instance",
				tc.platform, glUsername, glState)
		}
		if tc.wantGlID && (glID == nil || *glID != tc.userID) {
			t.Errorf("platform 2: gl_id = %v, want %d", glID, tc.userID)
		}
		if !tc.wantGlID && glID != nil {
			t.Errorf("platform %d: gl_id = %d, want NULL — a user id means nothing without its instance", tc.platform, *glID)
		}
	}
}
