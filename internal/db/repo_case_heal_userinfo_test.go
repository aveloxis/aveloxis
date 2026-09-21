// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestHealRepoCaseDriftRefusesUserinfo (AVELOXIS_TEST_DB) — v0.29.57
// fix-review round 5: the case heal rebuilt the URL from schemeAndHost,
// which kept a legacy row's userinfo, logged the stored URL verbatim and
// would have rewritten the row with the credential. Refused: an ERROR
// naming the row, the credential absent from the log, the row untouched.
func TestHealRepoCaseDriftRefusesUserinfo(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logs bytes.Buffer
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	const owner = "_avheal-userinfo-owner"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, owner)
	}
	clean()
	t.Cleanup(clean)
	id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: "https://github.com/" + owner + "/name", Owner: owner, Name: "name"})
	if err != nil {
		t.Fatal(err)
	}
	const secret = "s3cret-token"
	bad := "https://user:" + secret + "@github.com/" + owner + "/name"
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET repo_git = $2 WHERE repo_id = $1`, id, bad); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	healed, err := store.HealRepoCaseDrift(ctx, id, owner+"/Name")
	if healed || !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("HealRepoCaseDrift = (%v, %v), want (false, platform.ErrURLUserinfo)", healed, err)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stored); err != nil || stored != bad {
		t.Errorf("the refused row was changed: %q, %v", stored, err)
	}
	if bytes.Contains(logs.Bytes(), []byte(secret)) || !bytes.Contains(logs.Bytes(), []byte("level=ERROR")) {
		t.Errorf("the refusal must be an ERROR without the credential: %s", logs.String())
	}
	_ = io.Discard
}
