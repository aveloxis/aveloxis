// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 (multi-instance GitLab): the same numeric user id and the same
// note id on two GitLab instances are different people and different
// comments. Because each instance has its own platform_id, the existing keys
// — contributor_identities (platform_id, platform_user_id), the PlatformUUID
// byte, messages (platform_msg_id, platform_id, msg_kind) — keep them apart
// without any schema change.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestInstanceScopedIdentitiesAndMessages(t *testing.T) {
	ctx, store, ids := classifyFixture(t)
	other := ids["https://c.invalid"]
	const userID, noteID = int64(987650005), int64(987650077)
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE platform_msg_id = $1`, noteID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributor_identities WHERE platform_user_id = $1`, userID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avscope_%'`)
	})

	resolver := NewContributorResolver(store)
	onMain, err := resolver.Resolve(ctx, int16(model.PlatformGitLab), userID, "_avscope_main", "", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	onOther, err := resolver.Resolve(ctx, int16(other), userID, "_avscope_other", "", "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if onMain == onOther {
		t.Fatalf("user %d on platform 2 and on instance %d resolved to one contributor %s", userID, other, onMain)
	}
	if onMain != PlatformUUID(int(model.PlatformGitLab), userID).String() || onOther != PlatformUUID(int(other), userID).String() {
		t.Errorf("cntrb_ids = %s / %s, want PlatformUUID(2, %d) / PlatformUUID(%d, %d)", onMain, onOther, userID, other, userID)
	}
	var identities int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributor_identities WHERE platform_user_id = $1`, userID).Scan(&identities); err != nil || identities != 2 {
		t.Errorf("identity rows for user %d = %d (%v), want one per instance", userID, identities, err)
	}

	repoMain, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitLab, GitURL: "https://gitlab.main.invalid/" + classifySlug + "/m", Owner: classifySlug, Name: "m"})
	if err != nil {
		t.Fatal(err)
	}
	repoOther, err := store.UpsertRepo(ctx, &model.Repo{Platform: other, GitURL: "https://c.invalid/" + classifySlug + "/o", Owner: classifySlug, Name: "o"})
	if err != nil {
		t.Fatal(err)
	}
	m1, err := store.UpsertMessage(ctx, &model.Message{RepoID: repoMain, PlatformMsgID: noteID, PlatformID: model.PlatformGitLab, Text: "main note"})
	if err != nil {
		t.Fatal(err)
	}
	m2, err := store.UpsertMessage(ctx, &model.Message{RepoID: repoOther, PlatformMsgID: noteID, PlatformID: other, Text: "other note"})
	if err != nil {
		t.Fatal(err)
	}
	if m1 == m2 {
		t.Fatalf("note %d on platform 2 and on instance %d are one messages row (%d)", noteID, other, m1)
	}
	var text string
	if err := store.pool.QueryRow(ctx, `SELECT msg_text FROM aveloxis_data.messages WHERE msg_id = $1`, m1).Scan(&text); err != nil || text != "main note" {
		t.Errorf("platform 2's note text = %q (%v) — overwritten by the other instance's note", text, err)
	}
}
