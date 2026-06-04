// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestSchemaDeclaresMailingListStaging pins the staging table + its claim index
// + the per-list idempotency UNIQUE (summary/12 §11).
func TestSchemaDeclaresMailingListStaging(t *testing.T) {
	src := readSchema(t)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.mailing_list_staging",
		"UNIQUE (rgls_id, message_id_header)",
		"idx_mls_unprocessed",
		"WHERE NOT processed",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must contain %q for the mailing-list staging layer", needle)
		}
	}
}

func mlsPtr(v int64) *int64 { return &v }

// TestMailingListStagingRoundTrip exercises stage → list → batch → mark
// processed, plus idempotency of re-staging the same message.
func TestMailingListStagingRoundTrip(t *testing.T) {
	store, ctx := emConnect(t) // runs Migrate → creates mailing_list_staging
	defer store.Close()

	const rgls = int64(999000111)
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE rgls_id=$1`, rgls)
	}
	clean()
	t.Cleanup(clean)

	msg := model.MailingListStagedMessage{
		MessageID:   "<m1@example>",
		ListAddress: "dev@x.apache.org",
		Subject:     "[KAFKA-1] something",
		SenderEmail: "a@b.com",
		SentAt:      time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC),
		MsgClass:    "discuss",
		ExternalKey: "KAFKA-1",
		Body:        "hello",
	}
	if err := store.StageMailingListMessage(ctx, rgls, mlsPtr(5), mlsPtr(7), msg); err != nil {
		t.Fatalf("stage: %v", err)
	}
	// Idempotent re-stage (same rgls_id + message_id) — must be a no-op.
	if err := store.StageMailingListMessage(ctx, rgls, mlsPtr(5), mlsPtr(7), msg); err != nil {
		t.Fatalf("re-stage: %v", err)
	}

	lists, err := store.ListsWithStaging(ctx, 100)
	if err != nil {
		t.Fatalf("ListsWithStaging: %v", err)
	}
	if !containsInt64(lists, rgls) {
		t.Fatalf("ListsWithStaging should include %d", rgls)
	}

	batch, err := store.GetMailingListStagingBatch(ctx, rgls, 100)
	if err != nil {
		t.Fatalf("GetMailingListStagingBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("re-stage must dedup; got %d rows, want 1", len(batch))
	}
	got := batch[0]
	if got.Message.MsgClass != "discuss" || got.Message.MessageID != "<m1@example>" || got.Message.ExternalKey != "KAFKA-1" {
		t.Errorf("envelope round-trip wrong: %+v", got.Message)
	}
	if got.RepoID == nil || *got.RepoID != 7 || got.RepoGroupID == nil || *got.RepoGroupID != 5 {
		t.Errorf("repo_id/repo_group_id round-trip wrong: %+v / %+v", got.RepoID, got.RepoGroupID)
	}

	if err := store.MarkMailingListStagingProcessed(ctx, []int64{got.MlsID}); err != nil {
		t.Fatalf("mark processed: %v", err)
	}
	lists2, _ := store.ListsWithStaging(ctx, 100)
	if containsInt64(lists2, rgls) {
		t.Error("processed list must not appear in ListsWithStaging")
	}
	batch2, _ := store.GetMailingListStagingBatch(ctx, rgls, 100)
	if len(batch2) != 0 {
		t.Errorf("processed rows must not be returned; got %d", len(batch2))
	}
}

func containsInt64(xs []int64, v int64) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// TestStuckMailingLists pins the observability query: a list with
// staged-but-undrained rows whose repo_group has NO repo is reported; a list
// whose group HAS a repo is not (it can drain). summary/12 §11.
func TestStuckMailingLists(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	const (
		stuckList  = "dev@_av_stuck.apache.org"
		readyList  = "dev@_av_ready.apache.org"
		stuckGroup = "_av_stuck_grp"
		readyGroup = "_av_ready_grp"
		readyRepo  = "https://github.com/_av_ready/repo"
	)
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE rgls_id IN
			(SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email IN ($1,$2))`, stuckList, readyList)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email IN ($1,$2)`, stuckList, readyList)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, readyRepo)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name IN ($1,$2)`, stuckGroup, readyGroup)
	}
	clean()
	t.Cleanup(clean)

	seedGroup := func(name string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx,
			`INSERT INTO aveloxis_data.repo_groups (rg_name) VALUES ($1) RETURNING repo_group_id`, name).Scan(&id); err != nil {
			t.Fatalf("seed group %s: %v", name, err)
		}
		return id
	}
	stuckGID := seedGroup(stuckGroup)
	readyGID := seedGroup(readyGroup)
	// readyGroup gets a repo; stuckGroup does not.
	if _, err := store.pool.Exec(ctx,
		`INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name, repo_group_id)
		 VALUES (1, $1, '_av_ready', 'repo', $2)`, readyRepo, readyGID); err != nil {
		t.Fatalf("seed ready repo: %v", err)
	}
	if err := store.RegisterMailingList(ctx, stuckGID, stuckList, "apache_ponymail"); err != nil {
		t.Fatalf("register stuck list: %v", err)
	}
	if err := store.RegisterMailingList(ctx, readyGID, readyList, "apache_ponymail"); err != nil {
		t.Fatalf("register ready list: %v", err)
	}
	rgls := func(email string) int64 {
		var id int64
		store.pool.QueryRow(ctx, `SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email=$1`, email).Scan(&id)
		return id
	}
	stuckRgls, readyRgls := rgls(stuckList), rgls(readyList)

	// Stage messages for both lists.
	for i, lst := range []struct {
		rgls int64
		gid  int64
		addr string
	}{{stuckRgls, stuckGID, stuckList}, {readyRgls, readyGID, readyList}} {
		gid := lst.gid
		msg := model.MailingListStagedMessage{
			MessageID: fmt.Sprintf("<stuck-%d@x>", i), ListAddress: lst.addr,
			MsgClass: "discuss", Body: "x",
		}
		if err := store.StageMailingListMessage(ctx, lst.rgls, &gid, nil, msg); err != nil {
			t.Fatalf("stage for %s: %v", lst.addr, err)
		}
	}

	stuck, err := store.StuckMailingLists(ctx)
	if err != nil {
		t.Fatalf("StuckMailingLists: %v", err)
	}
	var sawStuck, sawReady bool
	for _, m := range stuck {
		if m.RglsID == stuckRgls {
			sawStuck = true
			if m.ListAddress != stuckList || m.RepoGroupID != stuckGID || m.StagedRows != 1 {
				t.Errorf("stuck row wrong: %+v", m)
			}
		}
		if m.RglsID == readyRgls {
			sawReady = true
		}
	}
	if !sawStuck {
		t.Error("the no-repo list must be reported as stuck")
	}
	if sawReady {
		t.Error("a list whose group has a repo must NOT be reported as stuck")
	}
}
