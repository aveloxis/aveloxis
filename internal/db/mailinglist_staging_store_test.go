// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
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
