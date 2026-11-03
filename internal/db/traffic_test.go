// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestUpsertRepoTrafficRefusesAReadWithoutSnapshotDate: the snapshot
// date keys which path and referrer rows a read replaces. A read
// without one (a zero time) is a caller defect; writing it would file
// the snapshot under 0001-01-01 and replace nothing, so the writer
// refuses it before touching the database (the store here has no
// pool: reaching the database would panic).
func TestUpsertRepoTrafficRefusesAReadWithoutSnapshotDate(t *testing.T) {
	s := &PostgresStore{}
	err := s.UpsertRepoTraffic(context.Background(), 1, &model.RepoTraffic{
		Views: []model.RepoView{{TotalViews: 1}},
	})
	if err == nil {
		t.Fatal("UpsertRepoTraffic accepted a traffic read without a snapshot date")
	}
}
