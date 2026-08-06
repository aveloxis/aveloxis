// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: repos.forked_from finally gets a writer. Phase 0 stamps
// it via UpdateRepoMetadata on every cycle, and UpsertRepo must STOP
// clobbering it back to '' on conflict (org scans re-upsert existing
// repos with a zero-valued model.Repo — the pre-v0.27.78
// `forked_from = EXCLUDED.forked_from` wiped any populated value on
// every scan tick).

package db

import (
	"os"
	"strings"
	"testing"
)

func forkCaptureSrc(t *testing.T, file string) string {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestUpdateRepoMetadataWritesForkedFrom(t *testing.T) {
	src := forkCaptureSrc(t, "repo_metadata.go")
	if !strings.Contains(src, "forkedFrom string") {
		t.Error("UpdateRepoMetadata must take the forkedFrom value (Phase 0 is the forked_from writer)")
	}
	if !strings.Contains(src, "forked_from") {
		t.Error("UpdateRepoMetadata's UPDATE must SET forked_from")
	}
}

func TestUpsertRepoPreservesForkedFromOnConflict(t *testing.T) {
	src := forkCaptureSrc(t, "postgres.go")
	// The conflict-update must be prefer-nonempty: a bare
	// `forked_from = EXCLUDED.forked_from` lets every org-scan
	// re-upsert (zero-valued model.Repo) wipe the Phase 0 value.
	if strings.Contains(src, "forked_from = EXCLUDED.forked_from") {
		t.Error("UpsertRepo must NOT blindly overwrite forked_from on conflict — " +
			"use COALESCE(NULLIF(EXCLUDED.forked_from, ''), repos.forked_from) so " +
			"org-scan re-upserts can't clobber the Phase 0 capture")
	}
	if !strings.Contains(src, "COALESCE(NULLIF(EXCLUDED.forked_from, ''), aveloxis_data.repos.forked_from)") &&
		!strings.Contains(src, "COALESCE(NULLIF(EXCLUDED.forked_from, ''), repos.forked_from)") {
		t.Error("UpsertRepo's conflict-update must preserve a populated forked_from " +
			"(prefer-nonempty COALESCE form)")
	}
}

func TestGetForkStatusBatchExists(t *testing.T) {
	src := forkCaptureSrc(t, "showcase_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) GetForkStatusBatch(") {
		t.Fatal("GetForkStatusBatch must exist — the showcase generator's fork filter reads it")
	}
	for _, needle := range []string{"ANY($1", "forked_from"} {
		if !strings.Contains(src, needle) {
			t.Errorf("GetForkStatusBatch must batch-read fork status: missing %q", needle)
		}
	}
}
