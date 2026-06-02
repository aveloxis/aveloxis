// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestApacheListBridgeStoreMethodsExist — source-contract for the Phase-0c
// store surface.
func TestApacheListBridgeStoreMethodsExist(t *testing.T) {
	data, err := os.ReadFile("email_message_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, sig := range []string{
		"func (s *PostgresStore) SetRepoGroup(",
		"func (s *PostgresStore) RegisterMailingList(",
		"func (s *PostgresStore) GetPrimaryRepoForGroup(",
	} {
		if !strings.Contains(src, sig) {
			t.Errorf("email_message_store.go must declare %s", sig)
		}
	}
}

// TestApacheListBridgeEndToEnd — the Phase-0c → worker link: after
// registering a list against a per-PMC group whose primary repo is set, the
// list is claimable AND GetPrimaryRepoForGroup resolves to that repo (so the
// worker can write bodies with a NOT-NULL repo_id).
func TestApacheListBridgeEndToEnd(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/apache/bridgetest",
		Owner: "apache", Name: "bridgetest",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	groupID, err := store.UpsertRepoGroup(ctx, "Apache PMC: bridgetest", "apache_pmc", "")
	if err != nil {
		t.Fatalf("repo_group: %v", err)
	}
	if err := store.SetRepoGroup(ctx, repoID, groupID); err != nil {
		t.Fatalf("SetRepoGroup: %v", err)
	}
	if err := store.RegisterMailingList(ctx, groupID, "dev@bridgetest.apache.org", "apache_ponymail"); err != nil {
		t.Fatalf("RegisterMailingList: %v", err)
	}

	// GetPrimaryRepoForGroup resolves to the linked repo.
	gotRepo, ok, err := store.GetPrimaryRepoForGroup(ctx, groupID)
	if err != nil || !ok || gotRepo != repoID {
		t.Fatalf("GetPrimaryRepoForGroup = (%d,%v,%v), want (%d,true,nil)", gotRepo, ok, err, repoID)
	}

	// The registered list is claimable by the apache_ponymail worker.
	job, err := store.ClaimNextList(ctx, "apache_ponymail", time.Hour, 1, "boot")
	if err != nil {
		t.Fatalf("ClaimNextList: %v", err)
	}
	if job == nil || job.ListAddress != "dev@bridgetest.apache.org" {
		// Other apache_ponymail lists may exist in the scratch DB; only fail
		// if OUR list never becomes claimable. Loop a few claims to find it.
		found := job != nil && job.ListAddress == "dev@bridgetest.apache.org"
		for i := 0; i < 50 && !found; i++ {
			j, _ := store.ClaimNextList(ctx, "apache_ponymail", time.Hour, 1, "boot")
			if j == nil {
				break
			}
			if j.ListAddress == "dev@bridgetest.apache.org" {
				found = true
			}
		}
		if !found {
			t.Fatalf("registered list was not claimable")
		}
	}

	// Cleanup.
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_email = $1`, "dev@bridgetest.apache.org")
}
