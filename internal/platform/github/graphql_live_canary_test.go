// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.27.30 — the GraphQL schema canary (audit G5). Our GraphQL
// selections are hand-written strings and every mock response body is
// authored from our own structs — the "Label has no databaseId" gap
// was discovered LIVE in production because nothing validated our
// field assumptions against GitHub's actual schema. This weekly
// canary runs the REAL listing + PR-batch queries against a small
// stable public repo and asserts the parity-gap fields arrive
// non-empty: a removed/renamed field parses to null and silently
// blanks a column fleet-wide otherwise.

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestLiveGraphQLListingAndPRBatchParityFields(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the GraphQL canary")
	}
	logger := slog.Default()
	keys := platform.NewKeyPool([]string{tok}, logger)
	client := New("https://api.github.com", keys, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// spf13/pflag: small, stable, has issues AND merged PRs with
	// commits — enough surface to exercise every parity field.
	batch, err := client.ListIssuesAndPRs(ctx, "spf13", "pflag", time.Time{})
	if err != nil {
		t.Fatalf("live GraphQL listing failed: %v", err)
	}
	if len(batch.Issues) == 0 || len(batch.PullRequests) == 0 {
		t.Fatalf("listing returned %d issues / %d PRs — a schema drift that nulls a connection parses as empty",
			len(batch.Issues), len(batch.PullRequests))
	}

	// PR batch on the first few listed PRs: the v0.26.4 parity fields
	// must arrive non-empty where the data exists.
	nums := []int{}
	for _, pr := range batch.PullRequests {
		nums = append(nums, pr.Number)
		if len(nums) == 3 {
			break
		}
	}
	staged, err := client.FetchPRBatch(ctx, "spf13", "pflag", nums)
	if err != nil {
		t.Fatalf("live FetchPRBatch failed: %v", err)
	}
	if len(staged) == 0 {
		t.Fatal("PR batch empty for known-existing PRs")
	}
	var sawCommitNodeID, sawDiffURL bool
	for _, s := range staged {
		if s.PR.DiffURL != "" {
			sawDiffURL = true
		}
		for _, c := range s.Commits {
			if c.NodeID != "" {
				sawCommitNodeID = true
			}
		}
	}
	if !sawDiffURL {
		t.Error("no PR carried pr_diff_url — the v0.26.4 synthesis or its inputs drifted")
	}
	if !sawCommitNodeID {
		t.Error("no PR commit carried a node id — the GraphQL Commit.id selection drifted (the Label-databaseId class)")
	}
}
