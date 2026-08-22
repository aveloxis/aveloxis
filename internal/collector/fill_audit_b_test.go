// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — Workstream B processor wiring: the staged processor must
// resolve the fork-owner ref (pr_cntrb_id) and persist the meta-row PKs
// it already holds (meta_head_id/meta_base_id) instead of discarding them.
package collector

import (
	"os"
	"strings"
	"testing"
)

func TestProcessStagedPRResolvesForkOwners(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "env.RepoHead.ContribID = p.resolveUser(") {
		t.Error("processStagedPR must resolve env.RepoHead.OwnerRef into ContribID before UpsertPRRepo (the v0.26.5 resolveUser contract)")
	}
	if !strings.Contains(s, "env.RepoBase.ContribID = p.resolveUser(") {
		t.Error("processStagedPR must resolve env.RepoBase.OwnerRef into ContribID before UpsertPRRepo")
	}
}

func TestProcessStagedPRPersistsMetaLinks(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "SetPRMetaLinks(")
	if i < 0 {
		t.Fatal("processStagedPR must call SetPRMetaLinks — it already holds headMetaID/baseMetaID from UpsertPRMeta and discarded them (meta_head_id/meta_base_id 100% dark)")
	}
	// The call must come AFTER the meta upserts that produce the PKs.
	j := strings.LastIndex(s[:i], "UpsertPRMeta(")
	if j < 0 {
		t.Error("SetPRMetaLinks must be called after the UpsertPRMeta calls that produce the ids")
	}
}

// v0.27.107 (ultrareview round 2, bug_005): the v0.27.106 whitespace
// gate checked result.Errors, but insertCommitBatch NEVER returns
// non-nil for DB failures — the gate was decorative. It now reads the
// CommitWriteFailures counter, which the swallow sites actually bump.
func TestWhitespaceGateIsOperative(t *testing.T) {
	src, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "result.CommitWriteFailures == 0") {
		t.Error("the whitespace-phase gate must read FacadeResult.CommitWriteFailures (result.Errors alone never fires — insertCommitBatch swallows DB failures)")
	}
	if strings.Count(s, "CommitWriteFailures +") < 1 || !strings.Contains(s, "CommitWriteFailures++") {
		t.Error("the commit-row swallow sites (fallback per-row failure + ctx-cancel bail) must bump CommitWriteFailures")
	}
}
