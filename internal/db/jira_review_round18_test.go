// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRegisterJiraProjectRefusesSingleRegistrationURLChange (Copilot
// round 18 on PR #193): the one-instance guard excluded the SAME
// project_key from the probe, so re-registering the ONLY registration
// with a DIFFERENT base_url found no conflicting row and overwrote the
// URL — even though instance-blind identities/comment ids from the old
// server remain. A same-URL re-register stays idempotent.
func TestRegisterJiraProjectRefusesSingleRegistrationURLChange(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	key := fmt.Sprintf("_AVR18%d", 1)
	cleanup := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key LIKE '_AVR18%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	urlA := "https://jira-a.example.org"
	urlB := "https://jira-b.example.org"
	if err := store.RegisterJiraProject(ctx, key, urlA, nil); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// Same key, DIFFERENT url, and it's the ONLY registration → must
	// be refused (pre-fix it silently overwrote base_url).
	if err := store.RegisterJiraProject(ctx, key, urlB, nil); err == nil {
		t.Fatal("re-registering the sole project with a different base_url must be refused — instance-blind identities from the old server remain")
	}
	// The stored URL is untouched.
	var got string
	if err := store.pool.QueryRow(ctx,
		`SELECT base_url FROM aveloxis_ops.jira_project_serve WHERE project_key = $1`, key).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != urlA {
		t.Fatalf("base_url = %q, want the original %q untouched", got, urlA)
	}
	// Same key, SAME url → idempotent (no refusal).
	if err := store.RegisterJiraProject(ctx, key, urlA, nil); err != nil {
		t.Fatalf("same-URL re-register must stay idempotent: %v", err)
	}
}

// TestInstanceGuardProbeCoversSameKey pins the SQL shape: the probe
// must NOT exclude the incoming project_key (that exclusion was the
// bypass).
func TestInstanceGuardProbeCoversSameKey(t *testing.T) {
	src := srctest.Read(t, "internal/db/jira_store.go")
	i := strings.Index(src, "SELECT base_url FROM aveloxis_ops.jira_project_serve")
	if i < 0 {
		t.Fatal("instance-guard probe not found")
	}
	region := src[i : i+200]
	if strings.Contains(region, "project_key <> $2") {
		t.Error("the instance guard still excludes the same project_key — a sole-registration URL change bypasses it")
	}
}
