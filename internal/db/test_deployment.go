// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
)

// PrepareTestDeployment makes a fresh test database look like an established
// deployment (v0.29.57, internal/testdb): this binary's schema; the 'Default'
// repo group, as id 1, which fixtures reference; and a bootstrap admin who
// signed up first, so every fixture user is a regular user unless the fixture
// promotes it. Test support only: testdb/prepare and internal/db's TestMain
// both call it, so the preparation exists once. It skips the materialized
// views (Copilot on PR #210): no DB-tier test reads one, and
// TestRunMigrationsOnFreshDB checks that each builds on an empty database.
func (s *PostgresStore) PrepareTestDeployment(ctx context.Context, bootstrapAdminLogin string) error {
	s.SetMatviewSkip(true)
	if err := s.Migrate(ctx); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}
	gid, err := s.EnsureDefaultRepoGroup(ctx)
	if err != nil {
		return err
	}
	if gid != 1 {
		return fmt.Errorf("the Default repo group on a fresh database is id %d, not 1 — fixtures that insert repos with repo_group_id 1 would point at another group", gid)
	}
	uid, err := s.UpsertOAuthUser(ctx, OAuthUserInfo{Login: bootstrapAdminLogin, Provider: "github"})
	if err != nil {
		return fmt.Errorf("signing up the bootstrap admin: %w", err)
	}
	admin, err := s.IsUserAdmin(ctx, uid)
	if err != nil {
		return fmt.Errorf("checking the bootstrap admin: %w", err)
	}
	if !admin {
		return fmt.Errorf("the first signup on an empty database did not become admin (v0.19.0's bootstrap rule is broken)")
	}
	return nil
}

// VerifyFailures turns a data-verify battery's FAIL findings into one error,
// nil when there are none. WARN and OK findings pass.
func VerifyFailures(results []VerifyResult) error {
	var fails []string
	for _, r := range results {
		if r.Severity == "FAIL" {
			fails = append(fails, r.Check+": "+r.Detail)
		}
	}
	if len(fails) == 0 {
		return nil
	}
	return fmt.Errorf("%d FAIL finding(s): %s", len(fails), strings.Join(fails, "; "))
}
