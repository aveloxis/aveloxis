// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
)

// ensureTestPlatform registers a scratch platforms row (v0.30.0 GitLab
// instance ids) for an integration test and removes it at cleanup. Every
// platform_id column references aveloxis_data.platforms, so a test row on
// an instance id needs its registry row first. Callers delete their own
// referencing rows in a cleanup registered AFTER this call (cleanups run in
// reverse order, so this one runs last).
func ensureTestPlatform(ctx context.Context, t *testing.T, store *PostgresStore, id int, name string) {
	t.Helper()
	del := `DELETE FROM aveloxis_data.platforms WHERE platform_id = $1 AND platform_name = $2`
	cleanupExecRetry(ctx, store, del, id, name)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.platforms (platform_id, platform_name)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, name)
	t.Cleanup(func() { cleanupExecRetry(context.Background(), store, del, id, name) })
}
