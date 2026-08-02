// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "testing"

// TestPingHealthyDB confirms Ping succeeds against a reachable database.
func TestPingHealthyDB(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping on a healthy DB must succeed; got %v", err)
	}
}
