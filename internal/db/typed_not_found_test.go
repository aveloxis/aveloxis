// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"
)

// TestTypedNotFoundSentinels pins the two typed not-founds follow-up 12's
// web half introduced (SR-5): GetRepoByID answers an id nobody has with
// ErrRepoNotFound and PrioritizeRepo a repository with no queue row with
// ErrRepoNotInQueue, so the pages tell "no such thing" from a store failure
// without matching error text. A cancelled context is neither.
func TestTypedNotFoundSentinels(t *testing.T) {
	store, ctx := openSR5Store(t)
	if _, err := store.GetRepoByID(ctx, -1); !errors.Is(err, ErrRepoNotFound) {
		t.Errorf("GetRepoByID(-1) = %v; want ErrRepoNotFound", err)
	}
	if err := store.PrioritizeRepo(ctx, -1); !errors.Is(err, ErrRepoNotInQueue) {
		t.Errorf("PrioritizeRepo(-1) = %v; want ErrRepoNotInQueue", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.GetRepoByID(canceled, -1); err == nil || errors.Is(err, ErrRepoNotFound) {
		t.Errorf("GetRepoByID on a cancelled context = %v; want the store's failure, not \"not found\"", err)
	}
	if err := store.PrioritizeRepo(canceled, -1); err == nil || errors.Is(err, ErrRepoNotInQueue) {
		t.Errorf("PrioritizeRepo on a cancelled context = %v; want the store's failure, not \"not in queue\"", err)
	}
}
