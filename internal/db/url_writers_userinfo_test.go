// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 6: UpsertRepo refused a credentialed URL, but
// the rename writers (fed by a redirect target) and the repo-group writer
// (fed by `add-repo` on an organisation) did not. Every writer of a URL
// column refuses before touching the pool — a zero-value store proves the
// order.
func TestEveryURLWriterRefusesUserinfo(t *testing.T) {
	s := &PostgresStore{}
	ctx := context.Background()
	const bad = "https://user:s3cret@github.com/owner/name"
	for name, call := range map[string]func() error{
		"UpdateRepoURLs": func() error { return s.UpdateRepoURLs(ctx, 1, "https://github.com/old/name", bad) },
		"UpdateRepoURL":  func() error { return s.UpdateRepoURL(ctx, 1, bad) },
		"UpsertRepoGroup": func() error {
			_, err := s.UpsertRepoGroup(ctx, "owner", "github_org", "https://user:s3cret@github.com/owner")
			return err
		},
	} {
		if err := call(); !errors.Is(err, platform.ErrURLUserinfo) {
			t.Errorf("%s = %v, want platform.ErrURLUserinfo before any statement", name, err)
		}
	}
}
