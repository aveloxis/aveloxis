// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57 fix-review round 3: a pending add-request item stored before the
// store refused credentialed URLs fails UpsertRepo with ErrURLUserinfo on
// every approval pass; classified as transient it blocked its request's
// approval forever (and the wrapped error carried the URL to the admin's
// page). Permanent: the item is stamped processed-with-error and the pass
// goes on.
func TestAddItemFailurePermanentForRefusedURLs(t *testing.T) {
	for _, err := range []error{platform.ErrURLUserinfo, ErrURLTooLong} {
		if !addItemFailurePermanent(err) {
			t.Errorf("addItemFailurePermanent(%v) = false; the item can never succeed on retry", err)
		}
		wrapped := errors.Join(errors.New("repo o/n"), err)
		if !addItemFailurePermanent(wrapped) {
			t.Errorf("addItemFailurePermanent(wrapped %v) = false", err)
		}
	}
	if addItemFailurePermanent(errors.New("connection reset")) {
		t.Error("a transient error classified permanent")
	}
}
