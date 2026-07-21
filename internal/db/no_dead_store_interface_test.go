// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.41 (summary/18 Phase 4): the producer-side db.Store interface
// is DELETED (remove-don't-deprecate). It had ~30 methods, a single
// implementation, ZERO non-test consumers, and was silently stale
// (missing hundreds of PostgresStore methods) — pure rot. The house
// pattern is CONSUMER-side role interfaces (breadthStore,
// sessionStore, digestMailer, ...): narrow, defined next to their
// consumer, mock-friendly. This tripwire keeps the dead pattern from
// returning.

package db

import (
	"os"
	"testing"
)

func TestNoProducerSideStoreInterface(t *testing.T) {
	if _, err := os.Stat("store.go"); !os.IsNotExist(err) {
		t.Error("internal/db/store.go must stay deleted — producer-side god-interfaces rot silently; define narrow role interfaces at the CONSUMER instead")
	}
}
