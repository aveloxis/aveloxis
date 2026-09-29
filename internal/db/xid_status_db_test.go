// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestXIDStatus (AVELOXIS_TEST_DB) — worklist item 78 (Stage 1, observation
// only): anti-wraparound vacuums were suspected in the kate stalls with no
// record of how fast transaction IDs are used. XIDStatus reads this
// database's frozen-XID age, the freeze threshold that triggers those
// vacuums, and the current transaction id (successive readings give the
// burn rate).
func TestXIDStatus(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, err := NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	st, err := store.XIDStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Age <= 0 || st.FreezeMaxAge <= 0 || st.CurrentXID <= 0 {
		t.Errorf("implausible reading: %+v", st)
	}
}

// TestXIDStatusAssignsNoTransactionID — review round 1 (minor): the hourly
// line is observation only, and pg_current_xact_id() ASSIGNS a
// transaction ID (one per hour, advancing the very age it reports). The
// read uses the snapshot's xmax, which assigns none.
func TestXIDStatusAssignsNoTransactionID(t *testing.T) {
	body := srctest.FuncBody(t, srctest.StripGoComments(srctest.Read(t, "internal/db/xid_status.go")), "func (s *PostgresStore) XIDStatus(")
	if strings.Contains(body, "pg_current_xact_id()") {
		t.Error("XIDStatus must not call pg_current_xact_id() — it assigns a transaction ID; read pg_snapshot_xmax(pg_current_snapshot())")
	}
	if !strings.Contains(body, "pg_snapshot_xmax(pg_current_snapshot())") {
		t.Error("XIDStatus must read the position from the snapshot's xmax")
	}
}
