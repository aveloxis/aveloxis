// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// XIDStatus is this database's transaction-ID position (worklist item 78).
type XIDStatus struct {
	// Age is age(datfrozenxid): transactions since the database's oldest
	// unfrozen XID. Anti-wraparound autovacuums start at FreezeMaxAge.
	Age          int64
	FreezeMaxAge int64
	// CurrentXID is the next transaction ID to be assigned (the snapshot's
	// xmax — reading it assigns none, unlike pg_current_xact_id()); two
	// readings an hour apart give the burn rate.
	CurrentXID int64
}

// XIDStatus reads the transaction-ID position of the connected database.
func (s *PostgresStore) XIDStatus(ctx context.Context) (XIDStatus, error) {
	var st XIDStatus
	err := s.pool.QueryRow(ctx, `
		SELECT age(datfrozenxid)::bigint,
		       current_setting('autovacuum_freeze_max_age')::bigint,
		       (pg_snapshot_xmax(pg_current_snapshot())::text)::bigint
		FROM pg_database WHERE datname = current_database()`).Scan(&st.Age, &st.FreezeMaxAge, &st.CurrentXID)
	if err != nil {
		return st, fmt.Errorf("read transaction-ID status: %w", err)
	}
	return st, nil
}
