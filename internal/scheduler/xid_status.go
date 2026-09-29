// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
)

// logXIDStatus writes the hourly transaction-ID line (worklist item 78,
// observation only): how far this database is from the anti-wraparound
// vacuum threshold, and the current XID (two lines an hour apart give the
// burn rate the savepoint question in item 78 needs).
func (s *Scheduler) logXIDStatus(ctx context.Context) {
	st, err := s.store.XIDStatus(ctx)
	if errors.Is(err, context.Canceled) {
		return // a stop, not a failure
	}
	if err != nil {
		s.logger.Warn("transaction-ID status could not be read", "error", err)
		return
	}
	s.logger.Info("transaction-ID status",
		"frozen_xid_age", st.Age, "autovacuum_freeze_max_age", st.FreezeMaxAge,
		"pct_of_freeze_max_age", st.Age*100/max(st.FreezeMaxAge, 1),
		"current_xid", st.CurrentXID)
}
