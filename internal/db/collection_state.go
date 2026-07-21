// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// CollectionState survived the v0.27.41 deletion of the dead
// producer-side Store interface (summary/18 Phase 4) — it is a real
// value type with real consumers; only the god-interface died.

package db

// CollectionState tracks per-phase status for a repo.
type CollectionState struct {
	RepoID                     int64
	CoreStatus                 string
	CoreTaskID                 string
	CoreDataLastCollected      *string // RFC3339 timestamp or nil
	CoreWeight                 *int64
	SecondaryStatus            string
	SecondaryTaskID            string
	SecondaryDataLastCollected *string
	SecondaryWeight            *int64
	FacadeStatus               string
	FacadeTaskID               string
	FacadeDataLastCollected    *string
	FacadeWeight               *int64
	EventLastCollected         *string
	IssuePRSum                 *int64
	CommitSum                  *int64
	MLStatus                   string
	MLTaskID                   string
	MLDataLastCollected        *string
	MLWeight                   *int64
}

// Convenience aliases used by the collector.
func (s *CollectionState) CoreLastCollected() *string     { return s.CoreDataLastCollected }
func (s *CollectionState) SetCoreLastCollected(v *string) { s.CoreDataLastCollected = v }
